"""
Main bot loop -- orchestrates all components.

Flow:
  1. Resolve current 15-minute round
  2. Subscribe to price feed for UP/DOWN tokens
  3. Run strategy engine on each price tick
  4. Execute trade signals via executor
  5. When round ends, start next round
  6. Repeat for 24 hours
"""

import time
import signal
import logging
import sys
from datetime import datetime

from bot.config import BotConfig
from bot.market_resolver import MarketResolver
from bot.price_feed import PriceFeed
from bot.strategy import StrategyEngine, CycleState
from bot.executor import OrderExecutor

logger = logging.getLogger(__name__)


class TradingBot:
    """
    Main trading bot for Polymarket 15-min BTC markets.
    """
    
    def __init__(self, config: BotConfig):
        self.config = config
        self.resolver = MarketResolver(
            gamma_host=config.api.gamma_host,
            coin=config.market_coin,
        )
        self.price_feed = PriceFeed(
            clob_host=config.api.clob_host,
            poll_interval=1.0,
        )
        self.strategy = StrategyEngine(
            strategy_cfg=config.strategy,
            risk_cfg=config.risk,
        )
        self.executor = OrderExecutor(
            api_config=config.api,
            dry_run=config.dry_run,
        )
        
        self._running = False
        self._start_time = 0.0
        self._rounds_processed = 0
        self._signals_generated = 0
    
    def start(self, duration_hours: float = 24.0):
        """
        Start the trading bot.
        
        Args:
            duration_hours: how long to run (default 24 hours)
        """
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        
        # Validate config
        issues = self.config.validate()
        if issues:
            for issue in issues:
                logger.error(f"Config issue: {issue}")
            if not self.config.dry_run:
                logger.error("Cannot start in live mode with config issues")
                return
        
        # Initialize executor
        if not self.executor.initialize():
            logger.error("Failed to initialize executor")
            return
        
        self._running = True
        self._start_time = time.time()
        end_time = self._start_time + (duration_hours * 3600)
        
        mode = "DRY-RUN" if self.config.dry_run else "LIVE"
        self._print_banner(mode, duration_hours)
        
        try:
            while self._running and time.time() < end_time:
                self._run_round()
                self._rounds_processed += 1
                
                # Print status between rounds
                self._print_status()
                
                # Wait for next round
                wait = self.resolver.seconds_until_next_round()
                if wait > 2:
                    logger.info(f"Waiting {wait:.0f}s for next round...")
                    # Sleep in small increments to allow shutdown
                    sleep_until = time.time() + wait - 1
                    while self._running and time.time() < sleep_until:
                        time.sleep(1)
        
        except Exception as e:
            logger.error(f"Bot error: {e}", exc_info=True)
        
        finally:
            self.price_feed.stop()
            self._print_final_report()
    
    def _run_round(self):
        """Execute strategy for one 15-minute round."""
        # Resolve current round
        slug = self.resolver.get_current_round_slug()
        round_info = self.resolver.get_current_round()
        
        if not round_info:
            logger.warning(f"Could not resolve round {slug}, using slug-only mode")
            # In dry-run, we can still operate with simulated data
            if not self.config.dry_run:
                logger.error("Cannot trade without market info in live mode")
                return
        
        # Initialize strategy for this round
        round_start = time.time() - self.resolver.seconds_into_round()
        self.strategy.start_new_round(slug, round_start)
        
        # Check risk limits
        should_skip, reason = self.strategy.should_skip_round()
        if should_skip:
            logger.info(f"Skipping round {slug}: {reason}")
            return
        
        logger.info(
            f"--- Round {slug} ---  "
            f"{self.resolver.seconds_into_round():.0f}s elapsed, "
            f"Balance=${self.strategy.balance:.2f}"
        )
        
        # Setup price feed for this round's tokens
        if round_info:
            self.price_feed.set_tokens(
                round_info.up_token_id,
                round_info.down_token_id,
            )
        
        # Monitor this round
        window_end_time = round_start + (self.config.strategy.window_min * 60)
        round_end_time = round_start + 900  # 15 minutes
        
        # Only need to actively monitor during strategy window + hedge window
        active_end = round_end_time  # monitor full round for hedge opportunities
        
        while self._running and time.time() < min(active_end, round_end_time):
            # Get current prices
            if round_info:
                tick = self.price_feed.fetch_prices_rest()
            else:
                # Dry-run without real market: generate a dummy tick
                tick = self._generate_dummy_tick()
            
            if tick:
                # Process through strategy engine
                signal = self.strategy.process_tick(tick)
                
                if signal:
                    self._execute_signal(signal, round_info)
            
            # Check if strategy is done with this round
            cycle = self.strategy.current_cycle
            if cycle and cycle.state in (CycleState.HEDGED, CycleState.SKIPPED, CycleState.ROUND_OVER):
                break
            
            # Poll interval
            time.sleep(1.0)
    
    def _execute_signal(self, signal: dict, round_info):
        """Execute a trade signal from the strategy engine."""
        action = signal["action"]
        side = signal["side"]
        price = signal["price"]
        shares = signal["shares"]
        
        self._signals_generated += 1
        
        # Determine token ID
        if round_info:
            token_id = round_info.up_token_id if side == "UP" else round_info.down_token_id
        else:
            token_id = f"dummy_{side.lower()}"
        
        # Execute order
        result = self.executor.buy(token_id, side, price, shares)
        
        if result.success:
            leg = "leg1" if action == "buy_leg1" else "leg2"
            self.strategy.record_fill(
                leg=leg,
                actual_price=result.filled_price,
                actual_shares=result.filled_shares,
                fees=result.fees,
            )
        else:
            logger.error(f"Order failed: {result.error}")
            # If Leg 1 failed, reset cycle
            if action == "buy_leg1" and self.strategy.current_cycle:
                self.strategy.current_cycle.state = CycleState.SKIPPED
    
    def _generate_dummy_tick(self):
        """Generate a dummy tick for dry-run without market connection."""
        from bot.price_feed import PriceTick
        import random
        
        # Simple random walk centered around 0.50
        base = 0.50 + random.gauss(0, 0.05)
        base = max(0.10, min(0.90, base))
        
        return PriceTick(
            timestamp=time.time(),
            up_best_ask=base + 0.01,
            up_best_bid=base - 0.01,
            down_best_ask=(1.0 - base) + 0.01,
            down_best_bid=(1.0 - base) - 0.01,
            source="dummy",
        )
    
    def _print_banner(self, mode: str, duration: float):
        """Print startup banner."""
        print(f"\n{'='*60}")
        print(f"  POLYMARKET BTC 15-MIN TRADING BOT")
        print(f"  Mode: {mode}")
        print(f"  Duration: {duration:.1f} hours")
        print(f"{'='*60}")
        print(f"  Strategy: Two-Leg Volatility Capture")
        print(f"  Params:")
        print(f"    windowMin  = {self.config.strategy.window_min} min")
        print(f"    movePct    = {self.config.strategy.move_pct:.0%}")
        print(f"    sumTarget  = {self.config.strategy.sum_target}")
        print(f"    shares     = {self.config.strategy.shares}")
        print(f"  Risk:")
        print(f"    Balance    = ${self.config.risk.initial_balance:.2f}")
        print(f"    DailyLimit = ${self.config.risk.daily_loss_limit:.2f}")
        print(f"    Floor      = ${self.config.risk.capital_floor:.2f}")
        print(f"{'='*60}\n")
    
    def _print_status(self):
        """Print current status."""
        status = self.strategy.get_status()
        elapsed = time.time() - self._start_time
        hours = elapsed / 3600
        
        print(
            f"  [{hours:.1f}h] Balance=${status['balance']:.2f} | "
            f"Daily=${status['daily_pnl']:+.2f} | "
            f"Total=${status['total_pnl']:+.2f} | "
            f"Trades={status['total_trades']} | "
            f"Rounds={self._rounds_processed} | "
            f"{status['cycle']}"
        )
    
    def _print_final_report(self):
        """Print final trading report."""
        status = self.strategy.get_status()
        elapsed = (time.time() - self._start_time) / 3600
        
        print(f"\n{'='*60}")
        print(f"  FINAL REPORT")
        print(f"{'='*60}")
        print(f"  Runtime          : {elapsed:.1f} hours")
        print(f"  Rounds Processed : {self._rounds_processed}")
        print(f"  Signals Generated: {self._signals_generated}")
        print(f"  Total Trades     : {status['total_trades']}")
        print(f"  Initial Balance  : ${self.config.risk.initial_balance:.2f}")
        print(f"  Final Balance    : ${status['balance']:.2f}")
        print(f"  Total P&L        : ${status['total_pnl']:+.2f}")
        print(f"  ROI              : {status['total_pnl']/self.config.risk.initial_balance*100:+.2f}%")
        print(f"{'='*60}\n")
    
    def _handle_shutdown(self, signum, frame):
        """Handle graceful shutdown."""
        logger.info("Shutdown signal received, stopping bot...")
        self._running = False
