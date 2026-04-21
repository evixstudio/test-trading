"""
Two-Leg Volatility Capture strategy engine.

Core decision logic for the trading bot:
  1. Monitor price feed for violent dumps in the first windowMin minutes
  2. Execute Leg 1: buy the dumped side at discount
  3. Monitor for hedge opportunity where Leg1Price + OppositeAsk <= sumTarget
  4. Execute Leg 2: buy the opposite side to lock in profit
"""

import time
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from bot.config import StrategyConfig, RiskConfig
from bot.price_feed import PriceTick

logger = logging.getLogger(__name__)


class CycleState(Enum):
    """State of the current trading cycle."""
    WATCHING = "watching"           # monitoring for dump
    LEG1_TRIGGERED = "leg1_triggered"  # Leg 1 filled, waiting for hedge
    HEDGED = "hedged"              # both legs filled
    ROUND_OVER = "round_over"      # round ended
    SKIPPED = "skipped"            # no opportunity


@dataclass
class TradingCycle:
    """A single round's trading cycle."""
    round_slug: str
    state: CycleState = CycleState.WATCHING
    
    # Leg 1
    leg1_side: str = ""          # "UP" or "DOWN"
    leg1_price: float = 0.0
    leg1_shares: float = 0.0
    leg1_time: float = 0.0       # seconds into round when triggered
    
    # Leg 2 (hedge)
    leg2_side: str = ""
    leg2_price: float = 0.0
    leg2_shares: float = 0.0
    leg2_time: float = 0.0
    
    # Result
    total_cost: float = 0.0
    payout: float = 0.0
    pnl: float = 0.0
    fees: float = 0.0


class StrategyEngine:
    """
    Two-Leg Volatility Capture strategy.
    
    Processes price ticks and emits trade signals.
    """
    
    def __init__(self, strategy_cfg: StrategyConfig, risk_cfg: RiskConfig):
        self.cfg = strategy_cfg
        self.risk = risk_cfg
        
        self._current_cycle: Optional[TradingCycle] = None
        self._round_start_time: float = 0.0
        self._price_history: list[PriceTick] = []
        
        # Risk state
        self._balance: float = risk_cfg.initial_balance
        self._daily_pnl: float = 0.0
        self._consecutive_losses: int = 0
        self._cooldown_until: float = 0.0
        self._day_start: float = time.time()
        self._total_trades: int = 0
        self._total_pnl: float = 0.0
    
    @property
    def balance(self) -> float:
        return self._balance
    
    @property
    def current_cycle(self) -> Optional[TradingCycle]:
        return self._current_cycle
    
    def start_new_round(self, slug: str, round_start_time: float):
        """Initialize for a new 15-minute round."""
        # Check if previous cycle had an open Leg 1 (unhedged)
        if self._current_cycle and self._current_cycle.state == CycleState.LEG1_TRIGGERED:
            logger.warning(f"Abandoning unhedged Leg 1 from {self._current_cycle.round_slug}")
            # Count as a loss (conservative)
            self._record_loss(self._current_cycle.leg1_price * self._current_cycle.leg1_shares)
        
        self._current_cycle = TradingCycle(round_slug=slug)
        self._round_start_time = round_start_time
        self._price_history.clear()
        
        # Daily reset check (every 24 hours)
        if time.time() - self._day_start > 86400:
            self._daily_pnl = 0.0
            self._consecutive_losses = 0
            self._day_start = time.time()
            logger.info("Daily counters reset")
    
    def should_skip_round(self) -> tuple[bool, str]:
        """Check if we should skip this round based on risk rules."""
        # Capital floor
        if self._balance < self.risk.capital_floor:
            return True, f"Capital floor breached (${self._balance:.2f} < ${self.risk.capital_floor:.2f})"
        
        # Daily loss limit
        if self._daily_pnl <= -self.risk.daily_loss_limit:
            return True, f"Daily loss limit hit (${self._daily_pnl:+.2f})"
        
        # Cooldown after consecutive losses
        if time.time() < self._cooldown_until:
            remaining = self._cooldown_until - time.time()
            return True, f"Cooldown active ({remaining:.0f}s remaining)"
        
        # Check minimum balance for a trade
        min_required = self.cfg.shares * 1.0 * 2  # worst case: 2 legs at $1
        if self._balance < min_required:
            return True, f"Insufficient balance for trade (${self._balance:.2f} < ${min_required:.2f})"
        
        return False, ""
    
    def process_tick(self, tick: PriceTick) -> Optional[dict]:
        """
        Process a new price tick and return trade signal if any.
        
        Returns:
            None: no action
            dict with:
                "action": "buy_leg1" or "buy_leg2"
                "side": "UP" or "DOWN"
                "price": target price
                "shares": number of shares
        """
        if not self._current_cycle:
            return None
        
        self._price_history.append(tick)
        cycle = self._current_cycle
        
        # Calculate seconds into round
        secs_into_round = tick.timestamp - self._round_start_time
        
        if cycle.state == CycleState.WATCHING:
            return self._check_for_dump(tick, secs_into_round)
        
        elif cycle.state == CycleState.LEG1_TRIGGERED:
            return self._check_for_hedge(tick, secs_into_round)
        
        return None
    
    def _check_for_dump(self, tick: PriceTick, secs_into_round: float) -> Optional[dict]:
        """Check if either side has dumped enough to trigger Leg 1."""
        # Only watch during the configured window
        window_seconds = self.cfg.window_min * 60
        if secs_into_round > window_seconds:
            self._current_cycle.state = CycleState.SKIPPED
            return None
        
        # Need at least lookback_seconds of history
        if len(self._price_history) < self.cfg.lookback_seconds + 1:
            return None
        
        lookback_tick = self._price_history[-(self.cfg.lookback_seconds + 1)]
        
        # Check UP side dump
        if lookback_tick.up_best_ask > 0.05:
            up_drop = (lookback_tick.up_best_ask - tick.up_best_ask) / lookback_tick.up_best_ask
            if up_drop >= self.cfg.move_pct:
                logger.info(
                    f"DUMP DETECTED: UP dropped {up_drop:.1%} in {self.cfg.lookback_seconds}s "
                    f"({lookback_tick.up_best_ask:.3f} -> {tick.up_best_ask:.3f})"
                )
                return self._trigger_leg1("UP", tick.up_best_ask, secs_into_round)
        
        # Check DOWN side dump
        if lookback_tick.down_best_ask > 0.05:
            down_drop = (lookback_tick.down_best_ask - tick.down_best_ask) / lookback_tick.down_best_ask
            if down_drop >= self.cfg.move_pct:
                logger.info(
                    f"DUMP DETECTED: DOWN dropped {down_drop:.1%} in {self.cfg.lookback_seconds}s "
                    f"({lookback_tick.down_best_ask:.3f} -> {tick.down_best_ask:.3f})"
                )
                return self._trigger_leg1("DOWN", tick.down_best_ask, secs_into_round)
        
        return None
    
    def _trigger_leg1(self, side: str, price: float, secs: float) -> dict:
        """Trigger Leg 1 buy signal."""
        cycle = self._current_cycle
        cycle.state = CycleState.LEG1_TRIGGERED
        cycle.leg1_side = side
        cycle.leg1_price = price
        cycle.leg1_shares = self.cfg.shares
        cycle.leg1_time = secs
        
        return {
            "action": "buy_leg1",
            "side": side,
            "price": price,
            "shares": self.cfg.shares,
            "round_slug": cycle.round_slug,
        }
    
    def _check_for_hedge(self, tick: PriceTick, secs_into_round: float) -> Optional[dict]:
        """Check if the hedge condition is met for Leg 2."""
        cycle = self._current_cycle
        
        # Get opposite side's ask price
        if cycle.leg1_side == "UP":
            opposite_ask = tick.down_best_ask
            hedge_side = "DOWN"
        else:
            opposite_ask = tick.up_best_ask
            hedge_side = "UP"
        
        # Check hedge condition: leg1_price + opposite_ask <= sum_target
        combined = cycle.leg1_price + opposite_ask
        
        if combined <= self.cfg.sum_target:
            logger.info(
                f"HEDGE CONDITION MET: {cycle.leg1_price:.3f} + {opposite_ask:.3f} = "
                f"{combined:.3f} <= {self.cfg.sum_target:.3f}"
            )
            
            cycle.state = CycleState.HEDGED
            cycle.leg2_side = hedge_side
            cycle.leg2_price = opposite_ask
            cycle.leg2_shares = self.cfg.shares
            cycle.leg2_time = secs_into_round
            
            return {
                "action": "buy_leg2",
                "side": hedge_side,
                "price": opposite_ask,
                "shares": self.cfg.shares,
                "round_slug": cycle.round_slug,
            }
        
        return None
    
    def record_fill(self, leg: str, actual_price: float, actual_shares: float, fees: float):
        """Record that a trade was actually filled."""
        if not self._current_cycle:
            return
        
        cycle = self._current_cycle
        
        if leg == "leg1":
            cost = actual_price * actual_shares + fees
            cycle.leg1_price = actual_price
            cycle.leg1_shares = actual_shares
            cycle.total_cost += cost
            cycle.fees += fees
            self._balance -= cost
            logger.info(f"Leg 1 filled: {cycle.leg1_side} @ ${actual_price:.3f} x {actual_shares} (cost: ${cost:.2f})")
        
        elif leg == "leg2":
            cost = actual_price * actual_shares + fees
            cycle.total_cost += cost
            cycle.fees += fees
            self._balance -= cost
            
            # Both legs filled -- calculate locked profit
            cycle.payout = 1.0 * cycle.leg1_shares  # one side will pay $1
            cycle.pnl = cycle.payout - cycle.total_cost
            self._balance += cycle.payout
            self._daily_pnl += cycle.pnl
            self._total_pnl += cycle.pnl
            self._total_trades += 1
            
            if cycle.pnl > 0:
                self._consecutive_losses = 0
            else:
                self._consecutive_losses += 1
                if self._consecutive_losses >= self.risk.consecutive_loss_stop:
                    self._cooldown_until = time.time() + (self.risk.cooldown_rounds * 900)
                    logger.warning(f"Consecutive loss stop triggered, cooldown until {self._cooldown_until}")
            
            logger.info(
                f"Cycle complete (HEDGED): PnL=${cycle.pnl:+.3f}, "
                f"Balance=${self._balance:.2f}, Daily=${self._daily_pnl:+.2f}"
            )
    
    def record_round_end(self, outcome: str):
        """Record the round's resolution."""
        if not self._current_cycle:
            return
        
        cycle = self._current_cycle
        
        if cycle.state == CycleState.LEG1_TRIGGERED:
            # Unhedged Leg 1 -- resolve based on outcome
            if outcome == cycle.leg1_side:
                payout = 1.0 * cycle.leg1_shares
                cycle.pnl = payout - cycle.total_cost
                self._balance += payout
            else:
                cycle.pnl = -cycle.total_cost
            
            self._daily_pnl += cycle.pnl
            self._total_pnl += cycle.pnl
            self._total_trades += 1
            
            if cycle.pnl > 0:
                self._consecutive_losses = 0
            else:
                self._consecutive_losses += 1
                if self._consecutive_losses >= self.risk.consecutive_loss_stop:
                    self._cooldown_until = time.time() + (self.risk.cooldown_rounds * 900)
            
            logger.info(
                f"Cycle complete (UNHEDGED, {outcome}): PnL=${cycle.pnl:+.3f}, "
                f"Balance=${self._balance:.2f}"
            )
        
        cycle.state = CycleState.ROUND_OVER
    
    def _record_loss(self, amount: float):
        """Record a loss from an abandoned cycle."""
        self._daily_pnl -= amount
        self._total_pnl -= amount
        self._consecutive_losses += 1
        if self._consecutive_losses >= self.risk.consecutive_loss_stop:
            self._cooldown_until = time.time() + (self.risk.cooldown_rounds * 900)
    
    def get_status(self) -> dict:
        """Get current strategy status for display."""
        cycle_info = "No active cycle"
        if self._current_cycle:
            c = self._current_cycle
            cycle_info = f"State={c.state.value}, Side={c.leg1_side or 'none'}"
            if c.state == CycleState.LEG1_TRIGGERED:
                cycle_info += f", L1@${c.leg1_price:.3f}"
            elif c.state == CycleState.HEDGED:
                cycle_info += f", L1@${c.leg1_price:.3f}+L2@${c.leg2_price:.3f}, PnL=${c.pnl:+.3f}"
        
        return {
            "balance": self._balance,
            "daily_pnl": self._daily_pnl,
            "total_pnl": self._total_pnl,
            "total_trades": self._total_trades,
            "consecutive_losses": self._consecutive_losses,
            "cycle": cycle_info,
        }
