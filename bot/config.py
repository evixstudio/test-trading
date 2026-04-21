"""
Bot configuration - strategy parameters, API settings, and risk limits.

All values derived from backtesting results:
  Best params: windowMin=1.5, movePct=0.20, sumTarget=0.90, shares=8
  Avg ROI: +8.90% over 4 days, Win Rate: 86.9%, Max DD: 5.84%
"""

import os
from dataclasses import dataclass, field


@dataclass
class StrategyConfig:
    """Two-Leg Volatility Capture strategy parameters."""
    window_min: float = 1.5         # minutes to watch for Leg 1 dump
    move_pct: float = 0.20          # 20% dump threshold
    sum_target: float = 0.90        # hedge when Leg1 + Leg2 <= 0.90
    shares: float = 8.0             # shares per leg
    lookback_seconds: int = 3       # dump detection window


@dataclass
class RiskConfig:
    """Risk management parameters."""
    initial_balance: float = 100.0
    daily_loss_limit: float = 10.0      # $10 max daily loss (10%)
    consecutive_loss_stop: int = 3      # pause after 3 consecutive losses
    cooldown_rounds: int = 4            # skip 4 rounds (1 hour) after consecutive losses
    capital_floor: float = 70.0         # stop trading if balance < $70
    max_position_pct: float = 0.05      # max 5% of capital per trade
    max_open_positions: int = 1         # only 1 position at a time


@dataclass
class ApiConfig:
    """Polymarket API configuration."""
    clob_host: str = "https://clob.polymarket.com"
    gamma_host: str = "https://gamma-api.polymarket.com"
    ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/"
    chain_id: int = 137  # Polygon mainnet
    
    # Loaded from environment
    private_key: str = ""
    proxy_address: str = ""
    signature_type: int = 0  # 0=EOA, 1=Magic, 2=Gnosis
    
    def load_from_env(self):
        """Load sensitive config from environment variables."""
        self.private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "")
        self.proxy_address = os.getenv("POLYMARKET_PROXY_ADDRESS", "")
        sig_type = os.getenv("POLYMARKET_SIGNATURE_TYPE", "0")
        self.signature_type = int(sig_type)


@dataclass
class BotConfig:
    """Complete bot configuration."""
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    api: ApiConfig = field(default_factory=ApiConfig)
    
    # Operating mode
    dry_run: bool = True  # if True, simulate trades without real execution
    log_level: str = "INFO"
    
    # Market identification
    market_coin: str = "btc"
    market_interval: str = "5m"
    
    def validate(self) -> list[str]:
        """Validate configuration, return list of issues."""
        issues = []
        if not self.dry_run and not self.api.private_key:
            issues.append("POLYMARKET_PRIVATE_KEY env var required for live trading")
        if self.risk.initial_balance <= 0:
            issues.append("initial_balance must be positive")
        if self.strategy.shares <= 0:
            issues.append("shares must be positive")
        if self.strategy.move_pct <= 0 or self.strategy.move_pct >= 1:
            issues.append("move_pct must be between 0 and 1")
        if self.strategy.sum_target <= 0 or self.strategy.sum_target >= 1:
            issues.append("sum_target must be between 0 and 1")
        return issues
