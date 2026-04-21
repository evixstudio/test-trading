"""
Market resolver -- finds and tracks the current 15-minute BTC round.

Handles:
  - Generating correct market slugs from timestamps
  - Fetching token IDs for UP and DOWN from the Gamma API
  - Detecting when a new round starts
  - Caching market data to reduce API calls
"""

import time
import json
import logging
from dataclasses import dataclass
from typing import Optional

import requests

logger = logging.getLogger(__name__)


@dataclass
class RoundInfo:
    """Information about a single 15-minute round."""
    slug: str                 # e.g., "btc-updown-15m-1768502700"
    event_slug: str           # same as slug for these markets
    start_timestamp: int      # unix timestamp of round start
    end_timestamp: int        # unix timestamp of round end
    up_token_id: str          # CLOB token ID for UP outcome
    down_token_id: str        # CLOB token ID for DOWN outcome
    condition_id: str         # market condition ID
    seconds_remaining: int    # seconds until resolution
    
    @property
    def is_active(self) -> bool:
        return time.time() < self.end_timestamp
    
    def update_remaining(self):
        self.seconds_remaining = max(0, int(self.end_timestamp - time.time()))


class MarketResolver:
    """Resolves current and upcoming Polymarket 15-min BTC rounds."""
    
    INTERVAL_SECONDS = 900  # 15 minutes
    
    def __init__(self, gamma_host: str = "https://gamma-api.polymarket.com", coin: str = "btc"):
        self.gamma_host = gamma_host
        self.coin = coin
        self._cache: dict[str, RoundInfo] = {}
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "User-Agent": "polymarket-bot/1.0",
        })
    
    def get_current_round_slug(self) -> str:
        """Generate the slug for the current 15-minute round."""
        ts = int(time.time() // self.INTERVAL_SECONDS) * self.INTERVAL_SECONDS
        return f"{self.coin}-updown-15m-{ts}"
    
    def get_next_round_slug(self) -> str:
        """Generate the slug for the next 15-minute round."""
        ts = int(time.time() // self.INTERVAL_SECONDS) * self.INTERVAL_SECONDS
        ts += self.INTERVAL_SECONDS
        return f"{self.coin}-updown-15m-{ts}"
    
    def get_round_timestamps(self, slug: str) -> tuple[int, int]:
        """Extract start/end timestamps from a slug."""
        # Slug format: btc-updown-15m-{start_timestamp}
        parts = slug.split("-")
        start_ts = int(parts[-1])
        end_ts = start_ts + self.INTERVAL_SECONDS
        return start_ts, end_ts
    
    def seconds_into_round(self) -> float:
        """How many seconds into the current round we are."""
        ts = int(time.time() // self.INTERVAL_SECONDS) * self.INTERVAL_SECONDS
        return time.time() - ts
    
    def seconds_until_next_round(self) -> float:
        """Seconds until the next round starts."""
        return self.INTERVAL_SECONDS - self.seconds_into_round()
    
    def fetch_round_info(self, slug: str) -> Optional[RoundInfo]:
        """
        Fetch market details for a specific round from the Gamma API.
        
        Returns RoundInfo with token IDs, or None if not found.
        """
        # Check cache first
        if slug in self._cache:
            cached = self._cache[slug]
            cached.update_remaining()
            return cached
        
        try:
            # Try fetching by event slug
            url = f"{self.gamma_host}/events"
            params = {"slug": slug, "limit": 1}
            resp = self._session.get(url, params=params, timeout=10)
            
            if resp.status_code == 200:
                events = resp.json()
                if events and len(events) > 0:
                    event = events[0]
                    markets = event.get("markets", [])
                    
                    if markets:
                        market = markets[0]
                        return self._parse_market(slug, market)
            
            # Fallback: search markets directly
            url = f"{self.gamma_host}/markets"
            params = {"slug": slug, "limit": 1}
            resp = self._session.get(url, params=params, timeout=10)
            
            if resp.status_code == 200:
                markets = resp.json()
                if markets and len(markets) > 0:
                    return self._parse_market(slug, markets[0])
            
            logger.warning(f"Could not find market for slug: {slug}")
            return None
            
        except requests.RequestException as e:
            logger.error(f"API error fetching round info: {e}")
            return None
    
    def _parse_market(self, slug: str, market: dict) -> Optional[RoundInfo]:
        """Parse a market response into RoundInfo."""
        try:
            tokens = market.get("tokens", [])
            clob_token_ids = market.get("clobTokenIds", [])
            
            up_token_id = ""
            down_token_id = ""
            
            # Try tokens array first
            if tokens:
                for token in tokens:
                    outcome = token.get("outcome", "").upper()
                    token_id = token.get("token_id", "")
                    if outcome == "UP" or outcome == "YES":
                        up_token_id = token_id
                    elif outcome == "DOWN" or outcome == "NO":
                        down_token_id = token_id
            
            # Fallback to clobTokenIds (first=UP/YES, second=DOWN/NO)
            if not up_token_id and clob_token_ids and len(clob_token_ids) >= 2:
                up_token_id = clob_token_ids[0]
                down_token_id = clob_token_ids[1]
            
            if not up_token_id or not down_token_id:
                logger.warning(f"Could not find token IDs for {slug}")
                return None
            
            start_ts, end_ts = self.get_round_timestamps(slug)
            
            info = RoundInfo(
                slug=slug,
                event_slug=slug,
                start_timestamp=start_ts,
                end_timestamp=end_ts,
                up_token_id=up_token_id,
                down_token_id=down_token_id,
                condition_id=market.get("conditionId", market.get("condition_id", "")),
                seconds_remaining=max(0, int(end_ts - time.time())),
            )
            
            self._cache[slug] = info
            return info
            
        except (KeyError, IndexError, ValueError) as e:
            logger.error(f"Error parsing market data: {e}")
            return None
    
    def get_current_round(self) -> Optional[RoundInfo]:
        """Get info for the current active round."""
        slug = self.get_current_round_slug()
        return self.fetch_round_info(slug)
    
    def cleanup_cache(self):
        """Remove expired rounds from cache."""
        now = time.time()
        expired = [
            slug for slug, info in self._cache.items()
            if info.end_timestamp < now - 300  # keep for 5 min after end
        ]
        for slug in expired:
            del self._cache[slug]
