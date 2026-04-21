"""
Real-time price feed via Polymarket WebSocket + REST fallback.

Provides a stream of best bid/ask prices for UP and DOWN tokens
with sub-second latency via WebSocket, falling back to REST polling.
"""

import json
import time
import threading
import logging
from dataclasses import dataclass, field
from typing import Optional, Callable
from collections import deque

import requests

logger = logging.getLogger(__name__)


@dataclass
class PriceTick:
    """A single price update for both sides."""
    timestamp: float           # time.time()
    up_best_ask: float = 0.0
    up_best_bid: float = 0.0
    down_best_ask: float = 0.0
    down_best_bid: float = 0.0
    source: str = "rest"       # "ws" or "rest"


class PriceFeed:
    """
    Real-time price feed for a Polymarket binary market.
    
    Uses REST polling (1 request/second) for reliability.
    WebSocket can be added as an enhancement but REST is sufficient
    for the 3-second dump detection window.
    """
    
    def __init__(
        self,
        clob_host: str = "https://clob.polymarket.com",
        poll_interval: float = 1.0,
    ):
        self.clob_host = clob_host
        self.poll_interval = poll_interval
        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "User-Agent": "polymarket-bot/1.0",
        })
        
        self._up_token_id: str = ""
        self._down_token_id: str = ""
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._ticks: deque[PriceTick] = deque(maxlen=900)  # 15 min of ticks
        self._latest: Optional[PriceTick] = None
        self._callbacks: list[Callable[[PriceTick], None]] = []
        self._lock = threading.Lock()
    
    def set_tokens(self, up_token_id: str, down_token_id: str):
        """Set the token IDs to monitor."""
        self._up_token_id = up_token_id
        self._down_token_id = down_token_id
        with self._lock:
            self._ticks.clear()
            self._latest = None
    
    def on_tick(self, callback: Callable[[PriceTick], None]):
        """Register a callback for each new price tick."""
        self._callbacks.append(callback)
    
    @property
    def latest(self) -> Optional[PriceTick]:
        """Get the most recent price tick."""
        return self._latest
    
    @property
    def history(self) -> list[PriceTick]:
        """Get all recorded price ticks for current round."""
        with self._lock:
            return list(self._ticks)
    
    def get_recent(self, seconds: int) -> list[PriceTick]:
        """Get ticks from the last N seconds."""
        cutoff = time.time() - seconds
        with self._lock:
            return [t for t in self._ticks if t.timestamp >= cutoff]
    
    def fetch_prices_rest(self) -> Optional[PriceTick]:
        """Fetch current best bid/ask from REST API."""
        if not self._up_token_id or not self._down_token_id:
            return None
        
        try:
            tick = PriceTick(timestamp=time.time(), source="rest")
            
            # Fetch UP token book
            up_book = self._fetch_book(self._up_token_id)
            if up_book:
                tick.up_best_ask = up_book.get("best_ask", 0.0)
                tick.up_best_bid = up_book.get("best_bid", 0.0)
            
            # Fetch DOWN token book
            down_book = self._fetch_book(self._down_token_id)
            if down_book:
                tick.down_best_ask = down_book.get("best_ask", 0.0)
                tick.down_best_bid = down_book.get("best_bid", 0.0)
            
            return tick
            
        except Exception as e:
            logger.error(f"Error fetching prices: {e}")
            return None
    
    def _fetch_book(self, token_id: str) -> Optional[dict]:
        """Fetch order book for a single token."""
        try:
            url = f"{self.clob_host}/book"
            params = {"token_id": token_id}
            resp = self._session.get(url, params=params, timeout=5)
            
            if resp.status_code == 200:
                data = resp.json()
                result = {}
                
                # Extract best ask (lowest ask price)
                asks = data.get("asks", [])
                if asks:
                    best_ask = min(float(a.get("price", 999)) for a in asks)
                    result["best_ask"] = best_ask
                
                # Extract best bid (highest bid price)
                bids = data.get("bids", [])
                if bids:
                    best_bid = max(float(b.get("price", 0)) for b in bids)
                    result["best_bid"] = best_bid
                
                return result
            
            return None
            
        except Exception as e:
            logger.debug(f"Error fetching book for {token_id}: {e}")
            return None
    
    def fetch_midpoint(self, token_id: str) -> Optional[float]:
        """Fetch midpoint price for a token."""
        try:
            url = f"{self.clob_host}/midpoint"
            params = {"token_id": token_id}
            resp = self._session.get(url, params=params, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                return float(data.get("mid", 0))
            return None
        except Exception:
            return None
    
    def start(self):
        """Start the price polling thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True)
        self._thread.start()
        logger.info("Price feed started (REST polling)")
    
    def stop(self):
        """Stop the price polling thread."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        logger.info("Price feed stopped")
    
    def _poll_loop(self):
        """Main polling loop -- runs in background thread."""
        while self._running:
            tick = self.fetch_prices_rest()
            if tick:
                with self._lock:
                    self._ticks.append(tick)
                    self._latest = tick
                
                for cb in self._callbacks:
                    try:
                        cb(tick)
                    except Exception as e:
                        logger.error(f"Callback error: {e}")
            
            time.sleep(self.poll_interval)
    
    def fetch_spreads(self, token_ids: list[str]) -> dict:
        """
        Fetch bid-ask spreads for multiple tokens in one call.
        Uses the /spreads endpoint for efficiency.
        """
        try:
            url = f"{self.clob_host}/spreads"
            # The spreads endpoint accepts comma-separated token IDs
            params = {"token_ids": ",".join(token_ids)}
            resp = self._session.get(url, params=params, timeout=5)
            if resp.status_code == 200:
                return resp.json()
            return {}
        except Exception as e:
            logger.error(f"Error fetching spreads: {e}")
            return {}
