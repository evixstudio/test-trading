"""
Order executor -- places and manages orders on Polymarket.

Supports:
  - Dry-run mode (simulated fills) for testing
  - Live mode using py-clob-client SDK
  - Order tracking and fill confirmation
"""

import time
import logging
from dataclasses import dataclass
from typing import Optional

from bot.config import ApiConfig
from backtest.fees import taker_fee_per_share

logger = logging.getLogger(__name__)


@dataclass
class OrderResult:
    """Result of an order execution."""
    success: bool
    order_id: str = ""
    filled_price: float = 0.0
    filled_shares: float = 0.0
    fees: float = 0.0
    error: str = ""
    is_simulated: bool = False


class OrderExecutor:
    """
    Executes orders on Polymarket CLOB.
    
    In dry_run mode, simulates immediate fills at the requested price.
    In live mode, places limit orders via the py-clob-client SDK.
    """
    
    def __init__(self, api_config: ApiConfig, dry_run: bool = True):
        self.api_config = api_config
        self.dry_run = dry_run
        self._client = None
        self._initialized = False
    
    def initialize(self) -> bool:
        """Initialize the CLOB client (live mode only)."""
        if self.dry_run:
            logger.info("Executor initialized in DRY-RUN mode (no real orders)")
            self._initialized = True
            return True
        
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds
            
            if not self.api_config.private_key:
                logger.error("Cannot initialize live executor: no private key")
                return False
            
            # Create client
            temp_client = ClobClient(
                self.api_config.clob_host,
                key=self.api_config.private_key,
                chain_id=self.api_config.chain_id,
            )
            
            # Derive API credentials
            api_creds = temp_client.create_or_derive_api_creds()
            
            self._client = ClobClient(
                self.api_config.clob_host,
                key=self.api_config.private_key,
                chain_id=self.api_config.chain_id,
                creds=api_creds,
                signature_type=self.api_config.signature_type,
            )
            
            # Verify connection
            ok = self._client.get_ok()
            if ok:
                logger.info("Executor initialized in LIVE mode")
                self._initialized = True
                return True
            else:
                logger.error("CLOB client health check failed")
                return False
                
        except ImportError:
            logger.error(
                "py-clob-client not installed. Install with: pip install py-clob-client"
            )
            return False
        except Exception as e:
            logger.error(f"Failed to initialize executor: {e}")
            return False
    
    def buy(
        self,
        token_id: str,
        side_name: str,
        price: float,
        shares: float,
    ) -> OrderResult:
        """
        Place a buy order.
        
        Args:
            token_id: CLOB token ID for the outcome
            side_name: "UP" or "DOWN" (for logging)
            price: target price per share
            shares: number of shares
        
        Returns:
            OrderResult with fill details
        """
        if not self._initialized:
            return OrderResult(success=False, error="Executor not initialized")
        
        if self.dry_run:
            return self._simulate_buy(token_id, side_name, price, shares)
        else:
            return self._live_buy(token_id, side_name, price, shares)
    
    def _simulate_buy(
        self,
        token_id: str,
        side_name: str,
        price: float,
        shares: float,
    ) -> OrderResult:
        """Simulate a buy order with immediate fill."""
        fees = taker_fee_per_share(price) * shares
        
        logger.info(
            f"[DRY-RUN] BUY {side_name}: {shares} shares @ ${price:.3f} "
            f"(fees: ${fees:.4f}, total: ${price * shares + fees:.4f})"
        )
        
        return OrderResult(
            success=True,
            order_id=f"sim_{int(time.time()*1000)}",
            filled_price=price,
            filled_shares=shares,
            fees=fees,
            is_simulated=True,
        )
    
    def _live_buy(
        self,
        token_id: str,
        side_name: str,
        price: float,
        shares: float,
    ) -> OrderResult:
        """Place a real buy order on Polymarket."""
        try:
            from py_clob_client.clob_types import OrderArgs
            from py_clob_client.order_builder.constants import BUY
            
            # Round price to 3 decimal places (Polymarket precision)
            price = round(price, 3)
            
            logger.info(
                f"[LIVE] Placing BUY {side_name}: {shares} shares @ ${price:.3f} "
                f"(token: {token_id[:16]}...)"
            )
            
            # Place limit order (GTC - Good Till Cancelled)
            response = self._client.create_and_post_order(
                OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=shares,
                    side=BUY,
                )
            )
            
            if response and response.get("orderID"):
                order_id = response["orderID"]
                fees = taker_fee_per_share(price) * shares
                
                logger.info(f"[LIVE] Order placed: {order_id}")
                
                # Wait briefly for fill confirmation
                filled = self._wait_for_fill(order_id, timeout=5.0)
                
                if filled:
                    return OrderResult(
                        success=True,
                        order_id=order_id,
                        filled_price=price,
                        filled_shares=shares,
                        fees=fees,
                    )
                else:
                    # Order may still be open -- try to cancel
                    logger.warning(f"Order {order_id} not filled within timeout, cancelling")
                    self._cancel_order(order_id)
                    return OrderResult(
                        success=False,
                        order_id=order_id,
                        error="Order not filled within timeout",
                    )
            else:
                return OrderResult(
                    success=False,
                    error=f"Order placement failed: {response}",
                )
                
        except Exception as e:
            logger.error(f"Order execution error: {e}")
            return OrderResult(success=False, error=str(e))
    
    def _wait_for_fill(self, order_id: str, timeout: float = 5.0) -> bool:
        """Wait for an order to be filled."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                # Check if order is still open
                open_orders = self._client.get_open_orders()
                if not any(o.get("id") == order_id for o in open_orders):
                    return True  # Not in open orders = filled or cancelled
            except Exception:
                pass
            time.sleep(0.5)
        return False
    
    def _cancel_order(self, order_id: str) -> bool:
        """Cancel an open order."""
        try:
            self._client.cancel(order_id)
            logger.info(f"Order {order_id} cancelled")
            return True
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return False
    
    def get_balance(self) -> Optional[float]:
        """Get current USDC balance (live mode only)."""
        if self.dry_run or not self._client:
            return None
        try:
            # This would need the proper balance check method
            # For now return None
            return None
        except Exception:
            return None
