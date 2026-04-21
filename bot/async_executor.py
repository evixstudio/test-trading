"""
Async Order Executor -- order placement for Polymarket via py-clob-client SDK.

Key design:
1. Background Cryptography: Offloads EIP-712 signing + order posting to a
   background thread via asyncio.to_thread so the WS event loop stays unblocked.
2. Concurrent Execution: Fires both legs of an arbitrage trade simultaneously
   using batch orders.
3. Fill-Or-Kill (FOK): All entry orders are FOK. Fallback orders use FAK.
"""

import asyncio
import math
import time
import logging
from typing import Tuple
from dataclasses import dataclass
import requests

from bot.config import ApiConfig
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, BalanceAllowanceParams, AssetType
from py_clob_client.clob_types import PostOrdersArgs
from py_clob_client.clob_types import MarketOrderArgs, PartialCreateOrderOptions
from py_clob_client.order_builder.constants import BUY, SELL
from py_clob_client.clob_types import OrderArgs
from py_clob_client.order_builder.builder import OrderBuilder


logger = logging.getLogger(__name__)

# Constants for OrderType
FOK = "FOK"  # Fill-Or-Kill
FAK = "FAK"  # Fill-And-Kill

SLIPPAGE_BUFFER = 0.005   # 1% (adjust 0.003–0.008 for 5-min BTC liquidity)
TICK_BUFFER = 0.01        # minimum 1-tick protection
RAW_AMOUNT_THRESHOLD = 10000.0
RAW_AMOUNT_SCALE = 1_000_000.0


@dataclass
class AsyncOrderResult:
    """Result of an async order execution."""
    success: bool
    order_id: str = ""
    filled_price: float = 0.0
    filled_shares: float = 0.0
    fees: float = 0.0
    error: str = ""
    is_simulated: bool = False
    raw_response: dict = None


class AsyncOrderExecutor:
    """
    Ultra-low latency executor for Polymarket.
    """
    
    TICK_SIZE = "0.01"
    NEG_RISK = False

    def __init__(self, api_config: ApiConfig, dry_run: bool = True):
        self.api_config = api_config
        self.dry_run = dry_run
        
        self._sync_client = None
        self._order_builder = None
        self._order_options = None
        self._initialized = False

    @staticmethod
    def _normalize_api_amount(value) -> float:
        try:
            amount = float(value or 0)
        except (TypeError, ValueError):
            return 0.0
        if abs(amount) >= RAW_AMOUNT_THRESHOLD:
            return amount / RAW_AMOUNT_SCALE
        return amount

    def _fetch_book_top(self, token_id: str) -> tuple[float, float]:
        try:
            resp = requests.get(
                f"{self.api_config.clob_host}/book",
                params={"token_id": token_id},
                timeout=3,
            )
            if resp.status_code != 200:
                return 0.0, 0.0
            data = resp.json()
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            best_bid = 0.0
            best_ask = 0.0
            if bids:
                best_bid = max(
                    float(b.get("price", 0)) if isinstance(b, dict) else float(b)
                    for b in bids
                )
            if asks:
                best_ask = min(
                    float(a.get("price", 999)) if isinstance(a, dict) else float(a)
                    for a in asks
                )
            return best_bid, best_ask
        except Exception:
            return 0.0, 0.0

    async def initialize(self) -> bool:
        """Initialize the client and persistent HTTP session."""
        if self.dry_run:
            logger.info("AsyncExecutor initialized in DRY-RUN mode")
            self._initialized = True
            return True

        try:

            if not self.api_config.private_key:
                logger.error("Cannot initialize live async executor: no private key")
                return False

            # Create a synchronous client purely for EIP-712 signing
            temp_client = ClobClient(
                self.api_config.clob_host,
                key=self.api_config.private_key,
                chain_id=self.api_config.chain_id,
            )

            # This makes a synchronous HTTP call to derive credentials
            api_creds = await asyncio.to_thread(temp_client.create_or_derive_api_creds)

            self._sync_client = ClobClient(
                self.api_config.clob_host,
                key=self.api_config.private_key,
                chain_id=self.api_config.chain_id,
                creds=api_creds,
                signature_type=self.api_config.signature_type,
                funder=self.api_config.proxy_address,
            )
            self._order_builder = OrderBuilder(
                signer=self._sync_client.signer,
                sig_type=self.api_config.signature_type,
                funder=self.api_config.proxy_address,
            )
            self._order_options = PartialCreateOrderOptions(
                tick_size=self.TICK_SIZE,
                neg_risk=self.NEG_RISK,
            )

            # Health check
            ok = await asyncio.to_thread(self._sync_client.get_ok)
            if not ok:
                logger.error("CLOB client health check failed")
                return False
                
            # Allowance check
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=self.api_config.signature_type)
            ba = await asyncio.to_thread(self._sync_client.get_balance_allowance, params)
            
            if ba.get("balance", "0") == "0":
                logger.warning("No USDC balance detected. Deposits needed for live trading.")
                
            allowances = ba.get("allowances", {})
            needs_allowance = all(v == "0" for v in allowances.values())
            if needs_allowance:
                logger.info("Setting USDC allowance for exchange contracts...")
                try:
                    await asyncio.to_thread(self._sync_client.update_balance_allowance, params)
                    logger.info("Allowance updated successfully.")
                except Exception as e:
                    logger.error(f"Failed to set allowance: {e}")
                    return False
                    
            logger.info("AsyncExecutor initialized in LIVE mode")
            self._initialized = True
            return True

        except ImportError:
            logger.error("py-clob-client not installed.")
            return False
        except Exception as e:
            logger.error(f"Failed to initialize async executor: {e}")
            return False

    async def close(self):
        """Clean up resources."""
        pass

    async def get_conditional_balance(self, token_id: str) -> float:
        """Query on-chain conditional token balance via the CLOB API.

        The balance API returns raw 6-decimal integers (e.g. '2083100' = 2.0831
        shares), confirmed by test_buy_sell_cycle.py on 2026-03-05.
        """
        if self.dry_run or not self._sync_client:
            return 0.0
        try:
            def _query():
                params = BalanceAllowanceParams(
                    asset_type=AssetType.CONDITIONAL,
                    token_id=token_id,
                    signature_type=self.api_config.signature_type,
                )
                return self._sync_client.get_balance_allowance(params)

            ba = await asyncio.to_thread(_query)
            raw = ba.get("balance", "0")
            return int(raw) / 1_000_000
        except Exception as e:
            logger.warning(f"Balance query failed for {token_id[:16]}...: {e}")
            return 0.0

    async def get_collateral_balance(self) -> float:
        if self.dry_run or not self._sync_client:
            return 0.0
        try:
            def _query():
                params = BalanceAllowanceParams(
                    asset_type=AssetType.COLLATERAL,
                    signature_type=self.api_config.signature_type,
                )
                return self._sync_client.get_balance_allowance(params)

            ba = await asyncio.to_thread(_query)
            return self._normalize_api_amount(ba.get("balance", "0"))
        except Exception as e:
            logger.warning(f"Collateral balance query failed: {e}")
            return 0.0

    async def execute_arbitrage(
        self,
        up_token_id: str,
        up_worst: float,
        down_token_id: str,
        dn_worst: float,
        target_shares: float = 100.0
    ) -> Tuple[AsyncOrderResult, AsyncOrderResult]:
        """
        Executes both legs of an arbitrage concurrently using batch orders and parallel signing.
        """
        if not self._initialized:
            return AsyncOrderResult(success=False, error="Not initialized"), AsyncOrderResult(success=False, error="Not initialized")

        if self.dry_run:
            await asyncio.sleep(0.008)  # realistic dry-run delay
            return (
                AsyncOrderResult(success=True, filled_price=up_worst, filled_shares=target_shares, is_simulated=True),
                AsyncOrderResult(success=True, filled_price=dn_worst, filled_shares=target_shares, is_simulated=True)
            )

        # t0 = time.perf_counter()

        # t_prep = time.perf_counter()
        # Pre-create builder once (in __init__ or here — cheap)
        # 2. Parallel signing (BIGGEST WIN)
        def _sign_leg(token_id: str, target_shares: float, price: float):
            # ts = time.perf_counter()
            # args = MarketOrderArgs(
            #     token_id=token_id,
            #     amount=amount,
            #     price=round(price, 4),
            #     side=BUY
            # )
            args = OrderArgs(
                token_id=token_id,
                size=target_shares,           # For OrderArgs, the parameter is called 'size', not 'amount'
                price=round(price, 4),
                side=BUY,
                fee_rate_bps=1000            # Polymarket current taker fee is 10% (1000 bps)
            )

            # signed = self._sync_client.create_market_order(
            #     args,
            #     PartialCreateOrderOptions(tick_size=self.TICK_SIZE, neg_risk=self.NEG_RISK)
            # )
            # options = PartialCreateOrderOptions(
            #     tick_size=self.TICK_SIZE, 
            #     neg_risk=self.NEG_RISK
            # )
            
            # USE create_order instead of create_market_order to avoid the HTTP fetch bug
                        # 1. Instantiate the raw order builder
            signed = self._order_builder.create_order(args, self._order_options)

            # sign_ms = (time.perf_counter() - ts) * 1000
            return signed

        try:
            up_signed, dn_signed = await asyncio.gather(
                asyncio.to_thread(_sign_leg, up_token_id, target_shares, up_worst),
                asyncio.to_thread(_sign_leg, down_token_id, target_shares, dn_worst)
            )
        except Exception as e:
            err = f"Signing failed: {e}"
            return AsyncOrderResult(success=False, error=err), AsyncOrderResult(success=False, error=err)

        # t_signed = time.perf_counter()

        # 3. Batch post (single HTTP, already optimal)
        def _post_batch():
            batch = [
                PostOrdersArgs(order=up_signed, orderType=FOK),
                PostOrdersArgs(order=dn_signed, orderType=FOK)
            ]
            return self._sync_client.post_orders(batch)

        try:
            # TODO: uncomment this when the bot is ready to post orders
            resp = await asyncio.to_thread(_post_batch) 
            # logger.info("Posted orders...")
            # resp = [
            #     {
            #         "success": True,
            #         "orderID": f"mock_up_{int(time.time()*1000)}",
            #         "takingAmount": str(target_shares),
            #         "makingAmount": "0"
            #     },
            #     {
            #         "success": True,
            #         "orderID": f"mock_dn_{int(time.time()*1000)}",
            #         "takingAmount": str(target_shares),
            #         "makingAmount": "0"
            #     }
            # ]
        except Exception as e:
            err = f"Post failed: {e}"
            return AsyncOrderResult(success=False, error=err), AsyncOrderResult(success=False, error=err)

        # t_total = (time.perf_counter() - t0) * 1000

        # Detailed latency log
        # logger.info(
        #     f"ARB EXEC | Prep:{(t_prep-t0)*1000:.1f}ms | "
        #     f"SignUP:{up_sign_ms:.1f}ms | SignDN:{dn_sign_ms:.1f}ms | "
        #     f"TotalSign:{(t_signed-t_prep)*1000:.1f}ms | "
        #     f"Post+Total:{t_total:.1f}ms"
        # )

        # Parse response
        if isinstance(resp, list) and len(resp) == 2:
            up_resp, dn_resp = resp[0], resp[1]
        else:
            up_resp = dn_resp = resp

        def _make_result(r: dict, best_ask: float):
            if not isinstance(r, dict):
                r = {"errorMsg": str(r)}
            success = r.get("success", False)
            if r.get("errorMsg"):
                success = False

            filled_shares = self._normalize_api_amount(r.get("takingAmount", "0"))
            actual_cost = self._normalize_api_amount(r.get("makingAmount", "0"))
            if success and filled_shares <= 0:
                filled_shares = target_shares
            actual_price = (actual_cost / filled_shares) if actual_cost > 0 and filled_shares > 0 else best_ask

            return AsyncOrderResult(
                success=success,
                order_id=r.get("orderID", ""),
                filled_price=actual_price,
                filled_shares=filled_shares,
                raw_response=r,
                error=r.get("errorMsg", "") if not success else ""
            )

        return _make_result(up_resp, up_worst), _make_result(dn_resp, dn_worst)

    async def market_buy_fak(self, token_id: str, price: float, shares: float) -> AsyncOrderResult:
        """
        Single FAK buy order for one token.
        Used by the late-round bot to buy only the winning side.
        Fills as many shares as possible at the limit price, cancels the rest.

        OPTIMIZED: Pre-round price/shares, single thread call, no intermediate objects.
        """
        if self.dry_run:
            await asyncio.sleep(0.005)
            return AsyncOrderResult(
                success=True, filled_price=price, filled_shares=shares, is_simulated=True
            )

        # Pre-round outside thread (save 0.5-1ms in hot path)
        rounded_shares = round(shares, 2)
        rounded_price = round(price, 4)

        try:
            def _buy_sync():
                # Direct OrderArgs construction (avoid intermediate variables)
                order_args = OrderArgs(
                    token_id=token_id,
                    size=rounded_shares,
                    price=rounded_price,
                    side=BUY,
                    fee_rate_bps=1000,
                )
                # Use pre-built options (avoid repeated object creation)
                signed_order = self._order_builder.create_order(order_args, self._order_options)
                return self._sync_client.post_order(signed_order, FAK)

            resp = await asyncio.to_thread(_buy_sync)

            # Fast path: early return on non-dict response
            if not isinstance(resp, dict):
                return AsyncOrderResult(success=False, error=str(resp))

            # Check error first (most rejections have errorMsg)
            error_msg = resp.get("errorMsg", "")
            success = resp.get("success", False) and not error_msg

            # Only normalize if success (avoid wasted work on rejections)
            if success:
                filled_shares = self._normalize_api_amount(resp.get("takingAmount", "0"))
                actual_cost = self._normalize_api_amount(resp.get("makingAmount", "0"))

                # Fallback to requested shares if API doesn't return takingAmount
                if filled_shares <= 0:
                    filled_shares = rounded_shares

                # Calculate actual fill price
                if actual_cost > 0 and filled_shares > 0:
                    actual_price = actual_cost / filled_shares
                else:
                    actual_price = rounded_price

                return AsyncOrderResult(
                    success=True,
                    order_id=resp.get("orderID", ""),
                    filled_price=actual_price,
                    filled_shares=filled_shares,
                    raw_response=resp,
                    error="",
                )
            else:
                # Rejection path: minimal processing
                return AsyncOrderResult(
                    success=False,
                    order_id="",
                    filled_price=0.0,
                    filled_shares=0.0,
                    raw_response=resp,
                    error=error_msg,
                )

        except Exception as e:
            return AsyncOrderResult(success=False, error=str(e))

    async def market_sell(
        self,
        token_id: str,
        shares: float,
        best_bid: float = 0.0,
        best_ask: float = 0.0,
    ) -> AsyncOrderResult:
        """
        Aggressive dump: sell conditional tokens at a deep discount via FAK.

        Before posting, queries the actual on-chain balance to avoid the
        'not enough balance / allowance' rejection.  If the balance hasn't
        settled yet, retries with exponential back-off (up to ~14s total).
        """
        if self.dry_run:
            return AsyncOrderResult(success=True, is_simulated=True)

        actual_bal = await self.get_conditional_balance(token_id)
        if actual_bal <= 0:
            for wait_s in (0.3, 0.5, 0.5, 0.5):
                logger.info(
                    f"SELL: balance=0, waiting {wait_s}s for settlement..."
                )
                await asyncio.sleep(wait_s)
                actual_bal = await self.get_conditional_balance(token_id)
                if actual_bal > 0:
                    break

        if actual_bal <= 0:
            return AsyncOrderResult(
                success=False,
                error=f"Conditional token balance is 0 after settlement wait "
                      f"(requested sell of {shares})",
            )

        requested_qty = shares if shares > 0 else actual_bal
        sell_qty = math.floor(min(actual_bal, requested_qty) * 100) / 100
        if sell_qty <= 0:
            return AsyncOrderResult(
                success=False,
                error=f"sell_qty={sell_qty} after floor (bal={actual_bal}, req={shares})",
            )

        try:
            if best_bid <= 0 and best_ask <= 0:
                best_bid, best_ask = self._fetch_book_top(token_id)
            if best_bid > 0:
                dump_price = max(0.01, min(0.99, round(best_bid - 0.01, 2)))
            elif best_ask > 0:
                dump_price = max(0.01, min(0.99, round(best_ask * 0.95, 2)))
            else:
                return AsyncOrderResult(
                    success=False,
                    error=f"No live bid/ask available for stop-loss sell (token {token_id[:16]}...)",
                )

            def _sell_sync():
                order_args = OrderArgs(
                    token_id=token_id,
                    size=sell_qty,
                    price=dump_price,
                    side=SELL,
                    fee_rate_bps=1000,
                )

                signed_order = self._order_builder.create_order(order_args, self._order_options)
                return self._sync_client.post_order(signed_order, FAK)

            resp = await asyncio.to_thread(_sell_sync)

            logger.info(f"SELL RESP raw: {resp}")

            if isinstance(resp, dict) and resp.get("success") and not resp.get("errorMsg"):
                sold_shares = self._normalize_api_amount(resp.get("makingAmount", 0))
                proceeds = self._normalize_api_amount(resp.get("takingAmount", 0))
                filled_price = (proceeds / sold_shares) if proceeds > 0 and sold_shares > 0 else dump_price
                return AsyncOrderResult(
                    success=True,
                    filled_price=filled_price,
                    filled_shares=sold_shares or sell_qty,
                    raw_response=resp,
                )
            return AsyncOrderResult(
                success=False,
                error=resp.get("errorMsg", "") if isinstance(resp, dict) else str(resp),
            )

        except Exception as e:
            return AsyncOrderResult(success=False, error=str(e))
