"""
Auto-Redeemer — Gasless Proxy-wallet redemption via Polymarket Builder Relayer
================================================================================
Redeems winning outcome tokens for USDC.e after a resolved market round.

The official Python py-builder-relayer-client only supports Safe wallets.
This module re-implements the Proxy transaction path from the TypeScript
@polymarket/builder-relayer-client so we can redeem gaslessly from a
Proxy wallet (signature_type=1 / Magic-Link users).

Flow:
  1. Fetch conditionId from Gamma API for the round slug
  2. Encode redeemPositions() on the CTF contract
  3. Wrap it in the ProxyFactory's proxy(calls) call
  4. Get relay nonce from the Polymarket relayer
  5. Build struct hash, sign with personal_sign
  6. Submit to relayer with Builder HMAC auth
  7. Poll until confirmed / failed
"""

import os
import time
import logging
import threading

import requests
from web3 import Web3
from eth_account import Account as EthAccount
from eth_account.messages import encode_defunct
from hexbytes import HexBytes
from eth_utils import keccak as eth_keccak, to_bytes, to_checksum_address
from eth_abi.packed import encode_packed
from py_builder_signing_sdk.config import BuilderConfig
from py_builder_signing_sdk.sdk_types import BuilderApiKeyCreds

log = logging.getLogger("auto-redeemer")

# ── Contract addresses (Polygon mainnet) ─────────────────────────────────

RELAYER_URL = "https://relayer-v2.polymarket.com"
PROXY_FACTORY = to_checksum_address("0xaB45c5A4B0c941a2F231C04C3f49182e1A254052")
RELAY_HUB = to_checksum_address("0xD216153c06E857cD7f72665E0aF1d7D82172F494")
CTF_ADDRESS = to_checksum_address("0x4D97DCd97eC945f40cF65F87097ACe5EA0476045")
USDC_E_ADDRESS = to_checksum_address("0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174")
PROXY_INIT_CODE_HASH = "0xd21df8dc65880a8606f09fe0ce3df9b8869287ab0b058be05aa9e8af6330a00b"
GAMMA_HOST = "https://gamma-api.polymarket.com"

REDEEM_DELAY = 210  # 3.5 minutes after round end
GAS_LIMIT = 500_000

_w3 = Web3()

CTF_REDEEM_ABI = [{
    "name": "redeemPositions", "type": "function",
    "inputs": [
        {"name": "collateralToken", "type": "address"},
        {"name": "parentCollectionId", "type": "bytes32"},
        {"name": "conditionId", "type": "bytes32"},
        {"name": "indexSets", "type": "uint256[]"},
    ],
    "outputs": [],
}]

PROXY_CALL_ABI = [{
    "name": "proxy", "type": "function",
    "inputs": [{
        "components": [
            {"name": "typeCode", "type": "uint8"},
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"},
            {"name": "data", "type": "bytes"},
        ],
        "name": "calls", "type": "tuple[]",
    }],
    "outputs": [{"name": "returnValues", "type": "bytes[]"}],
}]


# ── Helpers ──────────────────────────────────────────────────────────────

def derive_proxy_wallet(eoa_address: str) -> str:
    """Derive the Polymarket proxy-wallet address from an EOA via CREATE2."""
    eoa = to_checksum_address(eoa_address)
    salt = eth_keccak(encode_packed(["address"], [eoa]))
    factory_bytes = to_bytes(hexstr=PROXY_FACTORY)
    init_hash_bytes = to_bytes(hexstr=PROXY_INIT_CODE_HASH)
    address_hash = eth_keccak(b"\xff" + factory_bytes + salt + init_hash_bytes)
    return to_checksum_address("0x" + address_hash[-20:].hex())


def fetch_condition_id(slug: str) -> str | None:
    """Get the conditionId for a market slug from the Gamma API."""
    try:
        resp = requests.get(
            f"{GAMMA_HOST}/events",
            params={"slug": slug, "limit": 1},
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        events = resp.json()
        if not events:
            return None
        market = events[0].get("markets", [{}])[0]
        cid = market.get("conditionId") or market.get("condition_id")
        if not cid:
            return None
        if not cid.startswith("0x"):
            cid = "0x" + cid
        if len(cid) != 66:  # "0x" + 64 hex chars = 32 bytes
            log.warning(f"conditionId unexpected length {len(cid)}: {cid[:20]}...")
            return None
        return cid
    except Exception as e:
        log.error(f"Failed to fetch conditionId for {slug}: {e}")
        return None


def _build_redeem_calldata(condition_id: str) -> str:
    """Encode redeemPositions(collateral, parent, conditionId, [1,2])."""
    ctf = _w3.eth.contract(address=CTF_ADDRESS, abi=CTF_REDEEM_ABI)
    cid_hex = condition_id[2:] if condition_id.startswith("0x") else condition_id
    cid_bytes = bytes.fromhex(cid_hex.zfill(64)[:64])
    return ctf.encode_abi(
        abi_element_identifier="redeemPositions",
        args=[USDC_E_ADDRESS, bytes(32), cid_bytes, [1, 2]],
    )


def _build_proxy_calldata(redeem_data: str) -> str:
    """Wrap the redeem calldata inside the ProxyFactory's proxy(calls) call."""
    proxy = _w3.eth.contract(address=PROXY_FACTORY, abi=PROXY_CALL_ABI)
    rd_hex = redeem_data[2:] if redeem_data.startswith("0x") else redeem_data
    calls = [(1, CTF_ADDRESS, 0, bytes.fromhex(rd_hex))]  # typeCode 1 = Call
    return proxy.encode_abi(abi_element_identifier="proxy", args=[calls])


def _create_proxy_struct_hash(
    from_addr: str, to_addr: str, data: str,
    tx_fee: str, gas_price: str, gas_limit: str,
    nonce: str, relay_hub: str, relay: str,
) -> bytes:
    """Replicate the TypeScript proxy struct-hash: keccak256(rlx: || fields)."""
    data_hex = data[2:] if data.startswith("0x") else data
    return eth_keccak(
        b"rlx:"
        + to_bytes(hexstr=from_addr)
        + to_bytes(hexstr=to_addr)
        + bytes.fromhex(data_hex)
        + int(tx_fee).to_bytes(32, "big")
        + int(gas_price).to_bytes(32, "big")
        + int(gas_limit).to_bytes(32, "big")
        + int(nonce).to_bytes(32, "big")
        + to_bytes(hexstr=relay_hub)
        + to_bytes(hexstr=relay)
    )


# ── Core redeem function ────────────────────────────────────────────────

def redeem_via_proxy_relayer(condition_id: str, slug: str) -> tuple[bool, str]:
    """Build a Proxy-type relayer transaction to redeem winning tokens."""
    private_key = os.getenv("POLYMARKET_PRIVATE_KEY", "")
    if not private_key:
        raise ValueError("POLYMARKET_PRIVATE_KEY not set")

    account = EthAccount.from_key(private_key)
    from_address = account.address
    proxy_wallet = derive_proxy_wallet(from_address)

    expected = os.getenv("POLYMARKET_PROXY_ADDRESS", "")
    if expected and proxy_wallet.lower() != to_checksum_address(expected).lower():
        raise ValueError(
            f"Derived proxy {proxy_wallet} != env proxy {expected}. Aborting."
        )

    # ── 1. Encode calldata ──
    redeem_data = _build_redeem_calldata(condition_id)
    proxy_data = _build_proxy_calldata(redeem_data)

    # ── 2. Builder auth config ──
    builder_cfg = BuilderConfig(
        local_builder_creds=BuilderApiKeyCreds(
            key=os.getenv("POLY_BUILDER_API_KEY", ""),
            secret=os.getenv("POLY_BUILDER_SECRET", ""),
            passphrase=os.getenv("POLY_BUILDER_PASSPHRASE", ""),
        )
    )

    # ── 3. Fetch relay payload (address + nonce) — no auth needed ──
    rp = requests.get(
        f"{RELAYER_URL}/relay-payload",
        params={"address": from_address, "type": "PROXY"},
        timeout=10,
    )
    if rp.status_code != 200:
        raise ValueError(f"Relay-payload HTTP {rp.status_code}: {rp.text[:200]}")
    relay_payload = rp.json()
    relay_address = relay_payload.get("address")
    nonce = relay_payload.get("nonce")
    if not relay_address or nonce is None:
        raise ValueError(f"Invalid relay payload: {relay_payload}")

    # ── 4. Struct hash + signature (personal_sign) ──
    gas_limit = str(GAS_LIMIT)
    struct_hash = _create_proxy_struct_hash(
        from_addr=from_address,
        to_addr=PROXY_FACTORY,
        data=proxy_data,
        tx_fee="0",
        gas_price="0",
        gas_limit=gas_limit,
        nonce=nonce,
        relay_hub=RELAY_HUB,
        relay=relay_address,
    )
    msg = encode_defunct(HexBytes(struct_hash))
    sig = EthAccount.sign_message(msg, private_key=private_key)
    signature_hex = "0x" + sig.signature.hex()

    # ── 5. Build request body ──
    request_body = {
        "type": "PROXY",
        "from": from_address,
        "to": PROXY_FACTORY,
        "proxyWallet": proxy_wallet,
        "data": proxy_data,
        "nonce": nonce,
        "signature": signature_hex,
        "signatureParams": {
            "gasPrice": "0",
            "gasLimit": gas_limit,
            "relayerFee": "0",
            "relayHub": RELAY_HUB,
            "relay": relay_address,
        },
        "metadata": f"redeem-{slug}",
    }

    # ── 6. Submit with builder auth headers ──
    body_for_hmac = str(request_body)
    hdrs_obj = builder_cfg.generate_builder_headers("POST", "/submit", body_for_hmac)
    hdrs = hdrs_obj.to_dict() if hdrs_obj else {}
    hdrs["Content-Type"] = "application/json"

    log.info(f"Submitting redeem txn for {slug} (proxy={proxy_wallet[:10]}...)")
    sub_resp = requests.post(
        f"{RELAYER_URL}/submit", json=request_body, headers=hdrs, timeout=30,
    )
    if sub_resp.status_code != 200:
        err_body = sub_resp.text[:300]
        raise ValueError(f"Relayer HTTP {sub_resp.status_code}: {err_body}")
    sub_data = sub_resp.json()
    txn_id = sub_data.get("transactionID") or sub_data.get("transactionId", "")

    if not txn_id:
        raise ValueError(f"Relayer returned no transactionID: {sub_data}")

    log.info(f"Redeem txn submitted: {txn_id}")

    # ── 7. Poll for terminal state (up to ~60s) ──
    for _ in range(30):
        try:
            poll = requests.get(
                f"{RELAYER_URL}/transaction",
                params={"id": txn_id},
                timeout=10,
            )
            if poll.status_code == 200:
                txns = poll.json()
                if txns and isinstance(txns, list):
                    state = txns[0].get("state", "")
                    if state in ("STATE_CONFIRMED", "STATE_MINED"):
                        log.info(f"Redeem CONFIRMED: {txn_id}")
                        return True, txn_id
                    if state == "STATE_FAILED":
                        tx_hash = txns[0].get("transactionHash", "")
                        log.error(f"Redeem FAILED on-chain: {txn_id} hash={tx_hash}")
                        return False, txn_id
        except Exception:
            pass
        time.sleep(2)

    log.error(f"Redeem poll timed out: {txn_id}")
    return False, txn_id


# ── Scheduler (called from telegram_dispatcher) ─────────────────────────

def _fetch_account_summary(clob_client) -> tuple[str, str]:
    """Fetch current USDC cash balance and portfolio summary from the CLOB client.

    Returns (cash_str, portfolio_str) for display in Telegram messages.
    """
    from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

    cash_str = "N/A"
    portfolio_str = "N/A"

    if not clob_client:
        return cash_str, portfolio_str

    try:
        sig_type = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0"))
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=sig_type)
        ba = clob_client.get_balance_allowance(params)
        balance = float(ba.get("balance", "0")) / 1e6
        cash_str = f"${balance:,.2f}"
        portfolio_str = f"${balance:,.2f} (all redeemed)"
    except Exception as e:
        log.warning(f"Failed to fetch account summary: {e}")

    return cash_str, portfolio_str


def schedule_auto_redeem(slug: str, won: bool, slug_to_et_fn, send_telegram_fn, clob_client=None):
    """Schedule gasless auto-redeem 3.5 min after a winning round.

    Parameters
    ----------
    slug : str
        Round slug, e.g. 'btc-updown-5m-1772392800'.
    won : bool
        Whether we won this round.
    slug_to_et_fn : callable
        Converts slug → human-readable ET time string.
    send_telegram_fn : callable
        Sends a Telegram message.
    clob_client : ClobClient | None
        Optional CLOB client for fetching post-redeem balance.
    """
    if not won:
        return
    if not os.getenv("POLY_BUILDER_API_KEY"):
        log.info("Builder API key not configured — skipping auto-redeem")
        return

    def _delayed_redeem():
        et_label = slug_to_et_fn(slug)
        log.info(f"Auto-redeem for {et_label} scheduled in {REDEEM_DELAY}s...")
        time.sleep(REDEEM_DELAY)

        try:
            condition_id = fetch_condition_id(slug)
            if not condition_id:
                send_telegram_fn(
                    f"*AUTO-REDEEM FAILED*\n"
                    f"Round: `{et_label}`\n"
                    f"_Could not find conditionId from Gamma API._"
                )
                return

            success, txn_id = redeem_via_proxy_relayer(condition_id, slug)

            if success:
                time.sleep(3)
                cash, portfolio = _fetch_account_summary(clob_client)
                send_telegram_fn(
                    f"*AUTO-REDEEM SUCCESS*\n"
                    f"Round: `{et_label}`\n"
                    f"TxnID: `{txn_id}`\n\n"
                    f"*Account Status:*\n"
                    f"Cash: `{cash}`\n"
                    f"Portfolio: `{portfolio}`"
                )
            else:
                send_telegram_fn(
                    f"*AUTO-REDEEM FAILED*\n"
                    f"Round: `{et_label}`\n"
                    f"TxnID: `{txn_id}`\n"
                    f"_On-chain failure. Claim manually on Polymarket._\n\n"
                    f"*Account Status:*\n"
                    f"Cash: `{cash}`\n"
                    f"Portfolio: `{portfolio}`"
                )
        except Exception as e:
            log.error(f"Auto-redeem error: {e}", exc_info=True)
            send_telegram_fn(
                f"*AUTO-REDEEM ERROR*\n"
                f"Round: `{slug_to_et_fn(slug)}`\n"
                f"Error: `{str(e)[:200]}`"
            )

    t = threading.Thread(
        target=_delayed_redeem, daemon=True, name=f"redeem-{slug}"
    )
    t.start()
