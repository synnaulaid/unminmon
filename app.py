import os
import time
import hmac
import hashlib
import json
import threading
import queue
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import requests
from flask import Flask, jsonify, render_template, Response, stream_with_context
from dotenv import load_dotenv


# ============================================================
# CONFIG
# ============================================================

load_dotenv()

API_BASE = "https://api.unmineable.dev"

API_KEY = os.getenv("UNMINEABLE_API_KEY", "").strip()
API_SECRET = os.getenv("UNMINEABLE_API_SECRET", "").strip()

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))
HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "360"))


def parse_payout_minimums(value):
    minimums = {}
    for entry in value.split(","):
        coin, separator, amount = entry.strip().partition(":")
        if not separator or not coin:
            continue
        try:
            minimum = Decimal(amount.strip())
        except (InvalidOperation, ValueError):
            continue
        if minimum.is_finite() and minimum > 0:
            minimums[coin.strip().upper()] = minimum
    return minimums


PAYOUT_MINIMUMS = {
    "LTC": Decimal("0.00075"),
}
PAYOUT_MINIMUMS.update(
    parse_payout_minimums(os.getenv("PAYOUT_MINIMUMS", ""))
)

app = Flask(__name__)


# ============================================================
# IN-MEMORY STATE
# ============================================================

state_lock = threading.Lock()

state = {
    "connected": False,
    "last_update": None,
    "error": None,

    "account": {},
    "workers": [],

    "worker_count": 0,
    "online_workers": 0,
    "offline_workers": 0,

    "hashrate": 0.0,

    "balance": None,
    "balance_asset": None,
    "balance_asset_logo": None,
    "amount_mined": None,
    "amount_referral": None,
    "reward_ratio": None,
    "reward_algorithm": None,
    "algorithm_count": None,
    "algorithm_hashrates": {},
    "earnings": {
        "hour": None,
        "day": None,
        "month": None,
        "year": None,
    },
    "payout": {
        "balance": None,
        "minimum": None,
        "remaining": None,
        "percent": None,
        "ready": False,
    },
    "total_paid": None,
}


history = {
    "hashrate": [],
    "balance": [],
}

events = []

earnings_tracker = {
    "asset": None,
    "started_at": None,
    "last_balance": None,
    "earned": Decimal("0"),
}

# SSE clients
clients = set()
clients_lock = threading.Lock()


# ============================================================
# HELPERS
# ============================================================

def now_iso():
    return datetime.now(timezone.utc).isoformat()


def add_event(event_type, message, data=None):
    event = {
        "time": now_iso(),
        "type": event_type,
        "message": message,
        "data": data or {},
    }

    with state_lock:
        events.append(event)

    broadcast({
        "event": "log",
        "data": event,
    })


def set_state(**kwargs):
    with state_lock:
        state.update(kwargs)


def get_state():
    with state_lock:
        return json.loads(json.dumps(state))


# ============================================================
# UNMINEABLE HMAC AUTH
# ============================================================

def make_signature(method, path, query="", body=""):
    """
    unMineable signature:

    METHOD
    PATH
    RAW_QUERY
    TIMESTAMP
    SHA256(BODY)
    """

    timestamp = str(int(time.time() * 1000))

    body_hash = hashlib.sha256(
        body.encode("utf-8")
    ).hexdigest()

    payload = "\n".join([
        method.upper(),
        path,
        query,
        timestamp,
        body_hash,
    ])

    signature = hmac.new(
        API_SECRET.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    return timestamp, signature


def api_request(method, path, query="", body=""):
    """
    Make authenticated request to unMineable.

    IMPORTANT:
    path MUST include /v1

    Example:
        /v1/me
        /v1/workers
        /v1/workers/counts
    """

    if not API_KEY or not API_SECRET:
        raise RuntimeError(
            "UNMINEABLE_API_KEY / UNMINEABLE_API_SECRET belum di-set"
        )

    timestamp, signature = make_signature(
        method,
        path,
        query,
        body,
    )

    headers = {
        "x-user-api-key": API_KEY,
        "x-user-api-timestamp": timestamp,
        "x-user-api-signature": signature,
        "Accept": "application/json",
    }

    url = API_BASE + path

    response = requests.request(
        method=method.upper(),
        url=url,
        headers=headers,
        data=body if body else None,
        timeout=15,
    )

    # Convert API errors into useful exception
    if not response.ok:
        try:
            error_json = response.json()
            raise RuntimeError(
                f"unMineable HTTP {response.status_code}: "
                f"{json.dumps(error_json, separators=(',', ':'))}"
            )
        except ValueError:
            raise RuntimeError(
                f"unMineable HTTP {response.status_code}: "
                f"{response.text[:500]}"
            )

    try:
        result = response.json()
    except ValueError:
        raise RuntimeError(
            f"Invalid JSON response: {response.text[:500]}"
        )

    if isinstance(result, dict) and result.get("success") is False:
        raise RuntimeError(
            json.dumps(result, separators=(",", ":"))
        )

    return result


# ============================================================
# API ENDPOINTS
# ============================================================

def get_me():
    return api_request(
        "GET",
        "/v1/me",
    )


def get_workers():
    return api_request(
        "GET",
        "/v1/workers",
    )


def get_worker_counts():
    return api_request(
        "GET",
        "/v1/workers/counts",
    )


def get_dashboard_summary():
    return api_request(
        "GET",
        "/v1/dashboard/summary",
    )


def get_dashboard_assets():
    return api_request(
        "GET",
        "/v1/dashboard/assets",
    )


def get_payments():
    return api_request(
        "GET",
        "/v1/payments",
    )


# ============================================================
# WORKER NORMALIZATION
# ============================================================

def normalize_worker(worker):
    return {
        "uuid": worker.get("uuid"),
        "name": worker.get("name", "unknown"),
        "online": bool(worker.get("online", False)),
        "hashrate": float(worker.get("hashrate") or 0),
        "algorithm": worker.get("algorithm"),
        "region": worker.get("region"),
        "ip": worker.get("ip"),
        "agent": worker.get("agent"),
        "last": worker.get("last"),
    }


def detect_worker_events(previous_workers, current_workers):
    def index_workers(workers):
        indexed = {}
        for index, worker in enumerate(workers):
            key = worker.get("uuid") or worker.get("name") or f"worker-{index}"
            indexed[key] = worker
        return indexed

    previous = index_workers(previous_workers)
    current = index_workers(current_workers)
    detected = []

    for key, worker in current.items():
        name = worker.get("name") or key
        old_worker = previous.get(key)

        if old_worker is None:
            status = "online" if worker["online"] else "offline"
            detected.append((
                "worker",
                f"Worker {name} detected and is {status}",
                {"worker": key, "online": worker["online"]},
            ))
        elif old_worker["online"] != worker["online"]:
            status = "online" if worker["online"] else "offline"
            detected.append((
                "worker",
                f"Worker {name} is now {status}",
                {"worker": key, "online": worker["online"]},
            ))

    for key, worker in previous.items():
        if key not in current:
            name = worker.get("name") or key
            detected.append((
                "worker",
                f"Worker {name} removed",
                {"worker": key, "online": worker["online"]},
            ))

    return detected


# ============================================================
# HASHRATE
# ============================================================

def calculate_hashrate(workers):
    return sum(
        float(worker.get("hashrate") or 0)
        for worker in workers
    )


def find_value(data, keys):
    if isinstance(data, dict):
        for key in keys:
            value = data.get(key)
            if value is not None and not isinstance(value, (dict, list)):
                return value

        for value in data.values():
            result = find_value(value, keys)
            if result is not None:
                return result

    elif isinstance(data, list):
        for item in data:
            result = find_value(item, keys)
            if result is not None:
                return result

    return None


def extract_miner_metrics(summary_response, assets_response):
    assets_data = (
        assets_response.get("data", {})
        if isinstance(assets_response, dict)
        else {}
    )
    assets = assets_data.get("list", []) if isinstance(assets_data, dict) else []
    if isinstance(assets, dict):
        assets = list(assets.values())

    asset = None
    if isinstance(assets, list) and assets:
        active_assets = [
            asset for asset in assets
            if isinstance(asset, dict) and asset.get("is_active") is not False
        ]
        asset = (active_assets or assets)[0]

    asset = asset if isinstance(asset, dict) else {}
    summary_data = (
        summary_response.get("data", {})
        if isinstance(summary_response, dict)
        else {}
    )
    summary_raw = summary_data.get("raw", {}) if isinstance(summary_data, dict) else {}
    balance = find_value(asset, ("amount", "balance", "amount_mined"))
    if balance is None:
        balance = find_value(
            summary_response,
            ("balance", "available_balance", "total_balance"),
        )

    return {
        "balance": balance,
        "balance_asset": asset.get("coin") or asset.get("coin_canonical"),
        "balance_asset_logo": asset.get("logo"),
        "payout_minimum": (
            find_value(
                asset,
                (
                    "payout_minimum",
                    "minimum_payout",
                    "min_payout",
                    "min_amount",
                    "minimum_amount",
                    "payout_threshold",
                ),
            )
            or PAYOUT_MINIMUMS.get(
                str(asset.get("coin") or asset.get("coin_canonical") or "").upper()
            )
        ),
        "amount_mined": asset.get("amount_mined"),
        "amount_referral": asset.get("amount_referral"),
        "reward_ratio": asset.get("reward_ratio"),
        "reward_algorithm": asset.get("reward_algorithm"),
        "algorithm_count": summary_raw.get("algorithm_count"),
        "algorithm_hashrates": summary_raw.get("hr", {}),
        "total_paid": find_value(
            summary_response,
            ("total_paid", "paid", "totalPaid"),
        ),
    }


def calculate_payout_progress(balance, minimum):
    try:
        current = Decimal(str(balance))
        target = Decimal(str(minimum))
    except (InvalidOperation, TypeError, ValueError):
        return {
            "balance": None,
            "minimum": None,
            "remaining": None,
            "percent": None,
            "ready": False,
        }

    if not current.is_finite() or not target.is_finite() or target <= 0:
        return {
            "balance": float(current),
            "minimum": None,
            "remaining": None,
            "percent": None,
            "ready": False,
        }

    percent = min(Decimal("100"), max(Decimal("0"), current / target * 100))
    remaining = max(Decimal("0"), target - current)
    return {
        "balance": float(current),
        "minimum": float(target),
        "remaining": float(remaining),
        "percent": float(percent),
        "ready": current >= target,
    }


def payout_was_completed(previous_balance, current_balance, previous_progress):
    if not previous_progress.get("ready"):
        return False

    try:
        return Decimal(str(current_balance)) < Decimal(str(previous_balance))
    except (InvalidOperation, TypeError, ValueError):
        return False


def balance_increase(previous, current):
    if previous is None or current is None:
        return None

    try:
        increase = Decimal(str(current)) - Decimal(str(previous))
    except (InvalidOperation, TypeError, ValueError):
        return None

    if increase <= 0:
        return None

    return format(increase.normalize(), "f")


def estimate_earnings(balance, asset, observed_at=None):
    try:
        current_balance = Decimal(str(balance))
    except (InvalidOperation, TypeError, ValueError):
        return {"hour": None, "day": None, "month": None, "year": None}

    observed_at = time.time() if observed_at is None else observed_at

    if earnings_tracker["started_at"] is None or earnings_tracker["asset"] != asset:
        earnings_tracker.update({
            "asset": asset,
            "started_at": observed_at,
            "last_balance": current_balance,
            "earned": Decimal("0"),
        })
        return {"hour": None, "day": None, "month": None, "year": None}

    increase = current_balance - earnings_tracker["last_balance"]
    if increase > 0:
        earnings_tracker["earned"] += increase

    earnings_tracker["last_balance"] = current_balance
    elapsed = Decimal(str(observed_at - earnings_tracker["started_at"]))
    earned = earnings_tracker["earned"]

    if elapsed <= 0 or earned <= 0:
        return {"hour": None, "day": None, "month": None, "year": None}

    hourly = earned * Decimal("3600") / elapsed
    daily = hourly * Decimal("24")

    return {
        "hour": float(hourly),
        "day": float(daily),
        "month": float(daily * Decimal("30")),
        "year": float(daily * Decimal("365")),
    }


# ============================================================
# HISTORY
# ============================================================

def add_history(hashrate, balance=None):
    timestamp = int(time.time() * 1000)

    with state_lock:

        history["hashrate"].append({
            "time": timestamp,
            "value": float(hashrate),
        })

        if len(history["hashrate"]) > HISTORY_LIMIT:
            del history["hashrate"][:-HISTORY_LIMIT]

        if balance is not None:
            try:
                balance_value = float(balance)

                history["balance"].append({
                    "time": timestamp,
                    "value": balance_value,
                })

                if len(history["balance"]) > HISTORY_LIMIT:
                    del history["balance"][:-HISTORY_LIMIT]

            except (TypeError, ValueError):
                pass


# ============================================================
# SSE
# ============================================================

def broadcast(payload):
    message = f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"

    with clients_lock:
        current_clients = list(clients)

    for client_queue in current_clients:
        try:
            client_queue.put_nowait(message)
        except Exception:
            pass


@app.route("/api/events")
def sse_events():

    client_queue = queue.Queue()

    with clients_lock:
        clients.add(client_queue)

    def generate():
        try:

            # Initial connection event
            yield (
                "data: "
                + json.dumps({
                    "event": "connected",
                    "data": {
                        "time": now_iso()
                    }
                })
                + "\n\n"
            )

            while True:
                try:
                    message = client_queue.get(
                        timeout=25
                    )

                    yield message

                except queue.Empty:
                    # SSE keepalive
                    yield ": keepalive\n\n"

        finally:
            with clients_lock:
                clients.discard(client_queue)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ============================================================
# POLLER
# ============================================================

def poll_unmineable():

    add_event(
        "system",
        "Unmineable monitor started"
    )

    while True:

        try:

            # ------------------------------------------------
            # ME
            # ------------------------------------------------

            me_response = get_me()

            account = (
                me_response.get("data")
                if isinstance(me_response, dict)
                else {}
            )

            # ------------------------------------------------
            # WORKERS
            # ------------------------------------------------

            workers_response = get_workers()

            workers_data = (
                workers_response.get("data", {})
                if isinstance(workers_response, dict)
                else {}
            )

            raw_workers = workers_data.get(
                "workers",
                []
            )

            workers = [
                normalize_worker(worker)
                for worker in raw_workers
            ]

            online_workers = sum(
                1
                for worker in workers
                if worker["online"]
            )

            offline_workers = (
                len(workers) - online_workers
            )

            total_hashrate = calculate_hashrate(
                workers
            )

            # ------------------------------------------------
            # OPTIONAL DASHBOARD DATA
            # ------------------------------------------------

            summary_response = {}
            assets_response = {}

            try:
                summary_response = get_dashboard_summary()
            except Exception:
                pass

            try:
                assets_response = get_dashboard_assets()
            except Exception:
                pass

            miner_metrics = extract_miner_metrics(
                summary_response,
                assets_response,
            )

            # ------------------------------------------------
            # UPDATE STATE
            # ------------------------------------------------

            previous_state = get_state()
            for key in (
                "balance",
                "balance_asset",
                "balance_asset_logo",
                "amount_mined",
                "amount_referral",
                "reward_ratio",
                "reward_algorithm",
                "algorithm_count",
                "total_paid",
            ):
                if miner_metrics[key] is None:
                    miner_metrics[key] = previous_state.get(key)

            if (
                miner_metrics["payout_minimum"] is None
                and miner_metrics["balance_asset"] == previous_state["balance_asset"]
            ):
                miner_metrics["payout_minimum"] = previous_state["payout"]["minimum"]

            if not miner_metrics["algorithm_hashrates"]:
                miner_metrics["algorithm_hashrates"] = previous_state[
                    "algorithm_hashrates"
                ]

            balance = miner_metrics["balance"]
            balance_asset = miner_metrics["balance_asset"]
            balance_asset_logo = miner_metrics["balance_asset_logo"]
            total_paid = miner_metrics["total_paid"]
            payout = calculate_payout_progress(
                balance,
                miner_metrics["payout_minimum"],
            )
            previous_payout = previous_state.get("payout", {})
            payout_completed = (
                previous_state["balance_asset"] == balance_asset
                and payout_was_completed(
                    previous_state["balance"],
                    balance,
                    previous_payout,
                )
            )
            earnings = (
                estimate_earnings(balance, balance_asset)
                if balance is not None
                else previous_state["earnings"]
            )

            set_state(
                connected=True,
                last_update=now_iso(),
                error=None,

                account=account,

                workers=workers,

                worker_count=len(workers),
                online_workers=online_workers,
                offline_workers=offline_workers,

                hashrate=total_hashrate,

                balance=balance,
                balance_asset=balance_asset,
                balance_asset_logo=balance_asset_logo,
                payout=payout,
                amount_mined=miner_metrics["amount_mined"],
                amount_referral=miner_metrics["amount_referral"],
                reward_ratio=miner_metrics["reward_ratio"],
                reward_algorithm=miner_metrics["reward_algorithm"],
                algorithm_count=miner_metrics["algorithm_count"],
                algorithm_hashrates=miner_metrics["algorithm_hashrates"],
                earnings=earnings,
                total_paid=total_paid,
            )

            if payout_completed:
                add_event(
                    "payout",
                    f"Payout confirmed for {balance_asset or 'coin'}; progress reset",
                    {
                        "coin": balance_asset,
                        "coin_logo": balance_asset_logo,
                        "previous_balance": previous_state["balance"],
                        "balance": balance,
                        "minimum": previous_payout.get("minimum"),
                    },
                )

            if (
                previous_state["balance"] is not None
                and previous_state["balance_asset"] == balance_asset
            ):
                increase = balance_increase(
                    previous_state["balance"],
                    balance,
                )
                if increase is not None:
                    add_event(
                        "balance",
                        f"Your balance has increased by {increase} {balance_asset or 'coin'}",
                        {
                            "amount": increase,
                            "coin": balance_asset,
                            "coin_logo": balance_asset_logo,
                        },
                    )

            if previous_state["last_update"] is not None:
                for event_type, message, data in detect_worker_events(
                    previous_state["workers"],
                    workers,
                ):
                    add_event(event_type, message, data)

            add_history(
                total_hashrate,
                balance
            )

            # ------------------------------------------------
            # BROADCAST UPDATE
            # ------------------------------------------------

            payload = {
                "connected": True,
                "last_update": now_iso(),

                "workers": workers,

                "worker_count": len(workers),
                "online_workers": online_workers,
                "offline_workers": offline_workers,

                "hashrate": total_hashrate,

                "balance": balance,
                "balance_asset": balance_asset,
                "balance_asset_logo": balance_asset_logo,
                "payout": payout,
                "amount_mined": miner_metrics["amount_mined"],
                "amount_referral": miner_metrics["amount_referral"],
                "reward_ratio": miner_metrics["reward_ratio"],
                "reward_algorithm": miner_metrics["reward_algorithm"],
                "algorithm_count": miner_metrics["algorithm_count"],
                "algorithm_hashrates": miner_metrics["algorithm_hashrates"],
                "earnings": earnings,
                "total_paid": total_paid,
            }

            broadcast({
                "event": "update",
                "data": payload,
            })

            # ------------------------------------------------
            # EVENT
            # ------------------------------------------------

            add_event(
                "update",
                (
                    f"{online_workers}/{len(workers)} workers online "
                    f"• {total_hashrate:.2f} H/s"
                ),
                {
                    "hashrate": total_hashrate,
                    "workers": len(workers),
                    "online": online_workers,
                    "offline": offline_workers,
                }
            )

        except Exception as exc:

            error_message = str(exc)

            set_state(
                connected=False,
                error=error_message,
                last_update=now_iso(),
            )

            broadcast({
                "event": "error",
                "data": {
                    "time": now_iso(),
                    "message": error_message,
                },
            })

            add_event(
                "error",
                error_message
            )

        time.sleep(POLL_INTERVAL)


# ============================================================
# HTTP ROUTES
# ============================================================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():

    with state_lock:

        return jsonify({
            "success": True,

            "connected": state["connected"],
            "last_update": state["last_update"],
            "error": state["error"],

            "worker_count": state["worker_count"],
            "online_workers": state["online_workers"],
            "offline_workers": state["offline_workers"],

            "hashrate": state["hashrate"],

            "balance": state["balance"],
            "balance_asset": state["balance_asset"],
            "balance_asset_logo": state["balance_asset_logo"],
            "payout": state["payout"],
            "amount_mined": state["amount_mined"],
            "amount_referral": state["amount_referral"],
            "reward_ratio": state["reward_ratio"],
            "reward_algorithm": state["reward_algorithm"],
            "algorithm_count": state["algorithm_count"],
            "algorithm_hashrates": state["algorithm_hashrates"],
            "earnings": state["earnings"],
            "total_paid": state["total_paid"],

            "account": state["account"],
        })


@app.route("/api/workers")
def api_workers():

    with state_lock:
        return jsonify({
            "success": True,
            "workers": state["workers"],
            "count": state["worker_count"],
            "online": state["online_workers"],
            "offline": state["offline_workers"],
        })


@app.route("/api/history")
def api_history():

    with state_lock:
        return jsonify({
            "success": True,
            "hashrate": history["hashrate"],
            "balance": history["balance"],
        })


@app.route("/api/logs")
def api_logs():

    with state_lock:
        return jsonify({
            "success": True,
            "events": list(events),
        })


@app.route("/api/account")
def api_account():

    try:

        response = get_me()

        return jsonify(response)

    except Exception as exc:

        return jsonify({
            "success": False,
            "error": str(exc),
        }), 500


@app.route("/api/test")
def api_test():

    result = {
        "api": False,
        "workers": False,
        "error": None,
    }

    try:

        get_me()
        result["api"] = True

        get_workers()
        result["workers"] = True

        result["success"] = True

    except Exception as exc:

        result["success"] = False
        result["error"] = str(exc)

    return jsonify(result)


# ============================================================
# STARTUP
# ============================================================

def validate_config():

    missing = []

    if not API_KEY:
        missing.append("UNMINEABLE_API_KEY")

    if not API_SECRET:
        missing.append("UNMINEABLE_API_SECRET")

    if missing:
        raise RuntimeError(
            "Missing environment variables: "
            + ", ".join(missing)
        )


def start_background_thread():

    thread = threading.Thread(
        target=poll_unmineable,
        daemon=True,
        name="unmineable-poller",
    )

    thread.start()

    return thread


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    validate_config()

    print("=" * 60)
    print(" Unmineable Monitor")
    print("=" * 60)
    print(f" API       : {API_BASE}")
    print(f" Poll      : {POLL_INTERVAL}s")
    print(f" History   : {HISTORY_LIMIT}")
    print(" Events    : unlimited")
    print("=" * 60)

    start_background_thread()

    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
        debug=False,
        threaded=True,
    )