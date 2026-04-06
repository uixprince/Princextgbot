import json
import logging
import os
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict

import requests

PORT = int(os.getenv("PORT", "10000"))
REQUEST_TIMEOUT = float(os.getenv("RUNNER_REQUEST_TIMEOUT", "75"))
ERROR_BACKOFF = float(os.getenv("RUNNER_ERROR_BACKOFF", "3"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
COMMON_AUTH_TOKEN = os.getenv("BACKEND_AUTH_TOKEN", "").strip()

RUNNER_CONFIGS = [
    {
        "name": "telegram-poll",
        "url_env": "TELEGRAM_POLL_FUNCTION_URL",
        "gap_env": "TELEGRAM_POLL_LOOP_GAP",
        "default_gap": 0.15,
        "body": {},
    },
    {
        "name": "logo-worker",
        "url_env": "LOGO_WORKER_FUNCTION_URL",
        "gap_env": "LOGO_WORKER_LOOP_GAP",
        "default_gap": 1.0,
        "body": {},
    },
]

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)
LOGGER = logging.getLogger("render-bot-runner")
STOP_EVENT = threading.Event()
STATE_LOCK = threading.Lock()
STARTED_AT = time.time()
RUNNER_STATE: Dict[str, Dict[str, Any]] = {}


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def set_runner_state(name: str, **updates: Any) -> None:
    with STATE_LOCK:
        current = RUNNER_STATE.get(name, {"name": name})
        current.update(updates)
        RUNNER_STATE[name] = current


def get_state_snapshot() -> Dict[str, Any]:
    with STATE_LOCK:
        runners = {name: dict(data) for name, data in RUNNER_STATE.items()}
    return {
        "ok": True,
        "uptime_seconds": round(time.time() - STARTED_AT, 2),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(STARTED_AT)),
        "runners": runners,
    }


def build_headers(token: str) -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
        headers["apikey"] = token
    return headers


def parse_json_safe(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return {"raw": response.text}


def run_loop(config: Dict[str, Any]) -> None:
    name = config["name"]
    url = os.getenv(config["url_env"], "").strip()
    gap = float(os.getenv(config["gap_env"], str(config["default_gap"])))
    token = os.getenv(f"{name.upper().replace('-', '_')}_AUTH_TOKEN", "").strip() or COMMON_AUTH_TOKEN

    if not url:
        LOGGER.warning("%s disabled because %s is not set", name, config["url_env"])
        set_runner_state(
            name,
            enabled=False,
            status="disabled",
            reason=f"Missing {config['url_env']}",
            updated_at=now_iso(),
        )
        return

    set_runner_state(
        name,
        enabled=True,
        status="starting",
        url=url,
        updated_at=now_iso(),
        calls=0,
        failures=0,
    )

    session = requests.Session()
    session.headers.update(build_headers(token))

    while not STOP_EVENT.is_set():

        # 🔥 FIX
        if "example.com" in url:
            LOGGER.warning("%s ignored because dummy URL used", name)
            time.sleep(5)
            continue

        started = time.time()
        try:
            response = session.post(url, json=config["body"], timeout=REQUEST_TIMEOUT)
            duration_ms = round((time.time() - started) * 1000, 2)
            payload = parse_json_safe(response)
            calls = int(RUNNER_STATE.get(name, {}).get("calls", 0)) + 1
            failures = int(RUNNER_STATE.get(name, {}).get("failures", 0))

            if response.ok:
                set_runner_state(
                    name,
                    enabled=True,
                    status="healthy",
                    http_status=response.status_code,
                    last_duration_ms=duration_ms,
                    last_response=payload,
                    last_success_at=now_iso(),
                    updated_at=now_iso(),
                    calls=calls,
                    failures=failures,
                )
                LOGGER.info("%s ok | status=%s | %.2fms", name, response.status_code, duration_ms)
                if STOP_EVENT.wait(gap):
                    break
            else:
                failures += 1
                set_runner_state(
                    name,
                    enabled=True,
                    status="degraded",
                    http_status=response.status_code,
                    last_duration_ms=duration_ms,
                    last_response=payload,
                    last_error=f"HTTP {response.status_code}",
                    updated_at=now_iso(),
                    calls=calls,
                    failures=failures,
                )
                LOGGER.error("%s failed | status=%s | body=%s", name, response.status_code, json.dumps(payload, ensure_ascii=False)[:1200])
                if STOP_EVENT.wait(max(gap, ERROR_BACKOFF)):
                    break
        except requests.RequestException as exc:
            failures = int(RUNNER_STATE.get(name, {}).get("failures", 0)) + 1
            calls = int(RUNNER_STATE.get(name, {}).get("calls", 0)) + 1
            set_runner_state(
                name,
                enabled=True,
                status="degraded",
                last_error=str(exc),
                updated_at=now_iso(),
                calls=calls,
                failures=failures,
            )
            LOGGER.exception("%s request error", name)
            if STOP_EVENT.wait(max(gap, ERROR_BACKOFF)):
                break

    set_runner_state(name, updated_at=now_iso(), stopped=True, status="stopped")
    session.close()


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        payload = get_state_snapshot()
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, format: str, *args: Any) -> None:
        LOGGER.info("http | " + format, *args)


def main() -> None:
    threads = []
    for config in RUNNER_CONFIGS:
        thread = threading.Thread(target=run_loop, args=(config,), daemon=True, name=f"runner-{config['name']}")
        thread.start()
        threads.append(thread)

    server = ThreadingHTTPServer(("0.0.0.0", PORT), HealthHandler)
    LOGGER.info("Health server listening on port %s", PORT)

    def shutdown_handler(signum: int, _frame: Any) -> None:
        LOGGER.info("Received signal %s, shutting down", signum)
        STOP_EVENT.set()
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    try:
        server.serve_forever()
    finally:
        STOP_EVENT.set()
        server.server_close()
        for thread in threads:
            thread.join(timeout=5)
        LOGGER.info("Exited cleanly")


if __name__ == "__main__":
    main()
