#!/usr/bin/env python3
"""
AME Bot Monitor — Watches the trading bot and reports via Telegram.
Uses Ollama (Qwen 2.5) for smart commentary, keeps it lightweight.

Usage:
  python3 monitor.py

Reads TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from ../.env
State files expected at ../state/status.json and ../state/opportunities.json
"""

import os
import sys
import json
import time
import signal
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta

# ── Paths ──
SCRIPT_DIR = Path(__file__).parent
BOT_DIR = SCRIPT_DIR.parent if SCRIPT_DIR.name == "monitor" else SCRIPT_DIR
STATE_DIR = BOT_DIR / "state"
ENV_FILE = BOT_DIR / ".env"
LOG_DIR = BOT_DIR / "logs"

STATUS_FILE = STATE_DIR / "status.json"
OPP_FILE = STATE_DIR / "opportunities.json"
TRADES_FILE = STATE_DIR / "trades.jsonl"

# ── Config ──
POLL_INTERVAL = 30          # seconds between state checks
STALE_THRESHOLD = 120       # seconds before we consider bot dead
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "qwen2.5:32b"
OLLAMA_TIMEOUT = 30         # seconds
LLM_COOLDOWN = 300          # min seconds between LLM calls (save resources)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [MON] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("monitor")


# ═══════════════════════════════════════════════════════════════════
# ENV LOADER
# ═══════════════════════════════════════════════════════════════════

def load_env(path: Path) -> dict:
    """Parse a .env file into a dict, ignoring comments and blank lines."""
    env = {}
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


# ═══════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════

class TelegramBot:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base = f"https://api.telegram.org/bot{token}"
        self.last_update_id = 0
        # We use urllib to avoid extra deps
        import urllib.request
        self._urlopen = urllib.request.urlopen
        self._Request = urllib.request.Request

    def send(self, text: str, parse_mode: str = "HTML"):
        """Send a message to the configured chat."""
        import urllib.parse
        url = f"{self.base}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode,
        }).encode()
        try:
            req = self._Request(url, data=data)
            self._urlopen(req, timeout=10)
            log.info(f"Telegram: sent {len(text)} chars")
        except Exception as e:
            log.error(f"Telegram send failed: {e}")

    def get_updates(self) -> list:
        """Poll for new messages (commands)."""
        url = f"{self.base}/getUpdates?offset={self.last_update_id + 1}&timeout=1"
        try:
            resp = self._urlopen(url, timeout=5)
            data = json.loads(resp.read())
            if data.get("ok"):
                updates = data.get("result", [])
                if updates:
                    self.last_update_id = max(u["update_id"] for u in updates)
                return updates
        except Exception:
            pass
        return []


# ═══════════════════════════════════════════════════════════════════
# OLLAMA (lightweight, on-demand)
# ═══════════════════════════════════════════════════════════════════

class LLMClient:
    def __init__(self):
        self.last_call = 0
        import urllib.request
        self._urlopen = urllib.request.urlopen
        self._Request = urllib.request.Request

    def can_call(self) -> bool:
        return time.time() - self.last_call > LLM_COOLDOWN

    def ask(self, prompt: str, max_tokens: int = 300) -> str:
        """Call Ollama and return the response text. Returns fallback on failure."""
        if not self.can_call():
            return "(LLM cooling down — try again in a few minutes)"

        self.last_call = time.time()
        payload = json.dumps({
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {
                "num_predict": max_tokens,
                "temperature": 0.4,
            }
        }).encode()

        try:
            req = self._Request(OLLAMA_URL, data=payload)
            req.add_header("Content-Type", "application/json")
            resp = self._urlopen(req, timeout=OLLAMA_TIMEOUT)
            data = json.loads(resp.read())
            return data.get("response", "No response from model").strip()
        except Exception as e:
            log.error(f"Ollama call failed: {e}")
            return f"(LLM unavailable: {e})"


# ═══════════════════════════════════════════════════════════════════
# STATE READER
# ═══════════════════════════════════════════════════════════════════

def read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def fmt_dollars(cents: int) -> str:
    d = cents / 100
    sign = "+" if d >= 0 else ""
    return f"{sign}${abs(d):.2f}"


def fmt_uptime(secs: int) -> str:
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m {secs % 60}s"
    h = secs // 3600
    m = (secs % 3600) // 60
    return f"{h}h {m}m"


def file_age_secs(path: Path) -> float:
    try:
        return time.time() - path.stat().st_mtime
    except Exception:
        return 9999


# ═══════════════════════════════════════════════════════════════════
# MONITOR CORE
# ═══════════════════════════════════════════════════════════════════

class Monitor:
    def __init__(self, tg: TelegramBot, llm: LLMClient):
        self.tg = tg
        self.llm = llm
        self.prev_status = None
        self.prev_opps = None
        self.bot_was_offline = False
        self.last_heartbeat = time.time()
        self.started_at = datetime.now(timezone.utc)
        self.daily_summary_sent = False

    def run(self):
        log.info("Monitor started")
        self.tg.send("🟢 <b>AME Monitor online</b>\nWatching bot state, will alert on changes.")

        while True:
            try:
                self.check_state()
                self.handle_commands()
                self.check_daily_summary()
            except KeyboardInterrupt:
                log.info("Shutting down")
                self.tg.send("🔴 <b>AME Monitor stopped</b>")
                break
            except Exception as e:
                log.error(f"Loop error: {e}")

            time.sleep(POLL_INTERVAL)

    def check_state(self):
        status = read_json(STATUS_FILE)
        opps = read_json(OPP_FILE)
        age = file_age_secs(STATUS_FILE)

        if age > STALE_THRESHOLD:
            if not self.bot_was_offline:
                self.bot_was_offline = True
                self.tg.send(
                    f"🔴 <b>BOT OFFLINE</b>\n"
                    f"No state update in {int(age)}s.\n"
                    f"Last seen: {status.get('last_update', 'unknown') if status else 'unknown'}"
                )
            return

        if self.bot_was_offline:
            self.bot_was_offline = False
            self.tg.send("🟢 <b>Bot back online!</b>")

        if not status:
            return

        if self.prev_status:
            for key, label in [("kalshi_ws", "Kalshi WS"), ("poly_ws", "Poly WS"), ("circuit_breaker", "Circuit Breaker")]:
                old = self.prev_status.get(key)
                new = status.get(key)
                if old != new:
                    emoji = "🟢" if new in ("connected", "ok") else "🔴"
                    self.tg.send(f"{emoji} <b>{label}</b>: {old} → {new}")

        if opps and self.prev_opps:
            old_opps = self.prev_opps.get("opportunities", [])
            new_opps = opps.get("opportunities", [])
            if len(new_opps) > len(old_opps):
                for opp in new_opps:
                    desc = opp.get("description", opp.get("pair_id", "Unknown"))
                    profit = opp.get("expected_profit_cents", "?")
                    self.tg.send(
                        f"🎯 <b>ARB DETECTED</b>\n"
                        f"{desc}\n"
                        f"Expected profit: {fmt_dollars(profit) if isinstance(profit, int) else profit}"
                    )

        if self.prev_status:
            old_both = self.prev_status.get("markets_with_both", 0)
            new_both = status.get("markets_with_both", 0)
            if abs(new_both - old_both) >= 20:
                self.tg.send(
                    f"📊 Coverage shift: {old_both} → {new_both} dual-feed markets"
                )

        self.prev_status = status
        self.prev_opps = opps

    def handle_commands(self):
        updates = self.tg.get_updates()
        for update in updates:
            msg = update.get("message", {})
            text = msg.get("text", "").strip().lower()
            chat_id = str(msg.get("chat", {}).get("id", ""))

            if chat_id != self.tg.chat_id:
                continue

            if text == "/status" or text == "/s":
                self.cmd_status()
            elif text == "/opps" or text == "/o":
                self.cmd_opps()
            elif text == "/analysis" or text == "/a":
                self.cmd_analysis()
            elif text == "/help" or text == "/h":
                self.cmd_help()
            elif text.startswith("/ask "):
                self.cmd_ask(text[5:])

    def cmd_status(self):
        status = read_json(STATUS_FILE)
        if not status:
            self.tg.send("❌ No status file found")
            return

        age = file_age_secs(STATUS_FILE)
        fresh = "🟢" if age < STALE_THRESHOLD else "🔴"

        self.tg.send(
            f"{fresh} <b>AME Bot Status</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Mode: <code>{status.get('mode', '?')}</code>\n"
            f"Uptime: <code>{fmt_uptime(status.get('uptime_secs', 0))}</code>\n"
            f"Daily P&L: <code>{fmt_dollars(status.get('daily_pnl_cents', 0))}</code>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Kalshi WS: {status.get('kalshi_ws', '?')}\n"
            f"Poly WS: {status.get('poly_ws', '?')}\n"
            f"Circuit Breaker: {status.get('circuit_breaker', '?')}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Markets: {status.get('markets_tracked', 0)} tracked\n"
            f"Kalshi prices: {status.get('markets_with_kalshi_prices', 0)}\n"
            f"Poly prices: {status.get('markets_with_poly_prices', 0)}\n"
            f"Both feeds: {status.get('markets_with_both', 0)}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"State age: {int(age)}s"
        )

    def cmd_opps(self):
        opps = read_json(OPP_FILE)
        if not opps:
            self.tg.send("❌ No opportunities file found")
            return

        arbs = opps.get("opportunities", [])
        spreads = opps.get("wide_spreads", [])

        if not arbs and not spreads:
            self.tg.send("📊 No arb opportunities or wide spreads right now.")
            return

        lines = []
        if arbs:
            lines.append(f"🎯 <b>{len(arbs)} Arb Opportunities</b>")
            for o in arbs[:5]:
                desc = o.get("description", o.get("pair_id", "?"))
                profit = o.get("expected_profit_cents")
                p_str = fmt_dollars(profit) if isinstance(profit, int) else "?"
                lines.append(f"  • {desc}: {p_str}")

        if spreads:
            lines.append(f"\n📊 <b>{len(spreads)} Wide Spreads</b>")
            for s in spreads[:5]:
                desc = s.get("description", s.get("pair_id", "?"))
                spread = s.get("spread_cents", "?")
                lines.append(f"  • {desc}: {spread}¢")

        self.tg.send("\n".join(lines))

    def cmd_analysis(self):
        status = read_json(STATUS_FILE)
        opps = read_json(OPP_FILE)

        if not status:
            self.tg.send("❌ No data to analyze")
            return

        self.tg.send("🤖 Analyzing... (this takes a few seconds)")

        prompt = f"""You are a concise trading assistant monitoring a prediction market arbitrage bot.

Current state:
- Mode: {status.get('mode')}
- Uptime: {fmt_uptime(status.get('uptime_secs', 0))}
- Daily P&L: {fmt_dollars(status.get('daily_pnl_cents', 0))}
- Kalshi WS: {status.get('kalshi_ws')}
- Poly WS: {status.get('poly_ws')}
- Circuit breaker: {status.get('circuit_breaker')}
- Markets tracked: {status.get('markets_tracked', 0)}
- Dgal-feed markets: {status.get('markets_with_both', 0)}
- Arb threshold: 97 cents (3% edge required)
- Opportunities right now: {len(opps.get('opportunities', [])) if opps else 0}
- Wide spreads: {len(opps.get('wide_spreads', [])) if opps else 0}

The bot has $110 on Polymarket US. It trades sports prediction markets (NBA, NHL, EPL, etc.) looking for cross-platform arbitrage between Kalshi and Polymarket.

Give a brief 3-4 sentence analysis: Is the bot healthy? What should the operator expect? Any concerns? Be direct and practical."""

        response = self.llm.ask(prompt, max_tokens=250)
        self.tg.send(f"🤖 <b>Analysis</b>\n\n{response}")

    def cmd_ask(self, question: str):
        status = read_json(STATUS_FILE)
        context = json.dumps(status, indent=2) if status else "No status data available"

        prompt = f"""You are a concise trading assistant. The user is asking about their prediction market arbitrage bot.

Bot state:
{context}

User question: {question}

Answer briefly and practically in 2-3 sentences."""

        self.tg.send("🤖 Thinking...")
        response = self.llm.ask(prompt, max_tokens=200)
        self.tg.send(f"🤖 {response}")

    def cmd_help(self):
        self.tg.send(
            "🤖 <b>AME Monitor Commands</b>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "/status — Bot status overview\n"
            "/opps — Current opportunities\n"
            "/analysis — AI analysis of bot state\n"
            "/ask [question] — Ask anything about the bot\n"
            "/help — This message\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "Auto-alerts: connection changes, new arbs, bot offline"
        )

    def check_daily_summary(self):
        now = datetime.now(timezone.utc)
        if now.hour == 0 and not self.daily_summary_sent:
            self.daily_summary_sent = True
            status = read_json(STATUS_FILE)
            if status:
                self.tg.send(
                    f"📅 <b>Daily Summary</b>\n"
                    f"P&L: {fmt_dollars(status.get('daily_pnl_cents', 0))}\n"
                    f"Uptime: {fmt_uptime(status.get('uptime_secs', 0))}\n"
                    f"Markets: {status.get('markets_with_both', 0)} dual-feed"
                )
        elif now.hour != 0:
            self.daily_summary_sent = False


def main():
    env = load_env(ENV_FILE)
    token = env.get("TELEGRAM_BOT_TOKEN") or os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = env.get("TELEGRAM_CHAT_ID") or os.environ.get("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print(f"ERROR: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID required")
        print(f"Looked in: {ENV_FILE}")
        sys.exit(1)

    log.info(f"Bot dir: {BOT_DIR}")
    log.info(f"State dir: {STATE_DIR}")
    log.info(f"Telegram chat: {chat_id}")

    tg = TelegramBot(token, chat_id)
    llm = LLMClient()
    monitor = Monitor(tg, llm)

    def handle_signal(sig, frame):
        log.info("Signal received, stopping...")
        tg.send("🔴 <b>AME Monitor stopped</b>")
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    monitor.run()


if __name__ == "__main__":
    main()
