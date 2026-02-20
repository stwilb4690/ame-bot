# AME Bot — Adaptive Market Engine

A Rust-based prediction market monitoring and arbitrage bot for Kalshi and Polymarket.

Built on a lock-free atomic orderbook architecture for ultra-low-latency price tracking, with
Telegram alerts, Henrik/OpenClaw state file integration, and graceful shutdown.

---

## Modes

| `APP_MODE`    | Kalshi WebSocket | Polymarket WebSocket | Execution     |
|---------------|:----------------:|:--------------------:|:-------------:|
| `kalshi-only` | ✅               | ❌                  | ❌            |
| `monitor`     | ✅               | ✅ (read-only)       | ❌ **(default)** |
| `full`        | ✅               | ✅                   | ✅ cross-platform arbs |

**monitor** is the default — both WebSocket feeds run, cross-platform spreads are logged and
alerted, but no orders are placed. Switch to `full` once you've verified pricing looks right.

---

## Quick Start

### Prerequisites

- Rust 1.75+ (`curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`)
- Kalshi API key (RSA, generated in the Kalshi dashboard)
- Optional: Telegram bot token + chat ID

### 1. Clone and build

```bash
git clone https://github.com/your-fork/ame-bot
cd ame-bot
cargo build --release
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env with your credentials
```

Minimum required env vars:
```
KALSHI_API_KEY_ID=your-key-id
KALSHI_PRIVATE_KEY_PATH=/path/to/kalshi_private_key.pem
APP_ENV=prod        # or demo for sandbox testing
APP_MODE=monitor    # start read-only
DRY_RUN=1
```

### 3. Run

```bash
# Standard run
cargo run --release

# Diagnostic mode (60s connectivity check, then exits)
cargo run --release -- --diagnose

# With verbose logging
RUST_LOG=ame_bot=debug cargo run --release
```

---

## Environment Variables

### Core

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_MODE` | `monitor` | `kalshi-only` \| `monitor` \| `full` |
| `APP_ENV` | `demo` | `demo` \| `prod` |
| `DRY_RUN` | `1` | Set `0` to place real orders |
| `KALSHI_API_HOST` | *(auto)* | Override Kalshi API hostname |

### Kalshi (required)

| Variable | Description |
|----------|-------------|
| `KALSHI_API_KEY_ID` | Your Kalshi API key ID |
| `KALSHI_PRIVATE_KEY_PATH` | Path to your RSA private key PEM file |
| `KALSHI_FEE_RATE` | Fee rate (default `0.07`; set `0.0` for zero-fee) |

### Polymarket (required only for `full` mode)

| Variable | Description |
|----------|-------------|
| `POLY_PRIVATE_KEY` | Ethereum private key (0x-prefixed) |
| `POLY_FUNDER` | Your Polygon wallet address |

### Discovery

| Variable | Default | Description |
|----------|---------|-------------|
| `FORCE_DISCOVERY` | `0` | Set `1` to ignore cache and re-discover |
| `ENABLED_LEAGUES` | *(all)* | Comma-separated league codes to monitor |
| `KALSHI_EXTRA_SERIES` | — | Kalshi-only series (e.g. `KXFED,KXCPI,KXBTC`) |

### Telegram Alerts

| Variable | Default | Description |
|----------|---------|-------------|
| `TELEGRAM_BOT_TOKEN` | — | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | — | Your chat/channel ID |
| `ALERT_COOLDOWN_SECS` | `300` | Min seconds between repeat alerts per market |

Alert types: 🎯 ARB · 📊 WIDE SPREAD · 🚨 CIRCUIT BREAKER · 💓 HEARTBEAT · 🚀 STARTED · 🛑 SHUTDOWN

### Thresholds

| Variable | Default | Description |
|----------|---------|-------------|
| `ARB_THRESHOLD` | `0.995` | Alert when YES+NO total < this × 100¢ |
| `WIDE_SPREAD_THRESHOLD` | `10` | Alert when round-trip premium > this ¢ |

### Circuit Breaker

| Variable | Default | Description |
|----------|---------|-------------|
| `CB_MAX_POSITION_PER_MARKET` | `50000` | Max contracts per market |
| `CB_MAX_TOTAL_POSITION` | `100000` | Max contracts across all markets |
| `CB_MAX_DAILY_LOSS` | `500` | Max daily loss in USD |
| `CB_MAX_CONSECUTIVE_ERRORS` | `5` | Errors before trip |
| `CB_COOLDOWN_SECS` | `300` | Cooldown after trip |
| `CB_ENABLED` | `1` | Set `0` to disable (testing only) |

### State Files / Henrik

| Variable | Default | Description |
|----------|---------|-------------|
| `STATE_WRITE_INTERVAL_SECS` | `10` | How often to refresh `state/*.json` |

### Diagnostic

| Variable | Default | Description |
|----------|---------|-------------|
| `DIAGNOSE` | `0` | Set `1` to run diagnostic mode |
| `DIAGNOSE_SECS` | `60` | Duration before auto-exit in diagnose mode |

---

## Henrik / OpenClaw Integration

The bot writes to `./state/` every 10 seconds (configurable):

### `state/status.json`
Bot health, connection status, market counts, circuit breaker state.

### `state/opportunities.json`
Current Kalshi same-platform arbs and wide spreads — updated live.

### `state/trade_log.md`
Append-only trade log with timestamps.

### `state/config_overrides.json` ← **Henrik writes this**

Henrik can dynamically reconfigure the bot by writing this file:

```json
{
  "paused": false,
  "min_profit_threshold_cents": 2,
  "max_position_per_market": 100,
  "watched_leagues": ["nba", "nhl"],
  "alert_cooldown_secs": 300,
  "dry_run": true
}
```

Changes are applied within `STATE_WRITE_INTERVAL_SECS` seconds (default 10).

---

## Mac Mini M4 Setup

### Install Rust
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
```

### Build
```bash
cd ~/ame-bot
cargo build --release
```

### launchctl Service (auto-start on login)

Create `~/Library/LaunchAgents/com.amebot.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.amebot</string>
  <key>ProgramArguments</key>
  <array>
    <string>/Users/yourname/ame-bot/target/release/ame-bot</string>
  </array>
  <key>WorkingDirectory</key>
  <string>/Users/yourname/ame-bot</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>RUST_LOG</key>
    <string>ame_bot=info</string>
  </dict>
  <key>StandardOutPath</key>
  <string>/Users/yourname/ame-bot/logs/stdout.log</string>
  <key>StandardErrorPath</key>
  <string>/Users/yourname/ame-bot/logs/stderr.log</string>
  <key>KeepAlive</key>
  <true/>
  <key>RunAtLoad</key>
  <true/>
</dict>
</plist>
```

```bash
mkdir -p ~/ame-bot/logs
launchctl load ~/Library/LaunchAgents/com.amebot.plist
launchctl start com.amebot
```

---

## Supported Leagues / Markets

### Sports (cross-platform with Polymarket)
EPL, Bundesliga, La Liga, Serie A, Ligue 1, UCL, UEL, EFL Championship,
NBA, NFL, NHL, MLB, MLS, NCAAF

### Kalshi-only (via `KALSHI_EXTRA_SERIES`)
Fed interest rate decisions (`KXFED`), CPI (`KXCPI`), GDP (`KXGDP`),
Bitcoin price (`KXBTC`), Elections (`KXELECTION`), and others.

---

## Architecture

```
WebSocket (Kalshi)          WebSocket (Polymarket)
      ↓                              ↓
AtomicOrderbook (lock-free CAS)    AtomicOrderbook
      ↓                              ↓
      └──────── check_arbs() ────────┘  (cross-platform)
                check_kalshi_opps()     (same-platform)
                      ↓
             FastExecutionRequest
                      ↓
          ExecutionEngine.process()
           ├─ Circuit breaker check
           ├─ tokio::join!(Kalshi IOC, Poly FAK)
           └─ Position tracker recording

Background tasks (non-hot-path):
  State writer  → state/status.json, opportunities.json every 10s
                   reads state/config_overrides.json
  Opp scanner   → check_kalshi_opps() every 10s → Telegram alerts
  Heartbeat     → log diagnostics every 60s
  Shutdown      → SIGINT/SIGTERM → final state write → Telegram
```

### Source layout
```
src/
├── main.rs              Entry point, mode-based orchestration, graceful shutdown
├── config.rs            Runtime URL functions, AppMode, AppEnv, fee rate
├── types.rs             Lock-free orderbook, arb detection, KalshiOpps, GlobalState
├── execution.rs         Concurrent leg execution, in-flight deduplication
├── position_tracker.rs  Channel-based fill recording, P&L tracking
├── circuit_breaker.rs   Risk limits, error tracking, auto-halt
├── discovery.rs         Kalshi↔Polymarket market matching + Kalshi-only series
├── cache.rs             Team code mappings (EPL, NBA, etc.)
├── kalshi.rs            Kalshi REST/WS client
├── polymarket.rs        Polymarket WS client (read-only, no auth required)
├── polymarket_clob.rs   Polymarket CLOB order execution (full mode only)
├── telegram.rs          Alert client with per-market cooldowns
├── state_writer.rs      Henrik state file writer + config override reader
└── diagnose.rs          Diagnostic table display mode
```

---

## Development

```bash
# Run unit tests (always works, no credentials needed)
cargo test --lib

# Run integration tests
cargo test --features polymarket-exec

# Diagnostic mode (30s, then exits)
DIAGNOSE=1 DIAGNOSE_SECS=30 cargo run

# Force market re-discovery
FORCE_DISCOVERY=1 cargo run

# Inject a fake arb to test execution flow
TEST_ARB=1 DRY_RUN=1 cargo run
```

---

## Obtaining Kalshi API Credentials

1. Log in to [Kalshi](https://kalshi.com)
2. Go to **Settings → API Keys**
3. Create a new API key with trading permissions
4. Download the private key (PEM file)
5. Note the API Key ID

---

## License

MIT
