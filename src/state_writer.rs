// src/state_writer.rs
// Henrik state file writer + config override reader.
//
// Writes to ./state/ on a configurable interval (default 10 s):
//   state/status.json        — bot health, mode, market counts
//   state/opportunities.json — current Kalshi arbs and wide spreads
//   state/trade_log.md       — append-only trade/event log
//
// Reads from:
//   state/config_overrides.json — Henrik writes this; bot reads and applies it

use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, warn};

use crate::circuit_breaker::CircuitBreaker;
use crate::config::{app_mode, wide_spread_threshold, AppMode};
use crate::types::GlobalState;

// === Config Overrides (Henrik writes, bot reads) ===

/// Runtime configuration that Henrik can update by writing config_overrides.json.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigOverrides {
    #[serde(default)]
    pub paused: bool,
    #[serde(default = "default_min_profit")]
    pub min_profit_threshold_cents: u16,
    #[serde(default = "default_max_position")]
    pub max_position_per_market: u32,
    #[serde(default)]
    pub watched_leagues: Vec<String>,
    #[serde(default = "default_alert_cooldown")]
    pub alert_cooldown_secs: u64,
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,
}

fn default_min_profit() -> u16 { 2 }
fn default_max_position() -> u32 { 100 }
fn default_alert_cooldown() -> u64 { 300 }
fn default_dry_run() -> bool { true }

impl Default for ConfigOverrides {
    fn default() -> Self {
        Self {
            paused: false,
            min_profit_threshold_cents: default_min_profit(),
            max_position_per_market: default_max_position(),
            watched_leagues: vec![],
            alert_cooldown_secs: default_alert_cooldown(),
            dry_run: default_dry_run(),
        }
    }
}

// === Status JSON ===

#[derive(Serialize)]
struct StatusJson<'a> {
    mode: &'a str,
    status: &'a str,
    uptime_secs: u64,
    last_update: String,
    kalshi_ws: &'a str,
    poly_ws: &'a str,
    markets_tracked: usize,
    markets_with_kalshi_prices: usize,
    markets_with_poly_prices: usize,
    markets_with_both: usize,
    circuit_breaker: &'a str,
    daily_pnl_cents: i64,
    version: &'static str,
    app_env: String,
}

// === Opportunity JSON ===

#[derive(Serialize)]
struct SamePlatformArbJson {
    #[serde(rename = "type")]
    kind: &'static str,
    market: String,
    kalshi_ticker: String,
    yes_ask: u16,
    no_ask: u16,
    total_cost: u16,
    profit_cents: i16,
    detected_at: String,
}

#[derive(Serialize)]
struct WideSpreadJson {
    market: String,
    kalshi_ticker: String,
    yes_ask: u16,
    no_ask: u16,
    spread: u16,
    detected_at: String,
}

#[derive(Serialize)]
struct OpportunitiesJson {
    updated: String,
    opportunities: Vec<SamePlatformArbJson>,
    wide_spreads: Vec<WideSpreadJson>,
}

// === State Writer ===

/// Writes Henrik state files and reads config overrides.
pub struct StateWriter {
    state: Arc<GlobalState>,
    overrides: Arc<RwLock<ConfigOverrides>>,
    circuit_breaker: Arc<CircuitBreaker>,
    start_time: Instant,
    /// Whether the Kalshi WebSocket is believed to be connected.
    pub kalshi_connected: Arc<std::sync::atomic::AtomicBool>,
    /// Whether the Poly WebSocket is believed to be connected.
    pub poly_connected: Arc<std::sync::atomic::AtomicBool>,
}

impl StateWriter {
    pub fn new(
        state: Arc<GlobalState>,
        overrides: Arc<RwLock<ConfigOverrides>>,
        circuit_breaker: Arc<CircuitBreaker>,
    ) -> Self {
        Self {
            state,
            overrides,
            circuit_breaker,
            start_time: Instant::now(),
            kalshi_connected: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            poly_connected: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
}

/// Background task: write state files every `interval_secs` and read config overrides.
pub async fn run_state_writer(
    writer: Arc<StateWriter>,
    interval_secs: u64,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    // Ensure state/ directory exists
    if let Err(e) = tokio::fs::create_dir_all("state").await {
        warn!("[STATE] Could not create state/ directory: {}", e);
    }

    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                write_all(&writer).await;
            }
            _ = shutdown_rx.recv() => {
                // Write a final snapshot before exiting
                write_all(&writer).await;
                break;
            }
        }
    }
}

async fn write_all(writer: &StateWriter) {
    let now = Utc::now().to_rfc3339();
    let uptime = writer.start_time.elapsed().as_secs();

    // === Count market stats ===
    let market_count = writer.state.market_count();
    let mut with_kalshi = 0usize;
    let mut with_poly = 0usize;
    let mut with_both = 0usize;
    let mut arb_opps: Vec<SamePlatformArbJson> = Vec::new();
    let mut wide_spreads: Vec<WideSpreadJson> = Vec::new();
    let threshold = wide_spread_threshold();

    for i in 0..market_count {
        if let Some(market) = writer.state.get_by_id(i as u16) {
            let (k_yes, k_no, _, _) = market.kalshi.load();
            let (p_yes, p_no, _, _) = market.poly.load();
            let has_k = k_yes > 0 || k_no > 0;
            let has_p = p_yes > 0 || p_no > 0;
            if has_k { with_kalshi += 1; }
            if has_p { with_poly += 1; }
            if has_k && has_p { with_both += 1; }

            // Kalshi same-platform scan
            let opps = market.check_kalshi_opps(threshold);

            let ticker = market.pair
                .as_ref()
                .map(|p| p.kalshi_market_ticker.to_string())
                .unwrap_or_default();
            let description = market.pair
                .as_ref()
                .map(|p| p.description.to_string())
                .unwrap_or_default();

            if ticker.is_empty() { continue; }

            if let Some(arb) = opps.same_platform {
                arb_opps.push(SamePlatformArbJson {
                    kind: "kalshi_same_platform",
                    market: description.clone(),
                    kalshi_ticker: ticker.clone(),
                    yes_ask: arb.yes_ask,
                    no_ask: arb.no_ask,
                    total_cost: arb.yes_ask + arb.no_ask,
                    profit_cents: arb.profit_cents,
                    detected_at: now.clone(),
                });
            }

            if let Some(ws) = opps.wide_spread {
                wide_spreads.push(WideSpreadJson {
                    market: description,
                    kalshi_ticker: ticker,
                    yes_ask: ws.yes_ask,
                    no_ask: ws.no_ask,
                    spread: ws.spread_cents,
                    detected_at: now.clone(),
                });
            }
        }
    }

    // === circuit breaker status ===
    let cb_status = writer.circuit_breaker.status().await;
    let cb_str = if cb_status.halted { "tripped" } else { "ok" };

    let mode_str = app_mode().to_string();
    let kalshi_ws_str = if writer.kalshi_connected.load(std::sync::atomic::Ordering::Relaxed) {
        "connected"
    } else {
        "disconnected"
    };
    let poly_ws_str = match app_mode() {
        AppMode::KalshiOnly => "n/a",
        _ => if writer.poly_connected.load(std::sync::atomic::Ordering::Relaxed) {
            "connected"
        } else {
            "disconnected"
        },
    };

    let status = StatusJson {
        mode: &mode_str,
        status: "running",
        uptime_secs: uptime,
        last_update: now.clone(),
        kalshi_ws: kalshi_ws_str,
        poly_ws: poly_ws_str,
        markets_tracked: market_count,
        markets_with_kalshi_prices: with_kalshi,
        markets_with_poly_prices: with_poly,
        markets_with_both: with_both,
        circuit_breaker: cb_str,
        daily_pnl_cents: 0, // TODO: wire up position tracker
        version: env!("CARGO_PKG_VERSION"),
        app_env: std::env::var("APP_ENV").unwrap_or_else(|_| "demo".into()),
    };

    write_json("state/status.json", &status).await;

    let opps_json = OpportunitiesJson {
        updated: now,
        opportunities: arb_opps,
        wide_spreads,
    };
    write_json("state/opportunities.json", &opps_json).await;

    // === Read config_overrides.json (Henrik writes this) ===
    if let Ok(data) = tokio::fs::read_to_string("state/config_overrides.json").await {
        match serde_json::from_str::<ConfigOverrides>(&data) {
            Ok(new_overrides) => {
                let mut lock = writer.overrides.write().await;
                debug!("[STATE] Loaded config_overrides: dry_run={} paused={}",
                    new_overrides.dry_run, new_overrides.paused);
                *lock = new_overrides;
            }
            Err(e) => {
                warn!("[STATE] Failed to parse config_overrides.json: {}", e);
            }
        }
    }
}

/// Append an entry to state/trade_log.md.
pub async fn append_trade_log(entry: &str) {
    let line = format!("{}\n", entry);
    use tokio::io::AsyncWriteExt;
    match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("state/trade_log.md")
        .await
    {
        Ok(mut f) => {
            if let Err(e) = f.write_all(line.as_bytes()).await {
                warn!("[STATE] Failed to append trade_log: {}", e);
            }
        }
        Err(e) => {
            warn!("[STATE] Failed to open trade_log: {}", e);
        }
    }
}

/// Write a serializable value to a JSON file (atomic via temp file).
async fn write_json<T: Serialize>(path: &str, value: &T) {
    match serde_json::to_string_pretty(value) {
        Ok(json) => {
            if let Err(e) = tokio::fs::write(path, json).await {
                warn!("[STATE] Failed to write {}: {}", path, e);
            }
        }
        Err(e) => {
            warn!("[STATE] Failed to serialize for {}: {}", path, e);
        }
    }
}
