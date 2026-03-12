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
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::Utc;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::circuit_breaker::CircuitBreaker;
use crate::config::{app_mode, arb_threshold, wide_spread_threshold, AppMode};
use crate::kalshi::KalshiApiClient;
use crate::polymarket_us::PolymarketUsClient;
use crate::position_tracker::SharedPositionTracker;
use crate::types::{GlobalState, fxhash_str};

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
    arb_threshold_cents: u16,
    poly_us_balance_cents: Option<i64>,
    kalshi_balance_cents: Option<i64>,
    version: &'static str,
    app_env: String,
}

// === Position JSON ===

#[derive(Serialize)]
struct PositionJson {
    market_id: String,
    pair_id: String,
    description: String,
    arb_type: &'static str,
    entry_cost_cents: i64,
    contracts: f64,
    opened_at: String,
}

#[derive(Serialize)]
struct PositionsJson {
    updated: String,
    open_count: usize,
    positions: Vec<PositionJson>,
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

/// Sentinel value meaning "balance not yet fetched".
const BALANCE_UNKNOWN: i64 = i64::MIN;

/// How often to poll wallet balances (seconds).
const BALANCE_POLL_SECS: u64 = 60;

/// Writes Henrik state files and reads config overrides.
pub struct StateWriter {
    state: Arc<GlobalState>,
    overrides: Arc<RwLock<ConfigOverrides>>,
    circuit_breaker: Arc<CircuitBreaker>,
    poly_us: Option<Arc<PolymarketUsClient>>,
    kalshi: Option<Arc<KalshiApiClient>>,
    position_tracker: Option<SharedPositionTracker>,
    start_time: Instant,
    /// ARB_THRESHOLD converted to cents (e.g. 0.97 → 97), captured once at startup.
    arb_threshold_cents: u16,
    /// Whether the Kalshi WebSocket is believed to be connected.
    pub kalshi_connected: Arc<std::sync::atomic::AtomicBool>,
    /// Whether the Poly WebSocket is believed to be connected.
    pub poly_connected: Arc<std::sync::atomic::AtomicBool>,
    /// Cached Polymarket US cash balance in cents (BALANCE_UNKNOWN if not yet fetched).
    poly_us_balance_cents: AtomicI64,
    /// Cached Kalshi cash balance in cents (BALANCE_UNKNOWN if not yet fetched).
    kalshi_balance_cents: AtomicI64,
    /// Unix-second timestamp of last successful balance fetch.
    last_balance_ts: AtomicU64,
}

impl StateWriter {
    pub fn new(
        state: Arc<GlobalState>,
        overrides: Arc<RwLock<ConfigOverrides>>,
        circuit_breaker: Arc<CircuitBreaker>,
        poly_us: Option<Arc<PolymarketUsClient>>,
        kalshi: Option<Arc<KalshiApiClient>>,
        position_tracker: Option<SharedPositionTracker>,
    ) -> Self {
        let arb_threshold_cents = (arb_threshold() * 100.0).round() as u16;
        Self {
            state,
            overrides,
            circuit_breaker,
            poly_us,
            kalshi,
            position_tracker,
            start_time: Instant::now(),
            arb_threshold_cents,
            kalshi_connected: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            poly_connected: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            poly_us_balance_cents: AtomicI64::new(BALANCE_UNKNOWN),
            kalshi_balance_cents: AtomicI64::new(BALANCE_UNKNOWN),
            last_balance_ts: AtomicU64::new(0),
        }
    }

    /// Kalshi balance in cents, or None if not yet fetched.
    pub fn kalshi_balance_cents_opt(&self) -> Option<i64> {
        let v = self.kalshi_balance_cents.load(Ordering::Relaxed);
        if v == BALANCE_UNKNOWN { None } else { Some(v) }
    }

    /// Polymarket US balance in cents, or None if not yet fetched.
    pub fn poly_balance_cents_opt(&self) -> Option<i64> {
        let v = self.poly_us_balance_cents.load(Ordering::Relaxed);
        if v == BALANCE_UNKNOWN { None } else { Some(v) }
    }

    /// Seconds elapsed since this StateWriter was created (process uptime proxy).
    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
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

/// Poll both wallet balances and cache the results in the writer's atomics.
async fn refresh_balances(writer: &StateWriter) {
    if let Some(ref poly_us) = writer.poly_us {
        match poly_us.get_cash_balance_cents().await {
            Ok(cents) => {
                writer.poly_us_balance_cents.store(cents, Ordering::Relaxed);
                info!("[STATE] Polymarket US balance: ${:.2}", cents as f64 / 100.0);
            }
            Err(e) => warn!("[STATE] Failed to fetch Polymarket US balance: {}", e),
        }
    }

    if let Some(ref kalshi) = writer.kalshi {
        match kalshi.get_balance().await {
            Ok(resp) => {
                writer.kalshi_balance_cents.store(resp.balance, Ordering::Relaxed);
                info!("[STATE] Kalshi balance: ${:.2}", resp.balance as f64 / 100.0);
            }
            Err(e) => warn!("[STATE] Failed to fetch Kalshi balance: {}", e),
        }
    }

    let now_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    writer.last_balance_ts.store(now_ts, Ordering::Relaxed);
}

async fn write_all(writer: &StateWriter) {
    // Poll balances on startup (last_ts == 0) and every BALANCE_POLL_SECS thereafter.
    let now_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let last_ts = writer.last_balance_ts.load(Ordering::Relaxed);
    if now_ts.saturating_sub(last_ts) >= BALANCE_POLL_SECS {
        refresh_balances(writer).await;
    }

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

    let poly_bal = writer.poly_us_balance_cents.load(Ordering::Relaxed);
    let kalshi_bal = writer.kalshi_balance_cents.load(Ordering::Relaxed);

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
        daily_pnl_cents: (cb_status.daily_pnl * 100.0).round() as i64,
        arb_threshold_cents: writer.arb_threshold_cents,
        poly_us_balance_cents: if poly_bal == BALANCE_UNKNOWN { None } else { Some(poly_bal) },
        kalshi_balance_cents: if kalshi_bal == BALANCE_UNKNOWN { None } else { Some(kalshi_bal) },
        version: env!("CARGO_PKG_VERSION"),
        app_env: std::env::var("APP_ENV").unwrap_or_else(|_| "demo".into()),
    };

    write_json("state/status.json", &status).await;

    let opps_json = OpportunitiesJson {
        updated: now.clone(),
        opportunities: arb_opps,
        wide_spreads,
    };
    write_json("state/opportunities.json", &opps_json).await;

    write_positions(writer, &now).await;

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

/// Write open positions to state/positions.json.
async fn write_positions(writer: &StateWriter, now: &str) {
    let Some(ref tracker_lock) = writer.position_tracker else { return; };
    let tracker = tracker_lock.read().await;

    let positions: Vec<PositionJson> = tracker
        .open_positions()
        .into_iter()
        .map(|pos| {
            let hash = fxhash_str(&pos.market_id);
            let pair_id = writer
                .state
                .kalshi_to_id
                .get(&hash)
                .and_then(|&id| writer.state.markets.get(id as usize))
                .and_then(|m| m.pair.as_ref())
                .map(|p| p.pair_id.to_string())
                .unwrap_or_else(|| pos.market_id.clone());

            let arb_type = if pos.kalshi_yes.contracts > 0.0 && pos.poly_no.contracts > 0.0 {
                "KalshiYesPolyNo"
            } else if pos.poly_yes.contracts > 0.0 && pos.kalshi_no.contracts > 0.0 {
                "PolyYesKalshiNo"
            } else if pos.kalshi_yes.contracts > 0.0 && pos.kalshi_no.contracts > 0.0 {
                "KalshiSamePlatform"
            } else {
                "Unknown"
            };

            PositionJson {
                market_id: pos.market_id.clone(),
                pair_id,
                description: pos.description.clone(),
                arb_type,
                entry_cost_cents: (pos.total_cost() * 100.0).round() as i64,
                contracts: pos.matched_contracts(),
                opened_at: pos.opened_at.clone(),
            }
        })
        .collect();

    let open_count = positions.len();
    let json = PositionsJson {
        updated: now.to_string(),
        open_count,
        positions,
    };
    write_json("state/positions.json", &json).await;
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

/// Write a serializable value to a JSON file (atomic via temp file + rename).
async fn write_json<T: Serialize>(path: &str, value: &T) {
    match serde_json::to_string_pretty(value) {
        Ok(json) => {
            let tmp_path = format!("{}.tmp", path);
            if let Err(e) = tokio::fs::write(&tmp_path, &json).await {
                warn!("[STATE] Failed to write {}: {}", tmp_path, e);
                return;
            }
            if let Err(e) = tokio::fs::rename(&tmp_path, path).await {
                warn!("[STATE] Failed to rename {} -> {}: {}", tmp_path, path, e);
                // Fallback: try direct write
                let _ = tokio::fs::write(path, json).await;
            }
        }
        Err(e) => {
            warn!("[STATE] Failed to serialize for {}: {}", path, e);
        }
    }
}
