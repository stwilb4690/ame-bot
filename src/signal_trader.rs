// src/signal_trader.rs
//
// Signal Trading Engine — Kalshi-only, using Polymarket international as a
// fair-value oracle.
//
// Strategy:
//   Mode 1 (PreGame): |spread| > effective_threshold → maker limit on Kalshi
//   Mode 2 (Snipe):   |spread| > effective_threshold AND market is live → IOC
//
// Exit conditions:
//   • Convergence: |spread| ≤ SIGNAL_CONVERGENCE_EXIT  → take profit
//   • Stop loss:   |spread| ≥ |entry_spread| × SIGNAL_STOP_LOSS_MULTIPLIER
//   • Time stop:   game date passed / today AND held > SIGNAL_TIME_STOP_MINUTES
//   • Snipe:       30-second hard timeout
//
// Intelligence layer (new):
//   • MarketSubcategory: 6 sub-types with per-category oracle confidence
//   • Effective threshold = base_threshold / oracle_confidence
//   • Spread velocity filter (skip if narrowing > 0.1¢/s)
//   • Signal quality score = spread × freshness × velocity × oracle_conf
//   • Safety filters: min_entry, max_spread, kalshi_yes range
//   • Data collection: SpreadObservation, SignalEvent, TradeOutcome (JSONL)

use anyhow::Result;
use chrono::{NaiveDate, Utc, Datelike, Timelike};
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::circuit_breaker::CircuitBreaker;
use crate::data_collector::{DataCollector, SignalEvent, SpreadObservation, TradeOutcome};
use crate::kalshi::KalshiApiClient;
use crate::risk_manager::{EntryDecision, RiskManager, RiskSummary};
use crate::telegram::TelegramClient;
use crate::types::{GlobalState, MarketCategory, PriceCents, kalshi_fee_cents, kalshi_maker_fee_cents};

// ── Configuration ─────────────────────────────────────────────────────────────

pub struct SignalConfig {
    pub pregame_threshold_cents:         i16,
    pub live_threshold_cents:            i16,
    /// Threshold for crypto markets (applies in pregame mode, i.e. outside live hours)
    pub crypto_threshold_cents:          i16,
    /// Threshold for economic / macro markets
    pub economic_threshold_cents:        i16,
    /// Threshold for 15-minute short-duration crypto markets (default 4¢)
    pub crypto_short15m_threshold_cents: i16,
    /// Threshold for hourly short-duration crypto markets (default 5¢)
    pub crypto_short_hourly_threshold_cents: i16,
    pub convergence_exit_cents:          i16,
    pub stop_loss_multiplier:            f64,
    pub time_stop_minutes:               u64,
    pub freshness_window_secs:           u64,
    pub dry_run:                         bool,
    /// Number of warmup trades per category before full sizing kicks in
    pub warmup_trades:                   u32,
}

impl SignalConfig {
    pub fn from_env() -> Self {
        let dry_run = std::env::var("SIGNAL_DRY_RUN")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(true); // Safe default: dry-run

        Self {
            pregame_threshold_cents:             env_u64("SIGNAL_PREGAME_THRESHOLD",       8) as i16,
            live_threshold_cents:                env_u64("SIGNAL_LIVE_THRESHOLD",          10) as i16,
            crypto_threshold_cents:              env_u64("SIGNAL_CRYPTO_THRESHOLD",         6) as i16,
            economic_threshold_cents:            env_u64("SIGNAL_ECONOMIC_THRESHOLD",       5) as i16,
            crypto_short15m_threshold_cents:     env_u64("SIGNAL_CRYPTO_SHORT15M_THRESHOLD", 4) as i16,
            crypto_short_hourly_threshold_cents: env_u64("SIGNAL_CRYPTO_HOURLY_THRESHOLD",   5) as i16,
            convergence_exit_cents:              env_u64("SIGNAL_CONVERGENCE_EXIT",          3) as i16,
            stop_loss_multiplier:                env_f64("SIGNAL_STOP_LOSS_MULTIPLIER",      2.0),
            time_stop_minutes:                   env_u64("SIGNAL_TIME_STOP_MINUTES",        15),
            freshness_window_secs:               env_u64("SIGNAL_FRESHNESS_SECS",           30),
            warmup_trades:                       env_u64("SIGNAL_WARMUP_TRADES",            10) as u32,
            dry_run,
        }
    }
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

// ── Market Subcategory ────────────────────────────────────────────────────────

/// Fine-grained market classification for oracle confidence routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum MarketSubcategory {
    SportsLive,
    SportsPregame,
    CryptoVolatile,
    CryptoStable,
    /// Short-duration 15-minute crypto up/down binary (highest oracle confidence)
    CryptoShort15m,
    /// Short-duration hourly crypto up/down binary
    CryptoShortHourly,
    EconomicEvent,
    EconomicQuiet,
}

impl std::fmt::Display for MarketSubcategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MarketSubcategory::SportsLive       => write!(f, "sports_live"),
            MarketSubcategory::SportsPregame    => write!(f, "sports_pre"),
            MarketSubcategory::CryptoVolatile   => write!(f, "crypto_vol"),
            MarketSubcategory::CryptoStable     => write!(f, "crypto_stable"),
            MarketSubcategory::CryptoShort15m   => write!(f, "crypto_short_15m"),
            MarketSubcategory::CryptoShortHourly => write!(f, "crypto_short_1h"),
            MarketSubcategory::EconomicEvent    => write!(f, "eco_event"),
            MarketSubcategory::EconomicQuiet    => write!(f, "eco_quiet"),
        }
    }
}

// ── Oracle Config ─────────────────────────────────────────────────────────────

/// Polymarket reliability (0.1–1.0) per subcategory.
///
/// High confidence → smaller effective_threshold (fires more easily).
/// Low confidence  → larger effective_threshold (harder to fire).
pub struct OracleConfig {
    pub sports_pregame:      f64,  // 0.3 — Poly is unreliable for pregame sports
    pub sports_live:         f64,  // 0.5
    pub crypto_volatile:     f64,  // 0.9 — Poly is very reliable for volatile crypto
    pub crypto_stable:       f64,  // 0.4
    /// 15-minute crypto: Poly crypto-native traders reprice faster → highest confidence
    pub crypto_short15m:     f64,  // 0.95
    /// Hourly crypto: high confidence, slightly lower than 15m
    pub crypto_short_hourly: f64,  // 0.85
    pub economic_event:      f64,  // 0.9 — Poly is very reliable for economic events
    pub economic_quiet:      f64,  // 0.3
}

impl OracleConfig {
    pub fn from_env() -> Self {
        Self {
            sports_pregame:      env_f64("ORACLE_CONF_SPORTS_PREGAME",   0.3),
            sports_live:         env_f64("ORACLE_CONF_SPORTS_LIVE",      0.5),
            crypto_volatile:     env_f64("ORACLE_CONF_CRYPTO_VOLATILE",  0.9),
            crypto_stable:       env_f64("ORACLE_CONF_CRYPTO_STABLE",    0.4),
            crypto_short15m:     env_f64("ORACLE_CONF_CRYPTO_SHORT15M",  0.95),
            crypto_short_hourly: env_f64("ORACLE_CONF_CRYPTO_SHORT_1H",  0.85),
            economic_event:      env_f64("ORACLE_CONF_ECONOMIC_EVENT",   0.9),
            economic_quiet:      env_f64("ORACLE_CONF_ECONOMIC_QUIET",   0.3),
        }
    }

    pub fn confidence(&self, sub: MarketSubcategory) -> f64 {
        match sub {
            MarketSubcategory::SportsPregame    => self.sports_pregame,
            MarketSubcategory::SportsLive       => self.sports_live,
            MarketSubcategory::CryptoVolatile   => self.crypto_volatile,
            MarketSubcategory::CryptoStable     => self.crypto_stable,
            MarketSubcategory::CryptoShort15m   => self.crypto_short15m,
            MarketSubcategory::CryptoShortHourly => self.crypto_short_hourly,
            MarketSubcategory::EconomicEvent    => self.economic_event,
            MarketSubcategory::EconomicQuiet    => self.economic_quiet,
        }
    }
}

// ── Safety Config ─────────────────────────────────────────────────────────────

/// Hard safety filters applied before quality scoring.
pub struct SafetyConfig {
    /// Skip if entry_price < this (prevents buying 1-2¢ contracts)
    pub min_entry_cents:              i16,
    /// Skip if |spread| > this (structural mispricing, not arb)
    pub max_spread_cents:             i16,
    /// Skip if kalshi_yes < this
    pub min_kalshi_yes:               u16,
    /// Skip if kalshi_yes > this
    pub max_kalshi_yes:               u16,
    /// Skip if computed signal quality < this
    pub min_quality:                  f64,
    /// Poly price change > N¢ in 60 min → CryptoVolatile
    pub crypto_volatile_delta_cents:  u16,
    /// Minutes before/after 14:00 UTC = EconomicEvent window
    pub economic_event_window_mins:   u32,
}

impl SafetyConfig {
    pub fn from_env() -> Self {
        Self {
            min_entry_cents:             env_u64("SIGNAL_MIN_ENTRY_CENTS",      5) as i16,
            max_spread_cents:            env_u64("SIGNAL_MAX_SPREAD_CENTS",    35) as i16,
            min_kalshi_yes:              env_u64("SIGNAL_MIN_KALSHI_YES",       3) as u16,
            max_kalshi_yes:              env_u64("SIGNAL_MAX_KALSHI_YES",      97) as u16,
            min_quality:                 env_f64("SIGNAL_MIN_QUALITY",         0.3),
            crypto_volatile_delta_cents: env_u64("SIGNAL_CRYPTO_VOLATILE_DELTA", 5) as u16,
            economic_event_window_mins:  env_u64("SIGNAL_ECONOMIC_EVENT_WINDOW", 30) as u32,
        }
    }
}

// ── Signal Quality ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
struct SignalQuality {
    spread_score:    f64,
    freshness_score: f64,
    velocity_score:  f64,
    oracle_conf:     f64,
    total:           f64,
}

// ── Enums ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TradeDirection {
    BuyYes,
    BuyNo,
}

impl std::fmt::Display for TradeDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TradeDirection::BuyYes => write!(f, "buy_yes"),
            TradeDirection::BuyNo  => write!(f, "buy_no"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum TradeMode {
    PreGame,
    Snipe,
}

impl std::fmt::Display for TradeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TradeMode::PreGame => write!(f, "pregame"),
            TradeMode::Snipe   => write!(f, "snipe"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitReason {
    Convergence,
    TimeStop,
    StopLoss,
    Manual,
}

impl std::fmt::Display for ExitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExitReason::Convergence => write!(f, "convergence"),
            ExitReason::TimeStop    => write!(f, "time_stop"),
            ExitReason::StopLoss    => write!(f, "stop_loss"),
            ExitReason::Manual      => write!(f, "manual"),
        }
    }
}

// ── Open Position ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SignalPosition {
    pub market_id:          u16,
    pub kalshi_ticker:      String,
    /// Kalshi event ticker (correlation group key for RiskManager).
    pub event_key:          String,
    pub market_desc:        String,
    pub direction:          TradeDirection,
    pub mode:               TradeMode,
    pub contracts:          i64,
    /// Price paid per contract at entry (in cents)
    pub entry_price_cents:  i16,
    /// Signed spread at entry: kalshi_yes - poly_yes (in cents)
    pub entry_spread_cents: i16,
    /// Poly YES ask at entry (for logging)
    pub entry_poly_yes:     PriceCents,
    /// Max possible loss for this position (dollars), recorded from RiskManager
    pub position_var:       f64,
    pub entry_ts:           Instant,
    pub entry_timestamp:    String,
    pub kalshi_order_id:    Option<String>,
    /// true = resting limit (maker), false = IOC (taker)
    pub entry_is_maker:     bool,
    /// Unix timestamp of market expiry (from MarketPair.expiry_ts — None for daily/weekly markets)
    pub expiry_ts:          Option<i64>,
    // ── Data collection fields ─────────────────────────────────────────────
    pub subcategory:         MarketSubcategory,
    pub oracle_conf:         f64,
    pub signal_quality:      f64,
    pub velocity_at_entry:   f64,
    pub freshness_at_entry:  u64,
    pub minutes_to_event:    Option<i64>,
    pub hour_utc_at_entry:   u32,
}

impl SignalPosition {
    /// Check whether this position should be exited.
    fn check_exit(
        &self,
        current_spread: i16,
        config: &SignalConfig,
        game_date: Option<NaiveDate>,
    ) -> Option<ExitReason> {
        let abs_spread = current_spread.unsigned_abs();
        let abs_entry  = self.entry_spread_cents.unsigned_abs();

        let is_short_duration = matches!(
            self.subcategory,
            MarketSubcategory::CryptoShort15m | MarketSubcategory::CryptoShortHourly
        );

        // 1. Convergence: spread has narrowed to exit threshold.
        //    Short-duration markets use tighter 2¢ exit.
        let convergence_threshold = if is_short_duration { 2u16 } else { config.convergence_exit_cents as u16 };
        if abs_spread <= convergence_threshold {
            return Some(ExitReason::Convergence);
        }

        // 2. Stop loss: spread has widened beyond multiplier × entry.
        //    Short-duration: 1.5× (tighter, these move fast)
        let stop_multiplier = if is_short_duration { 1.5 } else { config.stop_loss_multiplier };
        let stop_threshold = (abs_entry as f64 * stop_multiplier).round() as u16;
        if abs_spread >= stop_threshold {
            return Some(ExitReason::StopLoss);
        }

        // 3. Short-duration expiry: exit N minutes before market expires.
        if is_short_duration {
            if let Some(exp_ts) = self.expiry_ts {
                let now_unix = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                let secs_to_expiry = exp_ts - now_unix;
                let exit_secs = match self.subcategory {
                    MarketSubcategory::CryptoShort15m    => 2 * 60,  // 2 min before
                    MarketSubcategory::CryptoShortHourly => 5 * 60,  // 5 min before
                    _ => 0,
                };
                if secs_to_expiry <= exit_secs {
                    return Some(ExitReason::TimeStop);
                }
            }
            // Hard fallback: snipe timeout
            if self.entry_ts.elapsed().as_secs() >= 30 {
                return Some(ExitReason::TimeStop);
            }
            return None; // Short-duration exits handled above
        }

        // 4. Time stop for pre-game positions
        if self.mode == TradeMode::PreGame {
            if let Some(gd) = game_date {
                let today = Utc::now().date_naive();
                if gd < today {
                    return Some(ExitReason::TimeStop);
                }
                if gd == today {
                    let mins_held = self.entry_ts.elapsed().as_secs() / 60;
                    if mins_held >= config.time_stop_minutes {
                        return Some(ExitReason::TimeStop);
                    }
                }
            }
        }

        // 5. Snipe mode: hard 30-second timeout
        if self.mode == TradeMode::Snipe && self.entry_ts.elapsed().as_secs() >= 30 {
            return Some(ExitReason::TimeStop);
        }

        None
    }
}

// ── Trade Record (append-only log) ───────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct TradeRecord {
    pub entry_timestamp:    String,
    pub market_name:        String,
    pub kalshi_ticker:      String,
    pub direction:          String,
    pub kalshi_price:       i16,
    pub poly_price:         i16,
    pub spread_at_entry:    i16,
    pub mode:               String,
    pub order_type:         String,
    pub entry_cost_dollars: f64,
    pub contracts:          i64,
    pub position_var:       f64,
    pub kalshi_order_id:    Option<String>,
    pub exit_timestamp:     Option<String>,
    pub exit_price_cents:   Option<i16>,
    pub exit_spread_cents:  Option<i16>,
    pub exit_reason:        Option<String>,
    pub pnl_cents:          Option<f64>,
    pub pnl_dollars:        Option<f64>,
    pub fees_paid_cents:    Option<f64>,
}

// ── Price Freshness Tracking ──────────────────────────────────────────────────

struct PriceSnapshot {
    kalshi_yes: PriceCents,
    kalshi_no:  PriceCents,
    poly_yes:   PriceCents,
    poly_no:    PriceCents,
    kalshi_changed_at: Instant,
    poly_changed_at:   Instant,
    /// Number of distinct Kalshi price changes observed since market was first seen
    kalshi_update_count: u32,
    /// Number of distinct Poly price changes observed since market was first seen
    poly_update_count: u32,
}

impl PriceSnapshot {
    fn new(ky: PriceCents, kn: PriceCents, py: PriceCents, pn: PriceCents) -> Self {
        let now = Instant::now();
        Self {
            kalshi_yes: ky, kalshi_no: kn,
            poly_yes:   py, poly_no:   pn,
            kalshi_changed_at: now,
            poly_changed_at:   now,
            // Start at 0; first distinct change will bring each to 1
            kalshi_update_count: 0,
            poly_update_count:   0,
        }
    }

    fn update(&mut self, ky: PriceCents, kn: PriceCents, py: PriceCents, pn: PriceCents) {
        let now = Instant::now();
        // Always update prices; track whether they changed for freshness.
        if ky != self.kalshi_yes || kn != self.kalshi_no {
            self.kalshi_yes = ky;
            self.kalshi_no  = kn;
            self.kalshi_changed_at = now;
        }
        // Count every receipt of a live Kalshi price (non-zero = feed is alive).
        if ky > 0 || kn > 0 {
            self.kalshi_update_count = self.kalshi_update_count.saturating_add(1);
        }

        if py != self.poly_yes || pn != self.poly_no {
            self.poly_yes = py;
            self.poly_no  = pn;
            self.poly_changed_at = now;
        }
        // Count every receipt of a live Poly price (non-zero = feed is alive).
        if py > 0 || pn > 0 {
            self.poly_update_count = self.poly_update_count.saturating_add(1);
        }
    }

    fn is_fresh(&self, window_secs: u64) -> bool {
        let t = std::time::Duration::from_secs(window_secs);
        self.kalshi_changed_at.elapsed() < t || self.poly_changed_at.elapsed() < t
    }

    /// Returns true once both feeds have delivered at least 2 price updates (any price,
    /// not necessarily changing), confirming the websocket is live for this market.
    fn is_warmed_up(&self) -> bool {
        self.kalshi_update_count >= 2 && self.poly_update_count >= 2
    }
}

// ── Per-Category Statistics ───────────────────────────────────────────────────

/// Running P&L and trade counts for a single market category.
#[derive(Debug, Default, Clone, Serialize)]
pub struct CategoryStats {
    /// Total completed trades (entries, not exits, for warmup counting)
    pub trades_entered: u32,
    /// Profitable exits
    pub wins: u32,
    /// Cumulative net P&L in dollars
    pub pnl_dollars: f64,
    /// Sum of signal quality scores for averaging
    pub total_quality: f64,
}

// ── Activity Feed ─────────────────────────────────────────────────────────────

/// One event in the live activity log (max 100 kept in a ring).
#[derive(Debug, Clone, Serialize)]
pub struct ActivityEvent {
    pub ts:          String,      // UTC ISO-8601
    pub event_type:  String,      // "entry" | "exit" | "skip"
    pub market:      String,      // kalshi_ticker
    pub category:    String,
    pub subcategory: String,
    pub details:     String,      // human-readable summary
    pub pnl:         Option<f64>, // filled on exit
}

// ── Signal Counters ────────────────────────────────────────────────────────────

/// Funnel counters tracking how many signals pass each gate.
#[derive(Debug, Default, Clone, Serialize)]
pub struct SignalCounters {
    pub signals_evaluated: u64,
    pub skipped_safety:    u64,
    pub skipped_quality:   u64,
    pub skipped_velocity:  u64,
    pub skipped_risk:      u64,
    pub entered:           u64,
    pub trades_today:      u64,
    pub wins_today:        u64,
    pub losses_today:      u64,
}

// ── Signal Candidate (collected during scan) ──────────────────────────────────

struct SignalCandidate {
    market_id:     u16,
    /// Kalshi event ticker (for risk manager correlation grouping).
    event_key:     String,
    category:      MarketCategory,
    subcategory:   MarketSubcategory,
    oracle_conf:   f64,
    eff_threshold: i16,
    direction:     TradeDirection,
    mode:          TradeMode,
    spread:        i16,
    kalshi_yes:    PriceCents,
    poly_yes:      PriceCents,
    quality:       SignalQuality,
    velocity:      f64,
    freshness_secs: u64,
    ticker:        String,
    desc:          String,
}

// ── Signal Trader ─────────────────────────────────────────────────────────────

pub struct SignalTrader {
    config:           SignalConfig,
    oracle_config:    OracleConfig,
    safety_config:    SafetyConfig,
    state:            Arc<GlobalState>,
    kalshi_api:       Arc<KalshiApiClient>,
    telegram:         Option<Arc<TelegramClient>>,
    circuit_breaker:  Arc<CircuitBreaker>,
    risk_manager:     Arc<RiskManager>,
    pub data_collector: Arc<DataCollector>,
    positions:        Mutex<Vec<SignalPosition>>,
    snapshots:        Mutex<HashMap<u16, PriceSnapshot>>,
    /// Event dates extracted from market pairs at startup (for exit timing)
    game_dates:       HashMap<u16, NaiveDate>,
    /// Market category looked up by market_id (immutable after construction)
    market_categories: HashMap<u16, MarketCategory>,
    /// Per-category running statistics (trades entered + P&L)
    category_stats:   Mutex<HashMap<MarketCategory, CategoryStats>>,
    /// 60-min Poly price ring (1-min granularity): market_id → VecDeque<(Instant, PriceCents)>
    poly_price_history: Mutex<HashMap<u16, VecDeque<(Instant, PriceCents)>>>,
    /// 30-entry spread ring (1/sec): market_id → VecDeque<(Instant, i16)>
    spread_history:     Mutex<HashMap<u16, VecDeque<(Instant, i16)>>>,
    /// When spread first crossed effective_threshold: market_id → Instant
    threshold_crossed_at: Mutex<HashMap<u16, Instant>>,
    /// Bot startup time (for uptime reporting)
    start_time: Instant,
    /// Bug 1: Per-ticker cooldown after a snipe no-fill (skip for 60s)
    fill_cooldowns: Mutex<HashMap<String, Instant>>,
    /// True when Kalshi WebSocket is connected
    ws_kalshi_connected: Arc<AtomicBool>,
    /// True when Polymarket WebSocket is connected
    ws_poly_connected: Arc<AtomicBool>,
    /// Ring of last 100 activity events for the dashboard
    activity: Mutex<VecDeque<ActivityEvent>>,
    /// Signal funnel counters
    signal_counters: Mutex<SignalCounters>,
}

impl SignalTrader {
    pub fn new(
        config:               SignalConfig,
        state:                Arc<GlobalState>,
        kalshi_api:           Arc<KalshiApiClient>,
        telegram:             Option<Arc<TelegramClient>>,
        circuit_breaker:      Arc<CircuitBreaker>,
        risk_manager:         Arc<RiskManager>,
        data_collector:       Arc<DataCollector>,
        ws_kalshi_connected:  Arc<AtomicBool>,
        ws_poly_connected:    Arc<AtomicBool>,
    ) -> Self {
        let mut game_dates = HashMap::new();
        let mut market_categories = HashMap::new();

        for i in 0..state.market_count() {
            if let Some(market) = state.get_by_id(i as u16) {
                if let Some(pair) = &market.pair {
                    if let Some(d) = extract_event_date(&pair.kalshi_event_ticker) {
                        game_dates.insert(i as u16, d);
                    }
                    market_categories.insert(i as u16, pair.category);
                }
            }
        }
        info!("[SIGNAL] {} event dates extracted from discovery", game_dates.len());

        // Pre-populate category stats so heartbeat always shows all categories
        let mut cat_stats: HashMap<MarketCategory, CategoryStats> = HashMap::new();
        for &cat in &[MarketCategory::Sports, MarketCategory::Crypto, MarketCategory::Economic] {
            cat_stats.insert(cat, CategoryStats::default());
        }

        Self {
            oracle_config:    OracleConfig::from_env(),
            safety_config:    SafetyConfig::from_env(),
            config,
            state,
            kalshi_api,
            telegram,
            circuit_breaker,
            risk_manager,
            data_collector,
            positions:            Mutex::new(Vec::new()),
            snapshots:            Mutex::new(HashMap::new()),
            game_dates,
            market_categories,
            category_stats:       Mutex::new(cat_stats),
            poly_price_history:   Mutex::new(HashMap::new()),
            spread_history:       Mutex::new(HashMap::new()),
            threshold_crossed_at: Mutex::new(HashMap::new()),
            start_time:           Instant::now(),
            fill_cooldowns:       Mutex::new(HashMap::new()),
            ws_kalshi_connected,
            ws_poly_connected,
            activity:             Mutex::new(VecDeque::new()),
            signal_counters:      Mutex::new(SignalCounters::default()),
        }
    }

    /// One iteration of the main loop. Call every ~1 second.
    pub async fn scan(&self) {
        self.scan_signals().await;
        self.check_exits().await;
    }

    // ── Activity Ring ─────────────────────────────────────────────────────────

    async fn push_activity(&self, ev: ActivityEvent) {
        let mut q = self.activity.lock().await;
        if q.len() >= 100 { q.pop_front(); }
        q.push_back(ev);
    }

    // ── State File Output (every 5s) ──────────────────────────────────────────

    /// Write the 4 state JSON files to the `state/` directory.
    /// Called every 5 seconds from the binary's timer task.
    pub async fn write_state_files(&self) {
        let ts = Utc::now().to_rfc3339();
        let uptime_secs = self.start_time.elapsed().as_secs();

        // Snapshot all Mutex state synchronously (no await while holding)
        let positions_snap = self.positions.lock().await.clone();
        let counters_snap  = self.signal_counters.lock().await.clone();
        let cat_stats_snap = self.category_stats.lock().await.clone();
        let activity_snap: Vec<ActivityEvent> = self.activity.lock().await.iter().cloned().collect();

        let ws_kalshi = self.ws_kalshi_connected.load(Ordering::Relaxed);
        let ws_poly   = self.ws_poly_connected.load(Ordering::Relaxed);

        // Risk summary — async call outside lock scope
        let risk = self.risk_manager.summary().await;

        // ── signal_status.json ────────────────────────────────────────────────
        let cat_stats_json: serde_json::Map<String, serde_json::Value> = cat_stats_snap
            .iter()
            .map(|(cat, s)| {
                let avg_quality = if s.trades_entered > 0 {
                    s.total_quality / s.trades_entered as f64
                } else { 0.0 };
                let win_pct = if s.trades_entered > 0 {
                    s.wins as f64 / s.trades_entered as f64 * 100.0
                } else { 0.0 };
                (format!("{}", cat), serde_json::json!({
                    "trades":      s.trades_entered,
                    "wins":        s.wins,
                    "win_pct":     (win_pct * 10.0).round() / 10.0,
                    "pnl":         (s.pnl_dollars * 100.0).round() / 100.0,
                    "avg_quality": (avg_quality * 100.0).round() / 100.0,
                }))
            })
            .collect();

        let status_json = serde_json::json!({
            "ts":           ts.clone(),
            "uptime_secs":  uptime_secs,
            "dry_run":      self.config.dry_run,
            "ws_kalshi":    ws_kalshi,
            "ws_poly":      ws_poly,
            "risk": {
                "balance":          (risk.balance_dollars * 100.0).round() / 100.0,
                "daily_pnl":        (risk.daily_pnl * 100.0).round() / 100.0,
                "high_water_mark":  (risk.high_water_mark * 100.0).round() / 100.0,
                "portfolio_var":    (risk.portfolio_var * 100.0).round() / 100.0,
                "max_portfolio_var":(risk.max_portfolio_var * 100.0).round() / 100.0,
                "mode":             format!("{:?}", risk.risk_mode),
            },
            "counters":       serde_json::to_value(&counters_snap).unwrap_or_default(),
            "category_stats": serde_json::Value::Object(cat_stats_json),
            "open_positions": positions_snap.len(),
        });

        // ── signal_positions.json ─────────────────────────────────────────────
        let mut positions_json_arr = Vec::new();
        for pos in &positions_snap {
            let Some(market) = self.state.get_by_id(pos.market_id) else { continue };
            let (ky, _, _, _) = market.kalshi.load();
            let (py, _, _, _) = market.poly.load();
            let current_spread = if ky > 0 && py > 0 { ky as i16 - py as i16 } else { 0i16 };

            let exit_price = match pos.direction {
                TradeDirection::BuyYes => ky as i16,
                TradeDirection::BuyNo  => 100 - ky as i16,
            };
            let gross_pnl = (exit_price - pos.entry_price_cents) as f64 * pos.contracts as f64;
            let unrealized_pnl = (gross_pnl / 100.0 * 100.0).round() / 100.0;

            positions_json_arr.push(serde_json::json!({
                "market":            pos.kalshi_ticker,
                "direction":         pos.direction.to_string(),
                "mode":              pos.mode.to_string(),
                "contracts":         pos.contracts,
                "entry_price_cents": pos.entry_price_cents,
                "entry_spread":      pos.entry_spread_cents,
                "current_spread":    current_spread,
                "unrealized_pnl":    unrealized_pnl,
                "held_secs":         pos.entry_ts.elapsed().as_secs(),
                "quality":           (pos.signal_quality * 100.0).round() / 100.0,
                "oracle_conf":       (pos.oracle_conf * 100.0).round() / 100.0,
                "subcategory":       pos.subcategory.to_string(),
                "entry_ts":          pos.entry_timestamp,
                "position_var":      (pos.position_var * 100.0).round() / 100.0,
            }));
        }

        // ── signal_spreads.json — top 50 by |spread| ─────────────────────────
        let spread_entries = self.compute_spread_snapshot().await;

        // ── signal_activity.json ──────────────────────────────────────────────
        let activity_json = serde_json::to_value(&activity_snap).unwrap_or_default();

        // Write all 4 files (ignore individual errors — best effort)
        let _ = tokio::fs::create_dir_all("state").await;

        if let Ok(s) = serde_json::to_string(&status_json) {
            let _ = tokio::fs::write("state/signal_status.json", s).await;
        }
        if let Ok(s) = serde_json::to_string(&positions_json_arr) {
            let _ = tokio::fs::write("state/signal_positions.json", s).await;
        }
        if let Ok(s) = serde_json::to_string(&spread_entries) {
            let _ = tokio::fs::write("state/signal_spreads.json", s).await;
        }
        if let Ok(s) = serde_json::to_string(&activity_json) {
            let _ = tokio::fs::write("state/signal_activity.json", s).await;
        }
    }

    /// Compute spread snapshot for state file (top 50 by |spread|).
    async fn compute_spread_snapshot(&self) -> Vec<serde_json::Value> {
        let count   = self.state.market_count();
        let today   = Utc::now().date_naive();
        let now_utc = Utc::now();

        let mut entries: Vec<serde_json::Value> = {
            let poly_hist = self.poly_price_history.lock().await;
            let spd_hist  = self.spread_history.lock().await;
            let snapshots = self.snapshots.lock().await;
            let mut out   = Vec::new();

            for i in 0..count {
                let mid = i as u16;
                let Some(market) = self.state.get_by_id(mid) else { continue };
                let Some(pair)   = &market.pair else { continue };

                let (ky, _, _, _) = market.kalshi.load();
                let (py, _, _, _) = market.poly.load();
                if ky == 0 || py == 0 { continue; }

                let spread = ky as i16 - py as i16;
                let category = self.market_categories.get(&mid).copied().unwrap_or(MarketCategory::Sports);
                let game_date_opt = self.game_dates.get(&mid).copied();
                let (base_threshold_prelim, mode_prelim) = self.pick_threshold_and_mode(category, game_date_opt, today);
                let subcategory = detect_subcategory(
                    category, mode_prelim, now_utc,
                    poly_hist.get(&mid),
                    self.safety_config.crypto_volatile_delta_cents,
                    self.safety_config.economic_event_window_mins,
                    Some(&*pair.kalshi_market_ticker),
                );
                let (base_threshold, _mode) = match subcategory {
                    MarketSubcategory::CryptoShort15m =>
                        (self.config.crypto_short15m_threshold_cents, TradeMode::Snipe),
                    MarketSubcategory::CryptoShortHourly =>
                        (self.config.crypto_short_hourly_threshold_cents, TradeMode::Snipe),
                    _ => (base_threshold_prelim, mode_prelim),
                };
                let oracle_conf   = self.oracle_config.confidence(subcategory);
                let eff_threshold = ((base_threshold as f64 / oracle_conf).round() as i16).max(1);
                let abs_spread    = spread.abs() as i16;

                // Determine would_trade and skip_reason
                let (would_trade, skip_reason) = if ky < self.safety_config.min_kalshi_yes as u16
                    || ky > self.safety_config.max_kalshi_yes as u16
                {
                    (false, format!("safety_kalshi_range ({}¢ out of [{},{}])", ky,
                        self.safety_config.min_kalshi_yes, self.safety_config.max_kalshi_yes))
                } else if abs_spread >= self.safety_config.max_spread_cents {
                    (false, format!("safety_max_spread ({}¢ >= {}¢)", abs_spread, self.safety_config.max_spread_cents))
                } else if abs_spread < eff_threshold {
                    (false, format!("below_threshold ({}¢ < {}¢)", abs_spread, eff_threshold))
                } else {
                    let velocity = spd_hist.get(&mid).map(|r| compute_velocity(r, spread)).unwrap_or(0.0);
                    let fresh = snapshots.get(&mid).map(|s| s.is_fresh(self.config.freshness_window_secs)).unwrap_or(false);
                    let spread_score    = compute_spread_score(abs_spread as u16);
                    let freshness_secs  = 0u64; // approximate for display
                    let freshness_score = compute_freshness_score(freshness_secs);
                    let velocity_score  = compute_velocity_score(velocity);
                    let total           = spread_score * freshness_score * velocity_score * oracle_conf;
                    if !fresh {
                        (false, format!("stale_prices ({}s window)", self.config.freshness_window_secs))
                    } else if velocity < -0.1 {
                        (false, format!("velocity_narrowing ({:.3}¢/s)", velocity))
                    } else if total < self.safety_config.min_quality {
                        (false, format!("quality_too_low ({:.2} < {:.2})", total, self.safety_config.min_quality))
                    } else {
                        (true, String::new())
                    }
                };

                let quality = {
                    let abs_u = abs_spread as u16;
                    let velocity = spd_hist.get(&mid).map(|r| compute_velocity(r, spread)).unwrap_or(0.0);
                    compute_spread_score(abs_u) * compute_freshness_score(0) * compute_velocity_score(velocity) * oracle_conf
                };

                out.push(serde_json::json!({
                    "market_id":    mid,
                    "ticker":       &*pair.kalshi_market_ticker,
                    "spread":       spread,
                    "kalshi_yes":   ky,
                    "poly_yes":     py,
                    "subcategory":  subcategory.to_string(),
                    "eff_threshold":eff_threshold,
                    "would_trade":  would_trade,
                    "skip_reason":  skip_reason,
                    "quality":      (quality * 100.0).round() / 100.0,
                }));
            }
            out
        };

        // Sort by |spread| descending, keep top 50
        entries.sort_by(|a, b| {
            let sa = a["spread"].as_i64().unwrap_or(0).abs();
            let sb = b["spread"].as_i64().unwrap_or(0).abs();
            sb.cmp(&sa)
        });
        entries.truncate(50);
        entries
    }

    // ── Signal Detection ──────────────────────────────────────────────────────

    async fn scan_signals(&self) {
        // Fast early-out: skip entire scan when risk mode is stopped.
        if self.risk_manager.risk_mode().await == crate::risk_manager::RiskMode::Stopped {
            return;
        }

        // Bug 2: skip signal detection when Poly WS is not connected — all poly prices may be stale.
        if !self.ws_poly_connected.load(Ordering::Relaxed) {
            tracing::debug!("[SIGNAL] Poly WS disconnected — skipping signal scan");
            return;
        }

        let count = self.state.market_count();
        let now_utc = Utc::now();
        let today   = now_utc.date_naive();

        // Phase 1: update history rings, freshness snapshots, and collect signal candidates.
        // All state locks held together for synchronous scan — released before async Phase 2.
        // Track counter deltas here; apply after lock block exits.
        let mut delta_evaluated: u64 = 0;
        let mut delta_safety:    u64 = 0;
        let mut delta_velocity:  u64 = 0;
        let mut delta_quality:   u64 = 0;
        let mut skip_activities: Vec<ActivityEvent> = Vec::new();

        let candidates: Vec<SignalCandidate> = {
            let mut snapshots = self.snapshots.lock().await;
            let mut poly_hist = self.poly_price_history.lock().await;
            let mut spd_hist  = self.spread_history.lock().await;
            let mut crossed   = self.threshold_crossed_at.lock().await;
            let mut out       = Vec::new();

            let now = Instant::now();

            for i in 0..count {
                let mid = i as u16;
                let Some(market) = self.state.get_by_id(mid) else { continue };
                let Some(pair)   = &market.pair else { continue };

                let (ky, kn, _, _) = market.kalshi.load();
                let (py, pn, _, _) = market.poly.load();

                if ky == 0 || kn == 0 || py == 0 || pn == 0 { continue; }

                delta_evaluated += 1;

                let spread = ky as i16 - py as i16;

                // A. Update poly_price_history ring (max 60 entries, 1-min cadence)
                {
                    let ring = poly_hist.entry(mid).or_insert_with(VecDeque::new);
                    let needs_push = ring.back()
                        .map(|(t, _)| t.elapsed().as_secs() >= 60)
                        .unwrap_or(true);
                    if needs_push {
                        ring.push_back((now, py));
                        if ring.len() > 60 { ring.pop_front(); }
                    }
                }

                // B. Update spread_history ring (max 30 entries, push each call ~1/sec)
                {
                    let ring = spd_hist.entry(mid).or_insert_with(VecDeque::new);
                    ring.push_back((now, spread));
                    if ring.len() > 30 { ring.pop_front(); }
                }

                // Freshness check (existing)
                let snap = snapshots
                    .entry(mid)
                    .or_insert_with(|| PriceSnapshot::new(ky, kn, py, pn));
                snap.update(ky, kn, py, pn);

                // Per-market price warmup gate: require at least 2 price receipts on
                // EACH feed before this market is eligible for trading.  Repeated same
                // price counts — a quiet market still proves the feed is alive.
                if !snap.is_warmed_up() {
                    info!(
                        "[SIGNAL] Warming up {} — K={} P={} updates (need 2 each)",
                        pair.kalshi_market_ticker,
                        snap.kalshi_update_count,
                        snap.poly_update_count,
                    );
                    continue;
                }

                if !snap.is_fresh(self.config.freshness_window_secs) { continue; }

                // Bug 2: Poly price staleness check — skip if no WS update in 120s.
                // poly_update_ts is stamped by polymarket.rs on every WS message.
                // ts==0 means we never received a WS update for this market.
                {
                    let ts = self.state.poly_update_ts[mid as usize].load(Ordering::Acquire);
                    let now_secs = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let poly_age_secs = if ts == 0 { 999u64 } else { now_secs.saturating_sub(ts) };
                    if poly_age_secs > 120 {
                        info!(
                            "[SIGNAL] Skipping {} — Poly price stale ({}s old)",
                            pair.kalshi_market_ticker, poly_age_secs
                        );
                        continue;
                    }
                }

                // C. Category, game_date, base_threshold, mode (preliminary)
                let category = self.market_categories
                    .get(&mid)
                    .copied()
                    .unwrap_or(MarketCategory::Sports);
                let game_date_opt = self.game_dates.get(&mid).copied();
                let (base_threshold_prelim, mode_prelim) = self.pick_threshold_and_mode(
                    category, game_date_opt, today,
                );

                // D. Detect subcategory (with ticker for short-duration detection)
                let ticker_str = &*pair.kalshi_market_ticker;
                let subcategory = detect_subcategory(
                    category,
                    mode_prelim,
                    now_utc,
                    poly_hist.get(&mid),
                    self.safety_config.crypto_volatile_delta_cents,
                    self.safety_config.economic_event_window_mins,
                    Some(ticker_str),
                );

                // E. Override threshold/mode for short-duration crypto markets.
                //    These always use Snipe (IOC) with tighter thresholds.
                let (base_threshold, mode) = match subcategory {
                    MarketSubcategory::CryptoShort15m =>
                        (self.config.crypto_short15m_threshold_cents, TradeMode::Snipe),
                    MarketSubcategory::CryptoShortHourly =>
                        (self.config.crypto_short_hourly_threshold_cents, TradeMode::Snipe),
                    _ => (base_threshold_prelim, mode_prelim),
                };

                // F. Oracle confidence
                let oracle_conf = self.oracle_config.confidence(subcategory);

                // F. Effective threshold (min 1¢)
                let eff_threshold = ((base_threshold as f64 / oracle_conf).round() as i16).max(1);

                // G. Safety filters (log at debug)
                let abs_spread = spread.abs() as i16;
                if ky < self.safety_config.min_kalshi_yes as u16
                    || ky > self.safety_config.max_kalshi_yes as u16
                {
                    tracing::debug!(
                        "[SIGNAL] SKIP {} — kalshi_yes={}¢ outside [{},{}]",
                        pair.kalshi_market_ticker, ky,
                        self.safety_config.min_kalshi_yes, self.safety_config.max_kalshi_yes
                    );
                    delta_safety += 1;
                    continue;
                }
                // Bug 3: use >= so that spread exactly equal to max_spread is also rejected
                // (a 35¢ spread is almost certainly stale data, not a real signal)
                if abs_spread >= self.safety_config.max_spread_cents {
                    tracing::debug!(
                        "[SIGNAL] SKIP {} — spread={}¢ >= max_spread={}¢",
                        pair.kalshi_market_ticker, abs_spread, self.safety_config.max_spread_cents
                    );
                    delta_safety += 1;
                    continue;
                }

                // H. Effective threshold check
                if abs_spread < eff_threshold {
                    // If was above, clear threshold record
                    crossed.remove(&mid);
                    continue;
                }

                // I. Compute spread velocity from history ring
                let velocity = spd_hist.get(&mid)
                    .map(|r| compute_velocity(r, spread))
                    .unwrap_or(0.0);

                // J. Velocity filter: skip if narrowing faster than 0.1¢/s
                if velocity < -0.1 {
                    tracing::debug!(
                        "[SIGNAL] SKIP {} — velocity={:.3}¢/s (narrowing spread)",
                        pair.kalshi_market_ticker, velocity
                    );
                    delta_velocity += 1;
                    continue;
                }

                // K. Record threshold_crossed_at; structural filter
                let freshness_secs = match crossed.get(&mid) {
                    None => {
                        crossed.insert(mid, now);
                        0u64
                    }
                    Some(&cross_time) => {
                        let elapsed = cross_time.elapsed().as_secs();
                        // Structural: spread has been wide >5min with low velocity → skip
                        if elapsed >= 300 && velocity.abs() < 0.1 {
                            tracing::debug!(
                                "[SIGNAL] SKIP {} — structural spread ({}s, vel={:.3})",
                                pair.kalshi_market_ticker, elapsed, velocity
                            );
                            delta_quality += 1;
                            continue;
                        }
                        elapsed
                    }
                };

                // L. Signal quality scores
                let spread_score    = compute_spread_score(abs_spread as u16);
                let freshness_score = compute_freshness_score(freshness_secs);
                let velocity_score  = compute_velocity_score(velocity);
                let total           = spread_score * freshness_score * velocity_score * oracle_conf;

                let quality = SignalQuality {
                    spread_score,
                    freshness_score,
                    velocity_score,
                    oracle_conf,
                    total,
                };

                // M. Quality filter
                if total < self.safety_config.min_quality {
                    tracing::debug!(
                        "[SIGNAL] SKIP {} {} — quality={:.3}<{} (s={:.2} f={:.2} v={:.2} o={:.2})",
                        pair.kalshi_market_ticker, subcategory, total,
                        self.safety_config.min_quality,
                        spread_score, freshness_score, velocity_score, oracle_conf
                    );
                    delta_quality += 1;
                    // Record skip in activity feed (threshold was crossed → interesting signal)
                    skip_activities.push(ActivityEvent {
                        ts:          Utc::now().to_rfc3339(),
                        event_type:  "skip".into(),
                        market:      pair.kalshi_market_ticker.to_string(),
                        category:    format!("{}", category),
                        subcategory: format!("{}", subcategory),
                        details:     format!("quality={:.2} spread={:+}¢ eff={}¢", total, spread, eff_threshold),
                        pnl:         None,
                    });
                    continue;
                }

                let direction = if spread < 0 {
                    TradeDirection::BuyYes
                } else {
                    TradeDirection::BuyNo
                };

                // N. Add candidate
                out.push(SignalCandidate {
                    market_id: mid,
                    event_key: pair.kalshi_event_ticker.to_string(),
                    category,
                    subcategory,
                    oracle_conf,
                    eff_threshold,
                    direction,
                    mode,
                    spread,
                    kalshi_yes: ky,
                    poly_yes:   py,
                    quality,
                    velocity,
                    freshness_secs,
                    ticker: pair.kalshi_market_ticker.to_string(),
                    desc:   pair.description.to_string(),
                });
            }
            out
        }; // all locks released

        // Apply counter deltas (brief Mutex hold, no await inside)
        {
            let mut ctr = self.signal_counters.lock().await;
            ctr.signals_evaluated += delta_evaluated;
            ctr.skipped_safety    += delta_safety;
            ctr.skipped_velocity  += delta_velocity;
            ctr.skipped_quality   += delta_quality;
        }

        // Push skip activity events
        for ev in skip_activities {
            self.push_activity(ev).await;
        }

        // Bug 1: Purge expired fill cooldowns before processing candidates (every scan cycle).
        {
            let mut cooldowns = self.fill_cooldowns.lock().await;
            cooldowns.retain(|_, t| t.elapsed().as_secs() < 60);
        }

        // Phase 2: enter positions for each candidate (may involve async HTTP calls).
        for cand in candidates {
            // Skip if already have a position on this specific market
            if self.positions.lock().await.iter().any(|p| p.market_id == cand.market_id) {
                continue;
            }

            // Bug 1: Skip if this ticker is in a post-no-fill cooldown (60s window).
            {
                let cooldowns = self.fill_cooldowns.lock().await;
                if let Some(ts) = cooldowns.get(&cand.ticker) {
                    let age_secs = ts.elapsed().as_secs();
                    if age_secs < 60 {
                        tracing::debug!(
                            "[SIGNAL] SKIP {} — fill cooldown ({}s remaining)",
                            cand.ticker, 60 - age_secs
                        );
                        continue;
                    }
                }
            }

            // For buy_no: the fair cost is (100 - K_yes), not the K_no ask price.
            let entry_price = match cand.direction {
                TradeDirection::BuyYes => cand.kalshi_yes as i16,
                TradeDirection::BuyNo  => 100 - cand.kalshi_yes as i16,
            };

            if entry_price <= 0 { continue; }

            // Safety: min_entry_cents
            if entry_price < self.safety_config.min_entry_cents {
                tracing::debug!(
                    "[SIGNAL] SKIP {} — entry_price={}¢ < min={}¢",
                    cand.ticker, entry_price, self.safety_config.min_entry_cents
                );
                continue;
            }

            // Sanity: skip if risking >85¢ to make <15¢
            if entry_price > 85 {
                tracing::debug!(
                    "[SIGNAL] SKIP {} {} — entry_price={}¢ > 85¢ sanity limit (K_yes={}¢)",
                    cand.ticker, cand.direction, entry_price, cand.kalshi_yes
                );
                continue;
            }

            info!(
                "[SIGNAL] 🎯 SIGNAL | {} | {} | spread={:+}¢ eff={}¢ | K={}¢ P={}¢ | {} [{}/{}] | \
                 SCORE={:.3} (s={:.2} f={:.2} v={:.2} o={:.2})",
                cand.ticker, cand.direction, cand.spread, cand.eff_threshold,
                cand.kalshi_yes, cand.poly_yes, cand.mode, cand.category, cand.subcategory,
                cand.quality.total,
                cand.quality.spread_score, cand.quality.freshness_score,
                cand.quality.velocity_score, cand.quality.oracle_conf,
            );

            let game_date = self.game_dates.get(&cand.market_id).copied();
            self.enter_position(
                cand.market_id,
                cand.ticker,
                cand.event_key,
                cand.category,
                cand.subcategory,
                cand.oracle_conf,
                cand.quality,
                cand.velocity,
                cand.freshness_secs,
                cand.desc,
                cand.direction,
                cand.mode,
                entry_price,
                cand.spread,
                cand.eff_threshold,
                cand.poly_yes,
                game_date,
            ).await;
        }
    }

    // ── Entry ─────────────────────────────────────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    async fn enter_position(
        &self,
        market_id:      u16,
        ticker:         String,
        event_key:      String,
        category:       MarketCategory,
        subcategory:    MarketSubcategory,
        oracle_conf:    f64,
        quality:        SignalQuality,
        velocity:       f64,
        freshness_secs: u64,
        desc:           String,
        direction:      TradeDirection,
        mode:           TradeMode,
        entry_price:    i16,
        spread:         i16,
        eff_threshold:  i16,
        poly_yes:       PriceCents,
        game_date:      Option<NaiveDate>,
    ) {
        let is_snipe       = mode == TradeMode::Snipe;
        let base_threshold = self.category_base_threshold(category, is_snipe);

        // Look up expiry timestamp for short-duration markets
        let expiry_ts: Option<i64> = self.state.get_by_id(market_id)
            .and_then(|m| m.pair.as_ref())
            .and_then(|p| p.expiry_ts);

        // Telegram duration prefix for short-duration trades
        let duration_prefix = match subcategory {
            MarketSubcategory::CryptoShort15m    => "[15M] ",
            MarketSubcategory::CryptoShortHourly => "[1H] ",
            _ => "",
        };

        let minutes_to_event = game_date.map(|gd| {
            let ev_utc = gd.and_hms_opt(23, 0, 0)
                .map(|dt| dt.and_utc())
                .unwrap_or_else(|| Utc::now());
            (ev_utc - Utc::now()).num_minutes()
        });
        let hour_utc = Utc::now().hour();

        // ── Risk manager: size position and run all VAR checks ─────────────────
        let decision = self.risk_manager.check_entry(
            entry_price,
            spread.unsigned_abs(),
            &event_key,
            game_date,
            is_snipe,
            base_threshold,
            &desc,
            quality.total.min(2.0),  // quality_factor
        ).await;

        let (mut contracts, position_var) = match decision {
            EntryDecision::Rejected { reason } => {
                warn!("[SIGNAL] REJECTED {} {} — {}", ticker, direction, reason);
                {
                    let mut ctr = self.signal_counters.lock().await;
                    ctr.skipped_risk += 1;
                }
                self.push_activity(ActivityEvent {
                    ts:          Utc::now().to_rfc3339(),
                    event_type:  "skip".into(),
                    market:      ticker.clone(),
                    category:    format!("{}", category),
                    subcategory: format!("{}", subcategory),
                    details:     format!("risk_rejected: {}", reason),
                    pnl:         None,
                }).await;
                // Record rejection
                self.data_collector.record_signal_event(SignalEvent {
                    ts:                  Utc::now().to_rfc3339(),
                    market:              ticker.clone(),
                    category:            format!("{}", category),
                    subcategory:         format!("{}", subcategory),
                    oracle_conf,
                    spread,
                    effective_threshold: eff_threshold,
                    spread_velocity:     velocity,
                    freshness_seconds:   freshness_secs,
                    signal_quality:      quality.total,
                    spread_score:        quality.spread_score,
                    freshness_score:     quality.freshness_score,
                    velocity_score:      quality.velocity_score,
                    action:              "rejected".into(),
                    rejection_reason:    Some(reason),
                    entry_price:         None,
                    contracts:           None,
                    position_var:        None,
                }).await;
                return;
            }
            EntryDecision::Approved { contracts, position_var } => (contracts, position_var),
        };

        // Warmup: halve size for the first N trades in each new category
        let trades_so_far = {
            let stats = self.category_stats.lock().await;
            stats.get(&category).map(|s| s.trades_entered).unwrap_or(0)
        };
        if trades_so_far < self.config.warmup_trades {
            contracts = (contracts / 2).max(1);
            info!("[SIGNAL] [WARMUP] {} trade {}/{} — halved to {} contracts",
                  category, trades_so_far + 1, self.config.warmup_trades, contracts);
        }

        let entry_cost     = contracts * entry_price as i64;
        let is_maker       = mode == TradeMode::PreGame;
        let side           = match direction { TradeDirection::BuyYes => "yes", TradeDirection::BuyNo => "no" };
        let order_type_str = if is_maker { "maker" } else { "taker" };

        if self.config.dry_run {
            info!("[SIGNAL] [DRY] ENTRY {} {} {} @{}¢ ×{} spread={:+}¢ cost=${:.2} pos_var=${:.2}",
                  mode, side, order_type_str, entry_price, contracts, spread,
                  entry_cost as f64 / 100.0, position_var);

            let pos = SignalPosition {
                market_id,
                kalshi_ticker:      ticker.clone(),
                event_key:          event_key.clone(),
                market_desc:        desc.clone(),
                direction,
                mode,
                contracts,
                entry_price_cents:  entry_price,
                entry_spread_cents: spread,
                entry_poly_yes:     poly_yes,
                position_var,
                entry_ts:           Instant::now(),
                entry_timestamp:    Utc::now().to_rfc3339(),
                kalshi_order_id:    None,
                entry_is_maker:     is_maker,
                expiry_ts,
                subcategory,
                oracle_conf,
                signal_quality:     quality.total,
                velocity_at_entry:  velocity,
                freshness_at_entry: freshness_secs,
                minutes_to_event,
                hour_utc_at_entry:  hour_utc,
            };
            self.positions.lock().await.push(pos);

            // Track in risk manager (simulated positions still count for VAR)
            self.risk_manager.record_entry(
                ticker.clone(), event_key, entry_price, contracts, position_var,
            ).await;

            // Record category stats (warmup counter + quality accumulation)
            {
                let mut stats = self.category_stats.lock().await;
                let s = stats.entry(category).or_default();
                s.trades_entered += 1;
                s.total_quality  += quality.total;
            }

            // Counter + activity
            {
                let mut ctr = self.signal_counters.lock().await;
                ctr.entered      += 1;
                ctr.trades_today += 1;
            }
            let entry_cost_dollars = entry_cost as f64 / 100.0;
            self.push_activity(ActivityEvent {
                ts:          Utc::now().to_rfc3339(),
                event_type:  "entry".into(),
                market:      ticker.clone(),
                category:    format!("{}", category),
                subcategory: format!("{}", subcategory),
                details:     format!(
                    "{} {} @{}¢ ×{} spread={:+}¢ quality={:.2} cost=${:.2}",
                    direction, mode, entry_price, contracts, spread, quality.total, entry_cost_dollars,
                ),
                pnl: None,
            }).await;

            // Record signal event
            self.data_collector.record_signal_event(SignalEvent {
                ts:                  Utc::now().to_rfc3339(),
                market:              ticker.clone(),
                category:            format!("{}", category),
                subcategory:         format!("{}", subcategory),
                oracle_conf,
                spread,
                effective_threshold: eff_threshold,
                spread_velocity:     velocity,
                freshness_seconds:   freshness_secs,
                signal_quality:      quality.total,
                spread_score:        quality.spread_score,
                freshness_score:     quality.freshness_score,
                velocity_score:      quality.velocity_score,
                action:              "entered".into(),
                rejection_reason:    None,
                entry_price:         Some(entry_price),
                contracts:           Some(contracts),
                position_var:        Some(position_var),
            }).await;

            let record = TradeRecord {
                entry_timestamp:    Utc::now().to_rfc3339(),
                market_name:        desc,
                kalshi_ticker:      ticker,
                direction:          direction.to_string(),
                kalshi_price:       entry_price,
                poly_price:         poly_yes as i16,
                spread_at_entry:    spread,
                mode:               mode.to_string(),
                order_type:         order_type_str.to_string(),
                entry_cost_dollars: entry_cost as f64 / 100.0,
                contracts,
                position_var,
                kalshi_order_id:    None,
                exit_timestamp:     None,
                exit_price_cents:   None,
                exit_spread_cents:  None,
                exit_reason:        None,
                pnl_cents:          None,
                pnl_dollars:        None,
                fees_paid_cents:    None,
            };
            if let Err(e) = append_trade_record(&record).await {
                warn!("[SIGNAL] Failed to log trade record: {}", e);
            }
            return;
        }

        // Live execution
        let result = if is_maker {
            self.kalshi_api.buy_resting_limit(&ticker, side, entry_price as i64, contracts).await
        } else {
            self.kalshi_api.buy_ioc(&ticker, side, entry_price as i64, contracts).await
        };

        match result {
            Ok(resp) => {
                let order_id = resp.order.order_id.clone();
                let filled   = resp.order.filled_count();

                info!("[SIGNAL] ✅ ENTRY order_id={} status={} filled={}",
                      order_id, resp.order.status, filled);

                // For snipe (IOC): skip if nothing was filled.
                // Bug 1: Also add a 60s cooldown so we don't immediately retry the same market.
                if !is_maker && filled == 0 {
                    warn!("[SIGNAL] Snipe order not filled — added to fill cooldown (60s)");
                    self.fill_cooldowns.lock().await.insert(ticker.clone(), Instant::now());
                    return;
                }

                let actual_contracts = if is_maker {
                    // For maker (resting) orders, use actual fill count. Order may
                    // sit on the book unfilled -- don't count as position until filled.
                    let maker_filled = filled;
                    if maker_filled == 0 {
                        warn!("[SIGNAL] Maker order resting (0 fills) for {} -- tracking as pending", ticker);
                        // Don't create position entry for unfilled maker orders
                        // TODO: implement pending order tracking with fill polling
                        0
                    } else {
                        maker_filled
                    }
                } else {
                    filled.max(1)
                };
                let actual_var       = (entry_price as f64 / 100.0) * actual_contracts as f64;

                // For maker orders with 0 fills, skip position creation entirely
                if actual_contracts == 0 {
                    return;
                }

                let pos = SignalPosition {
                    market_id,
                    kalshi_ticker:      ticker.clone(),
                    event_key:          event_key.clone(),
                    market_desc:        desc.clone(),
                    direction,
                    mode,
                    contracts:          actual_contracts,
                    entry_price_cents:  entry_price,
                    entry_spread_cents: spread,
                    entry_poly_yes:     poly_yes,
                    position_var:       actual_var,
                    entry_ts:           Instant::now(),
                    entry_timestamp:    Utc::now().to_rfc3339(),
                    kalshi_order_id:    Some(order_id.clone()),
                    entry_is_maker:     is_maker,
                    expiry_ts,
                    subcategory,
                    oracle_conf,
                    signal_quality:     quality.total,
                    velocity_at_entry:  velocity,
                    freshness_at_entry: freshness_secs,
                    minutes_to_event,
                    hour_utc_at_entry:  hour_utc,
                };
                self.positions.lock().await.push(pos);

                // Record in risk manager with actual (possibly IOC-partial) contracts
                self.risk_manager.record_entry(
                    ticker.clone(), event_key, entry_price, actual_contracts, actual_var,
                ).await;

                // Record category stats (warmup counter + quality accumulation)
                {
                    let mut stats = self.category_stats.lock().await;
                    let s = stats.entry(category).or_default();
                    s.trades_entered += 1;
                    s.total_quality  += quality.total;
                }

                // Counter + activity
                {
                    let mut ctr = self.signal_counters.lock().await;
                    ctr.entered      += 1;
                    ctr.trades_today += 1;
                }
                let live_cost = (actual_contracts * entry_price as i64) as f64 / 100.0;
                self.push_activity(ActivityEvent {
                    ts:          Utc::now().to_rfc3339(),
                    event_type:  "entry".into(),
                    market:      ticker.clone(),
                    category:    format!("{}", category),
                    subcategory: format!("{}", subcategory),
                    details:     format!(
                        "{} {} @{}¢ ×{} spread={:+}¢ quality={:.2} cost=${:.2}",
                        direction, mode, entry_price, actual_contracts, spread, quality.total, live_cost,
                    ),
                    pnl: None,
                }).await;

                // Record signal event
                self.data_collector.record_signal_event(SignalEvent {
                    ts:                  Utc::now().to_rfc3339(),
                    market:              ticker.clone(),
                    category:            format!("{}", category),
                    subcategory:         format!("{}", subcategory),
                    oracle_conf,
                    spread,
                    effective_threshold: eff_threshold,
                    spread_velocity:     velocity,
                    freshness_seconds:   freshness_secs,
                    signal_quality:      quality.total,
                    spread_score:        quality.spread_score,
                    freshness_score:     quality.freshness_score,
                    velocity_score:      quality.velocity_score,
                    action:              "entered".into(),
                    rejection_reason:    None,
                    entry_price:         Some(entry_price),
                    contracts:           Some(actual_contracts),
                    position_var:        Some(actual_var),
                }).await;

                let record = TradeRecord {
                    entry_timestamp:    Utc::now().to_rfc3339(),
                    market_name:        desc.clone(),
                    kalshi_ticker:      ticker.clone(),
                    direction:          direction.to_string(),
                    kalshi_price:       entry_price,
                    poly_price:         poly_yes as i16,
                    spread_at_entry:    spread,
                    mode:               mode.to_string(),
                    order_type:         order_type_str.to_string(),
                    entry_cost_dollars: (actual_contracts * entry_price as i64) as f64 / 100.0,
                    contracts:          actual_contracts,
                    position_var:       actual_var,
                    kalshi_order_id:    Some(order_id),
                    exit_timestamp:     None,
                    exit_price_cents:   None,
                    exit_spread_cents:  None,
                    exit_reason:        None,
                    pnl_cents:          None,
                    pnl_dollars:        None,
                    fees_paid_cents:    None,
                };
                if let Err(e) = append_trade_record(&record).await {
                    warn!("[SIGNAL] Failed to log trade record: {}", e);
                }

                if let Some(tg) = &self.telegram {
                    let ky_display = match direction {
                        TradeDirection::BuyYes => entry_price,
                        TradeDirection::BuyNo  => 100 - entry_price,
                    };
                    tg.send_alert(
                        crate::telegram::AlertType::TradeExecuted,
                        market_id,
                        format!(
                            "🟢 ENTRY — {}{}\nDir: {} | Mode: {} | Sub: {}\nSpread: {:+}¢ (K:{}¢ P:{}¢)\nQuality: {:.2} | Oracle: {:.2}\nSize: {} @ {}¢ = ${:.2}\nVAR: ${:.2}",
                            duration_prefix, ticker, direction, mode, subcategory, spread,
                            ky_display, poly_yes,
                            quality.total, oracle_conf,
                            actual_contracts, entry_price,
                            (actual_contracts * entry_price as i64) as f64 / 100.0,
                            actual_var,
                        ),
                    );
                }
            }
            Err(e) => {
                warn!("[SIGNAL] Entry order failed for {}: {}", ticker, e);
                self.circuit_breaker.record_error().await;
            }
        }
    }

    // ── Exit Monitoring ───────────────────────────────────────────────────────

    async fn check_exits(&self) {
        let positions = self.positions.lock().await.clone();
        let mut to_exit: Vec<(SignalPosition, ExitReason)> = Vec::new();

        for pos in &positions {
            let Some(market) = self.state.get_by_id(pos.market_id) else { continue };
            let (ky, kn, _, _) = market.kalshi.load();
            let (py, _,  _, _) = market.poly.load();

            if ky == 0 || py == 0 { continue; }

            let spread    = ky as i16 - py as i16;
            let game_date = self.game_dates.get(&pos.market_id).copied();

            if let Some(reason) = pos.check_exit(spread, &self.config, game_date) {
                let exit_price = match pos.direction {
                    TradeDirection::BuyYes => ky as i16,
                    TradeDirection::BuyNo  => kn as i16,
                };
                info!("[SIGNAL] 🚪 EXIT signal: {} {} reason={} spread={:+}¢ exit_price={}¢",
                      pos.kalshi_ticker, pos.direction, reason, spread, exit_price);
                to_exit.push((pos.clone(), reason));
            }
        }

        for (pos, reason) in to_exit {
            self.execute_exit(pos, reason).await;
        }
    }

    async fn execute_exit(&self, pos: SignalPosition, reason: ExitReason) {
        // Remove position from tracking (even on order failure, to avoid infinite loops)
        self.positions.lock().await.retain(|p| p.market_id != pos.market_id);

        let Some(market) = self.state.get_by_id(pos.market_id) else { return };
        let (ky, kn, _, _) = market.kalshi.load();
        let (py, _,  _, _) = market.poly.load();

        let exit_price = match pos.direction {
            TradeDirection::BuyYes => ky as i16,
            TradeDirection::BuyNo  => kn as i16,
        };
        let current_spread = if ky > 0 && py > 0 { ky as i16 - py as i16 } else { 0 };
        let side = match pos.direction {
            TradeDirection::BuyYes => "yes",
            TradeDirection::BuyNo  => "no",
        };
        let ts_exit = Utc::now().to_rfc3339();

        let mut exit_succeeded = false;

        if self.config.dry_run {
            info!("[SIGNAL] [DRY] EXIT {} {} @{}¢ ×{} reason={}",
                  pos.kalshi_ticker, side, exit_price, pos.contracts, reason);
            exit_succeeded = true; // dry run counts as success for P&L tracking
        } else if exit_price >= 1 && exit_price <= 99 {
            match self.kalshi_api
                .sell_ioc(&pos.kalshi_ticker, side, exit_price as i64, pos.contracts)
                .await
            {
                Ok(resp) => {
                    info!("[SIGNAL] ✅ EXIT order_id={} status={}",
                          resp.order.order_id, resp.order.status);
                    exit_succeeded = resp.order.filled_count() > 0;
                    if !exit_succeeded {
                        warn!("[SIGNAL] Exit order placed but 0 fills for {}", pos.kalshi_ticker);
                    }
                }
                Err(e) => {
                    warn!("[SIGNAL] Exit order failed for {}: {}", pos.kalshi_ticker, e);
                }
            }
        } else {
            warn!("[SIGNAL] Exit skipped — invalid price {}¢ for {}", exit_price, pos.kalshi_ticker);
        }

        if !exit_succeeded && !self.config.dry_run {
            warn!("[SIGNAL] P&L NOT recorded -- exit did not fill for {}", pos.kalshi_ticker);
            // Still record exit in risk manager to free up capacity, but with 0 P&L
            self.risk_manager.record_exit(&pos.kalshi_ticker, 0.0).await;
            return;
        }

        // P&L calculation (only reached on successful exit or dry run)
        let entry_fee_per = if pos.entry_is_maker {
            kalshi_maker_fee_cents(pos.entry_price_cents.max(0) as PriceCents) as f64
        } else {
            kalshi_fee_cents(pos.entry_price_cents.max(0) as PriceCents) as f64
        };
        let exit_fee_per = kalshi_fee_cents(exit_price.max(0) as PriceCents) as f64;
        let gross_pnl    = (exit_price - pos.entry_price_cents) as f64 * pos.contracts as f64;
        let total_fees   = (entry_fee_per + exit_fee_per) * pos.contracts as f64;
        let net_pnl      = gross_pnl - total_fees;
        let hold_secs    = pos.entry_ts.elapsed().as_secs();

        info!("[SIGNAL] P&L: gross={:.1}¢ fees={:.1}¢ net={:.1}¢ (${:.3}) | pos_var=${:.2}",
              gross_pnl, total_fees, net_pnl, net_pnl / 100.0, pos.position_var);

        // Notify risk manager
        self.risk_manager.record_exit(&pos.kalshi_ticker, net_pnl / 100.0).await;

        // Update per-category P&L and win tracking
        let cat_for_exit = self.market_categories
            .get(&pos.market_id)
            .copied()
            .unwrap_or(MarketCategory::Sports);
        {
            let mut stats = self.category_stats.lock().await;
            let s = stats.entry(cat_for_exit).or_default();
            s.pnl_dollars += net_pnl / 100.0;
            if net_pnl > 0.0 { s.wins += 1; }
        }

        // Update win/loss counters
        {
            let mut ctr = self.signal_counters.lock().await;
            if net_pnl > 0.0 {
                ctr.wins_today += 1;
            } else {
                ctr.losses_today += 1;
            }
        }

        // Record exit activity
        let held_mins = hold_secs / 60;
        self.push_activity(ActivityEvent {
            ts:          Utc::now().to_rfc3339(),
            event_type:  "exit".into(),
            market:      pos.kalshi_ticker.clone(),
            category:    format!("{}", cat_for_exit),
            subcategory: format!("{}", pos.subcategory),
            details:     format!(
                "reason={} spread={:+}¢ held={}m entry={}¢ exit={}¢",
                reason, current_spread, held_mins, pos.entry_price_cents, exit_price,
            ),
            pnl: Some((net_pnl / 100.0 * 10000.0).round() / 10000.0),
        }).await;

        // Record trade outcome for analysis
        self.data_collector.record_trade_outcome(TradeOutcome {
            ts_entry:                   pos.entry_timestamp.clone(),
            ts_exit:                    ts_exit.clone(),
            market:                     pos.kalshi_ticker.clone(),
            category:                   format!("{}", cat_for_exit),
            subcategory:                format!("{}", pos.subcategory),
            oracle_conf:                pos.oracle_conf,
            direction:                  pos.direction.to_string(),
            mode:                       pos.mode.to_string(),
            entry_price:                pos.entry_price_cents,
            exit_price:                 exit_price,
            contracts:                  pos.contracts,
            spread_at_entry:            pos.entry_spread_cents,
            spread_at_exit:             current_spread,
            spread_velocity_at_entry:   pos.velocity_at_entry,
            signal_quality_at_entry:    pos.signal_quality,
            freshness_at_entry_seconds: pos.freshness_at_entry,
            hold_time_seconds:          hold_secs,
            pnl_cents_per_contract:     net_pnl / pos.contracts as f64,
            pnl_dollars:                net_pnl / 100.0,
            fees_dollars:               total_fees / 100.0,
            exit_reason:                reason.to_string(),
            minutes_to_event_at_entry:  pos.minutes_to_event,
            hour_utc_at_entry:          pos.hour_utc_at_entry,
        }).await;

        let record = TradeRecord {
            entry_timestamp:    pos.entry_timestamp.clone(),
            market_name:        pos.market_desc.clone(),
            kalshi_ticker:      pos.kalshi_ticker.clone(),
            direction:          pos.direction.to_string(),
            kalshi_price:       pos.entry_price_cents,
            poly_price:         pos.entry_poly_yes as i16,
            spread_at_entry:    pos.entry_spread_cents,
            mode:               pos.mode.to_string(),
            order_type:         if pos.entry_is_maker { "maker" } else { "taker" }.to_string(),
            entry_cost_dollars: (pos.entry_price_cents as i64 * pos.contracts) as f64 / 100.0,
            contracts:          pos.contracts,
            position_var:       pos.position_var,
            kalshi_order_id:    pos.kalshi_order_id.clone(),
            exit_timestamp:     Some(ts_exit),
            exit_price_cents:   Some(exit_price),
            exit_spread_cents:  Some(current_spread),
            exit_reason:        Some(reason.to_string()),
            pnl_cents:          Some(net_pnl),
            pnl_dollars:        Some(net_pnl / 100.0),
            fees_paid_cents:    Some(total_fees),
        };
        if let Err(e) = append_trade_record(&record).await {
            warn!("[SIGNAL] Failed to log exit record: {}", e);
        }

        if let Some(tg) = &self.telegram {
            let emoji = if net_pnl > 0.0 { "💚" } else { "🔴" };
            let cat_wins = {
                let stats = self.category_stats.lock().await;
                let s = stats.get(&cat_for_exit).cloned().unwrap_or_default();
                (s.wins, s.trades_entered)
            };
            tg.send_alert(
                crate::telegram::AlertType::TradeExecuted,
                pos.market_id,
                format!(
                    "{} EXIT — {}\nReason: {} | P&L: ${:+.3}\nHeld: {}m | Exit spread: {:+}¢\nCategory wins: {}/{}",
                    emoji, pos.kalshi_ticker, reason, net_pnl / 100.0,
                    held_mins, current_spread,
                    cat_wins.0, cat_wins.1,
                ),
            );
        }
    }

    // ── Spread Snapshot Collection (every 30s) ────────────────────────────────

    /// Iterate all markets with dual prices and record SpreadObservation records.
    /// Called every 30s from the binary's timer task.
    pub async fn collect_spread_snapshots(&self) {
        let now_utc = Utc::now();
        let today   = now_utc.date_naive();
        let count   = self.state.market_count();

        // Collect data while holding locks (synchronous), then release before I/O
        let observations: Vec<SpreadObservation> = {
            let poly_hist = self.poly_price_history.lock().await;
            let spd_hist  = self.spread_history.lock().await;
            let crossed   = self.threshold_crossed_at.lock().await;
            let mut out   = Vec::new();

            for i in 0..count {
                let mid = i as u16;
                let Some(market) = self.state.get_by_id(mid) else { continue };
                let Some(pair)   = &market.pair else { continue };

                let (ky, _, _, _) = market.kalshi.load();
                let (py, _, _, _) = market.poly.load();
                if ky == 0 || py == 0 { continue; }

                let spread   = ky as i16 - py as i16;
                let category = self.market_categories.get(&mid).copied().unwrap_or(MarketCategory::Sports);
                let game_date_opt = self.game_dates.get(&mid).copied();
                let (base_threshold_prelim, mode_prelim) = self.pick_threshold_and_mode(category, game_date_opt, today);

                let subcategory = detect_subcategory(
                    category, mode_prelim, now_utc,
                    poly_hist.get(&mid),
                    self.safety_config.crypto_volatile_delta_cents,
                    self.safety_config.economic_event_window_mins,
                    Some(&*pair.kalshi_market_ticker),
                );
                let (base_threshold, mode) = match subcategory {
                    MarketSubcategory::CryptoShort15m =>
                        (self.config.crypto_short15m_threshold_cents, TradeMode::Snipe),
                    MarketSubcategory::CryptoShortHourly =>
                        (self.config.crypto_short_hourly_threshold_cents, TradeMode::Snipe),
                    _ => (base_threshold_prelim, mode_prelim),
                };
                let oracle_conf  = self.oracle_config.confidence(subcategory);
                let eff_threshold = ((base_threshold as f64 / oracle_conf).round() as i16).max(1);

                let velocity = spd_hist.get(&mid)
                    .map(|r| compute_velocity(r, spread))
                    .unwrap_or(0.0);

                let time_above = crossed.get(&mid)
                    .map(|t| t.elapsed().as_secs())
                    .unwrap_or(0);

                let is_live = mode == TradeMode::Snipe;

                let minutes_to_event = game_date_opt.map(|gd| {
                    let ev_utc = gd.and_hms_opt(23, 0, 0)
                        .map(|dt| dt.and_utc())
                        .unwrap_or_else(|| Utc::now());
                    (ev_utc - now_utc).num_minutes()
                });

                out.push(SpreadObservation {
                    ts:                        now_utc.to_rfc3339(),
                    market:                    pair.kalshi_market_ticker.to_string(),
                    market_name:               pair.description.to_string(),
                    category:                  format!("{}", category),
                    subcategory:               format!("{}", subcategory),
                    oracle_conf,
                    kalshi_yes:                ky,
                    poly_yes:                  py,
                    spread,
                    spread_velocity:           velocity,
                    effective_threshold:       eff_threshold,
                    time_above_threshold_secs: time_above,
                    is_live,
                    minutes_to_event,
                    hour_utc:                  now_utc.hour(),
                    day_of_week:               now_utc.weekday().num_days_from_monday(),
                });
            }
            out
        }; // locks released

        for obs in observations {
            self.data_collector.record_spread_observation(obs).await;
        }
    }

    // ── Public Accessors ──────────────────────────────────────────────────────

    pub async fn daily_pnl_dollars(&self) -> f64 {
        self.risk_manager.summary().await.daily_pnl
    }

    pub async fn open_position_count(&self) -> usize {
        self.positions.lock().await.len()
    }

    /// Return a risk summary snapshot for heartbeat logging.
    pub async fn risk_summary(&self) -> RiskSummary {
        self.risk_manager.summary().await
    }

    /// Return a snapshot of signal counters for heartbeat logging.
    pub async fn counters_snapshot(&self) -> SignalCounters {
        self.signal_counters.lock().await.clone()
    }

    /// Return a snapshot of per-category statistics for heartbeat logging.
    pub async fn category_stats_snapshot(&self) -> Vec<(MarketCategory, CategoryStats)> {
        let stats = self.category_stats.lock().await;
        let mut out: Vec<_> = stats.iter().map(|(&k, v)| (k, v.clone())).collect();
        out.sort_by_key(|(k, _)| match k {
            MarketCategory::Sports   => 0,
            MarketCategory::Crypto   => 1,
            MarketCategory::Economic => 2,
        });
        out
    }

    // ── Threshold / Mode Helpers ──────────────────────────────────────────────

    /// Select the signal threshold and trade mode for a market.
    fn pick_threshold_and_mode(
        &self,
        category:  MarketCategory,
        game_date: Option<NaiveDate>,
        today:     NaiveDate,
    ) -> (i16, TradeMode) {
        match category {
            MarketCategory::Sports => {
                let is_live = game_date.map(|gd| gd <= today).unwrap_or(false);
                if is_live {
                    (self.config.live_threshold_cents, TradeMode::Snipe)
                } else {
                    (self.config.pregame_threshold_cents, TradeMode::PreGame)
                }
            }
            MarketCategory::Crypto => {
                if is_crypto_live_hours() {
                    (self.config.live_threshold_cents, TradeMode::Snipe)
                } else {
                    (self.config.crypto_threshold_cents, TradeMode::PreGame)
                }
            }
            MarketCategory::Economic => {
                (self.config.economic_threshold_cents, TradeMode::PreGame)
            }
        }
    }

    /// Base threshold for the risk manager's spread-to-size mapping.
    fn category_base_threshold(&self, category: MarketCategory, is_snipe: bool) -> i16 {
        if is_snipe {
            return self.config.live_threshold_cents;
        }
        match category {
            MarketCategory::Sports   => self.config.pregame_threshold_cents,
            MarketCategory::Crypto   => self.config.crypto_threshold_cents,
            MarketCategory::Economic => self.config.economic_threshold_cents,
        }
    }

    /// Log a spread summary for all tracked markets (call periodically for debugging).
    pub async fn log_spread_summary(&self) {
        let count = self.state.market_count();
        let mut logged = 0usize;
        for i in 0..count {
            let mid = i as u16;
            let Some(market) = self.state.get_by_id(mid) else { continue };
            let Some(pair)   = &market.pair else { continue };

            let (ky, kn, _, _) = market.kalshi.load();
            let (py, _,  _, _) = market.poly.load();

            if ky == 0 || py == 0 { continue; }

            let spread = ky as i16 - py as i16;
            info!("[SIGNAL] SPREAD | {} | spread={:+}¢ | K_yes={}¢ K_no={}¢ P_yes={}¢",
                  pair.description, spread, ky, kn, py);
            logged += 1;
        }
        if logged == 0 {
            info!("[SIGNAL] SPREAD | no markets with dual prices yet");
        }
    }
}

// ── Subcategory Detection ─────────────────────────────────────────────────────

/// Short-duration ticker prefixes (case-insensitive, uppercase match)
const SHORT_15M_PREFIXES: &[&str] = &["KXBTC15M", "KXETH15M", "KXSOL15M", "KXBTCUD", "KXETHUD", "KXSOLUD"];
const SHORT_1H_PREFIXES:  &[&str] = &["KXBTC1H", "KXETH1H", "KXSOL1H"];

fn detect_subcategory(
    category:               MarketCategory,
    mode:                   TradeMode,
    now_utc:                chrono::DateTime<Utc>,
    poly_ring:              Option<&VecDeque<(Instant, PriceCents)>>,
    crypto_volatile_delta:  u16,
    economic_event_window:  u32,
    ticker:                 Option<&str>,
) -> MarketSubcategory {
    // Short-duration detection: check ticker prefix before category dispatch
    if category == MarketCategory::Crypto {
        if let Some(t) = ticker {
            let t_up = t.to_uppercase();
            if SHORT_15M_PREFIXES.iter().any(|p| t_up.starts_with(p)) {
                return MarketSubcategory::CryptoShort15m;
            }
            if SHORT_1H_PREFIXES.iter().any(|p| t_up.starts_with(p)) {
                return MarketSubcategory::CryptoShortHourly;
            }
        }
    }

    match category {
        MarketCategory::Sports => {
            if mode == TradeMode::Snipe {
                MarketSubcategory::SportsLive
            } else {
                MarketSubcategory::SportsPregame
            }
        }
        MarketCategory::Crypto => {
            // Check poly price range over last 60 min
            let is_volatile = poly_ring
                .map(|ring| {
                    if ring.len() < 2 { return false; }
                    let min = ring.iter().map(|(_, p)| *p).min().unwrap_or(0);
                    let max = ring.iter().map(|(_, p)| *p).max().unwrap_or(0);
                    max.saturating_sub(min) > crypto_volatile_delta
                })
                .unwrap_or(false);
            if is_volatile {
                MarketSubcategory::CryptoVolatile
            } else {
                MarketSubcategory::CryptoStable
            }
        }
        MarketCategory::Economic => {
            // Economic event window: within N minutes of 14:00 UTC
            let hour_utc = now_utc.hour();
            let min_utc  = now_utc.minute();
            let total_mins_from_midnight = hour_utc * 60 + min_utc;
            let event_mins = 14 * 60u32; // 14:00 UTC
            let diff = (total_mins_from_midnight as i32 - event_mins as i32).unsigned_abs();
            if diff <= economic_event_window {
                MarketSubcategory::EconomicEvent
            } else {
                MarketSubcategory::EconomicQuiet
            }
        }
    }
}

// ── Signal Quality Scoring ────────────────────────────────────────────────────

/// Score based on spread magnitude.
/// Wider is not always better (very wide = structural), peak at 15-20¢.
fn compute_spread_score(abs_spread_cents: u16) -> f64 {
    match abs_spread_cents {
        s if s < 6           => 0.5,
        s if s < 10          => 1.0,
        s if s < 15          => 1.3,
        s if s < 20          => 1.5,
        s if s < 30          => 1.2,
        s if s < 35          => 0.7,
        _                    => 0.5,
    }
}

/// Score based on time since spread crossed effective threshold.
/// Fresh crossings are high quality; stale crossings are likely structural.
fn compute_freshness_score(secs_above_threshold: u64) -> f64 {
    match secs_above_threshold {
        s if s < 10  => 1.0,
        s if s < 30  => 0.8,
        s if s < 60  => 0.5,
        s if s < 300 => 0.3,
        _            => 0.1,
    }
}

/// Score based on spread velocity (¢/sec).
/// Widening is good (opportunity opening), narrowing already filtered out above.
fn compute_velocity_score(velocity: f64) -> f64 {
    if velocity > 0.3 {
        1.3  // Actively widening — high opportunity
    } else if velocity > 0.1 {
        1.1  // Mildly widening
    } else {
        0.8  // Stable or very slow velocity (already filtered narrowing)
    }
}

/// Compute spread velocity (¢/sec) from a VecDeque of (Instant, spread_cents).
///
/// Uses the oldest entry in the ring as the reference. Returns 0.0 if fewer
/// than 5 entries exist or the oldest is less than 5 seconds ago.
fn compute_velocity(ring: &VecDeque<(Instant, i16)>, spread_now: i16) -> f64 {
    if ring.len() < 5 { return 0.0; }
    let (oldest_time, oldest_spread) = ring.front().unwrap();
    let elapsed_secs = oldest_time.elapsed().as_secs_f64();
    if elapsed_secs < 5.0 { return 0.0; }
    (spread_now as f64 - *oldest_spread as f64) / elapsed_secs
}

// ── Crypto Live Hours ─────────────────────────────────────────────────────────

/// Returns true if the current UTC time falls within US trading hours (9am-5pm ET).
///
/// ET offset: UTC-5 (EST) approximation (conservative — ignores DST).
fn is_crypto_live_hours() -> bool {
    let now_utc = Utc::now();
    let et_hour = (now_utc.hour() as i32 - 5).rem_euclid(24) as u32;
    let is_weekday = now_utc.weekday().num_days_from_monday() < 5;
    is_weekday && et_hour >= 9 && et_hour < 17
}

// ── Game Date Extraction ──────────────────────────────────────────────────────

/// Extract game date from a Kalshi event ticker.
///
/// Handles formats:
///   "KXNBAGAME-26FEB22CLTOKC"      → 2026-02-22
///   "KXEPLGAME-25DEC27CFCAVL"      → 2025-12-27
///   "KXNFLGAME-25DEC27-NE-DAL"     → 2025-12-27  (3-part)
pub fn extract_event_date(ticker: &str) -> Option<NaiveDate> {
    let mut parts = ticker.splitn(3, '-');
    let _series = parts.next()?;
    let chunk   = parts.next()?;
    if chunk.len() < 7 { return None; }
    parse_kalshi_date(&chunk[..7])
}

fn parse_kalshi_date(s: &str) -> Option<NaiveDate> {
    if s.len() != 7 { return None; }
    let year: i32  = format!("20{}", &s[..2]).parse().ok()?;
    let month: u32 = match s[2..5].to_uppercase().as_str() {
        "JAN" => 1, "FEB" => 2, "MAR" => 3, "APR" => 4,
        "MAY" => 5, "JUN" => 6, "JUL" => 7, "AUG" => 8,
        "SEP" => 9, "OCT" => 10, "NOV" => 11, "DEC" => 12,
        _ => return None,
    };
    let day: u32 = s[5..7].parse().ok()?;
    NaiveDate::from_ymd_opt(year, month, day)
}

// ── Trade Log ─────────────────────────────────────────────────────────────────

const SIGNAL_TRADES_PATH: &str = "state/signal_trades.jsonl";

async fn append_trade_record(record: &TradeRecord) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let line = serde_json::to_string(record)?;
    let mut f = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(SIGNAL_TRADES_PATH)
        .await?;
    f.write_all(format!("{}\n", line).as_bytes()).await?;
    Ok(())
}

// ── Unit Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_event_date_two_part() {
        let d = extract_event_date("KXNBAGAME-26FEB22CLTOKC").unwrap();
        assert_eq!(d.to_string(), "2026-02-22");
    }

    #[test]
    fn test_extract_event_date_epl() {
        let d = extract_event_date("KXEPLGAME-25DEC27CFCAVL").unwrap();
        assert_eq!(d.to_string(), "2025-12-27");
    }

    #[test]
    fn test_extract_event_date_three_part() {
        let d = extract_event_date("KXNFLGAME-25DEC27-NE-DAL").unwrap();
        assert_eq!(d.to_string(), "2025-12-27");
    }

    #[test]
    fn test_extract_event_date_invalid() {
        assert!(extract_event_date("KXNBAGAME").is_none());
        assert!(extract_event_date("").is_none());
    }

    #[test]
    fn test_check_exit_convergence() {
        let config = SignalConfig {
            pregame_threshold_cents:             8,
            live_threshold_cents:                10,
            crypto_threshold_cents:              6,
            economic_threshold_cents:            5,
            crypto_short15m_threshold_cents:     4,
            crypto_short_hourly_threshold_cents: 5,
            convergence_exit_cents:              3,
            stop_loss_multiplier:                2.0,
            time_stop_minutes:                   15,
            freshness_window_secs:               30,
            warmup_trades:                       10,
            dry_run:                             true,
        };

        let pos = SignalPosition {
            market_id:          0,
            kalshi_ticker:      "TEST".into(),
            event_key:          "KXTEST-26FEB22".into(),
            market_desc:        "Test".into(),
            direction:          TradeDirection::BuyYes,
            mode:               TradeMode::PreGame,
            contracts:          10,
            entry_price_cents:  42,
            entry_spread_cents: -8,
            entry_poly_yes:     50,
            position_var:       4.20,
            entry_ts:           Instant::now(),
            entry_timestamp:    String::new(),
            kalshi_order_id:    None,
            entry_is_maker:     true,
            expiry_ts:          None,
            subcategory:        MarketSubcategory::SportsPregame,
            oracle_conf:        0.3,
            signal_quality:     0.5,
            velocity_at_entry:  0.0,
            freshness_at_entry: 0,
            minutes_to_event:   None,
            hour_utc_at_entry:  12,
        };

        // Spread converged to 2¢ → should exit
        assert_eq!(pos.check_exit(2, &config, None), Some(ExitReason::Convergence));
        // Spread still wide at -7¢ → no exit
        assert_eq!(pos.check_exit(-7, &config, None), None);
        // Stop loss: |spread| >= 8 * 2 = 16 → exit
        assert_eq!(pos.check_exit(-16, &config, None), Some(ExitReason::StopLoss));
    }

    #[test]
    fn test_spread_score() {
        assert_eq!(compute_spread_score(8),  1.0);
        assert_eq!(compute_spread_score(12), 1.3);
        assert_eq!(compute_spread_score(17), 1.5);
        assert_eq!(compute_spread_score(25), 1.2);
        assert_eq!(compute_spread_score(33), 0.7);
    }

    #[test]
    fn test_compute_velocity_empty() {
        let ring: VecDeque<(Instant, i16)> = VecDeque::new();
        assert_eq!(compute_velocity(&ring, 10), 0.0);
    }
}
