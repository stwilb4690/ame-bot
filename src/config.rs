// src/config.rs
// Configuration constants and runtime configuration functions

use std::sync::OnceLock;

// === Static URLs (Polymarket — never changes) ===

/// Polymarket WebSocket URL
pub const POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

/// Gamma API base URL (Polymarket market data — no auth required)
pub const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";

// === Kalshi URLs (runtime-configurable via env vars) ===

/// Get the Kalshi WebSocket URL.
///
/// Respects (in priority order):
///   KALSHI_API_HOST env var → full override of the host
///   APP_ENV=demo            → use demo-api.kalshi.com
///   default                 → trading-api.kalshi.com
pub fn kalshi_ws_url() -> &'static str {
    static URL: OnceLock<String> = OnceLock::new();
    URL.get_or_init(|| {
        let host = kalshi_api_host();
        format!("wss://{}/trade-api/ws/v2", host)
    })
}

/// Get the Kalshi REST API base URL.
pub fn kalshi_api_base() -> &'static str {
    static URL: OnceLock<String> = OnceLock::new();
    URL.get_or_init(|| {
        let host = kalshi_api_host();
        format!("https://{}/trade-api/v2", host)
    })
}

fn kalshi_api_host() -> String {
    // Explicit override takes highest priority
    if let Ok(host) = std::env::var("KALSHI_API_HOST") {
        if !host.is_empty() {
            return host;
        }
    }
    // APP_ENV=demo uses the demo environment
    match app_env() {
        AppEnv::Demo => "demo-api.kalshi.com".to_string(),
        AppEnv::Prod => "api.elections.kalshi.com".to_string(),
    }
}

// === App Environment ===

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppEnv {
    Demo,
    Prod,
}

/// Read APP_ENV env var. Default: Demo (safe).
pub fn app_env() -> AppEnv {
    static ENV: OnceLock<AppEnv> = OnceLock::new();
    *ENV.get_or_init(|| {
        match std::env::var("APP_ENV").as_deref() {
            Ok("prod") | Ok("production") => AppEnv::Prod,
            _ => AppEnv::Demo,
        }
    })
}

// === App Mode ===

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppMode {
    /// Only Kalshi WebSocket; monitor Kalshi same-platform opportunities.
    KalshiOnly,
    /// Both WebSockets; show cross-platform spreads but no execution.
    /// Default — suitable when you have Polymarket US sports access.
    Monitor,
    /// Both WebSockets + full cross-platform arb execution.
    Full,
}

impl std::fmt::Display for AppMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppMode::KalshiOnly => write!(f, "kalshi-only"),
            AppMode::Monitor => write!(f, "monitor"),
            AppMode::Full => write!(f, "full"),
        }
    }
}

/// Read APP_MODE env var. Default: Monitor.
pub fn app_mode() -> AppMode {
    static MODE: OnceLock<AppMode> = OnceLock::new();
    *MODE.get_or_init(|| {
        match std::env::var("APP_MODE").as_deref() {
            Ok("kalshi-only") | Ok("kalshi_only") | Ok("KALSHI_ONLY") => AppMode::KalshiOnly,
            Ok("full") | Ok("FULL") => AppMode::Full,
            _ => AppMode::Monitor, // default
        }
    })
}

// === Order Type ===

/// Kalshi order type: IOC (taker) or resting limit (maker).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KalshiOrderType {
    /// Immediate-or-cancel (taker). Default.
    Ioc,
    /// Resting limit order (maker).
    Limit,
}

/// Get the configured Kalshi order type.
///
/// Reads KALSHI_ORDER_TYPE env var. Default: "ioc".
/// Set to "limit" to place resting limit orders instead of IOC.
pub fn kalshi_order_type() -> KalshiOrderType {
    static OT: OnceLock<KalshiOrderType> = OnceLock::new();
    *OT.get_or_init(|| {
        match std::env::var("KALSHI_ORDER_TYPE").as_deref() {
            Ok("limit") => KalshiOrderType::Limit,
            _ => KalshiOrderType::Ioc,
        }
    })
}

// === Fee Rate ===

/// Get the Kalshi taker fee rate (0.0–1.0).
///
/// Reads KALSHI_FEE_RATE env var. Default: 0.07 (7%).
/// Used when KALSHI_ORDER_TYPE=ioc (default).
pub fn kalshi_fee_rate() -> f64 {
    static RATE: OnceLock<f64> = OnceLock::new();
    *RATE.get_or_init(|| {
        std::env::var("KALSHI_FEE_RATE")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .map(|r| r.clamp(0.0, 1.0))
            .unwrap_or(0.07)
    })
}

/// Get the Kalshi maker fee rate (0.0–1.0).
///
/// Reads KALSHI_MAKER_FEE_RATE env var. Default: 0.0175 (1.75%).
/// Used when KALSHI_ORDER_TYPE=limit.
pub fn kalshi_maker_fee_rate() -> f64 {
    static RATE: OnceLock<f64> = OnceLock::new();
    *RATE.get_or_init(|| {
        std::env::var("KALSHI_MAKER_FEE_RATE")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .map(|r| r.clamp(0.0, 1.0))
            .unwrap_or(0.0175)
    })
}

// === Thresholds ===

/// Arb threshold: alert when total cost < this (e.g., 0.995 = 0.5% profit).
pub fn arb_threshold() -> f64 { std::env::var("ARB_THRESHOLD").ok().and_then(|v| v.parse().ok()).unwrap_or(0.95) }

/// Wide spread threshold in cents (default 10¢ above $1.00 round-trip cost).
/// Fires when YES ask + NO ask > 100 + this value.
pub fn wide_spread_threshold() -> u16 {
    static T: OnceLock<u16> = OnceLock::new();
    *T.get_or_init(|| {
        std::env::var("WIDE_SPREAD_THRESHOLD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10)
    })
}

// === Operational ===

/// Polymarket ping interval (seconds) — keep connection alive.
pub const POLY_PING_INTERVAL_SECS: u64 = 30;

/// Kalshi API rate limit delay (milliseconds between requests).
/// Kalshi limit: 20 req/sec = 50ms minimum. We use 60ms for safety margin.
pub const KALSHI_API_DELAY_MS: u64 = 60;

/// WebSocket reconnect delay (seconds).
pub const WS_RECONNECT_DELAY_SECS: u64 = 5;

/// Which leagues to monitor (empty slice = all).
pub const ENABLED_LEAGUES: &[&str] = &[];

/// How often to log the heartbeat status summary (default: once per hour).
pub fn heartbeat_interval_secs() -> u64 {
    std::env::var("HEARTBEAT_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3600)
}

/// State write interval in seconds.
pub fn state_write_interval_secs() -> u64 {
    static S: OnceLock<u64> = OnceLock::new();
    *S.get_or_init(|| {
        std::env::var("STATE_WRITE_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10)
    })
}

/// Price logging enabled (set PRICE_LOGGING=1 to enable).
#[allow(dead_code)]
pub fn price_logging_enabled() -> bool {
    static CACHED: OnceLock<bool> = OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var("PRICE_LOGGING")
            .map(|v| v == "1" || v.to_lowercase() == "true")
            .unwrap_or(false)
    })
}

// === League Configuration ===

/// League configuration for market discovery.
#[derive(Debug, Clone)]
pub struct LeagueConfig {
    pub league_code: &'static str,
    pub poly_prefix: &'static str,
    pub kalshi_series_game: &'static str,
    pub kalshi_series_spread: Option<&'static str>,
    pub kalshi_series_total: Option<&'static str>,
    pub kalshi_series_btts: Option<&'static str>,
}

/// Get all supported leagues with their configurations.
pub fn get_league_configs() -> Vec<LeagueConfig> {
    vec![
        // Major European leagues (full market types)
        LeagueConfig {
            league_code: "epl",
            poly_prefix: "epl",
            kalshi_series_game: "KXEPLGAME",
            kalshi_series_spread: Some("KXEPLSPREAD"),
            kalshi_series_total: Some("KXEPLTOTAL"),
            kalshi_series_btts: Some("KXEPLBTTS"),
        },
        LeagueConfig {
            league_code: "bundesliga",
            poly_prefix: "bun",
            kalshi_series_game: "KXBUNDESLIGAGAME",
            kalshi_series_spread: Some("KXBUNDESLIGASPREAD"),
            kalshi_series_total: Some("KXBUNDESLIGATOTAL"),
            kalshi_series_btts: Some("KXBUNDESLIGABTTS"),
        },
        LeagueConfig {
            league_code: "laliga",
            poly_prefix: "lal",
            kalshi_series_game: "KXLALIGAGAME",
            kalshi_series_spread: Some("KXLALIGASPREAD"),
            kalshi_series_total: Some("KXLALIGATOTAL"),
            kalshi_series_btts: Some("KXLALIGABTTS"),
        },
        LeagueConfig {
            league_code: "seriea",
            poly_prefix: "sea",
            kalshi_series_game: "KXSERIEAGAME",
            kalshi_series_spread: Some("KXSERIEASPREAD"),
            kalshi_series_total: Some("KXSERIEATOTAL"),
            kalshi_series_btts: Some("KXSERIEABTTS"),
        },
        LeagueConfig {
            league_code: "ligue1",
            poly_prefix: "fl1",
            kalshi_series_game: "KXLIGUE1GAME",
            kalshi_series_spread: Some("KXLIGUE1SPREAD"),
            kalshi_series_total: Some("KXLIGUE1TOTAL"),
            kalshi_series_btts: Some("KXLIGUE1BTTS"),
        },
        LeagueConfig {
            league_code: "ucl",
            poly_prefix: "ucl",
            kalshi_series_game: "KXUCLGAME",
            kalshi_series_spread: Some("KXUCLSPREAD"),
            kalshi_series_total: Some("KXUCLTOTAL"),
            kalshi_series_btts: Some("KXUCLBTTS"),
        },
        // Secondary European leagues (moneyline only)
        LeagueConfig {
            league_code: "uel",
            poly_prefix: "uel",
            kalshi_series_game: "KXUELGAME",
            kalshi_series_spread: None,
            kalshi_series_total: None,
            kalshi_series_btts: None,
        },
        LeagueConfig {
            league_code: "eflc",
            poly_prefix: "elc",
            kalshi_series_game: "KXEFLCHAMPIONSHIPGAME",
            kalshi_series_spread: None,
            kalshi_series_total: None,
            kalshi_series_btts: None,
        },
        // US Sports
        LeagueConfig {
            league_code: "nba",
            poly_prefix: "nba",
            kalshi_series_game: "KXNBAGAME",
            kalshi_series_spread: Some("KXNBASPREAD"),
            kalshi_series_total: Some("KXNBATOTAL"),
            kalshi_series_btts: None,
        },
        LeagueConfig {
            league_code: "nfl",
            poly_prefix: "nfl",
            kalshi_series_game: "KXNFLGAME",
            kalshi_series_spread: Some("KXNFLSPREAD"),
            kalshi_series_total: Some("KXNFLTOTAL"),
            kalshi_series_btts: None,
        },
        LeagueConfig {
            league_code: "nhl",
            poly_prefix: "nhl",
            kalshi_series_game: "KXNHLGAME",
            kalshi_series_spread: Some("KXNHLSPREAD"),
            kalshi_series_total: Some("KXNHLTOTAL"),
            kalshi_series_btts: None,
        },
        LeagueConfig {
            league_code: "mlb",
            poly_prefix: "mlb",
            kalshi_series_game: "KXMLBGAME",
            kalshi_series_spread: Some("KXMLBSPREAD"),
            kalshi_series_total: Some("KXMLBTOTAL"),
            kalshi_series_btts: None,
        },
        LeagueConfig {
            league_code: "mls",
            poly_prefix: "mls",
            kalshi_series_game: "KXMLSGAME",
            kalshi_series_spread: None,
            kalshi_series_total: None,
            kalshi_series_btts: None,
        },
        LeagueConfig {
            league_code: "ncaaf",
            poly_prefix: "cfb",
            kalshi_series_game: "KXNCAAFGAME",
            kalshi_series_spread: Some("KXNCAAFSPREAD"),
            kalshi_series_total: Some("KXNCAAFTOTAL"),
            kalshi_series_btts: None,
        },
    ]
}

/// Get config for a specific league.
pub fn get_league_config(league: &str) -> Option<LeagueConfig> {
    get_league_configs()
        .into_iter()
        .find(|c| c.league_code == league || c.poly_prefix == league)
}
