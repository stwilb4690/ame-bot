//! AME Bot — Adaptive Market Engine v2.0
//!
//! Modes (APP_MODE env var):
//!   kalshi-only  — Kalshi WebSocket only; same-platform arb monitoring
//!   monitor      — Both WebSockets; cross-platform spread display, no execution (DEFAULT)
//!   full         — Both WebSockets + cross-platform arb execution
//!
//! Run with `--diagnose` (or DIAGNOSE=1) for a 60-second connectivity test.

mod cache;
mod circuit_breaker;
mod config;
mod diagnose;
mod discovery;
mod execution;
mod kalshi;
mod monitor;
mod polymarket;
mod polymarket_clob;
mod polymarket_us;
mod position_tracker;
mod risk_manager;
mod state_writer;
mod telegram;
mod types;

use anyhow::{Context, Result};
use clap::Parser;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tracing::{error, info, warn};
use chrono::Utc;

use cache::TeamCache;
use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use config::{AppMode, arb_threshold, ENABLED_LEAGUES, WS_RECONNECT_DELAY_SECS, app_mode, state_write_interval_secs, heartbeat_interval_secs};
use discovery::DiscoveryClient;
use execution::{ExecutionEngine, create_execution_channel, run_execution_loop};
use kalshi::{KalshiConfig, KalshiApiClient};
use polymarket_clob::{PolymarketAsyncClient, PreparedCreds, SharedAsyncClient};
use polymarket_us::PolymarketUsClient;
use monitor::run_position_monitor;
use position_tracker::{PositionTracker, create_position_channel, position_writer_loop};
use state_writer::{ConfigOverrides, StateWriter, run_state_writer};
use telegram::TelegramClient;
use types::{GlobalState, PriceCents, kalshi_effective_fee_cents};

/// Polymarket CLOB API host
const POLY_CLOB_HOST: &str = "https://clob.polymarket.com";
/// Polygon chain ID
const POLYGON_CHAIN_ID: u64 = 137;

// === CLI ===

#[derive(Parser, Debug)]
#[command(name = "ame-bot", about = "AME Bot — Adaptive Market Engine")]
struct Cli {
    /// Run diagnostic mode: connect, print market table for 60s, exit.
    /// Equivalent to setting DIAGNOSE=1.
    #[arg(long, default_value_t = false)]
    diagnose: bool,

    /// Test Polymarket US Ed25519 auth: call GET /v1/account/balances, print result, exit.
    /// Requires POLY_US_API_KEY and POLY_US_API_SECRET to be set.
    #[arg(long, default_value_t = false)]
    test_poly_us: bool,
}

// === Main ===

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env if present
    dotenvy::dotenv().ok();

    // Parse CLI
    let cli = Cli::parse();

    // --test-poly-us: verify Ed25519 auth against api.polymarket.us, then exit.
    if cli.test_poly_us {
        let key = std::env::var("POLY_US_API_KEY")
            .context("POLY_US_API_KEY must be set for --test-poly-us")?;
        let secret = std::env::var("POLY_US_API_SECRET")
            .context("POLY_US_API_SECRET must be set for --test-poly-us")?;
        let client = PolymarketUsClient::new(&key, &secret)
            .context("Failed to construct PolymarketUsClient")?;
        println!("[POLY-US] Calling GET /v1/account/balances...");
        match client.get_balances().await {
            Ok(resp) => {
                println!("[POLY-US] Success:");
                println!("{}", serde_json::to_string_pretty(&resp.data).unwrap_or_else(|_| format!("{:?}", resp.data)));
            }
            Err(e) => {
                eprintln!("[POLY-US] Error: {}", e);
                std::process::exit(1);
            }
        }
        return Ok(());
    }

    let diagnose_mode = cli.diagnose
        || std::env::var("DIAGNOSE").map(|v| v == "1" || v == "true").unwrap_or(false);
    let diagnose_secs: u64 = std::env::var("DIAGNOSE_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);

    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("ame_bot=info".parse().unwrap()),
        )
        .init();

    let mode = app_mode();
    let version = env!("CARGO_PKG_VERSION");

    info!("🚀 AME Bot v{}", version);
    info!("   Mode: {}", mode);
    info!("   Env:  {}", std::env::var("APP_ENV").unwrap_or_else(|_| "demo".into()));
    info!("   Kalshi API: {}", config::kalshi_api_base());

    if diagnose_mode {
        info!("   ⚙️  Diagnose mode — will exit after {}s", diagnose_secs);
    }

    // === Dry-run mode ===
    let dry_run = std::env::var("DRY_RUN")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(true);
    if dry_run {
        info!("   DRY RUN enabled (set DRY_RUN=0 to execute)");
    } else {
        warn!("   LIVE EXECUTION mode");
    }

    // === Load Kalshi credentials (always required) ===
    let kalshi_config = KalshiConfig::from_env()
        .context("Failed to load Kalshi credentials (KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH required)")?;
    info!("[KALSHI] API key loaded");

    // === Polymarket US Retail API client (optional, Ed25519 auth) ===
    let poly_us_client: Option<Arc<PolymarketUsClient>> =
        match (std::env::var("POLY_US_API_KEY"), std::env::var("POLY_US_API_SECRET")) {
            (Ok(key), Ok(secret)) if !key.is_empty() && !secret.is_empty() => {
                match PolymarketUsClient::new(&key, &secret) {
                    Ok(client) => {
                        info!("[POLY-US] Retail API client initialised (key={}...)", &key[..key.len().min(8)]);
                        Some(Arc::new(client))
                    }
                    Err(e) => {
                        warn!("[POLY-US] Failed to initialise client: {}", e);
                        None
                    }
                }
            }
            _ => {
                info!("[POLY-US] Not configured (set POLY_US_API_KEY + POLY_US_API_SECRET to enable)");
                None
            }
        };

    // === Load Polymarket CLOB credentials (required in full mode only when US client is absent) ===
    let poly_async: Option<Arc<SharedAsyncClient>> = if mode == AppMode::Full {
        match (std::env::var("POLY_PRIVATE_KEY"), std::env::var("POLY_FUNDER")) {
            (Ok(pk), Ok(funder)) if !pk.is_empty() && !funder.is_empty() => {
                info!("[POLYMARKET] Initialising CLOB client...");
                let client = PolymarketAsyncClient::new(
                    POLY_CLOB_HOST,
                    POLYGON_CHAIN_ID,
                    &pk,
                    &funder,
                )?;
                let api_creds = client.derive_api_key(0).await?;
                let prepared = PreparedCreds::from_api_creds(&api_creds)?;
                let shared = Arc::new(SharedAsyncClient::new(client, prepared, POLYGON_CHAIN_ID));
                match shared.load_cache(".clob_market_cache.json") {
                    Ok(n) => info!("[POLYMARKET] Loaded {} neg_risk entries", n),
                    Err(e) => warn!("[POLYMARKET] No neg_risk cache: {}", e),
                }
                info!("[POLYMARKET] Client ready");
                Some(shared)
            }
            _ => {
                if poly_us_client.is_none() {
                    warn!("[POLYMARKET] No CLOB credentials (POLY_PRIVATE_KEY/POLY_FUNDER) and no US client — Polymarket execution unavailable in full mode");
                } else {
                    info!("[POLYMARKET] CLOB credentials not set — using US client for Polymarket execution");
                }
                None
            }
        }
    } else {
        if mode == AppMode::Monitor {
            match (std::env::var("POLY_PRIVATE_KEY"), std::env::var("POLY_FUNDER")) {
                (Ok(pk), Ok(funder)) if !pk.is_empty() && !funder.is_empty() => {
                    info!("[POLYMARKET] Credentials present but mode=monitor — read-only WebSocket only");
                }
                _ => {
                    info!("[POLYMARKET] No credentials — read-only WebSocket (prices only, no execution)");
                }
            }
        }
        None
    };

    // === Telegram client (optional) ===
    let telegram = TelegramClient::from_env();
    if telegram.is_some() {
        info!("[TELEGRAM] Bot configured");
    } else {
        info!("[TELEGRAM] Not configured (set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID to enable)");
    }

    // === Market discovery ===
    let team_cache = TeamCache::load();
    info!("📂 Loaded {} team mappings", team_cache.len());

    let kalshi_api = Arc::new(KalshiApiClient::new(kalshi_config.clone()));
    let discovery = DiscoveryClient::new(
        KalshiApiClient::new(kalshi_config.clone()),
        team_cache,
        poly_us_client.clone(),
    );

    let force_discovery = std::env::var("FORCE_DISCOVERY")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);

    info!("🔍 Discovering markets{}...",
        if force_discovery { " (forced refresh)" } else { "" });

    let result = if force_discovery {
        discovery.discover_all_force(ENABLED_LEAGUES).await
    } else {
        discovery.discover_all(ENABLED_LEAGUES).await
    };

    info!("📊 Discovery: {} market pairs", result.pairs.len());
    for e in &result.errors {
        warn!("   ⚠️  {}", e);
    }

    // === Build GlobalState ===
    let state = Arc::new({
        let mut s = GlobalState::new();
        for pair in result.pairs {
            s.add_pair(pair);
        }

        // Kalshi-only extra series (non-sports markets)
        let extra_series: Vec<String> = std::env::var("KALSHI_EXTRA_SERIES")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if !extra_series.is_empty() {
            info!("🔍 Discovering Kalshi-only series: {:?}", extra_series);
            let series_strs: Vec<&str> = extra_series.iter().map(|s| s.as_str()).collect();
            let kalshi_only = discovery.discover_kalshi_only_series(&series_strs).await;
            info!("   Found {} Kalshi-only markets", kalshi_only.len());
            for km in kalshi_only {
                s.add_kalshi_only(&km.ticker, &km.description);
            }
        }

        info!("📡 Tracking {} total markets", s.market_count());
        s
    });

    // === Config overrides (shared with state writer) ===
    let overrides = Arc::new(RwLock::new(ConfigOverrides {
        dry_run,
        ..ConfigOverrides::default()
    }));

    // === Circuit breaker ===
    let circuit_breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig::from_env()));
    circuit_breaker.start_background_tasks();

    // === Position tracker (shared between execution engine and state writer) ===
    let position_tracker: Option<Arc<RwLock<PositionTracker>>> = if mode == AppMode::Full {
        Some(Arc::new(RwLock::new(PositionTracker::new())))
    } else {
        None
    };

    // === State writer ===
    let state_writer = Arc::new(StateWriter::new(
        state.clone(),
        overrides.clone(),
        circuit_breaker.clone(),
        poly_us_client.clone(),
        Some(kalshi_api.clone()),
        position_tracker.clone(),
    ));
    let kalshi_connected_flag = Arc::clone(&state_writer.kalshi_connected);
    let poly_connected_flag = Arc::clone(&state_writer.poly_connected);

    // === Shutdown channel ===
    let (shutdown_tx, _) = broadcast::channel::<()>(4);

    // === Diagnose mode (connect WS, print table, exit) ===
    if diagnose_mode {
        info!("[DIAGNOSE] Connecting Kalshi WebSocket...");
        let diag_state = state.clone();
        let diag_kalshi_config = kalshi_config.clone();
        let threshold_cents: PriceCents = ((arb_threshold() * 100.0).round() as u16).max(1);

        // Dummy exec channel for the WebSocket call
        let (diag_exec_tx, _diag_exec_rx) = create_execution_channel();

        let _ws_handle = tokio::spawn(async move {
            if let Err(e) = kalshi::run_ws(
                &diag_kalshi_config, diag_state, diag_exec_tx, threshold_cents,
            ).await {
                warn!("[DIAGNOSE] Kalshi WS: {}", e);
            }
        });

        // Give WebSocket a few seconds to receive prices
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        diagnose::run_diagnose(state.clone(), diagnose_secs).await;
        return Ok(());
    }

    // === Execution infrastructure (only for full mode) ===
    let threshold_cents: PriceCents = ((arb_threshold() * 100.0).round() as u16).max(1);
    info!("   Threshold: {}¢", threshold_cents);

    let (exec_tx, exec_rx) = create_execution_channel();

    if mode == AppMode::Full {
        let position_tracker = position_tracker.clone().expect("position_tracker set above for Full mode");
        let (position_channel, position_rx) = create_position_channel();
        tokio::spawn(position_writer_loop(position_rx, position_tracker.clone()));

        // Position monitor — checks open positions every 30 s for early-exit opportunities.
        {
            let mon_tracker  = position_tracker.clone();
            let mon_state    = state.clone();
            let mon_kalshi   = kalshi_api.clone();
            let mon_poly_async = poly_async.clone();
            let mon_poly_us  = poly_us_client.clone();
            let mon_shutdown = shutdown_tx.subscribe();
            tokio::spawn(async move {
                run_position_monitor(
                    mon_tracker, mon_state, mon_kalshi,
                    mon_poly_async, mon_poly_us,
                    None, // no RiskManager in arb engine; signal_trader wires its own
                    dry_run, mon_shutdown,
                ).await;
            });
        }

        let poly_async_for_engine = poly_async.clone()
            ;

        let engine = Arc::new(ExecutionEngine::new(
            kalshi_api.clone(),
            poly_async_for_engine,
            poly_us_client.clone(),
            state.clone(),
            circuit_breaker.clone(),
            position_channel,
            telegram.clone(),
            dry_run,
        ));

        tokio::spawn(run_execution_loop(exec_rx, engine));
    } else {
        // In monitor/kalshi-only modes there is no execution engine.
        // Drain the channel so WebSocket senders never block on a full buffer.
        tokio::spawn(async move {
            let mut rx = exec_rx;
            while rx.recv().await.is_some() {
                // Intentionally discard — no execution in this mode.
            }
        });
    }

    // === Kalshi WebSocket ===
    let kalshi_state = state.clone();
    let kalshi_exec_tx = exec_tx.clone();
    let kalshi_ws_config = kalshi_config.clone();
    let kalshi_threshold = threshold_cents;
    let kalshi_connected_flag2 = Arc::clone(&kalshi_connected_flag);
    let mut kalshi_shutdown = shutdown_tx.subscribe();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = kalshi::run_ws(&kalshi_ws_config, kalshi_state.clone(), kalshi_exec_tx.clone(), kalshi_threshold) => {
                    if let Err(e) = result {
                        error!("[KALSHI] Disconnected: {} — reconnecting in {}s", e, WS_RECONNECT_DELAY_SECS);
                    }
                    kalshi_connected_flag2.store(false, std::sync::atomic::Ordering::Relaxed);
                }
                _ = kalshi_shutdown.recv() => { break; }
            }
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(WS_RECONNECT_DELAY_SECS)) => {}
                _ = kalshi_shutdown.recv() => { break; }
            }
        }
        info!("[KALSHI] WebSocket task stopped");
    });

    // === Polymarket WebSocket (monitor or full mode) ===
    if mode != AppMode::KalshiOnly {
        let poly_state = state.clone();
        let poly_exec_tx = exec_tx.clone();
        let poly_threshold = threshold_cents;
        let poly_connected_flag2 = Arc::clone(&poly_connected_flag);
        let mut poly_shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = polymarket::run_ws(poly_state.clone(), poly_exec_tx.clone(), poly_threshold) => {
                        if let Err(e) = result {
                            error!("[POLYMARKET] Disconnected: {} — reconnecting in {}s", e, WS_RECONNECT_DELAY_SECS);
                        }
                        poly_connected_flag2.store(false, std::sync::atomic::Ordering::Relaxed);
                    }
                    _ = poly_shutdown.recv() => { break; }
                }
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(WS_RECONNECT_DELAY_SECS)) => {}
                    _ = poly_shutdown.recv() => { break; }
                }
            }
            info!("[POLYMARKET] WebSocket task stopped");
        });
    }

    // === State writer background task ===
    {
        let sw = Arc::clone(&state_writer);
        let interval = state_write_interval_secs();
        let sw_shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            run_state_writer(sw, interval, sw_shutdown).await;
        });
    }

    // === Kalshi opportunity scanner (non-hot-path scan) ===
    {
        let scan_state = state.clone();
        let tg = telegram.clone();
        let spread_threshold = config::wide_spread_threshold();
        let mut scan_shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        scan_markets(&scan_state, tg.as_ref(), spread_threshold);
                    }
                    _ = scan_shutdown.recv() => { break; }
                }
            }
        });
    }

    // === Heartbeat ===
    {
        let hb_state = state.clone();
        let hb_threshold = threshold_cents;
        let tg = telegram.clone();
        let hb_writer = Arc::clone(&state_writer);
        let hb_interval = heartbeat_interval_secs();
        let mut hb_shutdown = shutdown_tx.subscribe();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(tokio::time::Duration::from_secs(60));
            let mut last_logged: Option<std::time::Instant> = None;
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let should_log = last_logged
                            .map(|t| t.elapsed().as_secs() >= hb_interval)
                            .unwrap_or(true);
                        run_heartbeat(
                            &hb_state, hb_threshold, tg.as_ref(),
                            &hb_writer, should_log,
                        ).await;
                        if should_log {
                            last_logged = Some(std::time::Instant::now());
                        }
                    }
                    _ = hb_shutdown.recv() => { break; }
                }
            }
        });
    }

    // === Send startup Telegram notification ===
    if let Some(tg) = &telegram {
        tg.alert_startup(&mode.to_string(), version);
    }

    // === Graceful shutdown ===
    wait_for_shutdown().await;
    info!("🛑 Shutdown signal received — stopping...");

    if let Some(tg) = &telegram {
        tg.alert_shutdown("SIGINT/SIGTERM");
    }

    // Broadcast shutdown to all tasks
    let _ = shutdown_tx.send(());

    // Give tasks a moment to write final state
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    info!("✅ AME Bot stopped cleanly.");
    Ok(())
}

// === Kalshi opportunity scanner ===

fn scan_markets(
    state: &GlobalState,
    telegram: Option<&Arc<TelegramClient>>,
    spread_threshold: u16,
) {
    let count = state.market_count();
    for i in 0..count {
        if let Some(market) = state.get_by_id(i as u16) {
            let opps = market.check_kalshi_opps(spread_threshold);

            if let Some(ref arb) = opps.same_platform {
                if let Some(tg) = telegram {
                    let (ticker, desc) = market_info(market);
                    tg.alert_arb(
                        market.market_id, &desc, &ticker,
                        arb.yes_ask, arb.no_ask, arb.profit_cents,
                        "same-platform",
                    );
                }
            }

            if let Some(ref ws) = opps.wide_spread {
                if let Some(tg) = telegram {
                    let (ticker, desc) = market_info(market);
                    tg.alert_wide_spread(
                        market.market_id, &desc, &ticker,
                        ws.yes_ask, ws.no_ask, ws.spread_cents,
                    );
                }
            }
        }
    }
}

fn market_info(market: &types::AtomicMarketState) -> (String, String) {
    match &market.pair {
        Some(p) => (p.kalshi_market_ticker.to_string(), p.description.to_string()),
        None => (format!("market_{}", market.market_id), format!("market_{}", market.market_id)),
    }
}

// === Heartbeat ===

async fn run_heartbeat(
    state: &GlobalState,
    threshold_cents: PriceCents,
    telegram: Option<&Arc<TelegramClient>>,
    writer: &state_writer::StateWriter,
    log_summary: bool,
) {
    let market_count = state.market_count();
    let mut with_kalshi = 0u32;
    let mut with_poly = 0u32;
    let mut with_both = 0u32;
    let mut best_arb: Option<(u16, u16, u16, u16, u16, u16, u16, bool)> = None;

    for market in state.markets.iter().take(market_count) {
        let (k_yes, k_no, _, _) = market.kalshi.load();
        let (p_yes, p_no, _, _) = market.poly.load();
        let has_k = k_yes > 0 && k_no > 0;
        let has_p = p_yes > 0 && p_no > 0;
        if k_yes > 0 || k_no > 0 { with_kalshi += 1; }
        if p_yes > 0 || p_no > 0 { with_poly += 1; }
        if has_k && has_p {
            with_both += 1;
            let fee1 = kalshi_effective_fee_cents(k_no);
            let cost1 = p_yes + k_no + fee1;
            let fee2 = kalshi_effective_fee_cents(k_yes);
            let cost2 = k_yes + fee2 + p_no;
            let (best_cost, best_fee, is_poly_yes) = if cost1 <= cost2 {
                (cost1, fee1, true)
            } else {
                (cost2, fee2, false)
            };
            if best_arb.is_none() || best_cost < best_arb.as_ref().unwrap().0 {
                best_arb = Some((best_cost, market.market_id, p_yes, k_no, k_yes, p_no, best_fee, is_poly_yes));
            }
        }
    }

    // Always log immediately when best cross-platform spread is below threshold.
    if let Some((cost, mid, p_yes, k_no, k_yes, p_no, fee, is_poly_yes)) = best_arb {
        if cost < threshold_cents {
            let desc = state.get_by_id(mid)
                .and_then(|m| m.pair.as_ref())
                .map(|p| &*p.description)
                .unwrap_or("Unknown");
            let leg_str = if is_poly_yes {
                format!("P_yes({}¢)+K_no({}¢)+fee({}¢)={}¢", p_yes, k_no, fee, cost)
            } else {
                format!("K_yes({}¢)+P_no({}¢)+fee({}¢)={}¢", k_yes, p_no, fee, cost)
            };
            let gap = cost as i16 - threshold_cents as i16;
            info!("🎯 Opportunity | {} | {} | gap={:+}¢", desc, leg_str, gap);
        }
    }

    if !log_summary {
        return;
    }

    // Full status summary (logged at most every HEARTBEAT_INTERVAL_SECS).
    let uptime_str = format_uptime(writer.uptime_secs());
    let k_bal = format_balance_dollars(writer.kalshi_balance_cents_opt());
    let p_bal = format_balance_dollars(writer.poly_balance_cents_opt());
    let trades = count_trades_today().await;
    let best_spread_str = match best_arb {
        Some((cost, mid, _, _, _, _, _, _)) => {
            let desc = state.get_by_id(mid)
                .and_then(|m| m.pair.as_ref())
                .map(|p| p.description.to_string())
                .unwrap_or_else(|| "Unknown".to_string());
            format!("{}¢ ({})", cost, desc)
        }
        None => "none".to_string(),
    };

    info!(
        "💓 Status | uptime: {} | markets: {} dual-feed | balances: K={} P={} | threshold: {}¢ | best spread: {} | daily P&L: +$0.00 | trades today: {}",
        uptime_str, with_both, k_bal, p_bal, threshold_cents, best_spread_str, trades
    );

    if let Some((cost, _mid, p_yes, k_no, k_yes, p_no, fee, is_poly_yes)) = best_arb {
        let gap = cost as i16 - threshold_cents as i16;
        let leg_str = if is_poly_yes {
            format!("P_yes({}¢)+K_no({}¢)+fee({}¢)={}¢", p_yes, k_no, fee, cost)
        } else {
            format!("K_yes({}¢)+P_no({}¢)+fee({}¢)={}¢", k_yes, p_no, fee, cost)
        };
        if let Some(tg) = telegram {
            let summary = format!(
                "Markets: {} total | {}/{}/{} K/P/Both\nBest spread: {} | gap={:+}¢",
                market_count, with_kalshi, with_poly, with_both, leg_str, gap
            );
            tg.alert_heartbeat(&summary);
        }
    } else if with_both == 0 {
        warn!("   ⚠️  No markets with both Kalshi + Poly prices");
    }
}

fn format_uptime(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    if h > 0 { format!("{}h {}m", h, m) } else { format!("{}m", m) }
}

fn format_balance_dollars(cents: Option<i64>) -> String {
    match cents {
        Some(c) => format!("${:.2}", c as f64 / 100.0),
        None => "?".to_string(),
    }
}

/// Count entries in state/trades.jsonl whose timestamp starts with today's UTC date.
async fn count_trades_today() -> usize {
    let today = Utc::now().format("%Y-%m-%d").to_string();
    match tokio::fs::read_to_string("state/trades.jsonl").await {
        Ok(content) => content
            .lines()
            .filter(|l| l.contains(&format!("\"timestamp\":\"{}",  today)))
            .count(),
        Err(_) => 0,
    }
}

// === Signal handling ===

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to register SIGINT");
        let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM");
        tokio::select! {
            _ = sigint.recv() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
    }
}
