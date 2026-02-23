// src/bin/signal_trader.rs
//
// AME Signal Trader — Kalshi-only signal trading using Polymarket international
// as a fair-value oracle.
//
// Usage:
//   source .env
//   SIGNAL_DRY_RUN=true cargo run --release --bin signal-trader
//
// Required env vars: KALSHI_API_KEY_ID, KALSHI_PRIVATE_KEY_PATH
// Optional:          TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
//                    SIGNAL_* (thresholds), VAR_* (risk limits)

use ame_bot::{
    cache::TeamCache,
    circuit_breaker::{CircuitBreaker, CircuitBreakerConfig},
    config::{ENABLED_LEAGUES, WS_RECONNECT_DELAY_SECS},
    data_collector::DataCollector,
    discovery::DiscoveryClient,
    kalshi::{KalshiConfig, KalshiApiClient},
    risk_manager::{RiskConfig, RiskManager},
    signal_trader::{SignalConfig, SignalTrader},
    telegram::TelegramClient,
    types::{GlobalState, FastExecutionRequest, MarketPair},
};

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("ame_bot=info".parse().unwrap()),
        )
        .init();

    let version = env!("CARGO_PKG_VERSION");
    info!("🚀 AME Signal Trader v{}", version);
    info!("   Env: {}", std::env::var("APP_ENV").unwrap_or_else(|_| "demo".into()));
    info!("   Kalshi API: {}", ame_bot::config::kalshi_api_base());

    // ── Signal config ────────────────────────────────────────────────────────
    let signal_config = SignalConfig::from_env();

    if signal_config.dry_run {
        info!("   [DRY RUN] — logging signals only, no live orders (set SIGNAL_DRY_RUN=0 to execute)");
    } else {
        warn!("   [LIVE] — live Kalshi orders enabled");
    }

    info!("   thresholds: sports_pregame={}¢ sports_live={}¢ crypto={}¢ economic={}¢ | convergence_exit={}¢ | stop_loss={}×",
          signal_config.pregame_threshold_cents,
          signal_config.live_threshold_cents,
          signal_config.crypto_threshold_cents,
          signal_config.economic_threshold_cents,
          signal_config.convergence_exit_cents,
          signal_config.stop_loss_multiplier);
    info!("   freshness_window={}s | time_stop={}min",
          signal_config.freshness_window_secs,
          signal_config.time_stop_minutes);

    // ── Kalshi credentials ───────────────────────────────────────────────────
    let kalshi_config = KalshiConfig::from_env()
        .context("Kalshi credentials required (KALSHI_API_KEY_ID + KALSHI_PRIVATE_KEY_PATH)")?;
    info!("[KALSHI] API key loaded");

    // ── Market discovery — NO Poly US client so we get ALL Kalshi/Poly-intl pairs ──
    let team_cache = TeamCache::load();
    info!("📂 {} team mappings loaded", team_cache.len());

    let discovery = Arc::new(DiscoveryClient::new(
        KalshiApiClient::new(kalshi_config.clone()),
        team_cache,
        None, // Don't filter by Poly US — signal trader uses all Poly-intl pairs
    ));

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

    info!("📊 Discovery: {} sports pairs found", result.pairs.len());
    for e in &result.errors {
        warn!("   ⚠️  {}", e);
    }

    // ── Filter three-way soccer moneyline markets ────────────────────────────
    let (binary_pairs, filtered_count) = filter_three_way_markets(result.pairs);
    info!("Filtered {} three-way soccer moneyline markets, keeping {} binary sports markets",
          filtered_count, binary_pairs.len());
    let sports_count = binary_pairs.len();

    // ── Crypto / economic discovery ───────────────────────────────────────────
    let crypto_eco_result = discovery.discover_crypto_economic().await;
    info!("📊 Discovery: {} crypto/economic pairs found", crypto_eco_result.pairs.len());
    for e in &crypto_eco_result.errors {
        warn!("   ⚠️  {}", e);
    }
    let crypto_eco_count = crypto_eco_result.pairs.len();

    // ── Short-duration crypto discovery ───────────────────────────────────────
    // 15-minute and hourly BTC/ETH/SOL up/down markets — highest-frequency arb opportunity.
    // This runs at startup to capture currently-active short-duration markets, AND
    // every 10 minutes to log newly listed tickers (for analysis).
    info!("🔍 Discovering short-duration crypto markets (15m / 1h BTC/ETH/SOL)...");
    let short_result = discovery.discover_short_duration_crypto().await;
    let short_count  = short_result.pairs.len();
    info!("📊 Discovery: {} short-duration pairs found ({} Kalshi-only with no Gamma match)",
          short_count, short_result.poly_misses);

    if sports_count == 0 && crypto_eco_count == 0 && short_count == 0 {
        warn!("[SIGNAL] No market pairs discovered — bot will monitor but not trade");
    }

    // ── Build GlobalState ────────────────────────────────────────────────────
    let state = Arc::new({
        let mut s = GlobalState::new();
        for pair in binary_pairs {
            s.add_pair(pair);
        }
        for pair in crypto_eco_result.pairs {
            s.add_pair(pair);
        }
        for pair in short_result.pairs {
            s.add_pair(pair);
        }
        info!("📡 Tracking {} markets ({} sports + {} crypto/eco + {} short-duration)",
              s.market_count(), sports_count, crypto_eco_count, short_count);
        s
    });

    // ── Support services ─────────────────────────────────────────────────────
    let telegram = TelegramClient::from_env();
    if telegram.is_some() {
        info!("[TELEGRAM] Configured");
    } else {
        info!("[TELEGRAM] Not configured (set TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID to enable)");
    }

    let circuit_breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig::from_env()));
    let kalshi_api      = Arc::new(KalshiApiClient::new(kalshi_config.clone()));

    // ── Risk manager ─────────────────────────────────────────────────────────
    // Fetches current Kalshi balance at startup and computes all dollar limits.
    // Environment variables (all optional, defaults shown):
    //   VAR_PORTFOLIO_PCT=9      VAR_SINGLE_PCT=2       VAR_CORRELATED_PCT=3
    //   VAR_DAILY_DRAWDOWN_PCT=5 VAR_TRAILING_STOP_PCT=2 VAR_RESERVE_FLOOR_PCT=18
    let risk_config  = RiskConfig::from_env();
    let risk_manager = RiskManager::new(risk_config, kalshi_api.clone(), telegram.clone())
        .await
        .context("Failed to initialise risk manager (Kalshi balance fetch failed)")?;

    // Spawn background task: re-fetches balance every 5 minutes and adjusts limits.
    risk_manager.start_refresh_loop();

    // ── Data collector ───────────────────────────────────────────────────────
    let data_collector = DataCollector::new("state");
    info!("[DATA] Collecting to state/spread_observations_*.jsonl, signal_events_*.jsonl, trade_outcomes_*.jsonl");

    // ── WebSocket connection status flags ────────────────────────────────────
    let ws_kalshi_connected = Arc::new(AtomicBool::new(false));
    let ws_poly_connected   = Arc::new(AtomicBool::new(false));

    // ── Signal trader ────────────────────────────────────────────────────────
    let trader = Arc::new(SignalTrader::new(
        signal_config,
        state.clone(),
        kalshi_api.clone(),
        telegram.clone(),
        circuit_breaker.clone(),
        risk_manager.clone(),
        data_collector,
        ws_kalshi_connected.clone(),
        ws_poly_connected.clone(),
    ));

    // ── Shutdown channel ─────────────────────────────────────────────────────
    let (shutdown_tx, _) = broadcast::channel::<()>(4);

    // ── Shared exec channel — drained; WS runners require it but signal trader
    //    drives its own execution loop separately ──────────────────────────────
    let (exec_tx, mut exec_rx) = mpsc::channel::<FastExecutionRequest>(16);
    tokio::spawn(async move {
        while exec_rx.recv().await.is_some() {
            // intentionally discard — signal trader is not an arb execution engine
        }
    });

    // ── Kalshi WebSocket ─────────────────────────────────────────────────────
    {
        let ws_state     = state.clone();
        let ws_config    = kalshi_config.clone();
        let ws_tx        = exec_tx.clone();
        let ws_connected = ws_kalshi_connected.clone();
        let mut ws_shutdown = shutdown_tx.subscribe();

        tokio::spawn(async move {
            loop {
                ws_connected.store(true, Ordering::Relaxed);
                tokio::select! {
                    result = ame_bot::kalshi::run_ws(
                        &ws_config, ws_state.clone(), ws_tx.clone(),
                        u16::MAX, // disable arb detection threshold
                    ) => {
                        ws_connected.store(false, Ordering::Relaxed);
                        if let Err(e) = result {
                            error!("[KALSHI] Disconnected: {} — reconnecting in {}s",
                                   e, WS_RECONNECT_DELAY_SECS);
                        }
                    }
                    _ = ws_shutdown.recv() => {
                        ws_connected.store(false, Ordering::Relaxed);
                        break;
                    }
                }
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(WS_RECONNECT_DELAY_SECS)) => {}
                    _ = ws_shutdown.recv() => break,
                }
            }
            info!("[KALSHI] WebSocket task stopped");
        });
    }

    // ── Polymarket international WebSocket (price oracle) ────────────────────
    {
        let poly_state     = state.clone();
        let poly_tx        = exec_tx.clone();
        let poly_connected = ws_poly_connected.clone();
        let mut poly_shutdown = shutdown_tx.subscribe();

        tokio::spawn(async move {
            loop {
                poly_connected.store(true, Ordering::Relaxed);
                tokio::select! {
                    result = ame_bot::polymarket::run_ws(
                        poly_state.clone(), poly_tx.clone(),
                        u16::MAX, // disable arb detection threshold
                    ) => {
                        poly_connected.store(false, Ordering::Relaxed);
                        if let Err(e) = result {
                            error!("[POLY] Disconnected: {} — reconnecting in {}s",
                                   e, WS_RECONNECT_DELAY_SECS);
                        }
                    }
                    _ = poly_shutdown.recv() => break,
                }
                tokio::select! {
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(WS_RECONNECT_DELAY_SECS)) => {}
                    _ = poly_shutdown.recv() => break,
                }
            }
            info!("[POLY] WebSocket task stopped");
        });
    }

    // ── Signal scan loop (1-second cadence) ──────────────────────────────────
    {
        let scan_trader   = trader.clone();
        let mut scan_shutdown = shutdown_tx.subscribe();

        tokio::spawn(async move {
            // Give WebSockets time to deliver initial snapshots and begin streaming.
            // 30s ensures both feeds have had multiple opportunities to push updates,
            // so the per-market warmup counters can accumulate meaningful data.
            info!("[SIGNAL] 30s startup warmup — letting websocket prices stabilize");
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
            info!("[SIGNAL] Starting signal scan loop (1s cadence)");

            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        scan_trader.scan().await;
                    }
                    _ = scan_shutdown.recv() => break,
                }
            }
            info!("[SIGNAL] Scan loop stopped");
        });
    }

    // ── Spread snapshot collection every 30 seconds ───────────────────────────
    {
        let snap_trader   = trader.clone();
        let mut snap_shutdown = shutdown_tx.subscribe();

        tokio::spawn(async move {
            // Wait for initial prices before first snapshot
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        snap_trader.collect_spread_snapshots().await;
                    }
                    _ = snap_shutdown.recv() => break,
                }
            }
        });
    }

    // ── Spread summary every 5 minutes (for debugging) ───────────────────────
    {
        let summary_trader = trader.clone();
        let mut summary_shutdown = shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        summary_trader.log_spread_summary().await;
                    }
                    _ = summary_shutdown.recv() => break,
                }
            }
        });
    }

    // ── Short-duration refresh (every 10 minutes) ────────────────────────────
    // Re-runs discover_short_duration_crypto() to surface any new 15m/1h markets
    // that appeared since startup. Newly found tickers are logged — the currently
    // running WebSocket connections won't pick them up automatically, but the logs
    // tell us exactly what to expect on the next restart.
    {
        let disc_refresh  = discovery.clone();
        let state_refresh = state.clone();
        let mut refresh_shutdown = shutdown_tx.subscribe();

        tokio::spawn(async move {
            // First refresh after 10 minutes
            let mut tick = tokio::time::interval(tokio::time::Duration::from_secs(10 * 60));
            // Skip the immediate first tick
            tick.tick().await;
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        info!("[SHORT-REFRESH] 🔄 Running 10-minute short-duration refresh...");
                        let fresh = disc_refresh.discover_short_duration_crypto().await;
                        let known = state_refresh.market_count();
                        let mut new_count   = 0usize;
                        let mut still_found = 0usize;
                        for pair in &fresh.pairs {
                            let ticker = &*pair.kalshi_market_ticker;
                            let already_tracked = (0..known).any(|i| {
                                state_refresh.get_by_id(i as u16)
                                    .and_then(|m| m.pair.as_ref())
                                    .map(|p| &*p.kalshi_market_ticker == ticker)
                                    .unwrap_or(false)
                            });
                            if already_tracked {
                                still_found += 1;
                            } else {
                                new_count += 1;
                                info!("[SHORT-REFRESH] 🆕 New ticker not in state (restart to track): {}",
                                      pair.kalshi_market_ticker);
                            }
                        }
                        info!("[SHORT-REFRESH] {} total found | {} still tracked | {} new (need restart) | {} Kalshi-only",
                              fresh.pairs.len(), still_found, new_count, fresh.poly_misses);
                    }
                    _ = refresh_shutdown.recv() => break,
                }
            }
            info!("[SHORT-REFRESH] Refresh task stopped");
        });
    }

    // ── Heartbeat (hourly) ────────────────────────────────────────────────────
    {
        let hb_trader = trader.clone();
        let tg        = telegram.clone();
        let mut hb_shutdown = shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut tick = tokio::time::interval(tokio::time::Duration::from_secs(3600));
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let open = hb_trader.open_position_count().await;
                        let risk = hb_trader.risk_summary().await;
                        let cat_stats = hb_trader.category_stats_snapshot().await;

                        // Build per-category P&L lines
                        let mut cat_lines = String::new();
                        for (cat, stats) in &cat_stats {
                            if stats.trades_entered > 0 || stats.pnl_dollars != 0.0 {
                                cat_lines.push_str(&format!(
                                    "\n  {}: {} trades | P&L ${:.2}",
                                    cat, stats.trades_entered, stats.pnl_dollars,
                                ));
                            }
                        }

                        // Check today's observation file size
                        let obs_size = tokio::fs::metadata(
                            format!("state/spread_observations_{}.jsonl",
                                chrono::Utc::now().format("%Y-%m-%d"))
                        ).await.map(|m| m.len()).unwrap_or(0);

                        let summary = format!(
                            "Signal Trader\n\
                             Open positions: {}\n\
                             Daily P&L: ${:.2} | HWM: ${:.2}\n\
                             Portfolio VAR: ${:.2}/${:.2}\n\
                             Balance: ${:.2} | Mode: {}\n\
                             Obs file: {} bytes{}",
                            open,
                            risk.daily_pnl, risk.high_water_mark,
                            risk.portfolio_var, risk.max_portfolio_var,
                            risk.balance_dollars, risk.risk_mode,
                            obs_size,
                            cat_lines,
                        );
                        info!("💓 {}", summary.replace('\n', " | "));
                        if let Some(tg) = &tg {
                            tg.alert_heartbeat(&summary);
                        }
                    }
                    _ = hb_shutdown.recv() => break,
                }
            }
        });
    }

    // ── State file write loop (every 5 seconds) ───────────────────────────────
    {
        let state_trader = trader.clone();
        let mut state_shutdown = shutdown_tx.subscribe();

        tokio::spawn(async move {
            // Wait for WebSocket initial data before first write
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        state_trader.write_state_files().await;
                    }
                    _ = state_shutdown.recv() => break,
                }
            }
        });
    }

    // ── Write dashboard HTML to state/ directory ──────────────────────────────
    {
        let dashboard_html = include_str!("dashboard.html");
        tokio::fs::create_dir_all("state").await.ok();
        if let Err(e) = tokio::fs::write("state/signal_dashboard.html", dashboard_html).await {
            warn!("[DASHBOARD] Failed to write dashboard HTML: {}", e);
        } else {
            info!("[DASHBOARD] Wrote state/signal_dashboard.html");
        }
    }

    // ── HTTP server (python3 http.server on port 3001) ────────────────────────
    let mut http_child = match std::process::Command::new("python3")
        .args(["-m", "http.server", "3001", "--directory", "state"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(child) => {
            info!("[DASHBOARD] HTTP server started on http://localhost:3001/signal_dashboard.html");
            Some(child)
        }
        Err(e) => {
            warn!("[DASHBOARD] Failed to start HTTP server: {} — dashboard unavailable", e);
            None
        }
    };

    // ── Startup Telegram alert ────────────────────────────────────────────────
    if let Some(tg) = &telegram {
        tg.alert_startup("signal-trader", version);
    }

    // ── Wait for shutdown ─────────────────────────────────────────────────────
    wait_for_shutdown().await;
    info!("🛑 Shutdown signal received — stopping...");

    if let Some(tg) = &telegram {
        tg.alert_shutdown("SIGINT/SIGTERM");
    }

    // Kill HTTP server
    if let Some(ref mut child) = http_child {
        let _ = child.kill();
        info!("[DASHBOARD] HTTP server stopped");
    }

    let _ = shutdown_tx.send(());
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    info!("✅ Signal Trader stopped.");
    Ok(())
}

// ── Three-way market filter ───────────────────────────────────────────────────

/// Remove three-way soccer moneyline markets (home/draw/away).
///
/// Soccer moneylines produce 3 tickers per game sharing the same event prefix
/// (e.g. KXEPLGAME-26MAR05TOTCRY-TOT, -CRY, -TIE).  These break the binary
/// YES+NO=100¢ assumption and generate structural spreads that look like arbs.
///
/// Strategy: group market tickers by their prefix (strip last `-XXX` segment).
/// Any prefix that appears in exactly 3 markets is a three-way game → drop all 3.
/// Single or paired markets (binary: NBA/NHL/NFL totals/BTTS) are kept.
fn filter_three_way_markets(pairs: Vec<MarketPair>) -> (Vec<MarketPair>, usize) {
    let mut prefix_counts: HashMap<String, usize> = HashMap::new();
    for pair in &pairs {
        let ticker = &*pair.kalshi_market_ticker;
        let prefix = ticker.rfind('-')
            .map(|i| ticker[..i].to_string())
            .unwrap_or_else(|| ticker.to_string());
        *prefix_counts.entry(prefix).or_insert(0) += 1;
    }

    let total = pairs.len();
    let binary: Vec<MarketPair> = pairs.into_iter().filter(|pair| {
        let ticker = &*pair.kalshi_market_ticker;
        let prefix = ticker.rfind('-')
            .map(|i| &ticker[..i])
            .unwrap_or(ticker);
        prefix_counts.get(prefix).copied().unwrap_or(1) < 3
    }).collect();

    let filtered = total - binary.len();
    (binary, filtered)
}

// ── Signal handling ───────────────────────────────────────────────────────────

async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigint  = signal(SignalKind::interrupt()).expect("SIGINT handler");
        let mut sigterm = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = sigint.recv()  => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.expect("ctrl-c handler");
    }
}
