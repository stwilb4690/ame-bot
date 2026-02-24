// src/bin/market_maker.rs
//
// AME Market Maker — single-platform Kalshi market maker.
//
// Usage:
//   source .env
//   MM_DRY_RUN=1 cargo run --release --bin market-maker
//
// Required env vars: KALSHI_API_KEY_ID, KALSHI_PRIVATE_KEY_PATH
// Optional:          TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
//                    MM_* (thresholds / limits, see MmConfig::from_env)
//
// ALWAYS start with MM_DRY_RUN=1 to verify books and opportunities before
// setting MM_DRY_RUN=0 for live order placement.

use ame_bot::{
    config::{get_league_configs, kalshi_api_base, kalshi_ws_url, WS_RECONNECT_DELAY_SECS},
    kalshi::{KalshiConfig, KalshiApiClient},
    market_maker::{MarketMaker, MarketMeta, MmConfig},
    telegram::TelegramClient,
};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use futures_util::stream::FuturesUnordered;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{http::Request, Message},
};
use tracing::{debug, error, info, warn};

// ─── Market discovery ─────────────────────────────────────────────────────────

/// Fetch all currently-open sports + crypto market tickers from Kalshi REST.
///
/// Makes parallel per-series calls (GET /markets?series_ticker=X&status=open)
/// instead of bulk pagination, avoiding the thousands of KXMVE* parlay markets
/// that pollute the bulk endpoint and cause it to hang.
///
/// Max 5 concurrent calls; 5-second timeout per series; KXMVE* and zero-volume
/// markets are filtered out before returning.
async fn discover_mm_markets(api: Arc<KalshiApiClient>) -> Vec<MarketMeta> {
    // Collect every series ticker we care about
    let mut series_list: Vec<&'static str> = Vec::new();
    for league in get_league_configs() {
        series_list.push(league.kalshi_series_game);
        if let Some(sp)  = league.kalshi_series_spread { series_list.push(sp); }
        if let Some(tot) = league.kalshi_series_total  { series_list.push(tot); }
        if let Some(bt)  = league.kalshi_series_btts   { series_list.push(bt); }
    }
    series_list.push("KXBTC15M");
    series_list.push("KXETH15M");
    series_list.push("KXSOL15M");

    info!("[MM-DISC] Fetching {} series in parallel (max 5 concurrent, 5s timeout each)", series_list.len());

    let mut metas: Vec<MarketMeta> = Vec::new();
    let mut total_fetched: usize = 0;

    // Process in batches of 5 for bounded concurrency
    for chunk in series_list.chunks(5) {
        let mut futs = FuturesUnordered::new();

        for &series in chunk {
            let api = Arc::clone(&api);
            futs.push(async move {
                let result = tokio::time::timeout(
                    Duration::from_secs(5),
                    api.get_markets_by_series(series, 100),
                ).await;
                (series, result)
            });
        }

        while let Some((series, outcome)) = futs.next().await {
            match outcome {
                Err(_elapsed) => {
                    warn!("[MM-DISC] Series {} timed out — skipping", series);
                }
                Ok(Err(e)) => {
                    warn!("[MM-DISC] Series {} fetch error: {} — skipping", series, e);
                }
                Ok(Ok(markets)) => {
                    total_fetched += markets.len();
                    for market in markets {
                        // Drop multivariate parlay markets (KXMVE*)
                        if market.ticker.starts_with("KXMVE") {
                            continue;
                        }
                        // Drop dead markets with no volume
                        if market.volume.unwrap_or(0) == 0 {
                            continue;
                        }
                        let close_time = market.close_time.as_deref().and_then(|s| {
                            chrono::DateTime::parse_from_rfc3339(s).ok()
                                .map(|dt| SystemTime::UNIX_EPOCH + Duration::from_secs(dt.timestamp() as u64))
                        });
                        metas.push(MarketMeta {
                            ticker: market.ticker,
                            title: market.title,
                            close_time,
                            volume_cents: market.volume,
                        });
                    }
                }
            }
        }
    }

    info!("[MM-DISC] Discovered {} live markets ({} fetched across {} series)", metas.len(), total_fetched, series_list.len());
    metas
}

// ─── WebSocket runner ─────────────────────────────────────────────────────────

/// WebSocket runner for the market maker.
/// Subscribes to `orderbook_delta` (for all discovered tickers) and
/// `user_orders` (for fill notifications).
///
/// All incoming messages are routed to the MarketMaker instance.
async fn run_mm_ws(
    config: KalshiConfig,
    tickers: Vec<String>,
    mm: Arc<MarketMaker>,
) -> Result<()> {
    if tickers.is_empty() {
        info!("[MM-WS] No tickers to subscribe — waiting");
        tokio::time::sleep(Duration::from_secs(u64::MAX)).await;
        return Ok(());
    }

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string();

    let signature = config.sign(&format!("{}GET/trade-api/ws/v2", timestamp))?;

    let host = std::env::var("KALSHI_API_HOST").unwrap_or_else(|_| {
        match ame_bot::config::app_env() {
            ame_bot::config::AppEnv::Demo => "demo-api.kalshi.com".into(),
            ame_bot::config::AppEnv::Prod => "trading-api.kalshi.com".into(),
        }
    });

    let request = Request::builder()
        .uri(kalshi_ws_url())
        .header("KALSHI-ACCESS-KEY", &config.api_key_id)
        .header("KALSHI-ACCESS-SIGNATURE", &signature)
        .header("KALSHI-ACCESS-TIMESTAMP", &timestamp)
        .header("Host", &host)
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key(),
        )
        .body(())?;

    let (ws_stream, _) = connect_async(request).await.context("MM WebSocket connect failed")?;
    info!("[MM-WS] Connected ({}  tickers)", tickers.len());

    let (mut write, mut read) = ws_stream.split();

    // Subscribe to orderbook_delta for all tickers
    let sub_ob = serde_json::json!({
        "id": 1,
        "cmd": "subscribe",
        "params": {
            "channels": ["orderbook_delta"],
            "market_tickers": tickers,
        }
    });
    write.send(Message::Text(sub_ob.to_string())).await?;

    // Subscribe to fill channel (authenticated, no market_tickers needed)
    let sub_fill = serde_json::json!({
        "id": 2,
        "cmd": "subscribe",
        "params": {
            "channels": ["fill"],
        }
    });
    write.send(Message::Text(sub_fill.to_string())).await?;
    info!("[MM-WS] Subscribed to orderbook_delta + fill");

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Err(e) = handle_ws_message(&text, &mm).await {
                    debug!("[MM-WS] parse error: {} (msg: {}...)", e, &text[..text.len().min(120)]);
                }
            }
            Ok(Message::Ping(data)) => {
                let _ = write.send(Message::Pong(data)).await;
            }
            Err(e) => {
                error!("[MM-WS] Error: {}", e);
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

/// Parse and dispatch a single WebSocket text message.
async fn handle_ws_message(text: &str, mm: &Arc<MarketMaker>) -> Result<()> {
    let msg: serde_json::Value = serde_json::from_str(text)?;

    let msg_type = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let seq = msg.get("seq").and_then(|v| v.as_u64()).unwrap_or(0);
    let body = msg.get("msg");

    match msg_type {
        "orderbook_snapshot" => {
            let Some(body) = body else { return Ok(()); };
            let ticker = body.get("market_ticker")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if ticker.is_empty() { return Ok(()); }

            let yes_levels = body.get("yes")
                .and_then(|v| v.as_array())
                .map(|a| a.as_slice())
                .unwrap_or(&[]);
            let no_levels = body.get("no")
                .and_then(|v| v.as_array())
                .map(|a| a.as_slice())
                .unwrap_or(&[]);

            mm.on_snapshot(ticker, yes_levels, no_levels, seq).await;
        }

        "orderbook_delta" => {
            let Some(body) = body else { return Ok(()); };
            let ticker = body.get("market_ticker")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if ticker.is_empty() { return Ok(()); }

            // Array form: yes/no arrays of [price, qty]
            let empty: Vec<serde_json::Value> = vec![];
            let yes_deltas = body.get("yes")
                .and_then(|v| v.as_array())
                .unwrap_or(&empty);
            let no_deltas = body.get("no")
                .and_then(|v| v.as_array())
                .unwrap_or(&empty);

            // Scalar form: side + price + delta
            let side_scalar = body.get("side").and_then(|v| v.as_str());
            let price_scalar = body.get("price").cloned();
            let delta_scalar = body.get("delta").cloned();

            mm.on_delta(
                ticker,
                yes_deltas,
                no_deltas,
                side_scalar,
                price_scalar,
                delta_scalar,
                seq,
            ).await;
        }

        // fill channel — actual execution fills (correct field names)
        "fill" => {
            let payload = body.unwrap_or(&msg);
            mm.on_ws_fill(payload).await;
        }

        // user_order / order — cancel/reject notifications
        "user_order" | "order" => {
            let payload = body.unwrap_or(&msg);
            mm.on_user_order(payload).await;
        }

        // Subscription confirmations and other control messages
        "subscribed" | "unsubscribed" | "error" => {
            info!("[MM-WS] Control msg: {}", text);
        }

        _ => {
            debug!("[MM-WS] Unknown msg type: {}", msg_type);
        }
    }

    Ok(())
}

// ─── State file writer ────────────────────────────────────────────────────────

async fn write_state_files(mm: &Arc<MarketMaker>) {
    let state_dir = "state";

    let write_json = |path: &str, value: serde_json::Value| {
        if let Err(e) = std::fs::write(path, value.to_string()) {
            warn!("[MM] State write failed {}: {}", path, e);
        }
    };

    write_json(
        &format!("{}/mm_status.json", state_dir),
        mm.status_json().await,
    );
    write_json(
        &format!("{}/mm_positions.json", state_dir),
        mm.positions_json().await,
    );
    write_json(
        &format!("{}/mm_opportunities.json", state_dir),
        mm.opportunities_json().await,
    );
    write_json(
        &format!("{}/mm_trades.json", state_dir),
        mm.trades_json().await,
    );
}

// ─── Main ─────────────────────────────────────────────────────────────────────

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
    info!("🏦 AME Market Maker v{}", version);
    info!("   Env: {}", std::env::var("APP_ENV").unwrap_or_else(|_| "demo".into()));
    info!("   Kalshi API: {}", kalshi_api_base());

    // ── Config ────────────────────────────────────────────────────────────────
    let mm_config = MmConfig::from_env();

    if mm_config.dry_run {
        info!("   [DRY RUN] logging opportunities only — no live orders (set MM_DRY_RUN=0 to execute)");
    } else {
        warn!("   [LIVE] live Kalshi orders ENABLED");
    }

    info!(
        "   min_spread={}¢  min_net_spread={}¢  bid_offset=+{}¢  ask_offset=-{}¢  order_size={}  max_positions={}  daily_limit=${:.2}",
        mm_config.min_spread_cents,
        mm_config.min_net_spread_cents,
        mm_config.bid_offset_cents,
        mm_config.ask_offset_cents,
        mm_config.order_size,
        mm_config.max_positions,
        mm_config.daily_loss_limit,
    );

    // ── Kalshi credentials ────────────────────────────────────────────────────
    let kalshi_config = KalshiConfig::from_env()
        .context("KALSHI_API_KEY_ID + KALSHI_PRIVATE_KEY_PATH required")?;
    info!("[KALSHI] API key loaded");

    let kalshi_api = Arc::new(KalshiApiClient::new(kalshi_config.clone()));

    // ── Balance check ─────────────────────────────────────────────────────────
    let balance = kalshi_api.get_balance().await
        .context("Failed to fetch Kalshi balance")?;
    let balance_dollars = balance.balance as f64 / 100.0;
    info!("[KALSHI] Balance: ${:.2}", balance_dollars);

    let reserve = mm_config.balance_reserve;
    let reserve_f64: f64 = reserve.to_string().parse().unwrap_or(50.0);
    if (balance.balance as f64 / 100.0) < reserve_f64 {
        anyhow::bail!(
            "Balance ${:.2} below reserve floor ${:.2} — refusing to start live",
            balance_dollars,
            reserve
        );
    }

    // ── Telegram ──────────────────────────────────────────────────────────────
    let telegram = TelegramClient::from_env();
    match &telegram {
        Some(_) => info!("[TELEGRAM] Configured"),
        None => info!("[TELEGRAM] Not configured"),
    }

    // ── Market discovery ──────────────────────────────────────────────────────
    info!("[MM] Discovering sports markets...");
    let market_metas = discover_mm_markets(kalshi_api.clone()).await;
    let tickers: Vec<String> = market_metas.iter().map(|m| m.ticker.clone()).collect();

    if tickers.is_empty() {
        warn!("[MM] No markets discovered — bot will subscribe but have nothing to quote");
    }

    // ── Build market maker ────────────────────────────────────────────────────
    let balance_str = format!("{:.2}", balance_dollars);
    let mm = Arc::new(MarketMaker::new(mm_config, kalshi_api.clone(), telegram.clone(), balance_str));

    // Register market metadata for expiry/volume filtering
    for meta in market_metas {
        mm.register_market(meta).await;
    }

    // ── Telegram startup alert ────────────────────────────────────────────────
    if let Some(ref tg) = telegram {
        let mode = if mm.config.dry_run { "DRY RUN" } else { "LIVE" };
        tg.alert_startup(mode, version);
    }

    // ── Shutdown channel ──────────────────────────────────────────────────────
    let (shutdown_tx, _) = broadcast::channel::<()>(4);

    // ── WebSocket task ────────────────────────────────────────────────────────
    {
        let ws_config  = kalshi_config.clone();
        let ws_tickers = tickers.clone();
        let ws_mm      = mm.clone();
        let mut ws_shutdown = shutdown_tx.subscribe();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = run_mm_ws(ws_config.clone(), ws_tickers.clone(), ws_mm.clone()) => {
                        match result {
                            Ok(_)  => info!("[MM-WS] Disconnected cleanly"),
                            Err(e) => error!("[MM-WS] Disconnected: {} — reconnecting in {}s", e, WS_RECONNECT_DELAY_SECS),
                        }
                        // Emergency: if we had open positions, cancel and halt
                        let has_positions = !ws_mm.positions.lock().await.is_empty();
                        if has_positions {
                            error!("[MM-WS] WebSocket dropped with open positions — emergency cancel");
                            if let Err(e) = ws_mm.kalshi_api.cancel_all_orders().await {
                                error!("[MM-WS] Emergency cancel failed: {}", e);
                            }
                            ws_mm.positions.lock().await.clear();
                            ws_mm.order_to_ticker.lock().await.clear();
                        }
                    }
                    _ = ws_shutdown.recv() => break,
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(WS_RECONNECT_DELAY_SECS)) => {}
                    _ = ws_shutdown.recv() => break,
                }
            }
            info!("[MM-WS] Task stopped");
        });
    }

    // ── Scanner + timeout task (every 2 seconds) ──────────────────────────────
    {
        let scan_mm = mm.clone();
        let mut scan_shutdown = shutdown_tx.subscribe();

        tokio::spawn(async move {
            // Brief warmup: let WebSocket deliver initial order book snapshots
            info!("[MM] 15s warmup — waiting for order book snapshots");
            tokio::time::sleep(Duration::from_secs(15)).await;
            info!("[MM] Starting scan loop (2s cadence)");

            let mut interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        scan_mm.scan_and_act().await;
                        scan_mm.check_timeouts().await;
                    }
                    _ = scan_shutdown.recv() => break,
                }
            }
            info!("[MM] Scan loop stopped");
        });
    }

    // ── State file writer (every 5 seconds) ───────────────────────────────────
    {
        let state_mm = mm.clone();
        let mut state_shutdown = shutdown_tx.subscribe();

        // Ensure state directory exists
        std::fs::create_dir_all("state").ok();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        write_state_files(&state_mm).await;
                    }
                    _ = state_shutdown.recv() => break,
                }
            }
        });
    }

    // ── Heartbeat log (every 5 minutes) ──────────────────────────────────────
    {
        let hb_mm = mm.clone();
        let mut hb_shutdown = shutdown_tx.subscribe();
        let hb_telegram = telegram.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let status = hb_mm.status_json().await;
                        let books_count = hb_mm.books.lock().await.len();
                        info!(
                            "[MM] HEARTBEAT | mode={} pnl={} positions={} pending={} exposure=${} trades={} books={} uptime={}s",
                            status["mode"].as_str().unwrap_or("?"),
                            status["daily_pnl"].as_str().unwrap_or("?"),
                            status["open_positions"],
                            status["pending_orders"],
                            status["exposure"].as_str().unwrap_or("?"),
                            status["total_trades_today"],
                            books_count,
                            status["uptime_secs"],
                        );
                        if let Some(ref tg) = hb_telegram {
                            let summary = format!(
                                "Mode: {}\nP&L: ${}\nPositions: {}\nPending orders: {}\nExposure: ${}\nTrades today: {}\nWin rate: {:.1}%\nUptime: {}s\nBooks: {}",
                                status["mode"].as_str().unwrap_or("?"),
                                status["daily_pnl"].as_str().unwrap_or("?"),
                                status["open_positions"],
                                status["pending_orders"],
                                status["exposure"].as_str().unwrap_or("?"),
                                status["total_trades_today"],
                                status["win_rate"].as_f64().unwrap_or(0.0) * 100.0,
                                status["uptime_secs"],
                                books_count,
                            );
                            tg.alert_heartbeat(&summary);
                        }
                    }
                    _ = hb_shutdown.recv() => break,
                }
            }
        });
    }

    // ── Wait for Ctrl+C ───────────────────────────────────────────────────────
    info!("[MM] Running. Press Ctrl+C to stop.");
    tokio::signal::ctrl_c().await.context("Failed to listen for Ctrl+C")?;

    info!("[MM] Shutdown signal received");

    // Emergency cancel all orders before exit if any positions are open
    if !mm.positions.lock().await.is_empty() {
        warn!("[MM] Open positions on shutdown — canceling all resting orders");
        if let Err(e) = mm.kalshi_api.cancel_all_orders().await {
            error!("[MM] Shutdown cancel failed: {}", e);
        }
    }

    if let Some(ref tg) = telegram {
        tg.alert_shutdown("Ctrl+C — graceful shutdown");
        tokio::time::sleep(Duration::from_secs(2)).await; // Let Telegram send
    }

    let _ = shutdown_tx.send(());
    info!("[MM] Stopped.");
    Ok(())
}
