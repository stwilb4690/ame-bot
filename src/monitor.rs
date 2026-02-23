// src/monitor.rs
// Position Monitor — periodically checks open positions and closes them early for profit.
//
// Every MONITOR_INTERVAL_SECS (30 s):
//   - Reads open positions from PositionTracker
//   - For each, computes current exit value using live orderbook bids
//     (bid ≈ 100 - opposite_ask, the prediction-market price identity)
//   - If (exit_value - entry_cost) > MIN_EXIT_PROFIT_CENTS, closes both legs
//     concurrently and records realized P&L.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::kalshi::KalshiApiClient;
use crate::polymarket_clob::SharedAsyncClient;
use crate::polymarket_us::{LimitOrderRequest, PolymarketUsClient};
use crate::position_tracker::{ArbPosition, PositionTracker};
use crate::types::{ArbType, GlobalState, MarketPair};

/// Default min profit in cents to trigger an early close.
const DEFAULT_MIN_EXIT_PROFIT_CENTS: i64 = 3;

/// How often to scan open positions.
const MONITOR_INTERVAL_SECS: u64 = 30;

/// Minimum bid (cents) required to place a close order — rejects illiquid / stale prices.
const MIN_VALID_BID: u16 = 2;

pub async fn run_position_monitor(
    tracker: Arc<RwLock<PositionTracker>>,
    state: Arc<GlobalState>,
    kalshi: Arc<KalshiApiClient>,
    poly_async: Option<Arc<SharedAsyncClient>>,
    poly_us: Option<Arc<PolymarketUsClient>>,
    dry_run: bool,
    mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
) {
    let min_profit_cents: i64 = std::env::var("MIN_EXIT_PROFIT_CENTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MIN_EXIT_PROFIT_CENTS);

    info!(
        "[MONITOR] Position monitor started (interval={}s, min_exit_profit={}¢, dry_run={})",
        MONITOR_INTERVAL_SECS, min_profit_cents, dry_run
    );

    let mut interval = tokio::time::interval(Duration::from_secs(MONITOR_INTERVAL_SECS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                scan_and_close(
                    &tracker, &state, &kalshi, &poly_async, &poly_us,
                    dry_run, min_profit_cents,
                ).await;
            }
            _ = shutdown_rx.recv() => { break; }
        }
    }

    info!("[MONITOR] Position monitor stopped");
}

async fn scan_and_close(
    tracker: &Arc<RwLock<PositionTracker>>,
    state: &GlobalState,
    kalshi: &Arc<KalshiApiClient>,
    poly_async: &Option<Arc<SharedAsyncClient>>,
    poly_us: &Option<Arc<PolymarketUsClient>>,
    dry_run: bool,
    min_profit_cents: i64,
) {
    // Build pair_id → market_id map once per scan (O(n), called every 30 s).
    let n = state.market_count();
    let mut pair_map: HashMap<String, u16> = HashMap::with_capacity(n);
    for i in 0..n as u16 {
        if let Some(ms) = state.get_by_id(i) {
            if let Some(ref pair) = ms.pair {
                pair_map.insert(pair.pair_id.to_string(), i);
            }
        }
    }

    // Snapshot open positions while holding the read lock briefly.
    let positions: Vec<ArbPosition> = {
        let guard = tracker.read().await;
        guard.open_positions().iter().map(|p| (*p).clone()).collect()
    };

    if positions.is_empty() {
        return;
    }

    info!("[MONITOR] Scanning {} open position(s) for early exit", positions.len());

    for pos in &positions {
        let mid = match pair_map.get(&pos.market_id) {
            Some(&id) => id,
            None => {
                warn!("[MONITOR] No live market state for position '{}'", pos.market_id);
                continue;
            }
        };

        let ms = match state.get_by_id(mid) {
            Some(ms) => ms,
            None => continue,
        };

        let pair = match &ms.pair {
            Some(p) => Arc::clone(p),
            None => continue,
        };

        let (k_yes_ask, k_no_ask, _, _) = ms.kalshi.load();
        let (p_yes_ask, p_no_ask, _, _) = ms.poly.load();

        // Skip if either platform has no live prices yet.
        if k_yes_ask == 0 || k_no_ask == 0 || p_yes_ask == 0 || p_no_ask == 0 {
            continue;
        }

        // Estimate bids using the prediction-market identity: bid ≈ 100 − opposite_ask.
        let k_yes_bid = 100u16.saturating_sub(k_no_ask);
        let k_no_bid  = 100u16.saturating_sub(k_yes_ask);
        let p_yes_bid = 100u16.saturating_sub(p_no_ask);
        let p_no_bid  = 100u16.saturating_sub(p_yes_ask);

        // Determine arb type from which legs are populated.
        let arb_type = if pos.poly_yes.contracts > 0.0 && pos.kalshi_no.contracts > 0.0 {
            ArbType::PolyYesKalshiNo
        } else if pos.kalshi_yes.contracts > 0.0 && pos.poly_no.contracts > 0.0 {
            ArbType::KalshiYesPolyNo
        } else {
            continue; // Unusual / partial position — skip.
        };

        // Matched contract count, total entry cost (USD), and per-side sell bids.
        let (contracts, entry_cost_usd, poly_bid, kalshi_bid) = match arb_type {
            ArbType::PolyYesKalshiNo => {
                // Sell Poly YES at p_yes_bid; sell Kalshi NO at k_no_bid.
                let c = pos.poly_yes.contracts.min(pos.kalshi_no.contracts);
                let cost = pos.poly_yes.cost_basis + pos.kalshi_no.cost_basis;
                (c, cost, p_yes_bid, k_no_bid)
            }
            ArbType::KalshiYesPolyNo => {
                // Sell Kalshi YES at k_yes_bid; sell Poly NO at p_no_bid.
                let c = pos.kalshi_yes.contracts.min(pos.poly_no.contracts);
                let cost = pos.kalshi_yes.cost_basis + pos.poly_no.cost_basis;
                (c, cost, p_no_bid, k_yes_bid)
            }
        };

        if contracts < 1.0 || poly_bid < MIN_VALID_BID || kalshi_bid < MIN_VALID_BID {
            continue;
        }

        let exit_value_usd = contracts * (poly_bid as f64 + kalshi_bid as f64) / 100.0;
        let profit_usd     = exit_value_usd - entry_cost_usd;
        let profit_cents   = (profit_usd * 100.0).round() as i64;

        if profit_cents < min_profit_cents {
            continue;
        }

        info!(
            "[MONITOR] 💰 Early exit: {} | {}x | entry=${:.2} exit=${:.2} profit={}¢",
            pos.description, contracts as i64, entry_cost_usd, exit_value_usd, profit_cents,
        );

        if dry_run {
            info!(
                "[MONITOR] 🏃 DRY RUN — would close {}x '{}' ({:?})",
                contracts as i64, pos.description, arb_type
            );
            continue;
        }

        // --- Execute close: both legs concurrently ---
        let (poly_ok, kalshi_ok) = close_both_legs(
            &pair, arb_type,
            contracts as i64, poly_bid, kalshi_bid,
            kalshi, poly_async, poly_us,
        ).await;

        if poly_ok && kalshi_ok {
            info!(
                "[MONITOR] ✅ Closed '{}': profit={}¢ (${:.4})",
                pos.description, profit_cents, profit_usd
            );
        } else {
            warn!(
                "[MONITOR] ⚠️ Partial close '{}': poly_ok={} kalshi_ok={}",
                pos.description, poly_ok, kalshi_ok
            );
        }

        // Mark closed in tracker regardless of partial failures to prevent re-attempts.
        tracker.write().await.close_position(&pos.market_id, profit_usd);
    }
}

/// Place sell orders on both legs concurrently. Returns `(poly_ok, kalshi_ok)`.
async fn close_both_legs(
    pair: &Arc<MarketPair>,
    arb_type: ArbType,
    qty: i64,
    poly_bid: u16,
    kalshi_bid: u16,
    kalshi: &Arc<KalshiApiClient>,
    poly_async: &Option<Arc<SharedAsyncClient>>,
    poly_us: &Option<Arc<PolymarketUsClient>>,
) -> (bool, bool) {
    let kalshi_ticker = pair.kalshi_market_ticker.clone();
    let poly_slug     = pair.poly_slug.clone();
    let poly_yes_tok  = pair.poly_yes_token.clone();
    let poly_no_tok   = pair.poly_no_token.clone();
    let k_bid_i       = kalshi_bid as i64;

    match arb_type {
        ArbType::PolyYesKalshiNo => {
            // Sell Poly YES + Sell Kalshi NO
            let k = Arc::clone(kalshi);
            let (poly_res, kalshi_res) = tokio::join!(
                sell_poly(poly_us, poly_async, &poly_slug, &poly_yes_tok, poly_bid, qty, "yes"),
                async move { k.sell_ioc(&kalshi_ticker, "no", k_bid_i, qty).await },
            );
            let kalshi_ok = kalshi_res
                .map(|r| {
                    let n = r.order.filled_count();
                    info!("[MONITOR] Kalshi sell-NO: filled={}", n);
                    n > 0
                })
                .unwrap_or_else(|e| { warn!("[MONITOR] Kalshi sell-NO failed: {}", e); false });
            (poly_res, kalshi_ok)
        }
        ArbType::KalshiYesPolyNo => {
            // Sell Kalshi YES + Sell Poly NO
            let k = Arc::clone(kalshi);
            let (poly_res, kalshi_res) = tokio::join!(
                sell_poly(poly_us, poly_async, &poly_slug, &poly_no_tok, poly_bid, qty, "no"),
                async move { k.sell_ioc(&kalshi_ticker, "yes", k_bid_i, qty).await },
            );
            let kalshi_ok = kalshi_res
                .map(|r| {
                    let n = r.order.filled_count();
                    info!("[MONITOR] Kalshi sell-YES: filled={}", n);
                    n > 0
                })
                .unwrap_or_else(|e| { warn!("[MONITOR] Kalshi sell-YES failed: {}", e); false });
            (poly_res, kalshi_ok)
        }
    }
}

/// Sell on Polymarket (US client preferred, CLOB fallback).
/// `side` is `"yes"` or `"no"`.
async fn sell_poly(
    poly_us: &Option<Arc<PolymarketUsClient>>,
    poly_clob: &Option<Arc<SharedAsyncClient>>,
    slug: &str,
    token: &str,
    bid_cents: u16,
    qty: i64,
    side: &str,
) -> bool {
    let price_usd = bid_cents as f64 / 100.0;

    if let Some(us) = poly_us {
        let req = if side == "yes" {
            LimitOrderRequest::sell_yes(slug, price_usd, qty as u64)
        } else {
            LimitOrderRequest::sell_no(slug, price_usd, qty as u64)
        };
        match us.place_limit_order(&req).await {
            Ok(resp) => {
                info!("[MONITOR] PolyUS sell-{}: id={:?}", side.to_uppercase(), resp.id);
                true
            }
            Err(e) => {
                warn!("[MONITOR] PolyUS sell-{} failed: {}", side.to_uppercase(), e);
                false
            }
        }
    } else if let Some(clob) = poly_clob {
        match clob.sell_fak(token, price_usd, qty as f64).await {
            Ok(fill) => {
                info!("[MONITOR] CLOB sell-{}: filled={:.0}x", side.to_uppercase(), fill.filled_size);
                fill.filled_size >= 1.0
            }
            Err(e) => {
                warn!("[MONITOR] CLOB sell-{} failed: {}", side.to_uppercase(), e);
                false
            }
        }
    } else {
        warn!("[MONITOR] No Poly client available for sell-{}", side.to_uppercase());
        false
    }
}
