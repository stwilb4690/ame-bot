// src/market_maker.rs
//
// AME Market Maker — single-platform Kalshi market maker engine.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use serde::Serialize;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::kalshi::KalshiApiClient;
use crate::telegram::TelegramClient;

// ─── Side Enum ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Side {
    Yes,
    No,
}

// ─── Config ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct MmConfig {
    pub min_spread_cents: i64,
    pub min_net_spread_cents: i64,
    pub bid_offset_cents: i64,
    pub ask_offset_cents: i64,
    pub order_size: i64,
    pub max_positions: usize,
    pub daily_loss_limit: f64,
    pub dry_run: bool,
    pub balance_reserve: f64,
    /// Max seconds before a resting buy order is canceled (MM_BUY_TIMEOUT_SECS)
    pub buy_timeout_secs: u64,
    /// Max total capital at risk across pending orders + open positions (MM_MAX_EXPOSURE, dollars)
    pub max_exposure: f64,
}

impl MmConfig {
    pub fn from_env() -> Self {
        let parse_env = |key: &str, default: &str| -> String {
            std::env::var(key).unwrap_or_else(|_| default.to_string())
        };

        Self {
            min_spread_cents: parse_env("MM_MIN_SPREAD_CENTS", "2").parse().unwrap_or(2),
            min_net_spread_cents: parse_env("MM_MIN_NET_SPREAD_CENTS", "1").parse().unwrap_or(1),
            bid_offset_cents: parse_env("MM_BID_OFFSET_CENTS", "1").parse().unwrap_or(1),
            ask_offset_cents: parse_env("MM_ASK_OFFSET_CENTS", "1").parse().unwrap_or(1),
            order_size: parse_env("MM_ORDER_SIZE", "10").parse().unwrap_or(10),
            max_positions: parse_env("MM_MAX_POSITIONS", "5").parse().unwrap_or(5),
            daily_loss_limit: parse_env("MM_DAILY_LOSS_LIMIT", "15").parse().unwrap_or(15.0),
            dry_run: std::env::var("MM_DRY_RUN").map(|v| v != "0").unwrap_or(true),
            balance_reserve: parse_env("MM_BALANCE_RESERVE", "50").parse().unwrap_or(50.0),
            buy_timeout_secs: parse_env("MM_BUY_TIMEOUT_SECS", "120").parse().unwrap_or(120),
            max_exposure: parse_env("MM_MAX_EXPOSURE", "15").parse().unwrap_or(15.0),
        }
    }
}

// ─── Market Metadata ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct MarketMeta {
    pub ticker: String,
    pub title: String,
    pub close_time: Option<SystemTime>,
    pub volume_cents: Option<i64>,
}

// ─── Local OrderBook ───────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct LocalOrderBook {
    pub yes_bids: HashMap<i64, i64>, // price -> qty
    pub no_bids: HashMap<i64, i64>,  // price -> qty
    // yes_asks and no_asks are derived: YES ask = 100 - best NO bid, NO ask = 100 - best YES bid
    pub seq: u64,
    pub last_update: Instant,
    pub warmup_count: u32,

    // Track best price stability
    pub last_best_yes_bid: Option<i64>,
    pub last_best_yes_ask: Option<i64>,
    pub last_best_no_bid: Option<i64>,
    pub last_best_no_ask: Option<i64>,
    pub yes_bid_changes: u32,
    pub yes_ask_changes: u32,
    pub no_bid_changes: u32,
    pub no_ask_changes: u32,

    // Recency tracking
    pub last_activity: Instant,
}

impl LocalOrderBook {
    pub fn new() -> Self {
        Self {
            yes_bids: HashMap::new(),
            no_bids: HashMap::new(),
            seq: 0,
            last_update: Instant::now(),
            warmup_count: 0,
            last_best_yes_bid: None,
            last_best_yes_ask: None,
            last_best_no_bid: None,
            last_best_no_ask: None,
            yes_bid_changes: 0,
            yes_ask_changes: 0,
            no_bid_changes: 0,
            no_ask_changes: 0,
            last_activity: Instant::now(),
        }
    }

    pub fn is_warm(&self) -> bool {
        self.warmup_count >= 5
    }

    pub fn best_yes_bid(&self) -> Option<(i64, i64)> {
        self.yes_bids.iter().max_by_key(|(p, _)| *p).map(|(p, q)| (*p, *q))
    }

    pub fn best_yes_ask(&self) -> Option<(i64, i64)> {
        // YES ask = 100 - best NO bid
        self.no_bids.iter().max_by_key(|(p, _)| *p).map(|(p, q)| (100 - *p, *q))
    }

    pub fn best_no_bid(&self) -> Option<(i64, i64)> {
        self.no_bids.iter().max_by_key(|(p, _)| *p).map(|(p, q)| (*p, *q))
    }

    pub fn best_no_ask(&self) -> Option<(i64, i64)> {
        // NO ask = 100 - best YES bid
        self.yes_bids.iter().max_by_key(|(p, _)| *p).map(|(p, q)| (100 - *p, *q))
    }

    /// Apply order book deltas (array form from WebSocket)
    pub fn apply_delta(
        &mut self,
        yes_deltas: &[serde_json::Value],
        no_deltas: &[serde_json::Value],
        side_scalar: Option<&str>,
        price_scalar: Option<serde_json::Value>,
        delta_scalar: Option<serde_json::Value>,
        seq: u64,
    ) {
        // Capture previous best prices before any modifications
        let prev_yes_bid = self.best_yes_bid().map(|(p, _)| p);
        let prev_yes_ask = self.best_yes_ask().map(|(p, _)| p);
        let prev_no_bid = self.best_no_bid().map(|(p, _)| p);
        let prev_no_ask = self.best_no_ask().map(|(p, _)| p);

        self.seq = seq;
        self.last_update = Instant::now();
        self.warmup_count = self.warmup_count.saturating_add(1);

        // Process array form deltas
        for delta in yes_deltas {
            if let Some(arr) = delta.as_array() {
                if arr.len() >= 2 {
                    let price = arr[0].as_i64().unwrap_or(0);
                    let qty = arr[1].as_i64().unwrap_or(0);
                    if qty > 0 {
                        self.yes_bids.insert(price, qty);
                    } else {
                        self.yes_bids.remove(&price);
                    }
                }
            }
        }

        for delta in no_deltas {
            if let Some(arr) = delta.as_array() {
                if arr.len() >= 2 {
                    let price = arr[0].as_i64().unwrap_or(0);
                    let qty = arr[1].as_i64().unwrap_or(0);
                    if qty > 0 {
                        self.no_bids.insert(price, qty);
                    } else {
                        self.no_bids.remove(&price);
                    }
                }
            }
        }

        // Process scalar form (side + price + delta)
        if let (Some(side), Some(price_val), Some(delta_val)) = (side_scalar, price_scalar, delta_scalar) {
            let price = price_val.as_i64().unwrap_or(0);
            let qty = delta_val.as_i64().unwrap_or(0);

            match side {
                "yes" => {
                    if qty > 0 {
                        self.yes_bids.insert(price, qty);
                    } else {
                        self.yes_bids.remove(&price);
                    }
                }
                "no" => {
                    if qty > 0 {
                        self.no_bids.insert(price, qty);
                    } else {
                        self.no_bids.remove(&price);
                    }
                }
                _ => {}
            }
        }

        // Check for best price changes and update stability counters + activity
        self.update_bid_stability(prev_yes_bid, prev_no_bid);
        self.update_ask_stability(prev_yes_ask, prev_no_ask);
    }

    pub fn on_snapshot(
        &mut self,
        yes_levels: &[serde_json::Value],
        no_levels: &[serde_json::Value],
        seq: u64,
    ) {
        self.seq = seq;
        self.last_update = Instant::now();
        self.warmup_count = self.warmup_count.saturating_add(1);

        // Replace entire order book
        self.yes_bids.clear();
        self.no_bids.clear();

        for level in yes_levels {
            if let Some(arr) = level.as_array() {
                if arr.len() >= 2 {
                    let price = arr[0].as_i64().unwrap_or(0);
                    let qty = arr[1].as_i64().unwrap_or(0);
                    if qty > 0 {
                        self.yes_bids.insert(price, qty);
                    }
                }
            }
        }

        for level in no_levels {
            if let Some(arr) = level.as_array() {
                if arr.len() >= 2 {
                    let price = arr[0].as_i64().unwrap_or(0);
                    let qty = arr[1].as_i64().unwrap_or(0);
                    if qty > 0 {
                        self.no_bids.insert(price, qty);
                    }
                }
            }
        }
    }

    fn update_bid_stability(&mut self, prev_yes_bid: Option<i64>, prev_no_bid: Option<i64>) {
        let new_yes_bid = self.best_yes_bid().map(|(p, _)| p);
        let new_no_bid = self.best_no_bid().map(|(p, _)| p);

        if new_yes_bid != prev_yes_bid {
            self.yes_bid_changes += 1;
            self.last_activity = Instant::now();
        }
        if new_no_bid != prev_no_bid {
            self.no_bid_changes += 1;
            self.last_activity = Instant::now();
        }

        self.last_best_yes_bid = new_yes_bid;
        self.last_best_no_bid = new_no_bid;
    }

    fn update_ask_stability(&mut self, prev_yes_ask: Option<i64>, prev_no_ask: Option<i64>) {
        let new_yes_ask = self.best_yes_ask().map(|(p, _)| p);
        let new_no_ask = self.best_no_ask().map(|(p, _)| p);

        if new_yes_ask != prev_yes_ask {
            self.yes_ask_changes += 1;
            self.last_activity = Instant::now();
        }
        if new_no_ask != prev_no_ask {
            self.no_ask_changes += 1;
            self.last_activity = Instant::now();
        }

        self.last_best_yes_ask = new_yes_ask;
        self.last_best_no_ask = new_no_ask;
    }

    pub fn seconds_since_activity(&self) -> u64 {
        self.last_activity.elapsed().as_secs()
    }

    /// Best bid for a given side
    pub fn best_bid_for_side(&self, side: Side) -> Option<i64> {
        match side {
            Side::Yes => self.best_yes_bid().map(|(p, _)| p),
            Side::No  => self.best_no_bid().map(|(p, _)| p),
        }
    }
}

// ─── Position ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Position {
    pub ticker: String,
    pub side: Side,
    pub entry_price: i64,
    pub qty: i64,
    pub entry_time: Instant,
    pub order_id: String,
    /// Intended exit price (used to seed the sell order)
    pub target_exit_price: i64,
}

// ─── Sell-side fade state ─────────────────────────────────────────────────────

/// Tracks an active resting sell order + fade schedule for a position.
#[derive(Debug, Clone)]
pub struct SellState {
    /// Kalshi order_id of the current resting sell order
    pub order_id: String,
    /// Current ask price (may be lowered by fade)
    pub ask_price: i64,
    /// Breakeven floor — never fade below this
    pub entry_price: i64,
    pub qty: i64,
    /// When the sell order was first placed
    pub placed_time: Instant,
    /// When the last fade step was applied
    pub last_fade: Instant,
}

// ─── Quote Request ────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct QuoteRequest {
    pub ticker: String,
    pub bid_price: i64,
    pub ask_price: i64,
    pub size: i64,
}

// ─── Spread Opportunity ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SpreadOpportunity {
    pub ticker: String,
    pub side: Side,
    pub entry_price: i64,
    pub exit_price: i64,
    pub spread: i64,
    pub score: i64,
    pub recency_secs: u64,
}

// ─── MarketMaker ───────────────────────────────────────────────────────────────

pub struct MarketMaker {
    pub config: MmConfig,
    pub kalshi_api: Arc<KalshiApiClient>,
    pub telegram: Option<Arc<TelegramClient>>,
    pub books: Mutex<HashMap<String, LocalOrderBook>>,
    pub positions: Mutex<HashMap<String, Position>>,
    pub pending_orders: Mutex<HashMap<String, (QuoteRequest, Instant)>>,
    pub order_to_ticker: Mutex<HashMap<String, String>>,
    pub sell_states: Mutex<HashMap<String, SellState>>,
    pub market_meta: RwLock<HashMap<String, MarketMeta>>,
    pub daily_pnl: Mutex<f64>,
    /// Current balance in cents — set at startup, updated on position close
    pub balance_cents: Mutex<i64>,
    pub total_trades_today: AtomicUsize,
    pub winning_trades: AtomicUsize,
    pub losing_trades: AtomicUsize,
    pub start_time: Instant,
    pub mode: AtomicU8, // 0=paper, 1=live
}

impl MarketMaker {
    pub fn new(
        config: MmConfig,
        kalshi_api: Arc<KalshiApiClient>,
        telegram: Option<Arc<TelegramClient>>,
        balance_str: String,
    ) -> Self {
        // Parse "$173.75" or "173.75" into cents
        let balance_cents_val: i64 = balance_str
            .trim_start_matches('$')
            .parse::<f64>()
            .map(|d| (d * 100.0).round() as i64)
            .unwrap_or(0);

        Self {
            config,
            kalshi_api,
            telegram,
            books: Mutex::new(HashMap::new()),
            positions: Mutex::new(HashMap::new()),
            pending_orders: Mutex::new(HashMap::new()),
            order_to_ticker: Mutex::new(HashMap::new()),
            sell_states: Mutex::new(HashMap::new()),
            market_meta: RwLock::new(HashMap::new()),
            daily_pnl: Mutex::new(0.0),
            balance_cents: Mutex::new(balance_cents_val),
            total_trades_today: AtomicUsize::new(0),
            winning_trades: AtomicUsize::new(0),
            losing_trades: AtomicUsize::new(0),
            start_time: Instant::now(),
            mode: AtomicU8::new(0),
        }
    }

    pub async fn register_market(&self, meta: MarketMeta) {
        self.market_meta.write().await.insert(meta.ticker.clone(), meta);
    }

    pub async fn on_snapshot(
        &self,
        ticker: &str,
        yes_levels: &[serde_json::Value],
        no_levels: &[serde_json::Value],
        seq: u64,
    ) {
        let mut books = self.books.lock().await;
        let book = books.entry(ticker.to_string()).or_insert_with(LocalOrderBook::new);
        book.on_snapshot(yes_levels, no_levels, seq);
    }

    pub async fn on_delta(
        &self,
        ticker: &str,
        yes_deltas: &[serde_json::Value],
        no_deltas: &[serde_json::Value],
        side_scalar: Option<&str>,
        price_scalar: Option<serde_json::Value>,
        delta_scalar: Option<serde_json::Value>,
        seq: u64,
    ) {
        let mut books = self.books.lock().await;
        let book = books.entry(ticker.to_string()).or_insert_with(LocalOrderBook::new);
        book.apply_delta(yes_deltas, no_deltas, side_scalar, price_scalar, delta_scalar, seq);
    }

    pub async fn on_user_order(&self, payload: &serde_json::Value) {
        let order_id = payload.get("order_id").and_then(|v| v.as_str()).unwrap_or("");
        let status = payload.get("status").and_then(|v| v.as_str()).unwrap_or("");

        if order_id.is_empty() {
            return;
        }

        let ticker = {
            let order_map = self.order_to_ticker.lock().await;
            order_map.get(order_id).cloned()
        };

        if let Some(ticker) = ticker {
            if status == "canceled" || status == "rejected" {
                // Remove from pending buy orders (no-op if it was a sell order)
                self.pending_orders.lock().await.remove(&ticker);
                self.order_to_ticker.lock().await.remove(order_id);
            }
        }
    }

    /// Handle a message from the `fill` WebSocket channel.
    ///
    /// Fill message format:
    /// ```json
    /// {
    ///   "type": "fill",
    ///   "msg": {
    ///     "order_id": "ee587a1c-...",
    ///     "market_ticker": "HIGHNY-22DEC23-B53.5",
    ///     "side": "yes",
    ///     "yes_price": 75,
    ///     "count": 278,
    ///     "action": "buy",
    ///     "is_taker": true,
    ///     "trade_id": "...",
    ///     "ts": 1671899397
    ///   }
    /// }
    /// ```
    pub async fn on_ws_fill(&self, payload: &serde_json::Value) {
        info!("[MM-FILL] Raw fill payload: {}", payload);

        let order_id = payload.get("order_id").and_then(|v| v.as_str()).unwrap_or("");
        let ticker   = payload.get("market_ticker").and_then(|v| v.as_str()).unwrap_or("");
        let side_str = payload.get("side").and_then(|v| v.as_str()).unwrap_or("");
        let price    = payload.get("yes_price").and_then(|v| v.as_i64()).unwrap_or(0);
        let qty      = payload.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
        let action   = payload.get("action").and_then(|v| v.as_str()).unwrap_or("");

        if order_id.is_empty() || ticker.is_empty() || qty == 0 {
            warn!("[MM-FILL] Dropping fill — missing required fields (order_id={:?} ticker={:?} qty={})", order_id, ticker, qty);
            return;
        }

        // Verify we know this order (ignore fills for orders we didn't place)
        let known = {
            let order_map = self.order_to_ticker.lock().await;
            order_map.contains_key(order_id)
        };
        if !known {
            warn!("[MM-FILL] Dropping fill — unknown order_id={} ticker={} action={} qty={}x@{}¢", order_id, ticker, action, qty, price);
            return;
        }

        if action == "buy" {
            info!(
                "[MM-FILL] BUY filled: {} {}x @ {}¢ order={}",
                ticker, qty, price, order_id
            );

            // Build a synthetic payload that on_fill understands
            let synthetic = serde_json::json!({
                "order_id":     order_id,
                "filled_count": qty,
                "filled_price": price,
                "side":         side_str,
            });
            self.on_fill(ticker, &synthetic).await;
        } else if action == "sell" {
            info!(
                "[MM-FILL] SELL filled: {} {}x @ {}¢ order={}",
                ticker, qty, price, order_id
            );

            // Build a synthetic payload that on_fill understands (position lookup
            // inside on_fill will find the existing position and close it)
            let synthetic = serde_json::json!({
                "order_id":     order_id,
                "filled_count": qty,
                "filled_price": price,
                "side":         side_str,
            });
            self.on_fill(ticker, &synthetic).await;
        }
    }

    async fn on_fill(&self, ticker: &str, payload: &serde_json::Value) {
        let filled_qty = payload.get("filled_count").and_then(|v| v.as_i64()).unwrap_or(0);
        let filled_price = payload.get("filled_price").and_then(|v| v.as_i64()).unwrap_or(0);
        let side_str = payload.get("side").and_then(|v| v.as_str()).unwrap_or("");
        let order_id_str = payload.get("order_id").and_then(|v| v.as_str()).unwrap_or("");

        let side = match side_str {
            "yes" => Side::Yes,
            "no" => Side::No,
            _ => return,
        };

        // Check whether this fill closes an existing position (sell fill) or opens one (buy fill)
        let existing = self.positions.lock().await.get(ticker).cloned();

        if let Some(existing) = existing {
            // ── Sell fill: close position ────────────────────────────────────
            let pnl = if existing.side == Side::Yes {
                (filled_price - existing.entry_price) as f64 * existing.qty as f64 / 100.0
            } else {
                (existing.entry_price - filled_price) as f64 * existing.qty as f64 / 100.0
            };

            *self.daily_pnl.lock().await += pnl;
            *self.balance_cents.lock().await += (pnl * 100.0).round() as i64;

            if pnl > 0.0 {
                self.winning_trades.fetch_add(1, Ordering::Relaxed);
            } else {
                self.losing_trades.fetch_add(1, Ordering::Relaxed);
            }
            self.total_trades_today.fetch_add(1, Ordering::Relaxed);

            self.positions.lock().await.remove(ticker);
            self.sell_states.lock().await.remove(ticker);
            if !order_id_str.is_empty() {
                self.order_to_ticker.lock().await.remove(order_id_str);
            }

            info!(
                "[MM-FILL] Closed {} on {} | entry={}¢ exit={}¢ qty={} pnl=${:.2}",
                if existing.side == Side::Yes { "YES" } else { "NO" },
                ticker,
                existing.entry_price,
                filled_price,
                existing.qty,
                pnl,
            );
        } else {
            // ── Buy fill: open position ─────────────────────────────────────
            // Retrieve intended exit price from the pending quote before removing it
            let target_ask = {
                let pending = self.pending_orders.lock().await;
                pending.get(ticker).map(|(q, _)| q.ask_price)
            };
            let target_exit = target_ask.unwrap_or(filled_price + 2).max(1).min(99);

            // Re-read current order book to check if our stale target is above market
            let target_exit = {
                let books = self.books.lock().await;
                if let Some(book) = books.get(ticker) {
                    let current_best_ask = match side {
                        Side::Yes => book.best_yes_ask().map(|(p, _)| p),
                        Side::No  => book.best_no_ask().map(|(p, _)| p),
                    };
                    if let Some(current_ask) = current_best_ask {
                        if current_ask < target_exit {
                            let adjusted = (current_ask - 1).max(filled_price + 1);
                            info!(
                                "[MM-FILL] Sell pricing adjusted: stale={}¢ current_ask={}¢ fresh={}¢ for {}",
                                target_exit, current_ask, adjusted, ticker
                            );
                            adjusted
                        } else {
                            target_exit
                        }
                    } else {
                        target_exit
                    }
                } else {
                    target_exit
                }
            };

            let position = Position {
                ticker: ticker.to_string(),
                side,
                entry_price: filled_price,
                qty: filled_qty,
                entry_time: Instant::now(),
                order_id: order_id_str.to_string(),
                target_exit_price: target_exit,
            };
            self.positions.lock().await.insert(ticker.to_string(), position);

            info!(
                "[MM-FILL] Opened {} on {} | price={}¢ qty={} target_exit={}¢",
                if side == Side::Yes { "YES" } else { "NO" },
                ticker,
                filled_price,
                filled_qty,
                target_exit,
            );

            // Immediately place a resting sell order at the target ask
            if !self.config.dry_run {
                let client_order_id = format!(
                    "mm_sell_{}_{}",
                    ticker,
                    self.start_time.elapsed().as_secs()
                );
                match self
                    .kalshi_api
                    .place_post_only_limit(
                        ticker,
                        "sell",
                        side_str,
                        target_exit,
                        self.config.order_size as u32,
                        &client_order_id,
                    )
                    .await
                {
                    Ok(resp) => {
                        let sell_order_id = resp
                            .get("order")
                            .and_then(|o| o.get("order_id"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                            .to_string();

                        self.order_to_ticker
                            .lock()
                            .await
                            .insert(sell_order_id.clone(), ticker.to_string());

                        self.sell_states.lock().await.insert(
                            ticker.to_string(),
                            SellState {
                                order_id: sell_order_id,
                                ask_price: target_exit,
                                entry_price: filled_price, // breakeven floor
                                qty: filled_qty,
                                placed_time: Instant::now(),
                                last_fade: Instant::now(),
                            },
                        );

                        info!(
                            "[MM-FILL] Sell order placed @ {}¢ for {}",
                            target_exit, ticker
                        );
                    }
                    Err(e) => {
                        warn!("[MM-FILL] Failed to place sell for {}: {}", ticker, e);
                    }
                }
            } else {
                // Dry-run: track the sell state without placing an order
                self.sell_states.lock().await.insert(
                    ticker.to_string(),
                    SellState {
                        order_id: "dry_run".to_string(),
                        ask_price: target_exit,
                        entry_price: filled_price,
                        qty: filled_qty,
                        placed_time: Instant::now(),
                        last_fade: Instant::now(),
                    },
                );
            }

            // Clean up the buy-side pending/order-map entries
            self.pending_orders.lock().await.remove(ticker);
            if !order_id_str.is_empty() {
                self.order_to_ticker.lock().await.remove(order_id_str);
            }
        }
    }

    pub async fn scan_and_act(&self) {
        // Kill switch first — bail out if any position has moved >10¢ against us
        self.check_kill_switch().await;

        let books = self.books.lock().await;
        let opportunities = self.find_opportunities(&books).await;
        drop(books);

        for opp in opportunities {
            let positions = self.positions.lock().await;
            let pending = self.pending_orders.lock().await;

            if positions.contains_key(&opp.ticker) || pending.contains_key(&opp.ticker) {
                continue;
            }
            // Count filled positions AND pending resting orders against the cap
            if positions.len() + pending.len() >= self.config.max_positions {
                break;
            }

            drop(positions);
            drop(pending);

            // Log opportunity with recency info
            {
                let book = self.books.lock().await;
                if let Some(b) = book.get(&opp.ticker) {
                    let secs = b.seconds_since_activity();
                    let boost = if secs <= 60 {
                        10.0
                    } else if secs <= 300 {
                        5.0
                    } else if secs <= 1800 {
                        2.0
                    } else {
                        0.1
                    };
                    info!(
                        "[MM-OPP] {} spread={}¢ score={} (boost={:.1}x, active {}s ago)",
                        opp.ticker, opp.spread, opp.score, boost, secs
                    );
                }
            }

            if !self.config.dry_run {
                self.execute_quote(&opp).await;
            }
        }
    }

    async fn find_opportunities(
        &self,
        books: &HashMap<String, LocalOrderBook>,
    ) -> Vec<SpreadOpportunity> {
        let mut opportunities = Vec::new();
        let meta_map = self.market_meta.read().await;

        for (ticker, book) in books.iter() {
            // Skip short-duration crypto markets — too fast, too noisy
            if ticker.contains("15M") || ticker.contains("5M") || ticker.contains("1M") {
                continue;
            }

            if !book.is_warm() {
                continue;
            }

            // Skip if already have position or pending order
            let positions = self.positions.lock().await;
            let pending = self.pending_orders.lock().await;
            if positions.contains_key(ticker) || pending.contains_key(ticker) {
                continue;
            }
            drop(positions);
            drop(pending);

            // Check market expiry
            if let Some(meta) = meta_map.get(ticker) {
                if let Some(close_time) = meta.close_time {
                    if SystemTime::now() > close_time {
                        continue;
                    }
                    if close_time
                        .duration_since(SystemTime::now())
                        .unwrap_or(Duration::ZERO)
                        < Duration::from_secs(300)
                    {
                        continue;
                    }
                }
            }

            // Get YES book (required for any opportunity)
            let (yes_bid_px, yes_bid_qty) = match book.best_yes_bid() {
                Some(v) => v,
                None => continue,
            };
            let (yes_ask_px, yes_ask_qty) = match book.best_yes_ask() {
                Some(v) => v,
                None => continue,
            };

            // Recency boost
            let secs_since_activity = book.seconds_since_activity();
            let recency_boost = if secs_since_activity <= 60 {
                10.0
            } else if secs_since_activity <= 300 {
                5.0
            } else if secs_since_activity <= 1800 {
                2.0
            } else {
                0.1
            };

            // Volume factor: log2(1 + volume/1000), default 1.0 if unknown
            let volume_factor = meta_map
                .get(ticker)
                .and_then(|m| m.volume_cents)
                .map(|v| (1.0_f64 + v as f64 / 1000.0).log2().max(1.0))
                .unwrap_or(1.0);

            // YES side same-side spread (bid → ask gap in the YES book)
            let yes_spread = yes_ask_px - yes_bid_px;
            if yes_spread >= self.config.min_spread_cents
                && yes_spread <= 25
                && yes_bid_px >= 15
                && yes_bid_px <= 85
            {
                let depth = yes_bid_qty.min(yes_ask_qty) as f64;
                // score = spread^1.5 * log2(1+depth) * recency_boost * volume_factor
                let score = ((yes_spread as f64).powf(1.5)
                    * (1.0 + depth).log2()
                    * recency_boost
                    * volume_factor
                    * 1000.0) as i64;

                opportunities.push(SpreadOpportunity {
                    ticker: ticker.clone(),
                    side: Side::Yes,
                    entry_price: yes_bid_px,
                    exit_price: yes_ask_px,
                    spread: yes_spread,
                    score,
                    recency_secs: secs_since_activity,
                });
            }

            // NO side same-side spread (only if NO book is populated)
            if let (Some((no_bid_px, no_bid_qty)), Some((no_ask_px, no_ask_qty))) =
                (book.best_no_bid(), book.best_no_ask())
            {
                let no_spread = no_ask_px - no_bid_px;
                if no_spread >= self.config.min_spread_cents
                    && no_spread <= 25
                    && no_bid_px >= 15
                    && no_bid_px <= 85
                {
                    let depth = no_bid_qty.min(no_ask_qty) as f64;
                    let score = ((no_spread as f64).powf(1.5)
                        * (1.0 + depth).log2()
                        * recency_boost
                        * volume_factor
                        * 1000.0) as i64;

                    opportunities.push(SpreadOpportunity {
                        ticker: ticker.clone(),
                        side: Side::No,
                        entry_price: no_bid_px,
                        exit_price: no_ask_px,
                        spread: no_spread,
                        score,
                        recency_secs: secs_since_activity,
                    });
                }
            }
        }

        opportunities.sort_by(|a, b| b.score.cmp(&a.score));

        // Deduplicate by event prefix (everything before the last hyphen segment).
        // After sorting, the first occurrence of each event key is the best-scoring.
        let mut seen_events = std::collections::HashSet::new();
        opportunities.retain(|opp| {
            let event_key = if let Some(pos) = opp.ticker.rfind('-') {
                opp.ticker[..pos].to_string()
            } else {
                opp.ticker.clone()
            };
            seen_events.insert(event_key)
        });

        opportunities.truncate(10);
        opportunities
    }

    /// Place a post-only resting buy order.
    ///
    /// The buy price is offset 15% of the spread from the entry (floored by
    /// `MM_BID_OFFSET_CENTS`).  The intended sell price is stored in the
    /// `QuoteRequest` and used to seed the sell order when we get filled.
    async fn execute_quote(&self, opp: &SpreadOpportunity) {
        let side_str = match opp.side {
            Side::Yes => "yes",
            Side::No => "no",
        };

        // 15% of spread, floored by per-side config values
        let scaled_offset = ((opp.spread as f64) * 0.15).round() as i64;
        let bid_offset = scaled_offset.max(self.config.bid_offset_cents);
        let ask_offset = scaled_offset.max(self.config.ask_offset_cents);

        // entry_price = best bid; buy inside the spread (above the bid)
        let buy_price = (opp.entry_price + bid_offset).max(1).min(99);
        // exit_price = best ask; sell inside the spread (below the ask)
        let sell_price = (opp.exit_price - ask_offset).max(1).min(99);
        // Guard: ensure sell > buy even when offsets eat into a narrow spread
        let sell_price = sell_price.max(buy_price + 1);

        // ── Capital exposure check (filled positions + pending resting orders) ──
        {
            let position_exposure: f64 = {
                let positions = self.positions.lock().await;
                positions
                    .values()
                    .map(|p| (p.entry_price * p.qty) as f64 / 100.0)
                    .sum()
            };
            let pending_exposure: f64 = {
                let pending = self.pending_orders.lock().await;
                pending
                    .values()
                    .map(|(q, _)| (q.bid_price * q.size) as f64 / 100.0)
                    .sum()
            };
            let new_order_cost = (buy_price * self.config.order_size) as f64 / 100.0;
            let total_exposure = position_exposure + pending_exposure + new_order_cost;
            if total_exposure > self.config.max_exposure {
                info!(
                    "[MM] Skipping {} — exposure ${:.2} (pos=${:.2} + pending=${:.2} + new=${:.2}) would exceed ${:.2} limit",
                    opp.ticker, total_exposure, position_exposure, pending_exposure, new_order_cost, self.config.max_exposure
                );
                return;
            }
        }

        let client_order_id = format!(
            "mm_{}_{}",
            opp.ticker,
            self.start_time.elapsed().as_secs()
        );

        match self
            .kalshi_api
            .place_post_only_limit(
                &opp.ticker,
                "buy",
                side_str,
                buy_price,
                self.config.order_size as u32,
                &client_order_id,
            )
            .await
        {
            Ok(resp) => {
                let order_id = resp
                    .get("order")
                    .and_then(|o| o.get("order_id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();

                info!(
                    "[MM-QUOTE] {} {} | buy={}¢ sell={}¢ spread={}¢ size={} | order_id={}",
                    opp.ticker,
                    if opp.side == Side::Yes { "YES" } else { "NO" },
                    buy_price,
                    sell_price,
                    opp.spread,
                    self.config.order_size,
                    order_id,
                );

                let quote = QuoteRequest {
                    ticker: opp.ticker.clone(),
                    bid_price: buy_price,
                    ask_price: sell_price,
                    size: self.config.order_size,
                };

                let mut pending = self.pending_orders.lock().await;
                let mut order_map = self.order_to_ticker.lock().await;
                pending.insert(opp.ticker.clone(), (quote, Instant::now()));
                order_map.insert(order_id, opp.ticker.clone());
            }
            Err(e) => {
                warn!("[MM-QUOTE] Failed to place quote for {}: {}", opp.ticker, e);
            }
        }
    }

    /// Cancel stale buy orders (> `MM_BUY_TIMEOUT_SECS`).
    /// Fade resting sell orders: lower ask 1¢ every 15s after 60s, floor = breakeven.
    /// Force-exit sell orders at breakeven after `MM_BUY_TIMEOUT_SECS`.
    pub async fn check_timeouts(&self) {
        let buy_timeout = Duration::from_secs(self.config.buy_timeout_secs);
        const FADE_START: Duration = Duration::from_secs(60);
        const FADE_INTERVAL: Duration = Duration::from_secs(15);

        // ── Buy order timeouts ────────────────────────────────────────────────
        let to_cancel_buy: Vec<String> = {
            let pending = self.pending_orders.lock().await;
            pending
                .iter()
                .filter(|(_, (_, ts))| ts.elapsed() > buy_timeout)
                .map(|(ticker, _)| ticker.clone())
                .collect()
        };

        for ticker in to_cancel_buy {
            // Find the specific order_id for this ticker before removing it
            let order_id = {
                let order_map = self.order_to_ticker.lock().await;
                order_map.iter()
                    .find(|(_, v)| v.as_str() == ticker.as_str())
                    .map(|(k, _)| k.clone())
            };
            self.pending_orders.lock().await.remove(&ticker);
            if let Some(ref oid) = order_id {
                self.order_to_ticker.lock().await.remove(oid);
                warn!("[MM-TIMEOUT] Buy order for {} timed out — canceling order {}", ticker, oid);
                if let Err(e) = self.kalshi_api.cancel_order(oid).await {
                    debug!("[MM-TIMEOUT] Cancel {} ignored (may have already filled): {}", oid, e);
                }
            } else {
                warn!("[MM-TIMEOUT] Buy order for {} timed out — no order_id found, skipping cancel", ticker);
            }
        }

        // ── Sell order fade + force-exit ──────────────────────────────────────
        let sell_tickers: Vec<String> = {
            self.sell_states.lock().await.keys().cloned().collect()
        };

        for ticker in sell_tickers {
            let state = {
                let sell_states = self.sell_states.lock().await;
                sell_states.get(&ticker).cloned()
            };
            let Some(state) = state else { continue };

            let side = {
                let positions = self.positions.lock().await;
                positions.get(&ticker).map(|p| p.side)
            };
            let Some(side) = side else { continue };
            let side_str = if side == Side::Yes { "yes" } else { "no" };

            // Immediate reprice: if our sell is above current market ask, reprice now
            {
                let current_best_ask = {
                    let books_guard = self.books.lock().await;
                    books_guard.get(&ticker).and_then(|b| match side {
                        Side::Yes => b.best_yes_ask().map(|(p, _)| p),
                        Side::No  => b.best_no_ask().map(|(p, _)| p),
                    })
                };
                if let Some(current_ask) = current_best_ask {
                    if state.ask_price > current_ask {
                        let new_ask = (current_ask - 1).max(state.entry_price);
                        info!(
                            "[MM-REPRICE] {} old={}¢ current_ask={}¢ new={}¢",
                            ticker, state.ask_price, current_ask, new_ask
                        );
                        if !self.config.dry_run {
                            let _ = self.kalshi_api.cancel_order(&state.order_id).await;
                            let client_order_id = format!(
                                "mm_reprice_{}_{}",
                                ticker,
                                self.start_time.elapsed().as_secs()
                            );
                            match self.kalshi_api.place_post_only_limit(
                                &ticker,
                                "sell",
                                side_str,
                                new_ask,
                                state.qty as u32,
                                &client_order_id,
                            ).await {
                                Ok(resp) => {
                                    let new_order_id = resp
                                        .get("order")
                                        .and_then(|o| o.get("order_id"))
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("unknown")
                                        .to_string();
                                    let mut ss = self.sell_states.lock().await;
                                    if let Some(s) = ss.get_mut(&ticker) {
                                        s.ask_price = new_ask;
                                        s.order_id = new_order_id;
                                        s.last_fade = Instant::now();
                                    }
                                }
                                Err(e) => {
                                    warn!("[MM-REPRICE] Failed to re-place sell for {}: {}", ticker, e);
                                }
                            }
                        } else {
                            let mut ss = self.sell_states.lock().await;
                            if let Some(s) = ss.get_mut(&ticker) {
                                s.ask_price = new_ask;
                                s.last_fade = Instant::now();
                            }
                        }
                        continue; // Skip normal fade logic
                    }
                }
            }

            // Force-exit if max hold time exceeded
            if state.placed_time.elapsed() >= buy_timeout {
                warn!(
                    "[MM-FADE] {} max hold reached — force-exit at breakeven {}¢",
                    ticker, state.entry_price
                );
                if !self.config.dry_run {
                    // Cancel current sell, place IOC sell at breakeven
                    let _ = self.kalshi_api.cancel_order(&state.order_id).await;
                    match self.kalshi_api.sell_ioc(&ticker, side_str, state.entry_price, state.qty).await {
                        Ok(resp) => {
                            info!(
                                "[MM-FADE] Force-exit IOC sell {} filled={} for {}",
                                ticker,
                                resp.order.filled_count(),
                                ticker
                            );
                        }
                        Err(e) => {
                            error!("[MM-FADE] Force-exit sell failed for {}: {}", ticker, e);
                        }
                    }
                }
                // State will be cleaned up when fill event arrives
                continue;
            }

            // Fade: adaptive speed based on market activity
            let is_live = {
                let books_guard = self.books.lock().await;
                books_guard.get(&ticker)
                    .map(|b| b.seconds_since_activity() <= 60)
                    .unwrap_or(false)
            };
            let (fade_start, fade_interval, fade_step) = if is_live {
                (Duration::from_secs(10), Duration::from_secs(5), 2i64)
            } else {
                (FADE_START, FADE_INTERVAL, 1i64)
            };

            if state.placed_time.elapsed() > fade_start
                && state.last_fade.elapsed() >= fade_interval
            {
                let new_ask = (state.ask_price - fade_step).max(state.entry_price);
                if new_ask < state.ask_price {
                    info!(
                        "[MM-FADE] {} lowering ask {}¢ → {}¢ (floor {}¢)",
                        ticker, state.ask_price, new_ask, state.entry_price
                    );

                    if !self.config.dry_run {
                        // Cancel current sell, re-post at lower price
                        let _ = self.kalshi_api.cancel_order(&state.order_id).await;
                        let client_order_id = format!(
                            "mm_fade_{}_{}",
                            ticker,
                            self.start_time.elapsed().as_secs()
                        );
                        match self
                            .kalshi_api
                            .place_post_only_limit(
                                &ticker,
                                "sell",
                                side_str,
                                new_ask,
                                state.qty as u32,
                                &client_order_id,
                            )
                            .await
                        {
                            Ok(resp) => {
                                let new_order_id = resp
                                    .get("order")
                                    .and_then(|o| o.get("order_id"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown")
                                    .to_string();
                                let mut sell_states = self.sell_states.lock().await;
                                if let Some(s) = sell_states.get_mut(&ticker) {
                                    s.ask_price = new_ask;
                                    s.order_id = new_order_id;
                                    s.last_fade = Instant::now();
                                }
                            }
                            Err(e) => {
                                warn!(
                                    "[MM-FADE] Failed to re-place sell for {}: {}",
                                    ticker, e
                                );
                            }
                        }
                    } else {
                        // Dry-run: just update in-memory state
                        let mut sell_states = self.sell_states.lock().await;
                        if let Some(s) = sell_states.get_mut(&ticker) {
                            s.ask_price = new_ask;
                            s.last_fade = Instant::now();
                        }
                    }
                }
            }
        }
    }

    /// Kill switch: if any position has moved >10¢ adversely, cancel all orders
    /// and attempt to exit at market (IOC sell near breakeven).
    async fn check_kill_switch(&self) {
        const KILL_CENTS: i64 = 10;

        let positions_snap: Vec<(String, Side, i64, i64)> = {
            let positions = self.positions.lock().await;
            positions
                .values()
                .map(|p| (p.ticker.clone(), p.side, p.entry_price, p.qty))
                .collect()
        };

        if positions_snap.is_empty() {
            return;
        }

        let mut kill_triggered = false;
        {
            let books = self.books.lock().await;
            for (ticker, side, entry_price, _qty) in &positions_snap {
                if let Some(book) = books.get(ticker) {
                    if let Some(current_bid) = book.best_bid_for_side(*side) {
                        if entry_price - current_bid >= KILL_CENTS {
                            error!(
                                "[MM-KILL] {} adverse move: entry={}¢ bid={}¢ ({}¢ loss) — KILL SWITCH",
                                ticker,
                                entry_price,
                                current_bid,
                                entry_price - current_bid
                            );
                            kill_triggered = true;
                            break;
                        }
                    }
                }
            }
        }

        if !kill_triggered {
            return;
        }

        // Cancel all resting orders first
        if let Err(e) = self.kalshi_api.cancel_all_orders().await {
            error!("[MM-KILL] Batch cancel failed: {}", e);
        }

        // IOC sell all open positions at 1¢ above breakeven (ensures fill)
        if !self.config.dry_run {
            for (ticker, side, entry_price, qty) in &positions_snap {
                let side_str = if *side == Side::Yes { "yes" } else { "no" };
                let exit_price = (entry_price - KILL_CENTS + 1).max(1);
                match self.kalshi_api.sell_ioc(ticker, side_str, exit_price, *qty).await {
                    Ok(resp) => {
                        info!(
                            "[MM-KILL] Emergency IOC sell {} filled={} for {}",
                            ticker,
                            resp.order.filled_count(),
                            ticker
                        );
                    }
                    Err(e) => {
                        error!("[MM-KILL] Emergency sell failed for {}: {}", ticker, e);
                    }
                }
            }
        }

        // Clear all in-memory state
        self.positions.lock().await.clear();
        self.sell_states.lock().await.clear();
        self.order_to_ticker.lock().await.clear();
        self.pending_orders.lock().await.clear();

        error!("[MM-KILL] Kill switch complete — pausing 60s before resuming");
        tokio::time::sleep(Duration::from_secs(60)).await;
    }

    // ─── REST Fill Polling ────────────────────────────────────────────────────

    /// Poll recent fills via REST as a safety net for missed WebSocket fill events.
    /// Called every ~10 seconds from the scanner task.
    pub async fn poll_rest_fills(&self) {
        let resp = match self.kalshi_api.get_recent_fills(10).await {
            Ok(v) => v,
            Err(e) => {
                debug!("[MM-REST] poll_rest_fills error: {}", e);
                return;
            }
        };

        let fills = match resp.get("fills").and_then(|v| v.as_array()) {
            Some(arr) => arr.clone(),
            None => return,
        };

        for fill in &fills {
            let order_id  = fill.get("order_id").and_then(|v| v.as_str()).unwrap_or("");
            let ticker    = fill.get("ticker").and_then(|v| v.as_str()).unwrap_or("");
            let side_str  = fill.get("side").and_then(|v| v.as_str()).unwrap_or("");
            let yes_price = fill.get("yes_price").and_then(|v| v.as_i64()).unwrap_or(0);
            let count     = fill.get("count").and_then(|v| v.as_i64()).unwrap_or(0);
            let action    = fill.get("action").and_then(|v| v.as_str()).unwrap_or("");

            if order_id.is_empty() || ticker.is_empty() {
                continue;
            }

            // Check if we missed the WS fill: ticker is in pending_orders but NOT in positions
            let in_pending  = self.pending_orders.lock().await.contains_key(ticker);
            let in_positions = self.positions.lock().await.contains_key(ticker);

            if in_pending && !in_positions {
                info!(
                    "[MM-REST-FILL] Missed WS fill detected: {} {}x@{}¢ order={} action={}",
                    ticker, count, yes_price, order_id, action
                );
                // Build synthetic payload matching WS fill format (market_ticker field)
                let synthetic = serde_json::json!({
                    "order_id":     order_id,
                    "market_ticker": ticker,
                    "side":         side_str,
                    "yes_price":    yes_price,
                    "count":        count,
                    "action":       action,
                });
                self.on_ws_fill(&synthetic).await;
            }
        }
    }

    // ─── State API ─────────────────────────────────────────────────────────────

    pub async fn status_json(&self) -> serde_json::Value {
        let daily_pnl = *self.daily_pnl.lock().await;
        let balance = *self.balance_cents.lock().await;
        let total = self.total_trades_today.load(Ordering::Relaxed);
        let wins = self.winning_trades.load(Ordering::Relaxed);
        let win_rate = if total > 0 {
            wins as f64 / total as f64
        } else {
            0.0
        };

        let (open_positions, position_exposure): (usize, f64) = {
            let positions = self.positions.lock().await;
            let exp = positions.values().map(|p| (p.entry_price * p.qty) as f64 / 100.0).sum();
            (positions.len(), exp)
        };
        let (pending_count, pending_exposure): (usize, f64) = {
            let pending = self.pending_orders.lock().await;
            let exp = pending.values().map(|(q, _)| (q.bid_price * q.size) as f64 / 100.0).sum();
            (pending.len(), exp)
        };
        let total_exposure = position_exposure + pending_exposure;

        serde_json::json!({
            "mode": if self.config.dry_run { "dry_run" } else { "live" },
            "balance": format!("{:.2}", balance as f64 / 100.0),
            "daily_pnl": format!("{:.2}", daily_pnl),
            "open_positions": open_positions,
            "pending_orders": pending_count,
            "exposure": format!("{:.2}", total_exposure),
            "total_trades_today": total,
            "win_rate": win_rate,
            "uptime_secs": self.start_time.elapsed().as_secs(),
        })
    }

    pub async fn positions_json(&self) -> serde_json::Value {
        let positions = self.positions.lock().await;
        let sell_states = self.sell_states.lock().await;
        let arr: Vec<_> = positions
            .values()
            .map(|p| {
                let current_ask = sell_states
                    .get(&p.ticker)
                    .map(|s| s.ask_price)
                    .unwrap_or(p.target_exit_price);
                serde_json::json!({
                    "ticker": p.ticker,
                    "side": if p.side == Side::Yes { "yes" } else { "no" },
                    "entry_price": p.entry_price,
                    "target_exit": p.target_exit_price,
                    "current_ask": current_ask,
                    "qty": p.qty,
                    "duration_secs": p.entry_time.elapsed().as_secs(),
                })
            })
            .collect();

        serde_json::json!(arr)
    }

    pub async fn opportunities_json(&self) -> serde_json::Value {
        let books = self.books.lock().await;
        let opportunities = self.find_opportunities(&books).await;

        let arr: Vec<_> = opportunities
            .iter()
            .map(|o| {
                serde_json::json!({
                    "ticker": o.ticker,
                    "side": if o.side == Side::Yes { "yes" } else { "no" },
                    "bid_price": o.entry_price,
                    "ask_price": o.exit_price,
                    "entry_price": o.entry_price,
                    "exit_price": o.exit_price,
                    "spread": o.spread,
                    "score": o.score,
                    "recency_secs": o.recency_secs,
                })
            })
            .collect();

        serde_json::json!(arr)
    }

    pub async fn trades_json(&self) -> serde_json::Value {
        let total = self.total_trades_today.load(Ordering::Relaxed);
        let wins = self.winning_trades.load(Ordering::Relaxed);
        let losses = self.losing_trades.load(Ordering::Relaxed);

        serde_json::json!({
            "total": total,
            "wins": wins,
            "losses": losses,
            "daily_pnl": *self.daily_pnl.lock().await,
        })
    }
}
