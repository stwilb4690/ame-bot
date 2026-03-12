// src/market_maker.rs
//
// AME Market Maker — single-platform Kalshi market maker engine.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use serde::Serialize;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info, warn};

use crate::kalshi::KalshiApiClient;
use crate::telegram::TelegramClient;
use crate::types::kalshi_maker_fee_cents;

// ─── Side Enum ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum Side {
    Yes,
    No,
}

// ─── OrderEvent ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, serde::Serialize)]
pub struct OrderEvent {
    pub timestamp: String,
    pub event_type: String,
    pub ticker: String,
    pub title: String,
    pub side: String,
    pub price: i64,
    pub qty: i64,
    pub order_id: String,
    pub book_bid: Option<i64>,
    pub book_ask: Option<i64>,
    pub book_spread: Option<i64>,
    pub book_age_secs: Option<u64>,
    pub fill_price: Option<i64>,
    pub entry_price: Option<i64>,
    pub pnl_dollars: Option<f64>,
    pub hold_secs: Option<u64>,
    pub spread_captured: Option<i64>,
    pub fades: Option<u32>,
    pub reprices: Option<u32>,
    pub skip_reason: Option<String>,
    pub exposure_at_event: Option<f64>,
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
            min_spread_cents: parse_env("MM_MIN_SPREAD_CENTS", "3").parse().unwrap_or(3),
            min_net_spread_cents: parse_env("MM_MIN_NET_SPREAD_CENTS", "1").parse().unwrap_or(1),
            bid_offset_cents: parse_env("MM_BID_OFFSET_CENTS", "1").parse().unwrap_or(1),
            ask_offset_cents: parse_env("MM_ASK_OFFSET_CENTS", "1").parse().unwrap_or(1),
            order_size: parse_env("MM_ORDER_SIZE", "10").parse().unwrap_or(10),
            max_positions: parse_env("MM_MAX_POSITIONS", "6").parse().unwrap_or(6),
            daily_loss_limit: parse_env("MM_DAILY_LOSS_LIMIT", "15").parse().unwrap_or(15.0),
            dry_run: std::env::var("MM_DRY_RUN").map(|v| v != "0").unwrap_or(true),
            balance_reserve: parse_env("MM_BALANCE_RESERVE", "50").parse().unwrap_or(50.0),
            buy_timeout_secs: parse_env("MM_BUY_TIMEOUT_SECS", "60").parse().unwrap_or(60),
            max_exposure: parse_env("MM_MAX_EXPOSURE", "40").parse().unwrap_or(40.0),
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
    pub fill_book_ask: Option<i64>,
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
    pub fades: u32,
    pub reprices: u32,
}

// ─── Quote Request ────────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct QuoteRequest {
    pub ticker: String,
    pub side: Side,
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
    pub depth: i64,
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
    pub paused: AtomicBool,
    pub order_events: Mutex<Vec<OrderEvent>>,
    pub skip_counts: Mutex<HashMap<String, u64>>,
    /// Tickers with an in-flight IOC sell (stop-loss or kill-switch).
    /// Prevents the WS fill handler from re-entering the buy branch
    /// when the position has already been removed from `positions`.
    pub closing_tickers: Mutex<HashSet<String>>,
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
            paused: AtomicBool::new(false),
            order_events: Mutex::new(Vec::new()),
            skip_counts: Mutex::new(HashMap::new()),
            closing_tickers: Mutex::new(HashSet::new()),
        }
    }

    pub async fn register_market(&self, meta: MarketMeta) {
        self.market_meta.write().await.insert(meta.ticker.clone(), meta);
    }

    async fn make_event(&self, event_type: &str, ticker: &str, side: &str, price: i64, qty: i64, order_id: &str) -> OrderEvent {
        let title = {
            let meta = self.market_meta.read().await;
            meta.get(ticker).map(|m| m.title.clone()).unwrap_or_default()
        };
        let (book_bid, book_ask, book_age) = {
            let books = self.books.lock().await;
            if let Some(book) = books.get(ticker) {
                let bid = if side == "yes" { book.best_yes_bid().map(|(p,_)| p) } else { book.best_no_bid().map(|(p,_)| p) };
                let ask = if side == "yes" { book.best_yes_ask().map(|(p,_)| p) } else { book.best_no_ask().map(|(p,_)| p) };
                (bid, ask, Some(book.seconds_since_activity()))
            } else {
                (None, None, None)
            }
        };
        let book_spread = match (book_bid, book_ask) { (Some(b), Some(a)) => Some(a - b), _ => None };
        let exposure = {
            let pos_exp: f64 = self.positions.lock().await.values().map(|p| (p.entry_price * p.qty) as f64 / 100.0).sum();
            let pend_exp: f64 = self.pending_orders.lock().await.values().map(|(q,_)| (q.bid_price * q.size) as f64 / 100.0).sum();
            Some(pos_exp + pend_exp)
        };
        OrderEvent {
            timestamp: chrono::Utc::now().to_rfc3339(),
            event_type: event_type.to_string(),
            ticker: ticker.to_string(),
            title,
            side: side.to_string(),
            price, qty,
            order_id: order_id.to_string(),
            book_bid, book_ask, book_spread, book_age_secs: book_age,
            fill_price: None, entry_price: None, pnl_dollars: None,
            hold_secs: None, spread_captured: None, fades: None, reprices: None,
            skip_reason: None, exposure_at_event: exposure,
        }
    }

    async fn log_event(&self, event: OrderEvent) {
        if let Ok(line) = serde_json::to_string(&event) {
            use std::io::Write;
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true).append(true)
                .open("logs/mm_events.jsonl")
            {
                let _ = writeln!(f, "{}", line);
            }
        }
        let mut events = self.order_events.lock().await;
        events.push(event);
        if events.len() > 200 { let excess = events.len() - 200; events.drain(0..excess); }
    }

    async fn record_skip(&self, reason: &str) {
        let mut skips = self.skip_counts.lock().await;
        *skips.entry(reason.to_string()).or_insert(0) += 1;
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

            let synthetic = serde_json::json!({
                "order_id":     order_id,
                "filled_count": qty,
                "filled_price": price,
                "side":         side_str,
                "action":       action,
            });
            self.on_fill(ticker, &synthetic).await;
        } else if action == "sell" {
            info!(
                "[MM-FILL] SELL filled: {} {}x @ {}¢ order={}",
                ticker, qty, price, order_id
            );

            let synthetic = serde_json::json!({
                "order_id":     order_id,
                "filled_count": qty,
                "filled_price": price,
                "side":         side_str,
                "action":       action,
            });
            self.on_fill(ticker, &synthetic).await;
        }
    }

    async fn on_fill(&self, ticker: &str, payload: &serde_json::Value) {
        let filled_qty = payload.get("filled_count").and_then(|v| v.as_i64()).unwrap_or(0);
        let filled_price = payload.get("filled_price").and_then(|v| v.as_i64()).unwrap_or(0);
        let side_str = payload.get("side").and_then(|v| v.as_str()).unwrap_or("");
        let order_id_str = payload.get("order_id").and_then(|v| v.as_str()).unwrap_or("");
        let action = payload.get("action").and_then(|v| v.as_str()).unwrap_or("");

        let side = match side_str {
            "yes" => Side::Yes,
            "no" => Side::No,
            _ => return,
        };

        // Use the action field from the WS fill to decide buy vs sell.
        // Fall back to position-existence check only when action is missing
        // (e.g. synthetic fills from REST polling).
        let existing = self.positions.lock().await.get(ticker).cloned();
        let is_closing = self.closing_tickers.lock().await.contains(ticker);
        let is_sell = action == "sell"
            || (action.is_empty() && (existing.is_some() || is_closing));

        if is_sell {
            // Remove from closing_tickers if present (stop-loss IOC has now filled)
            self.closing_tickers.lock().await.remove(ticker);

            let existing = match existing {
                Some(e) => e,
                None => {
                    warn!(
                        "[MM-FILL] Sell fill for {} but no position found (already closed?), skipping",
                        ticker
                    );
                    return;
                }
            };
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

            // Read fades/reprices BEFORE removing sell_states
            let (fades, reprices) = {
                let ss = self.sell_states.lock().await;
                ss.get(ticker).map(|s| (s.fades, s.reprices)).unwrap_or((0, 0))
            };

            self.positions.lock().await.remove(ticker);
            self.sell_states.lock().await.remove(ticker);
            if !order_id_str.is_empty() {
                self.order_to_ticker.lock().await.remove(order_id_str);
            }

            info!(
                "[MM-FILL] Closed {} on {} | entry={}¢ exit={}¢ qty={} pnl=${:.2} fades={} reprices={}",
                if existing.side == Side::Yes { "YES" } else { "NO" },
                ticker,
                existing.entry_price,
                filled_price,
                existing.qty,
                pnl,
                fades,
                reprices,
            );
            let sell_side_str = if existing.side == Side::Yes { "yes" } else { "no" };
            let mut event = self.make_event("sell_fill", ticker, sell_side_str, filled_price, existing.qty, order_id_str).await;
            event.fill_price = Some(filled_price);
            event.entry_price = Some(existing.entry_price);
            event.pnl_dollars = Some(pnl);
            event.hold_secs = Some(existing.entry_time.elapsed().as_secs());
            event.spread_captured = Some(filled_price - existing.entry_price);
            event.fades = Some(fades);
            event.reprices = Some(reprices);
            self.log_event(event).await;
        } else if action == "buy" || action.is_empty() {
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
                                "[MM-FILL] Sell target lowered: stale={}¢ current_ask={}¢ fresh={}¢ for {}",
                                target_exit, current_ask, adjusted, ticker
                            );
                            adjusted
                        } else if current_ask > target_exit + 2 {
                            // Market moved up — raise target to capture more profit (cap +5¢)
                            let adjusted = (current_ask - 1).min(target_exit + 5);
                            info!(
                                "[MM-FILL] Sell target raised: stale={}¢ current_ask={}¢ fresh={}¢ for {}",
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

            let fill_book_ask = {
                let books = self.books.lock().await;
                books.get(ticker).and_then(|b| match side {
                    Side::Yes => b.best_yes_ask().map(|(p, _)| p),
                    Side::No  => b.best_no_ask().map(|(p, _)| p),
                })
            };
            let position = Position {
                ticker: ticker.to_string(),
                side,
                entry_price: filled_price,
                qty: filled_qty,
                entry_time: Instant::now(),
                order_id: order_id_str.to_string(),
                target_exit_price: target_exit,
                fill_book_ask,
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

            let mut event = self.make_event("buy_fill", ticker, side_str, filled_price, filled_qty, order_id_str).await;
            event.fill_price = Some(filled_price);
            self.log_event(event).await;

            // Compute true breakeven: fill price + buy-side maker fee already paid.
            // The fade logic must never lower the sell below this floor.
            let buy_fee = kalshi_maker_fee_cents(filled_price as u16) as i64;
            let true_breakeven = filled_price + buy_fee;

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
                        filled_qty as u32,
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
                                entry_price: true_breakeven, // fee-aware breakeven floor
                                qty: filled_qty,
                                placed_time: Instant::now(),
                                last_fade: Instant::now(),
                                fades: 0,
                                reprices: 0,
                            },
                        );

                        info!(
                            "[MM-FILL] Sell order placed @ {}¢ for {}",
                            target_exit, ticker
                        );
                    }
                    Err(e) => {
                        warn!("[MM-FILL] Post-only sell failed for {} — IOC exit at bid: {}", ticker, e);
                        let fallback_price = {
                            let books = self.books.lock().await;
                            books.get(ticker)
                                .and_then(|b| b.best_bid_for_side(side))
                                .unwrap_or(true_breakeven)
                        };
                        match self.kalshi_api.sell_ioc(ticker, side_str, fallback_price, filled_qty).await {
                            Ok(resp) => {
                                info!("[MM-FILL] Fallback IOC sell {} at {}¢ filled={}", ticker, fallback_price, resp.order.filled_count());
                            }
                            Err(e2) => {
                                error!("[MM-FILL] Fallback IOC also failed for {}: {}", ticker, e2);
                            }
                        }
                        // Create sell state so check_timeouts can manage if IOC didn't fully fill
                        self.sell_states.lock().await.insert(
                            ticker.to_string(),
                            SellState {
                                order_id: "fallback".to_string(),
                                ask_price: true_breakeven,
                                entry_price: true_breakeven,
                                qty: filled_qty,
                                placed_time: Instant::now(),
                                last_fade: Instant::now(),
                                fades: 0,
                                reprices: 0,
                            },
                        );

                        // Alert operator: position has no active exit order
                        if let Some(tg) = &self.telegram {
                            tg.send_alert(
                                crate::telegram::AlertType::CircuitBreaker,
                                0,
                                format!(
                                    "[MM] STUCK POSITION: {} -- both sell attempts failed, manual intervention needed",
                                    ticker
                                ),
                            );
                        }
                    }
                }
            } else {
                // Dry-run: track the sell state without placing an order
                self.sell_states.lock().await.insert(
                    ticker.to_string(),
                    SellState {
                        order_id: "dry_run".to_string(),
                        ask_price: target_exit,
                        entry_price: true_breakeven, // fee-aware breakeven floor
                        qty: filled_qty,
                        placed_time: Instant::now(),
                        last_fade: Instant::now(),
                        fades: 0,
                        reprices: 0,
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

        if self.paused.load(Ordering::Relaxed) {
            return;
        }

        let books = self.books.lock().await;
        let opportunities = self.find_opportunities(&books).await;
        drop(books);

        for opp in opportunities {
            let positions = self.positions.lock().await;
            let pending = self.pending_orders.lock().await;

            if positions.contains_key(&opp.ticker) || pending.contains_key(&opp.ticker) {
                self.record_skip("duplicate_ticker").await;
                continue;
            }
            // Count filled positions AND pending resting orders against the cap
            if positions.len() + pending.len() >= self.config.max_positions {
                self.record_skip("max_positions").await;
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

            // Hard recency filter: skip stale books with no activity in 30s.
            // These are phantom spreads with no active counterparties.
            let secs_since_activity = book.seconds_since_activity();
            if secs_since_activity > 30 {
                continue;
            }
            let recency_boost = 10.0_f64;

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
                // Fee-aware net profit check: only trade if spread covers both legs' fees
                let buy_fee = kalshi_maker_fee_cents((yes_bid_px + 1) as u16) as i64;
                let sell_fee = kalshi_maker_fee_cents((yes_ask_px - 1) as u16) as i64;
                let net_spread = yes_spread - buy_fee - sell_fee;
                if net_spread >= self.config.min_net_spread_cents {
                    let min_depth = yes_bid_qty.min(yes_ask_qty);
                    if min_depth < 5 {
                        continue; // Too thin to trade safely
                    }
                    let depth = min_depth as f64;
                    let capped_spread = (yes_spread as f64).min(12.0);
                    let score = (capped_spread.powf(1.5)
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
                        depth: min_depth,
                        score,
                        recency_secs: secs_since_activity,
                    });
                }
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
                    // Fee-aware net profit check
                    let buy_fee = kalshi_maker_fee_cents((no_bid_px + 1) as u16) as i64;
                    let sell_fee = kalshi_maker_fee_cents((no_ask_px - 1) as u16) as i64;
                    let net_spread = no_spread - buy_fee - sell_fee;
                    if net_spread >= self.config.min_net_spread_cents {
                        let min_depth = no_bid_qty.min(no_ask_qty);
                        if min_depth < 5 {
                            continue;
                        }
                        let depth = min_depth as f64;
                        let capped_spread = (no_spread as f64).min(12.0);
                        let score = (capped_spread.powf(1.5)
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
                            depth: min_depth,
                            score,
                            recency_secs: secs_since_activity,
                        });
                    }
                }
            }
        }

        opportunities.sort_by(|a, b| b.score.cmp(&a.score));

        // No event-prefix dedup — allow multiple market types per game
        // (moneyline, spread, total) to increase quoting volume.
        // The max_positions cap already limits total exposure.

        opportunities.truncate(15);
        opportunities
    }

    /// Place a post-only resting buy order.
    ///
    /// The buy price is offset 15% of the spread from the entry (floored by
    /// `MM_BID_OFFSET_CENTS`).  The intended sell price is stored in the
    /// `QuoteRequest` and used to seed the sell order when we get filled.
    async fn execute_quote(&self, opp: &SpreadOpportunity) {
        // Daily loss limit check
        {
            let pnl = *self.daily_pnl.lock().await;
            if pnl <= -self.config.daily_loss_limit {
                info!("[MM] Daily loss limit reached (${:.2}) — halting", pnl);
                self.record_skip("daily_loss_limit").await;
                return;
            }
        }

        let side_str = match opp.side {
            Side::Yes => "yes",
            Side::No => "no",
        };

        // Buy at bid + 1¢ to stay on the maker side of the book.
        // Midpoint buying was crossing the spread and getting taker fills.
        let buy_price = (opp.entry_price + 1).max(1).min(99);

        // Skip if our buy would cross or equal the ask (post-only would reject)
        if buy_price >= opp.exit_price {
            self.record_skip("cross_spread").await;
            return;
        }

        // Sell inside the ask by 1¢ (or ask_offset if configured higher)
        let ask_offset = self.config.ask_offset_cents;
        let sell_price = (opp.exit_price - ask_offset).max(1).min(99);
        // Guard: ensure sell > buy even when offsets eat into a narrow spread
        let sell_price = sell_price.max(buy_price + 1);

        // Depth-aware sizing: don't place more contracts than the book can absorb
        let effective_size = self.config.order_size.min(opp.depth).max(1);

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
            let new_order_cost = (buy_price * effective_size) as f64 / 100.0;
            let total_exposure = position_exposure + pending_exposure + new_order_cost;
            if total_exposure > self.config.max_exposure {
                info!(
                    "[MM] Skipping {} — exposure ${:.2} (pos=${:.2} + pending=${:.2} + new=${:.2}) would exceed ${:.2} limit",
                    opp.ticker, total_exposure, position_exposure, pending_exposure, new_order_cost, self.config.max_exposure
                );
                self.record_skip("exposure_cap").await;
                let mut event = self.make_event("order_skipped", &opp.ticker, side_str, buy_price, effective_size, "").await;
                event.skip_reason = Some("exposure_cap".to_string());
                self.log_event(event).await;
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
                effective_size as u32,
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
                    "[MM-QUOTE] {} {} | buy={}¢ sell={}¢ spread={}¢ size={} depth={} | order_id={}",
                    opp.ticker,
                    if opp.side == Side::Yes { "YES" } else { "NO" },
                    buy_price,
                    sell_price,
                    opp.spread,
                    effective_size,
                    opp.depth,
                    order_id,
                );

                let quote = QuoteRequest {
                    ticker: opp.ticker.clone(),
                    side: opp.side,
                    bid_price: buy_price,
                    ask_price: sell_price,
                    size: effective_size,
                };

                let mut pending = self.pending_orders.lock().await;
                let mut order_map = self.order_to_ticker.lock().await;
                pending.insert(opp.ticker.clone(), (quote, Instant::now()));
                order_map.insert(order_id.clone(), opp.ticker.clone());

                let mut event = self.make_event("order_placed", &opp.ticker, side_str, buy_price, effective_size, &order_id).await;
                event.entry_price = Some(sell_price);
                self.log_event(event).await;
            }
            Err(e) => {
                warn!("[MM-QUOTE] Failed to place quote for {}: {}", opp.ticker, e);
            }
        }
    }

    /// Cancel stale buy orders (> `MM_BUY_TIMEOUT_SECS`).
    /// Reprice buy orders every 30s if the market has moved.
    /// Fade resting sell orders: lower ask after 20s, floor = entry_price + 1.
    /// Force-exit sell orders at breakeven after `MM_BUY_TIMEOUT_SECS`.
    pub async fn check_timeouts(&self) {
        let buy_timeout = Duration::from_secs(self.config.buy_timeout_secs);
        const BUY_REPRICE_INTERVAL: Duration = Duration::from_secs(30);

        // ── Buy order repricing + timeouts ──────────────────────────────────
        let pending_snap: Vec<(String, Side, i64, i64, i64, Instant)> = {
            let pending = self.pending_orders.lock().await;
            pending
                .iter()
                .map(|(ticker, (q, ts))| (ticker.clone(), q.side, q.bid_price, q.ask_price, q.size, *ts))
                .collect()
        };

        for (ticker, pending_side, old_bid, old_ask, old_size, placed_at) in pending_snap {
            // Hard timeout — cancel and move on
            if placed_at.elapsed() > buy_timeout {
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
                let event = self.make_event("buy_timeout", &ticker, "", 0, 0, order_id.as_deref().unwrap_or("")).await;
                self.log_event(event).await;
                continue;
            }

            // Mid-life reprice — if 30s+ old and market moved, cancel and re-place at fresh mid
            if placed_at.elapsed() >= BUY_REPRICE_INTERVAL && !self.config.dry_run {
                let fresh_mid = {
                    let books = self.books.lock().await;
                    books.get(&ticker).and_then(|b| {
                        let (bid, ask) = match pending_side {
                            Side::Yes => (b.best_yes_bid().map(|(p, _)| p)?, b.best_yes_ask().map(|(p, _)| p)?),
                            Side::No  => (b.best_no_bid().map(|(p, _)| p)?,  b.best_no_ask().map(|(p, _)| p)?),
                        };
                        Some((bid + ask) / 2)
                    })
                };
                if let Some(new_mid) = fresh_mid {
                    // Only reprice if price shifted by 2+ cents
                    if (new_mid - old_bid).abs() >= 2 {
                        let order_id = {
                            let order_map = self.order_to_ticker.lock().await;
                            order_map.iter()
                                .find(|(_, v)| v.as_str() == ticker.as_str())
                                .map(|(k, _)| k.clone())
                        };
                        if let Some(ref oid) = order_id {
                            let _ = self.kalshi_api.cancel_order(oid).await;
                            self.order_to_ticker.lock().await.remove(oid);
                        }
                        self.pending_orders.lock().await.remove(&ticker);

                        let new_buy = new_mid.max(1).min(99);
                        let client_order_id = format!(
                            "mm_repbuy_{}_{}",
                            ticker,
                            self.start_time.elapsed().as_secs()
                        );
                        let side_str = if pending_side == Side::Yes { "yes" } else { "no" };
                        match self
                            .kalshi_api
                            .place_post_only_limit(&ticker, "buy", side_str, new_buy, old_size as u32, &client_order_id)
                            .await
                        {
                            Ok(resp) => {
                                let new_oid = resp
                                    .get("order")
                                    .and_then(|o| o.get("order_id"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown")
                                    .to_string();
                                info!(
                                    "[MM-REPRICE-BUY] {} old={}¢ new={}¢ order={}",
                                    ticker, old_bid, new_buy, new_oid
                                );
                                let quote = QuoteRequest {
                                    ticker: ticker.clone(),
                                    side: pending_side,
                                    bid_price: new_buy,
                                    ask_price: old_ask,
                                    size: old_size,
                                };
                                self.pending_orders.lock().await.insert(ticker.clone(), (quote, Instant::now()));
                                self.order_to_ticker.lock().await.insert(new_oid, ticker.clone());
                            }
                            Err(e) => {
                                warn!("[MM-REPRICE-BUY] Failed to re-place buy for {}: {}", ticker, e);
                            }
                        }
                    }
                }
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

            // ── Breakeven stop-loss: if market bid <= our breakeven, IOC exit immediately ──
            {
                let current_bid = {
                    let books_guard = self.books.lock().await;
                    books_guard.get(&ticker).and_then(|b| b.best_bid_for_side(side))
                };
                if let Some(bid) = current_bid {
                    let sell_fee = kalshi_maker_fee_cents(bid as u16) as i64;
                    let net_proceeds = bid - sell_fee;
                    if net_proceeds <= state.entry_price {
                        warn!(
                            "[MM-STOPLOSS] {} bid={}¢ net={}¢ <= breakeven={}¢ — exiting immediately",
                            ticker, bid, net_proceeds, state.entry_price
                        );
                        if !self.config.dry_run {
                            let _ = self.kalshi_api.cancel_order(&state.order_id).await;
                            // Mark ticker as closing BEFORE the IOC sell so the WS fill
                            // handler knows this is a sell, not a new buy.
                            self.closing_tickers.lock().await.insert(ticker.clone());
                            match self.kalshi_api.sell_ioc(&ticker, side_str, bid, state.qty).await {
                                Ok(resp) => {
                                    let filled = resp.order.filled_count();
                                    info!("[MM-STOPLOSS] {} IOC sell at {}¢ filled={}", ticker, bid, filled);
                                    if filled == 0 {
                                        warn!("[MM-STOPLOSS] {} IOC didn't fill — retrying next cycle", ticker);
                                        self.closing_tickers.lock().await.remove(&ticker);
                                        continue;
                                    }
                                }
                                Err(e) => {
                                    error!("[MM-STOPLOSS] IOC sell failed for {}: {} — retrying next cycle", ticker, e);
                                    self.closing_tickers.lock().await.remove(&ticker);
                                    continue;
                                }
                            }
                        }
                        self.sell_states.lock().await.remove(&ticker);
                        self.positions.lock().await.remove(&ticker);
                        self.closing_tickers.lock().await.remove(&ticker);
                        self.order_to_ticker.lock().await.retain(|_, v| v != &ticker);

                        let mut event = self.make_event("stop_loss", &ticker, side_str, bid, state.qty, &state.order_id).await;
                        event.entry_price = Some(state.entry_price);
                        self.log_event(event).await;
                        continue;
                    }
                }
            }

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
                        {
                            let mut ss = self.sell_states.lock().await;
                            if let Some(s) = ss.get_mut(&ticker) {
                                s.reprices += 1;
                            }
                        }
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
                        let mut event = self.make_event("sell_reprice", &ticker, side_str, new_ask, state.qty, &state.order_id).await;
                        event.entry_price = Some(state.entry_price);
                        self.log_event(event).await;
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
                    let _ = self.kalshi_api.cancel_order(&state.order_id).await;
                    match self.kalshi_api.sell_ioc(&ticker, side_str, state.entry_price, state.qty).await {
                        Ok(resp) => {
                            let filled = resp.order.filled_count();
                            info!("[MM-FADE] Force-exit IOC {} filled={}", ticker, filled);
                            if filled > 0 {
                                self.sell_states.lock().await.remove(&ticker);
                                self.positions.lock().await.remove(&ticker);
                                self.order_to_ticker.lock().await.retain(|_, v| v != &ticker);
                            }
                        }
                        Err(e) => {
                            error!("[MM-FADE] Force-exit failed for {}: {}", ticker, e);
                        }
                    }
                } else {
                    self.sell_states.lock().await.remove(&ticker);
                    self.positions.lock().await.remove(&ticker);
                }
                let mut event = self.make_event("sell_force_exit", &ticker, side_str, state.entry_price, state.qty, &state.order_id).await;
                event.entry_price = Some(state.entry_price);
                self.log_event(event).await;
                continue;
            }

            // Fade: adaptive speed based on market activity
            let is_live = {
                let books_guard = self.books.lock().await;
                books_guard.get(&ticker)
                    .map(|b| b.seconds_since_activity() <= 60)
                    .unwrap_or(false)
            };
            // Slower fade: give the sell order more time at the profitable price
            // before starting to lower. Avoids eroding spread profit too fast.
            let (fade_start, fade_interval, fade_step) = if is_live {
                (Duration::from_secs(30), Duration::from_secs(15), 1i64)
            } else {
                (Duration::from_secs(45), Duration::from_secs(20), 1i64)
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

                    {
                        let mut sell_states = self.sell_states.lock().await;
                        if let Some(s) = sell_states.get_mut(&ticker) {
                            s.fades += 1;
                        }
                    }
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
                    let mut event = self.make_event("sell_fade", &ticker, side_str, new_ask, state.qty, &state.order_id).await;
                    event.entry_price = Some(state.entry_price);
                    self.log_event(event).await;
                }
            }
        }
    }

    /// Kill switch: if any position has moved >10¢ adversely, cancel all orders
    /// and attempt to exit at market (IOC sell near breakeven).
    async fn check_kill_switch(&self) {
        const KILL_CENTS: i64 = 5;

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
                let mut event = self.make_event("kill_switch", ticker, side_str, exit_price, *qty, "").await;
                event.entry_price = Some(*entry_price);
                self.log_event(event).await;
            }
        }

        // Clear all in-memory state
        self.positions.lock().await.clear();
        self.sell_states.lock().await.clear();
        self.order_to_ticker.lock().await.clear();
        self.pending_orders.lock().await.clear();
        self.closing_tickers.lock().await.clear();

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
            "paused": self.paused.load(Ordering::Relaxed),
        })
    }

    pub async fn positions_json(&self) -> serde_json::Value {
        let positions = self.positions.lock().await;
        let sell_states = self.sell_states.lock().await;
        let meta_map = self.market_meta.read().await;
        let arr: Vec<_> = positions
            .values()
            .map(|p| {
                let current_ask = sell_states
                    .get(&p.ticker)
                    .map(|s| s.ask_price)
                    .unwrap_or(p.target_exit_price);
                let title = meta_map.get(&p.ticker)
                    .map(|m| m.title.as_str())
                    .unwrap_or("");
                serde_json::json!({
                    "ticker": p.ticker,
                    "title": title,
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
        drop(books);
        let meta_map = self.market_meta.read().await;

        let arr: Vec<_> = opportunities
            .iter()
            .map(|o| {
                let title = meta_map.get(&o.ticker)
                    .map(|m| m.title.as_str())
                    .unwrap_or("");
                serde_json::json!({
                    "ticker": o.ticker,
                    "title": title,
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
        let events = self.order_events.lock().await;
        let skips = self.skip_counts.lock().await;

        serde_json::json!({
            "total": total,
            "wins": wins,
            "losses": losses,
            "daily_pnl": *self.daily_pnl.lock().await,
            "events": events.iter().rev().take(100).collect::<Vec<_>>(),
            "skip_reasons": *skips,
        })
    }

    // ─── Dashboard Command Dispatch ────────────────────────────────────────────

    pub async fn execute_command(&self, cmd: &str) {
        match cmd {
            "stop" => {
                self.paused.store(true, Ordering::Relaxed);
                info!("[MM-CMD] Bot paused via dashboard command");
            }
            "start" => {
                self.paused.store(false, Ordering::Relaxed);
                info!("[MM-CMD] Bot resumed via dashboard command");
            }
            "restart" => {
                info!("[MM-CMD] Restarting — canceling all orders and clearing state");
                if let Err(e) = self.kalshi_api.cancel_all_orders().await {
                    warn!("[MM-CMD] cancel_all_orders failed during restart: {}", e);
                }
                self.positions.lock().await.clear();
                self.sell_states.lock().await.clear();
                self.pending_orders.lock().await.clear();
                self.order_to_ticker.lock().await.clear();
                self.closing_tickers.lock().await.clear();
                self.paused.store(false, Ordering::Relaxed);
                info!("[MM-CMD] Restart complete — bot is running");
            }
            "cancel_all" => {
                info!("[MM-CMD] Canceling all resting orders via dashboard command");
                if let Err(e) = self.kalshi_api.cancel_all_orders().await {
                    warn!("[MM-CMD] cancel_all_orders failed: {}", e);
                }
                self.pending_orders.lock().await.clear();
                self.order_to_ticker.lock().await.clear();
                info!("[MM-CMD] All orders canceled");
            }
            "sell_all" => {
                warn!("[MM-CMD] SELL ALL triggered via dashboard — canceling orders and exiting all positions");
                if let Err(e) = self.kalshi_api.cancel_all_orders().await {
                    warn!("[MM-CMD] cancel_all_orders failed during sell_all: {}", e);
                }
                let positions_snap: Vec<(String, Side, i64, i64)> = {
                    let positions = self.positions.lock().await;
                    positions
                        .values()
                        .map(|p| (p.ticker.clone(), p.side, p.entry_price, p.qty))
                        .collect()
                };
                if !self.config.dry_run {
                    for (ticker, side, entry_price, qty) in &positions_snap {
                        let side_str = if *side == Side::Yes { "yes" } else { "no" };
                        // Use entry_price/2 (floored at 5c) to avoid catastrophic fills on thin books.
                        // Still aggressive enough to guarantee exit on liquid markets.
                        let floor_price = (*entry_price / 2).max(5);
                        match self.kalshi_api.sell_ioc(ticker, side_str, floor_price, *qty).await {
                            Ok(resp) => {
                                info!(
                                    "[MM-CMD] Sell all IOC {} at {}¢ filled={} for {}",
                                    ticker,
                                    floor_price,
                                    resp.order.filled_count(),
                                    ticker,
                                );
                            }
                            Err(e) => {
                                error!("[MM-CMD] sell_all IOC failed for {}: {}", ticker, e);
                            }
                        }
                        let mut event = self.make_event("sell_all", ticker, side_str, floor_price, *qty, "").await;
                        event.entry_price = Some(*entry_price);
                        self.log_event(event).await;
                    }
                } else {
                    info!("[MM-CMD] Dry run — skipping IOC sells for {} positions", positions_snap.len());
                }
                self.positions.lock().await.clear();
                self.sell_states.lock().await.clear();
                self.pending_orders.lock().await.clear();
                self.order_to_ticker.lock().await.clear();
                self.closing_tickers.lock().await.clear();
                self.paused.store(true, Ordering::Relaxed);
                warn!("[MM-CMD] Sell all complete — bot is now paused");
            }
            _ => {
                warn!("[MM-CMD] Unknown command: {}", cmd);
            }
        }
    }
}
