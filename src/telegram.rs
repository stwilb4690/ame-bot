// src/telegram.rs
// Telegram bot alert client
//
// Alert types:
//   🎯 ARB DETECTED    — same-platform or cross-platform opportunity
//   📊 WIDE SPREAD     — unusually wide bid-ask spread
//   🔄 PRICE MOVE      — significant price movement
//   🚨 CIRCUIT BREAKER — when CB trips
//   💓 HEARTBEAT       — daily/periodic summary
//   ✅ TRADE EXECUTED  — when a trade fills
//
// All sends are non-blocking (spawned tasks).
// Per-market+type cooldowns prevent alert spam.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Alert types for cooldown tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertType {
    ArbDetected,
    WideSpread,
    PriceMove,
    CircuitBreaker,
    Heartbeat,
    TradeExecuted,
}

impl AlertType {
    fn emoji(&self) -> &'static str {
        match self {
            AlertType::ArbDetected => "🎯",
            AlertType::WideSpread => "📊",
            AlertType::PriceMove => "🔄",
            AlertType::CircuitBreaker => "🚨",
            AlertType::Heartbeat => "💓",
            AlertType::TradeExecuted => "✅",
        }
    }
}

/// Telegram bot client.
/// Create via `TelegramClient::from_env()` — returns None if credentials are missing.
pub struct TelegramClient {
    token: String,
    chat_id: String,
    client: reqwest::Client,
    // (market_id, alert_type) → last sent Instant
    cooldowns: Mutex<HashMap<(u16, AlertType), Instant>>,
    cooldown_secs: u64,
}

impl TelegramClient {
    /// Create from environment variables.
    ///
    /// Required: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
    /// Optional: `ALERT_COOLDOWN_SECS` (default 300)
    ///
    /// Returns `None` if either required variable is missing.
    pub fn from_env() -> Option<Arc<Self>> {
        let token = std::env::var("TELEGRAM_BOT_TOKEN").ok()?;
        let chat_id = std::env::var("TELEGRAM_CHAT_ID").ok()?;
        if token.is_empty() || chat_id.is_empty() {
            return None;
        }

        let cooldown_secs = std::env::var("ALERT_COOLDOWN_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300u64);

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .ok()?;

        Some(Arc::new(Self {
            token,
            chat_id,
            client,
            cooldowns: Mutex::new(HashMap::new()),
            cooldown_secs,
        }))
    }

    /// Send an alert for a specific market (market_id 0 = global/system alert).
    ///
    /// This is non-blocking: the actual HTTP call is spawned onto the tokio runtime.
    /// Cooldown is enforced per (market_id, AlertType) pair.
    pub fn send_alert(self: &Arc<Self>, alert_type: AlertType, market_id: u16, message: String) {
        if !self.check_and_set_cooldown(market_id, alert_type) {
            debug!(
                "[TELEGRAM] Skipping {:?} for market {} (cooldown active)",
                alert_type, market_id
            );
            return;
        }

        let this = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = this.do_send(&message).await {
                warn!("[TELEGRAM] Send failed: {}", e);
                // On failure, reset cooldown so the next attempt isn't blocked
                let mut cd = this.cooldowns.lock().unwrap();
                cd.remove(&(market_id, alert_type));
            }
        });
    }

    /// Format and send an arb-detected alert.
    pub fn alert_arb(
        self: &Arc<Self>,
        market_id: u16,
        market_name: &str,
        kalshi_ticker: &str,
        yes_ask: u16,
        no_ask: u16,
        profit_cents: i16,
        kind: &str, // "same-platform" or "cross-platform"
    ) {
        let msg = format!(
            "{} ARB DETECTED ({})\nMarket: {}\nTicker: {}\nYES ask: {}¢  NO ask: {}¢\nProfit: {}¢/contract",
            AlertType::ArbDetected.emoji(), kind,
            market_name, kalshi_ticker,
            yes_ask, no_ask, profit_cents,
        );
        self.send_alert(AlertType::ArbDetected, market_id, msg);
    }

    /// Format and send a wide-spread alert.
    pub fn alert_wide_spread(
        self: &Arc<Self>,
        market_id: u16,
        market_name: &str,
        kalshi_ticker: &str,
        yes_ask: u16,
        no_ask: u16,
        spread_cents: u16,
    ) {
        let msg = format!(
            "{} WIDE SPREAD\nMarket: {}\nTicker: {}\nYES ask: {}¢  NO ask: {}¢\nRound-trip premium: {}¢",
            AlertType::WideSpread.emoji(),
            market_name, kalshi_ticker,
            yes_ask, no_ask, spread_cents,
        );
        self.send_alert(AlertType::WideSpread, market_id, msg);
    }

    /// Format and send a circuit-breaker alert.
    pub fn alert_circuit_breaker(self: &Arc<Self>, reason: &str) {
        let msg = format!(
            "{} CIRCUIT BREAKER TRIPPED\nReason: {}\nBot is paused until cooldown expires.",
            AlertType::CircuitBreaker.emoji(), reason
        );
        self.send_alert(AlertType::CircuitBreaker, 0, msg);
    }

    /// Format and send a trade-executed alert.
    pub fn alert_trade_executed(
        self: &Arc<Self>,
        market_id: u16,
        description: &str,
        arb_type: &str,
        kalshi_filled: i64,
        poly_filled: i64,
        total_cost_cents: i64,
        profit_cents: i64,
        poly_slug: &str,
    ) {
        let msg = format!(
            "{} TRADE EXECUTED\nMarket: {}\nType: {}\nKalshi: {}x  Poly: {}x\nTotal cost: ${:.2}\nExpected profit: {}¢\nSlug: {}",
            AlertType::TradeExecuted.emoji(),
            description, arb_type,
            kalshi_filled, poly_filled,
            total_cost_cents as f64 / 100.0,
            profit_cents,
            poly_slug,
        );
        self.send_alert(AlertType::TradeExecuted, market_id, msg);
    }

    /// Format and send a heartbeat/summary alert.
    pub fn alert_heartbeat(self: &Arc<Self>, summary: &str) {
        let msg = format!("{} HEARTBEAT\n{}", AlertType::Heartbeat.emoji(), summary);
        self.send_alert(AlertType::Heartbeat, 0, msg);
    }

    /// Format and send a shutdown notification (bypasses cooldown).
    pub fn alert_shutdown(self: &Arc<Self>, reason: &str) {
        let msg = format!("🛑 BOT SHUTTING DOWN\nReason: {}", reason);
        // Bypass cooldown for shutdown — always send
        let this = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = this.do_send(&msg).await {
                warn!("[TELEGRAM] Shutdown alert failed: {}", e);
            }
        });
    }

    /// Format and send a startup notification (bypasses cooldown).
    pub fn alert_startup(self: &Arc<Self>, mode: &str, version: &str) {
        let msg = format!(
            "🚀 AME BOT STARTED\nMode: {}\nVersion: {}\nEnvironment: {}",
            mode, version,
            std::env::var("APP_ENV").unwrap_or_else(|_| "demo".into()),
        );
        let this = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = this.do_send(&msg).await {
                warn!("[TELEGRAM] Startup alert failed: {}", e);
            }
        });
    }

    /// Returns true if the alert should be sent (and updates the last-sent time).
    fn check_and_set_cooldown(&self, market_id: u16, alert_type: AlertType) -> bool {
        let key = (market_id, alert_type);
        let mut cd = self.cooldowns.lock().unwrap();
        let now = Instant::now();

        if let Some(last) = cd.get(&key) {
            if now.duration_since(*last) < Duration::from_secs(self.cooldown_secs) {
                return false;
            }
        }
        cd.insert(key, now);
        true
    }

    /// Perform the actual HTTP POST to Telegram.
    async fn do_send(&self, text: &str) -> anyhow::Result<()> {
        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.token
        );
        let resp = self
            .client
            .post(&url)
            .json(&serde_json::json!({
                "chat_id": self.chat_id,
                "text": text,
                "parse_mode": "HTML",
            }))
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Telegram API error {}: {}", status, body);
        }
        Ok(())
    }
}
