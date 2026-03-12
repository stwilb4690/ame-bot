// src/risk_manager.rs
//
// Portfolio risk management for AME Signal Trader.
//
// All position-size limits are a percentage of the current Kalshi balance,
// fetched via GET /trade-api/v2/portfolio/balance every 5 minutes.
//
// Scaling guidelines (manual review only — not auto-implemented):
//   Week 1 (current):                  2% single, 9% portfolio
//   After 50+ trades, >55% win rate:   consider doubling percentages
//   After 100+ trades, >60% win rate:  double again
//   Hard ceiling:                       never let max_portfolio_var_pct exceed 25%

use anyhow::Result;
use chrono::{NaiveDate, NaiveTime, Utc};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::kalshi::KalshiApiClient;
use crate::telegram::{AlertType, TelegramClient};

// ── Config ────────────────────────────────────────────────────────────────────

/// Risk limits, all expressed as percentage of current Kalshi balance.
/// Load from environment variables; defaults match the conservative Week 1 profile.
pub struct RiskConfig {
    /// Max total VAR across all open positions (default 9 → ~$25 on $280)
    pub portfolio_pct:      f64,
    /// Max VAR on any single position (default 2 → ~$5.60 on $280)
    pub single_pct:         f64,
    /// Max combined VAR for positions on the same game/event (default 3 → ~$8.40)
    pub correlated_pct:     f64,
    /// Max daily loss, anchored to start-of-day balance (default 5 → ~$14 on $280)
    pub daily_drawdown_pct: f64,
    /// Trailing stop from intraday P&L peak → triggers cautious mode (default 2)
    pub trailing_stop_pct:  f64,
    /// Always keep this fraction of balance liquid (default 18 → ~$50 on $280)
    pub reserve_floor_pct:  f64,
}

impl RiskConfig {
    pub fn from_env() -> Self {
        let config = Self {
            portfolio_pct:      env_f64("VAR_PORTFOLIO_PCT",      9.0),
            single_pct:         env_f64("VAR_SINGLE_PCT",         2.0),
            correlated_pct:     env_f64("VAR_CORRELATED_PCT",     3.0),
            daily_drawdown_pct: env_f64("VAR_DAILY_DRAWDOWN_PCT", 5.0),
            trailing_stop_pct:  env_f64("VAR_TRAILING_STOP_PCT",  2.0),
            reserve_floor_pct:  env_f64("VAR_RESERVE_FLOOR_PCT",  18.0),
        };
        // Validate: zero or negative risk percentages disable safety limits
        if config.single_pct <= 0.0 || config.portfolio_pct <= 0.0 || config.reserve_floor_pct <= 0.0 {
            panic!(
                "[RISK] Invalid risk config: single_pct={}, portfolio_pct={}, reserve_floor_pct={} -- all must be > 0",
                config.single_pct, config.portfolio_pct, config.reserve_floor_pct
            );
        }
        config
    }
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

// ── Risk Mode ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskMode {
    /// All thresholds at configured values.
    Normal,
    /// Trailing stop triggered: raise entry thresholds by 50%.
    Cautious,
    /// Daily drawdown limit reached: no new entries allowed.
    Stopped,
}

impl std::fmt::Display for RiskMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RiskMode::Normal   => write!(f, "normal"),
            RiskMode::Cautious => write!(f, "cautious"),
            RiskMode::Stopped  => write!(f, "stopped"),
        }
    }
}

// ── Open Position Accounting ──────────────────────────────────────────────────

struct OpenPosition {
    /// Kalshi market ticker — unique identifier for this position.
    market_ticker: String,
    /// Kalshi event ticker — used for correlated VAR grouping.
    event_key:     String,
    /// Max possible loss = entry_price × contracts (dollars).
    position_var:  f64,
    /// Capital committed = entry_price × contracts (dollars).
    cost_dollars:  f64,
}

// ── Risk State ────────────────────────────────────────────────────────────────

struct RiskState {
    balance_dollars:       f64,
    sod_balance_dollars:   f64,   // start-of-day balance (daily drawdown anchor)
    sod_date:              NaiveDate,

    // Dollar limits — recomputed from balance × pct on every balance refresh
    max_portfolio_var:     f64,
    max_single_var:        f64,
    max_correlated_var:    f64,
    daily_drawdown_limit:  f64,   // anchored to sod_balance, not current balance
    trailing_stop_limit:   f64,
    reserve_floor:         f64,

    // Live tracking
    daily_pnl:             f64,   // cumulative realized P&L today (dollars)
    high_water_mark:       f64,   // intraday peak daily_pnl (dollars)
    risk_mode:             RiskMode,

    open_positions:        Vec<OpenPosition>,
    total_exposure:        f64,   // sum of open position costs (dollars)
    current_portfolio_var: f64,   // sum of open position VARs (dollars)
}

impl RiskState {
    fn recompute_limits(&mut self, config: &RiskConfig) {
        self.max_portfolio_var   = self.balance_dollars     * config.portfolio_pct      / 100.0;
        self.max_single_var      = self.balance_dollars     * config.single_pct         / 100.0;
        self.max_correlated_var  = self.balance_dollars     * config.correlated_pct     / 100.0;
        self.trailing_stop_limit = self.balance_dollars     * config.trailing_stop_pct  / 100.0;
        self.reserve_floor       = self.balance_dollars     * config.reserve_floor_pct  / 100.0;
        // Drawdown anchored to start-of-day balance to prevent "winning morning"
        // from unlocking more afternoon losses.
        self.daily_drawdown_limit = self.sod_balance_dollars * config.daily_drawdown_pct / 100.0;
    }

    /// Sum of position VARs for all open positions on the same game.
    fn correlated_var_for(&self, event_key: &str) -> f64 {
        self.open_positions.iter()
            .filter(|p| p.event_key == event_key)
            .map(|p| p.position_var)
            .sum()
    }

    /// Recompute risk mode from current P&L state.
    /// Returns (old_mode, new_mode) if a transition occurred, None otherwise.
    fn update_mode(&mut self) -> Option<(RiskMode, RiskMode)> {
        // Once Stopped, stay stopped until day rollover (sticky floor)
        if self.risk_mode == RiskMode::Stopped {
            return None;
        }

        let new_mode = if -self.daily_pnl >= self.daily_drawdown_limit {
            RiskMode::Stopped
        } else if self.daily_pnl < self.high_water_mark - self.trailing_stop_limit {
            RiskMode::Cautious
        } else {
            RiskMode::Normal
        };

        if new_mode != self.risk_mode {
            let old = self.risk_mode;
            self.risk_mode = new_mode;
            Some((old, new_mode))
        } else {
            None
        }
    }
}

// ── Entry Decision ────────────────────────────────────────────────────────────

pub enum EntryDecision {
    Approved { contracts: i64, position_var: f64 },
    Rejected { reason: String },
}

// ── Risk Summary (heartbeat snapshot) ─────────────────────────────────────────

pub struct RiskSummary {
    pub balance_dollars:   f64,
    pub portfolio_var:     f64,
    pub max_portfolio_var: f64,
    pub daily_pnl:         f64,
    pub high_water_mark:   f64,
    pub risk_mode:         RiskMode,
}

// ── Risk Manager ──────────────────────────────────────────────────────────────

pub struct RiskManager {
    config:     RiskConfig,
    state:      Mutex<RiskState>,
    kalshi_api: Arc<KalshiApiClient>,
    telegram:   Option<Arc<TelegramClient>>,
}

impl RiskManager {
    /// Initialise by fetching the current Kalshi balance and computing all limits.
    pub async fn new(
        config:     RiskConfig,
        kalshi_api: Arc<KalshiApiClient>,
        telegram:   Option<Arc<TelegramClient>>,
    ) -> Result<Arc<Self>> {
        let balance_cents   = kalshi_api.get_balance().await?.balance;
        let balance_dollars = balance_cents as f64 / 100.0;
        let today           = Utc::now().date_naive();

        let mut state = RiskState {
            balance_dollars,
            sod_balance_dollars:   balance_dollars,
            sod_date:              today,
            max_portfolio_var:     0.0,
            max_single_var:        0.0,
            max_correlated_var:    0.0,
            daily_drawdown_limit:  0.0,
            trailing_stop_limit:   0.0,
            reserve_floor:         0.0,
            daily_pnl:             0.0,
            high_water_mark:       0.0,
            risk_mode:             RiskMode::Normal,
            open_positions:        Vec::new(),
            total_exposure:        0.0,
            current_portfolio_var: 0.0,
        };
        state.recompute_limits(&config);

        info!(
            "[RISK] Balance=${:.2} | max_portfolio=${:.2} | max_single=${:.2} | \
             max_correlated=${:.2} | daily_drawdown=${:.2} | reserve=${:.2}",
            state.balance_dollars,
            state.max_portfolio_var,
            state.max_single_var,
            state.max_correlated_var,
            state.daily_drawdown_limit,
            state.reserve_floor,
        );

        Ok(Arc::new(Self {
            config,
            state: Mutex::new(state),
            kalshi_api,
            telegram,
        }))
    }

    /// Spawn the background balance refresh loop (every 5 minutes).
    pub fn start_refresh_loop(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300));
            interval.tick().await; // skip immediate first tick
            loop {
                interval.tick().await;
                this.refresh_balance().await;
            }
        });
    }

    async fn refresh_balance(&self) {
        // Fetch BEFORE acquiring the state lock — no lock held across await.
        let balance_result = self.kalshi_api.get_balance().await;
        match balance_result {
            Err(e) => {
                warn!("[RISK] Balance refresh failed: {}", e);
            }
            Ok(resp) => {
                let new_balance = resp.balance as f64 / 100.0;
                let today       = Utc::now().date_naive();
                let mut state   = self.state.lock().await;

                // Day rollover: reset daily P&L, HWM, and risk mode.
                if today != state.sod_date {
                    info!(
                        "[RISK] Day rollover: new SOD balance=${:.2}, resetting daily P&L and mode",
                        new_balance
                    );
                    state.sod_balance_dollars = new_balance;
                    state.sod_date            = today;
                    state.daily_pnl           = 0.0;
                    state.high_water_mark     = 0.0;
                    state.risk_mode           = RiskMode::Normal;
                }

                let old_portfolio = state.max_portfolio_var;
                state.balance_dollars = new_balance;
                state.recompute_limits(&self.config);

                if (state.max_portfolio_var - old_portfolio).abs() > 0.01 {
                    info!(
                        "[RISK] Balance updated: ${:.2} | max_portfolio=${:.2} | \
                         max_single=${:.2} | reserve=${:.2}",
                        state.balance_dollars,
                        state.max_portfolio_var,
                        state.max_single_var,
                        state.reserve_floor,
                    );
                }
            }
        }
    }

    /// Compute dynamic position size and run all portfolio risk checks.
    ///
    /// Returns Approved(contracts, position_var) or Rejected(reason).
    /// The contract count is sized from the spread/time factor first, then
    /// reduced to fit whichever VAR limit is most constraining.
    ///
    /// # Arguments
    /// * `entry_price_cents` — Price of the side being bought (1-99)
    /// * `spread_abs_cents`  — Absolute spread magnitude in cents
    /// * `event_key`         — Kalshi event ticker for correlation grouping
    /// * `event_date`        — Optional game date for time_factor calculation
    /// * `is_snipe`          — Snipe mode ignores time_factor (always 1.0)
    /// * `base_threshold`    — Normal entry threshold used for cautious-mode check
    /// * `market_name`       — Description string for log output
    /// * `quality_factor`    — Signal quality multiplier (0.1–2.0); scales adjusted_risk
    pub async fn check_entry(
        &self,
        entry_price_cents: i16,
        spread_abs_cents:  u16,
        event_key:         &str,
        event_date:        Option<NaiveDate>,
        is_snipe:          bool,
        base_threshold:    i16,
        market_name:       &str,
        quality_factor:    f64,
    ) -> EntryDecision {
        let state = self.state.lock().await;

        // ── Mode guard ────────────────────────────────────────────────────────

        if state.risk_mode == RiskMode::Stopped {
            let reason = format!(
                "risk_mode=stopped (daily_drawdown=${:.2} reached)",
                state.daily_drawdown_limit
            );
            info!("[RISK] {} | REJECTED: {}", market_name, reason);
            return EntryDecision::Rejected { reason };
        }

        // Cautious mode: require 50% wider spread before entering.
        if state.risk_mode == RiskMode::Cautious {
            let required = (base_threshold as f64 * 1.5).ceil() as u16;
            if spread_abs_cents < required {
                let reason = format!(
                    "risk_mode=cautious spread={}¢ < required {}¢",
                    spread_abs_cents, required
                );
                info!("[RISK] {} | REJECTED: {}", market_name, reason);
                return EntryDecision::Rejected { reason };
            }
        }

        let entry_price_dollars = entry_price_cents as f64 / 100.0;

        // ── Position sizing from spread magnitude ──────────────────────────────

        let base_risk_pct = if spread_abs_cents >= 15 {
            1.4_f64  // Wide spread → larger risk budget
        } else if spread_abs_cents >= 10 {
            1.0_f64
        } else {
            0.7_f64  // Narrow spread (8-9¢) → conservative
        };

        let time_factor       = compute_time_factor(event_date, is_snipe);
        let base_risk_dollars = state.balance_dollars * base_risk_pct / 100.0;
        let adjusted_risk     = base_risk_dollars * time_factor * quality_factor.clamp(0.1, 2.0);

        // ── Compute maximum contracts from each limit (take the minimum) ───────

        let max_by_size = (adjusted_risk / entry_price_dollars).floor() as i64;

        let max_by_single = (state.max_single_var / entry_price_dollars).floor() as i64;

        let remaining_portfolio = (state.max_portfolio_var - state.current_portfolio_var).max(0.0);
        let max_by_portfolio    = (remaining_portfolio / entry_price_dollars).floor() as i64;

        let corr_var_existing = state.correlated_var_for(event_key);
        let remaining_corr    = (state.max_correlated_var - corr_var_existing).max(0.0);
        let max_by_correlated = (remaining_corr / entry_price_dollars).floor() as i64;

        // Reserve floor: balance - current_exposure - new_cost ≥ reserve_floor
        let reserve_room   = (state.balance_dollars - state.total_exposure - state.reserve_floor).max(0.0);
        let max_by_reserve = (reserve_room / entry_price_dollars).floor() as i64;

        let contracts = max_by_size
            .min(max_by_single)
            .min(max_by_portfolio)
            .min(max_by_correlated)
            .min(max_by_reserve);

        if contracts < 1 {
            // Identify the binding constraint for a clear rejection message.
            let reason = if max_by_portfolio < 1 {
                format!(
                    "portfolio_var ${:.2}/${:.2} full",
                    state.current_portfolio_var, state.max_portfolio_var
                )
            } else if max_by_single < 1 {
                format!(
                    "entry ${:.2} > max_single ${:.2}",
                    entry_price_dollars, state.max_single_var
                )
            } else if max_by_correlated < 1 {
                format!(
                    "correlated ${:.2}+${:.2} would exceed limit ${:.2}",
                    corr_var_existing, entry_price_dollars, state.max_correlated_var
                )
            } else if max_by_reserve < 1 {
                format!(
                    "reserve_floor ${:.2}: only ${:.2} available",
                    state.reserve_floor, reserve_room
                )
            } else {
                format!(
                    "spread {}¢ too small for 1 contract at entry ${:.2}",
                    spread_abs_cents, entry_price_dollars
                )
            };

            info!(
                "[RISK] Signal {} spread={}¢ | base=${:.2} time={:.1} | \
                 pos_var=$0.00 | portfolio=${:.2}/${:.2} | balance=${:.2} reserve=${:.2} | \
                 REJECTED: {}",
                market_name, spread_abs_cents, base_risk_dollars, time_factor,
                state.current_portfolio_var, state.max_portfolio_var,
                state.balance_dollars, state.reserve_floor,
                reason,
            );
            return EntryDecision::Rejected { reason };
        }

        let position_var = entry_price_dollars * contracts as f64;

        info!(
            "[RISK] Signal {} spread={}¢ | base=${:.2} time={:.1} | {} contracts @ {}¢ | \
             pos_var=${:.2} | portfolio=${:.2}/${:.2} | correlated=${:.2}/${:.2} | \
             balance=${:.2} reserve=${:.2} | APPROVED",
            market_name, spread_abs_cents, base_risk_dollars, time_factor,
            contracts, entry_price_cents, position_var,
            state.current_portfolio_var + position_var, state.max_portfolio_var,
            corr_var_existing + position_var, state.max_correlated_var,
            state.balance_dollars, state.reserve_floor,
        );

        EntryDecision::Approved { contracts, position_var }
    }

    /// Record that a position was opened (call after successful order placement).
    pub async fn record_entry(
        &self,
        market_ticker:     String,
        event_key:         String,
        entry_price_cents: i16,
        contracts:         i64,
        position_var:      f64,
    ) {
        let cost = (entry_price_cents as f64 / 100.0) * contracts as f64;
        let mut state = self.state.lock().await;
        state.open_positions.push(OpenPosition {
            market_ticker,
            event_key,
            position_var,
            cost_dollars: cost,
        });
        state.total_exposure        += cost;
        state.current_portfolio_var += position_var;
    }

    /// Record that a position was closed and update daily P&L and risk mode.
    ///
    /// `market_ticker` must match what was passed to `record_entry`.
    /// `pnl_dollars` is net realized P&L (negative = loss).
    pub async fn record_exit(&self, market_ticker: &str, pnl_dollars: f64) {
        // Do all mutable work while holding the lock, then drop it before
        // any external calls (Telegram is fire-and-forget but cleaner outside).
        let mode_transition = {
            let mut state = self.state.lock().await;

            if let Some(idx) = state.open_positions.iter()
                .position(|p| p.market_ticker == market_ticker)
            {
                let pos = state.open_positions.remove(idx);
                state.total_exposure        = (state.total_exposure        - pos.cost_dollars).max(0.0);
                state.current_portfolio_var = (state.current_portfolio_var - pos.position_var).max(0.0);
            }

            state.daily_pnl += pnl_dollars;
            if state.daily_pnl > state.high_water_mark {
                state.high_water_mark = state.daily_pnl;
            }

            state.update_mode().map(|(old, new)| {
                (old, new, state.daily_pnl, state.high_water_mark, state.daily_drawdown_limit)
            })
        }; // lock released here

        if let Some((old_mode, new_mode, daily_pnl, hwm, drawdown_limit)) = mode_transition {
            warn!(
                "[RISK] Mode transition: {} → {} | daily_pnl=${:.2} | hwm=${:.2} | \
                 drawdown_limit=${:.2}",
                old_mode, new_mode, daily_pnl, hwm, drawdown_limit
            );
            if let Some(tg) = &self.telegram {
                tg.send_alert(
                    AlertType::CircuitBreaker,
                    0,
                    format!(
                        "🚦 RISK MODE: {} → {}\ndaily_pnl=${:.2} | hwm=${:.2}\n\
                         drawdown_limit=${:.2}",
                        old_mode, new_mode, daily_pnl, hwm, drawdown_limit
                    ),
                );
            }
        }
    }

    /// Returns a snapshot of current risk state for heartbeat logging.
    pub async fn summary(&self) -> RiskSummary {
        let state = self.state.lock().await;
        RiskSummary {
            balance_dollars:   state.balance_dollars,
            portfolio_var:     state.current_portfolio_var,
            max_portfolio_var: state.max_portfolio_var,
            daily_pnl:         state.daily_pnl,
            high_water_mark:   state.high_water_mark,
            risk_mode:         state.risk_mode,
        }
    }

    /// Current risk mode, for early-out checks in the scan loop.
    pub async fn risk_mode(&self) -> RiskMode {
        self.state.lock().await.risk_mode
    }
}

// ── Time Factor ───────────────────────────────────────────────────────────────

/// Compute the time-based position size multiplier.
///
/// Snipe mode always returns 1.0 (per spec — size is already constrained by
/// the higher live_threshold_cents requirement).
///
/// Pregame mode: since pregame only fires when event_date > today, the event
/// is at least 24 hours away, so the factor is almost always 1.0. The
/// sub-hour buckets activate only if the bot somehow reaches pregame mode on
/// game day (which normally flips to snipe mode instead).
fn compute_time_factor(event_date: Option<NaiveDate>, is_snipe: bool) -> f64 {
    if is_snipe {
        return 1.0;
    }
    let Some(ev_date) = event_date else {
        return 1.0; // Unknown date → default to full size
    };

    // Assume games start around 11 PM UTC (≈ 6 PM ET) for the time calculation.
    let game_time = ev_date.and_time(
        NaiveTime::from_hms_opt(23, 0, 0).expect("static valid time"),
    );
    let now           = Utc::now().naive_utc();
    let hours_until   = (game_time - now).num_hours().max(0);
    let minutes_until = (game_time - now).num_minutes().max(0);

    match hours_until {
        h if h >= 2 => 1.0,
        1           => 0.8,
        0 if minutes_until >= 30 => 0.6,
        _ => 0.4,
    }
}
