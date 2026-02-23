// src/data_collector.rs
//
// Append-only JSONL data collection for signal trading analysis.
//
// Three file types, all date-stamped in ET (UTC-5 approximation):
//   state/spread_observations_YYYY-MM-DD.jsonl  — every 30s per market
//   state/signal_events_YYYY-MM-DD.jsonl        — on each signal evaluation
//   state/trade_outcomes_YYYY-MM-DD.jsonl       — on each position exit
//
// All write failures are logged as warnings. Trading is NEVER interrupted.

use std::path::PathBuf;
use std::sync::Arc;
use chrono::{FixedOffset, TimeZone, Utc};
use serde::Serialize;
use tracing::warn;

// ── Record Types ──────────────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub struct SpreadObservation {
    pub ts:                         String,
    pub market:                     String,
    pub market_name:                String,
    pub category:                   String,
    pub subcategory:                String,
    pub oracle_conf:                f64,
    pub kalshi_yes:                 u16,
    pub poly_yes:                   u16,
    pub spread:                     i16,
    pub spread_velocity:            f64,
    pub effective_threshold:        i16,
    pub time_above_threshold_secs:  u64,
    pub is_live:                    bool,
    pub minutes_to_event:           Option<i64>,
    pub hour_utc:                   u32,
    pub day_of_week:                u32,
}

#[derive(Debug, Serialize)]
pub struct SignalEvent {
    pub ts:                  String,
    pub market:              String,
    pub category:            String,
    pub subcategory:         String,
    pub oracle_conf:         f64,
    pub spread:              i16,
    pub effective_threshold: i16,
    pub spread_velocity:     f64,
    pub freshness_seconds:   u64,
    pub signal_quality:      f64,
    pub spread_score:        f64,
    pub freshness_score:     f64,
    pub velocity_score:      f64,
    /// "entered" | "rejected" | "filtered"
    pub action:              String,
    pub rejection_reason:    Option<String>,
    pub entry_price:         Option<i16>,
    pub contracts:           Option<i64>,
    pub position_var:        Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct TradeOutcome {
    pub ts_entry:                   String,
    pub ts_exit:                    String,
    pub market:                     String,
    pub category:                   String,
    pub subcategory:                String,
    pub oracle_conf:                f64,
    pub direction:                  String,
    pub mode:                       String,
    pub entry_price:                i16,
    pub exit_price:                 i16,
    pub contracts:                  i64,
    pub spread_at_entry:            i16,
    pub spread_at_exit:             i16,
    pub spread_velocity_at_entry:   f64,
    pub signal_quality_at_entry:    f64,
    pub freshness_at_entry_seconds: u64,
    pub hold_time_seconds:          u64,
    pub pnl_cents_per_contract:     f64,
    pub pnl_dollars:                f64,
    pub fees_dollars:               f64,
    pub exit_reason:                String,
    pub minutes_to_event_at_entry:  Option<i64>,
    pub hour_utc_at_entry:          u32,
}

// ── DataCollector ─────────────────────────────────────────────────────────────

pub struct DataCollector {
    state_dir: String,
}

impl DataCollector {
    pub fn new(state_dir: &str) -> Arc<Self> {
        Arc::new(Self { state_dir: state_dir.to_string() })
    }

    /// Current ET date string (YYYY-MM-DD), using UTC-5 approximation.
    fn et_date_str() -> String {
        let et = FixedOffset::west_opt(5 * 3600).expect("valid tz offset");
        et.from_utc_datetime(&Utc::now().naive_utc())
            .format("%Y-%m-%d")
            .to_string()
    }

    fn spread_obs_path(&self) -> PathBuf {
        PathBuf::from(&self.state_dir)
            .join(format!("spread_observations_{}.jsonl", Self::et_date_str()))
    }

    fn signal_events_path(&self) -> PathBuf {
        PathBuf::from(&self.state_dir)
            .join(format!("signal_events_{}.jsonl", Self::et_date_str()))
    }

    fn trade_outcomes_path(&self) -> PathBuf {
        PathBuf::from(&self.state_dir)
            .join(format!("trade_outcomes_{}.jsonl", Self::et_date_str()))
    }

    async fn append<T: Serialize>(&self, path: &std::path::Path, record: &T) {
        use tokio::io::AsyncWriteExt;
        let line = match serde_json::to_string(record) {
            Ok(s)  => s,
            Err(e) => { warn!("[DATA] Serialize error: {}", e); return; }
        };
        match tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
        {
            Err(e) => warn!("[DATA] Open {:?} failed: {}", path, e),
            Ok(mut f) => {
                if let Err(e) = f.write_all(format!("{}\n", line).as_bytes()).await {
                    warn!("[DATA] Write {:?} failed: {}", path, e);
                }
            }
        }
    }

    pub async fn record_spread_observation(&self, obs: SpreadObservation) {
        let path = self.spread_obs_path();
        self.append(&path, &obs).await;
    }

    pub async fn record_signal_event(&self, event: SignalEvent) {
        let path = self.signal_events_path();
        self.append(&path, &event).await;
    }

    pub async fn record_trade_outcome(&self, outcome: TradeOutcome) {
        let path = self.trade_outcomes_path();
        self.append(&path, &outcome).await;
    }
}
