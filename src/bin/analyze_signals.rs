// src/bin/analyze_signals.rs
//
// Offline analysis tool for signal trading data.
//
// Reads JSONL files from the state/ directory and prints:
//   1. Spread distribution by category/subcategory
//   2. Oracle accuracy (convergence rates)
//   3. Spread half-life
//   4. Time-of-day patterns
//   5. Signal quality vs outcome
//   6. Recommendations
//
// Usage:
//   cargo build --release --bin analyze-signals
//   ./target/release/analyze-signals [--state-dir state]

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use serde::Deserialize;

// ── Data types (mirror of data_collector.rs, deserialized from JSONL) ─────────

#[derive(Debug, Deserialize)]
struct SpreadObservation {
    #[allow(dead_code)]
    ts:                         String,
    #[allow(dead_code)]
    market:                     String,
    #[allow(dead_code)]
    market_name:                String,
    category:                   String,
    subcategory:                String,
    oracle_conf:                f64,
    #[allow(dead_code)]
    kalshi_yes:                 u16,
    #[allow(dead_code)]
    poly_yes:                   u16,
    spread:                     i16,
    #[allow(dead_code)]
    spread_velocity:            f64,
    effective_threshold:        i16,
    time_above_threshold_secs:  u64,
    #[allow(dead_code)]
    is_live:                    bool,
    #[allow(dead_code)]
    minutes_to_event:           Option<i64>,
    hour_utc:                   u32,
    #[allow(dead_code)]
    day_of_week:                u32,
}

#[derive(Debug, Deserialize)]
struct SignalEvent {
    #[allow(dead_code)]
    ts:                  String,
    #[allow(dead_code)]
    market:              String,
    #[allow(dead_code)]
    category:            String,
    subcategory:         String,
    #[allow(dead_code)]
    oracle_conf:         f64,
    #[allow(dead_code)]
    spread:              i16,
    #[allow(dead_code)]
    effective_threshold: i16,
    #[allow(dead_code)]
    spread_velocity:     f64,
    #[allow(dead_code)]
    freshness_seconds:   u64,
    signal_quality:      f64,
    #[allow(dead_code)]
    spread_score:        f64,
    #[allow(dead_code)]
    freshness_score:     f64,
    #[allow(dead_code)]
    velocity_score:      f64,
    action:              String,
    #[allow(dead_code)]
    rejection_reason:    Option<String>,
    #[allow(dead_code)]
    entry_price:         Option<i16>,
    #[allow(dead_code)]
    contracts:           Option<i64>,
    #[allow(dead_code)]
    position_var:        Option<f64>,
}

#[derive(Debug, Deserialize)]
struct TradeOutcome {
    #[allow(dead_code)]
    ts_entry:                   String,
    #[allow(dead_code)]
    ts_exit:                    String,
    #[allow(dead_code)]
    market:                     String,
    category:                   String,
    subcategory:                String,
    #[allow(dead_code)]
    oracle_conf:                f64,
    #[allow(dead_code)]
    direction:                  String,
    #[allow(dead_code)]
    mode:                       String,
    #[allow(dead_code)]
    entry_price:                i16,
    #[allow(dead_code)]
    exit_price:                 i16,
    #[allow(dead_code)]
    contracts:                  i64,
    #[allow(dead_code)]
    spread_at_entry:            i16,
    spread_at_exit:             i16,
    #[allow(dead_code)]
    spread_velocity_at_entry:   f64,
    signal_quality_at_entry:    f64,
    #[allow(dead_code)]
    freshness_at_entry_seconds: u64,
    hold_time_seconds:          u64,
    pnl_dollars:                f64,
    #[allow(dead_code)]
    pnl_cents_per_contract:     f64,
    #[allow(dead_code)]
    fees_dollars:               f64,
    exit_reason:                String,
    #[allow(dead_code)]
    minutes_to_event_at_entry:  Option<i64>,
    hour_utc_at_entry:          u32,
}

// ── JSONL loading ─────────────────────────────────────────────────────────────

fn load_jsonl<T: for<'de> Deserialize<'de>>(paths: &[PathBuf]) -> Vec<T> {
    let mut out = Vec::new();
    for path in paths {
        match fs::File::open(path) {
            Err(_) => continue,
            Ok(f) => {
                let reader = BufReader::new(f);
                for (lineno, line) in reader.lines().enumerate() {
                    match line {
                        Err(e) => eprintln!("  read error {:?}:{}: {}", path, lineno + 1, e),
                        Ok(s) if s.trim().is_empty() => {}
                        Ok(s) => match serde_json::from_str::<T>(&s) {
                            Ok(v)  => out.push(v),
                            Err(e) => eprintln!("  parse error {:?}:{}: {}", path, lineno + 1, e),
                        }
                    }
                }
            }
        }
    }
    out
}

fn glob_jsonl(state_dir: &str, prefix: &str) -> Vec<PathBuf> {
    let dir = Path::new(state_dir);
    let mut paths: Vec<PathBuf> = match fs::read_dir(dir) {
        Err(_) => return vec![],
        Ok(it) => it
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(prefix) && n.ends_with(".jsonl"))
                    .unwrap_or(false)
            })
            .collect(),
    };
    paths.sort();
    paths
}

// ── Stats helpers ─────────────────────────────────────────────────────────────

fn median(mut v: Vec<f64>) -> f64 {
    if v.is_empty() { return 0.0; }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = v.len() / 2;
    if v.len() % 2 == 0 {
        (v[mid - 1] + v[mid]) / 2.0
    } else {
        v[mid]
    }
}

fn percentile(mut v: Vec<f64>, p: f64) -> f64 {
    if v.is_empty() { return 0.0; }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((p / 100.0) * (v.len() - 1) as f64).round() as usize;
    v[idx.min(v.len() - 1)]
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() {
    let state_dir = std::env::args()
        .zip(std::env::args().skip(1))
        .find(|(a, _)| a == "--state-dir")
        .map(|(_, v)| v)
        .unwrap_or_else(|| "state".to_string());

    println!("=== AME Signal Analysis — reading from {} ===\n", state_dir);

    // Load all JSONL files
    let obs_paths    = glob_jsonl(&state_dir, "spread_observations_");
    let event_paths  = glob_jsonl(&state_dir, "signal_events_");
    let outcome_paths= glob_jsonl(&state_dir, "trade_outcomes_");

    let observations: Vec<SpreadObservation> = load_jsonl(&obs_paths);
    let signal_events: Vec<SignalEvent>       = load_jsonl(&event_paths);
    let trade_outcomes: Vec<TradeOutcome>     = load_jsonl(&outcome_paths);

    println!("Loaded: {} spread observations, {} signal events, {} trade outcomes\n",
             observations.len(), signal_events.len(), trade_outcomes.len());

    if observations.is_empty() && signal_events.is_empty() {
        println!("No data yet. Run signal-trader for a while, then re-run analyze-signals.");
        return;
    }

    section_spread_distribution(&observations);
    section_oracle_accuracy(&signal_events, &trade_outcomes);
    section_spread_half_life(&signal_events, &trade_outcomes);
    section_time_of_day(&observations);
    if !trade_outcomes.is_empty() {
        section_quality_vs_outcome(&trade_outcomes);
    }
    section_recommendations(&observations, &signal_events, &trade_outcomes);
}

// ── Section 1: Spread Distribution ───────────────────────────────────────────

fn section_spread_distribution(obs: &[SpreadObservation]) {
    println!("=== Spread Distribution by Category/Subcategory ===");
    if obs.is_empty() {
        println!("  (no data)\n");
        return;
    }

    // Group by (category, subcategory)
    let mut groups: HashMap<(String, String), Vec<f64>> = HashMap::new();
    let mut above_threshold: HashMap<(String, String), u64> = HashMap::new();

    for o in obs {
        let key = (o.category.clone(), o.subcategory.clone());
        groups.entry(key.clone()).or_default().push(o.spread.unsigned_abs() as f64);
        if o.time_above_threshold_secs > 0 {
            *above_threshold.entry(key).or_insert(0) += 1;
        }
    }

    let mut keys: Vec<_> = groups.keys().cloned().collect();
    keys.sort();

    println!("  {:<20} {:<16} {:>6} {:>10} {:>10} {:>12}",
             "category", "subcategory", "count", "median|spread|", "p90|spread|", "above_thr%");
    for key in &keys {
        let spreads = groups[key].clone();
        let count   = spreads.len();
        let med     = median(spreads.clone());
        let p90     = percentile(spreads, 90.0);
        let above   = above_threshold.get(key).copied().unwrap_or(0);
        let above_pct = if count > 0 { above as f64 / count as f64 * 100.0 } else { 0.0 };
        println!("  {:<20} {:<16} {:>6} {:>13.1}¢ {:>9.1}¢ {:>11.1}%",
                 key.0, key.1, count, med, p90, above_pct);
    }
    println!();

    // Effective threshold check: oracle_conf breakdown
    let mut conf_by_sub: HashMap<String, Vec<f64>> = HashMap::new();
    for o in obs {
        conf_by_sub.entry(o.subcategory.clone()).or_default().push(o.oracle_conf);
    }
    println!("  Oracle confidence by subcategory:");
    let mut subs: Vec<_> = conf_by_sub.keys().cloned().collect();
    subs.sort();
    for sub in &subs {
        let confs = &conf_by_sub[sub];
        let avg = confs.iter().sum::<f64>() / confs.len() as f64;
        // Effective threshold = base_threshold / oracle_conf — compute median eff_thr
        let eff_thrs: Vec<f64> = obs.iter()
            .filter(|o| &o.subcategory == sub)
            .map(|o| o.effective_threshold as f64)
            .collect();
        let med_eff = median(eff_thrs);
        println!("    {:<16} oracle_conf={:.2}  median_eff_threshold={:.1}¢", sub, avg, med_eff);
    }
    println!();
}

// ── Section 2: Oracle Accuracy ────────────────────────────────────────────────

fn section_oracle_accuracy(events: &[SignalEvent], outcomes: &[TradeOutcome]) {
    println!("=== Oracle Accuracy (Convergence Rates) ===");

    if outcomes.is_empty() {
        println!("  (no trade outcomes yet)\n");
        return;
    }

    // Group outcomes by subcategory
    let mut by_sub: HashMap<String, Vec<&TradeOutcome>> = HashMap::new();
    for o in outcomes {
        by_sub.entry(o.subcategory.clone()).or_default().push(o);
    }

    println!("  {:<16} {:>6} {:>12} {:>14} {:>12}",
             "subcategory", "trades", "converge_rate", "avg_hold_mins", "recommendation");

    let mut subs: Vec<_> = by_sub.keys().cloned().collect();
    subs.sort();

    for sub in &subs {
        let trades = &by_sub[sub];
        let total  = trades.len();
        // "converged" = exit_reason == convergence AND |spread_at_exit| ≤ 3
        let converged = trades.iter()
            .filter(|t| t.exit_reason == "convergence" && t.spread_at_exit.unsigned_abs() <= 3)
            .count();
        let conv_rate = converged as f64 / total as f64 * 100.0;
        let avg_hold  = trades.iter().map(|t| t.hold_time_seconds as f64 / 60.0).sum::<f64>()
                        / total as f64;

        let rec = if conv_rate < 40.0 {
            "⚠️  lower oracle_conf"
        } else if conv_rate > 70.0 {
            "✅ good accuracy"
        } else {
            "🟡 moderate"
        };

        println!("  {:<16} {:>6} {:>11.1}% {:>13.1}m {:>12}",
                 sub, total, conv_rate, avg_hold, rec);
    }

    // Show how many events fired per subcategory
    let mut event_sub: HashMap<String, usize> = HashMap::new();
    for e in events.iter().filter(|e| e.action == "entered") {
        *event_sub.entry(e.subcategory.clone()).or_insert(0) += 1;
    }
    if !event_sub.is_empty() {
        println!();
        println!("  Signal entries by subcategory:");
        let mut subs2: Vec<_> = event_sub.iter().collect();
        subs2.sort_by_key(|(k, _)| k.as_str());
        for (sub, count) in subs2 {
            println!("    {:<16}  {} entries", sub, count);
        }
    }
    println!();
}

// ── Section 3: Spread Half-Life ───────────────────────────────────────────────

fn section_spread_half_life(events: &[SignalEvent], outcomes: &[TradeOutcome]) {
    println!("=== Spread Half-Life ===");

    // Find trades that exited by convergence
    let convergence_holds: Vec<f64> = outcomes.iter()
        .filter(|o| o.exit_reason == "convergence")
        .map(|o| o.hold_time_seconds as f64 / 60.0)
        .collect();

    if convergence_holds.is_empty() {
        println!("  (no convergence exits yet)\n");
        return;
    }

    let med_hold = median(convergence_holds.clone());
    let p90_hold = percentile(convergence_holds.clone(), 90.0);

    println!("  Convergence exits: {}", convergence_holds.len());
    println!("  Median hold time:  {:.1} min", med_hold);
    println!("  P90 hold time:     {:.1} min", p90_hold);

    if med_hold > 30.0 {
        println!("  ⚠️  STRUCTURAL WARNING: median hold > 30min — spreads may be structural");
        println!("     Consider raising effective thresholds or lowering oracle confidence");
    } else {
        println!("  ✅ Normal convergence speed");
    }

    // Per-category breakdown
    let mut by_cat: HashMap<String, Vec<f64>> = HashMap::new();
    for o in outcomes.iter().filter(|o| o.exit_reason == "convergence") {
        by_cat.entry(o.category.clone()).or_default()
            .push(o.hold_time_seconds as f64 / 60.0);
    }
    if !by_cat.is_empty() {
        println!();
        let mut cats: Vec<_> = by_cat.keys().cloned().collect();
        cats.sort();
        for cat in &cats {
            let holds = by_cat[cat].clone();
            println!("    {:<10}  median={:.1}min  p90={:.1}min  n={}",
                     cat, median(holds.clone()), percentile(holds.clone(), 90.0), holds.len());
        }
    }

    // Signal events vs outcomes ratio
    let entered = events.iter().filter(|e| e.action == "entered").count();
    if entered > 0 {
        let exited = outcomes.len();
        println!();
        println!("  Signals entered: {}  |  Outcomes recorded: {}  |  Still open: {}",
                 entered, exited, entered.saturating_sub(exited));
    }
    println!();
}

// ── Section 4: Time-of-Day Patterns ──────────────────────────────────────────

fn section_time_of_day(obs: &[SpreadObservation]) {
    println!("=== Time-of-Day Patterns (UTC hour) ===");
    if obs.is_empty() {
        println!("  (no data)\n");
        return;
    }

    // Average spread by hour per category
    let mut by_cat_hour: HashMap<(String, u32), Vec<f64>> = HashMap::new();
    for o in obs {
        by_cat_hour.entry((o.category.clone(), o.hour_utc)).or_default()
            .push(o.spread.unsigned_abs() as f64);
    }

    // Collect categories
    let mut cats: Vec<String> = obs.iter().map(|o| o.category.clone()).collect();
    cats.sort();
    cats.dedup();

    for cat in &cats {
        let mut hour_avgs: Vec<(u32, f64)> = (0..24u32)
            .filter_map(|h| {
                let key = (cat.clone(), h);
                by_cat_hour.get(&key).map(|v| {
                    let avg = v.iter().sum::<f64>() / v.len() as f64;
                    (h, avg)
                })
            })
            .collect();
        hour_avgs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        let top3: Vec<_> = hour_avgs.iter().take(3).collect();
        let bottom3: Vec<_> = hour_avgs.iter().rev().take(3).collect();

        println!("  {} — best hours (high spread): {}",
                 cat,
                 top3.iter().map(|(h, avg)| format!("{:02}:00 ({:.1}¢)", h, avg))
                     .collect::<Vec<_>>().join(", "));
        println!("         worst hours (low spread):  {}",
                 bottom3.iter().map(|(h, avg)| format!("{:02}:00 ({:.1}¢)", h, avg))
                     .collect::<Vec<_>>().join(", "));
    }
    println!();
}

// ── Section 5: Signal Quality vs Outcome ─────────────────────────────────────

fn section_quality_vs_outcome(outcomes: &[TradeOutcome]) {
    println!("=== Signal Quality vs Outcome ===");

    // Buckets: [0.3-0.5), [0.5-0.8), [0.8-1.2), [1.2+]
    let buckets = [
        (0.3f64, 0.5f64, "0.3–0.5"),
        (0.5,    0.8,    "0.5–0.8"),
        (0.8,    1.2,    "0.8–1.2"),
        (1.2,    f64::MAX, "1.2+"),
    ];

    println!("  By quality bucket:");
    println!("  {:<10} {:>6} {:>10} {:>12} {:>12}",
             "bucket", "trades", "win_rate", "avg_pnl_$", "avg_hold_m");
    for (lo, hi, label) in &buckets {
        let t: Vec<_> = outcomes.iter()
            .filter(|o| o.signal_quality_at_entry >= *lo && o.signal_quality_at_entry < *hi)
            .collect();
        if t.is_empty() { continue; }
        let wins = t.iter().filter(|o| o.pnl_dollars > 0.0).count();
        let win_rate = wins as f64 / t.len() as f64 * 100.0;
        let avg_pnl  = t.iter().map(|o| o.pnl_dollars).sum::<f64>() / t.len() as f64;
        let avg_hold = t.iter().map(|o| o.hold_time_seconds as f64 / 60.0).sum::<f64>()
                       / t.len() as f64;
        println!("  {:<10} {:>6} {:>9.1}% {:>11.3} {:>11.1}m",
                 label, t.len(), win_rate, avg_pnl, avg_hold);
    }

    // By subcategory
    let mut by_sub: HashMap<String, Vec<&TradeOutcome>> = HashMap::new();
    for o in outcomes {
        by_sub.entry(o.subcategory.clone()).or_default().push(o);
    }
    println!();
    println!("  By subcategory:");
    println!("  {:<16} {:>6} {:>10} {:>12}",
             "subcategory", "trades", "win_rate", "avg_pnl_$");
    let mut subs: Vec<_> = by_sub.keys().cloned().collect();
    subs.sort();
    for sub in &subs {
        let t = &by_sub[sub];
        let wins = t.iter().filter(|o| o.pnl_dollars > 0.0).count();
        let win_rate = wins as f64 / t.len() as f64 * 100.0;
        let avg_pnl  = t.iter().map(|o| o.pnl_dollars).sum::<f64>() / t.len() as f64;
        println!("  {:<16} {:>6} {:>9.1}% {:>11.3}",
                 sub, t.len(), win_rate, avg_pnl);
    }
    println!();

    // Time-of-day P&L
    let mut by_hour: HashMap<u32, Vec<f64>> = HashMap::new();
    for o in outcomes {
        by_hour.entry(o.hour_utc_at_entry).or_default().push(o.pnl_dollars);
    }
    if !by_hour.is_empty() {
        let mut hour_avgs: Vec<(u32, f64, usize)> = by_hour.iter()
            .map(|(&h, pnls)| {
                let avg = pnls.iter().sum::<f64>() / pnls.len() as f64;
                (h, avg, pnls.len())
            })
            .collect();
        hour_avgs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        println!("  Best entry hours by avg P&L:");
        for (h, avg, n) in hour_avgs.iter().take(5) {
            println!("    {:02}:00 UTC  avg_pnl={:.3}  n={}", h, avg, n);
        }
        println!();
    }
}

// ── Section 6: Recommendations ───────────────────────────────────────────────

fn section_recommendations(
    obs:      &[SpreadObservation],
    events:   &[SignalEvent],
    outcomes: &[TradeOutcome],
) {
    println!("=== Recommendations ===");
    let mut recs: Vec<String> = Vec::new();

    // Oracle confidence adjustments based on convergence rates
    let mut sub_outcomes: HashMap<String, Vec<&TradeOutcome>> = HashMap::new();
    for o in outcomes {
        sub_outcomes.entry(o.subcategory.clone()).or_default().push(o);
    }
    for (sub, trades) in &sub_outcomes {
        if trades.len() < 5 { continue; }
        let converged = trades.iter()
            .filter(|t| t.exit_reason == "convergence" && t.spread_at_exit.unsigned_abs() <= 3)
            .count();
        let rate = converged as f64 / trades.len() as f64;
        if rate < 0.4 {
            let current_conf = trades.iter().map(|t| t.oracle_conf).sum::<f64>() / trades.len() as f64;
            let suggested = (current_conf * 0.7).max(0.1);
            recs.push(format!(
                "  ⚠️  {} convergence rate {:.0}% — consider lowering oracle_conf to {:.2} (currently ~{:.2})",
                sub, rate * 100.0, suggested, current_conf
            ));
        } else if rate > 0.75 {
            let current_conf = trades.iter().map(|t| t.oracle_conf).sum::<f64>() / trades.len() as f64;
            let suggested = (current_conf * 1.2).min(0.95);
            recs.push(format!(
                "  ✅ {} convergence rate {:.0}% — could raise oracle_conf to {:.2} (currently ~{:.2})",
                sub, rate * 100.0, suggested, current_conf
            ));
        }
    }

    // Max spread ceiling suggestion: p95 of observed spreads
    if !obs.is_empty() {
        let all_spreads: Vec<f64> = obs.iter().map(|o| o.spread.unsigned_abs() as f64).collect();
        let p95 = percentile(all_spreads, 95.0);
        recs.push(format!(
            "  📊 p95 spread = {:.1}¢ — consider SIGNAL_MAX_SPREAD_CENTS={:.0}",
            p95, (p95 + 5.0).round()
        ));
    }

    // Best entry hour per category
    let mut by_cat_hour_pnl: HashMap<(String, u32), Vec<f64>> = HashMap::new();
    for o in outcomes {
        by_cat_hour_pnl.entry((o.category.clone(), o.hour_utc_at_entry))
            .or_default()
            .push(o.pnl_dollars);
    }
    let mut cats: Vec<String> = outcomes.iter().map(|o| o.category.clone()).collect();
    cats.sort();
    cats.dedup();
    for cat in &cats {
        let mut hour_avgs: Vec<(u32, f64)> = (0..24u32)
            .filter_map(|h| {
                by_cat_hour_pnl.get(&(cat.clone(), h))
                    .filter(|v| v.len() >= 3)
                    .map(|v| (h, v.iter().sum::<f64>() / v.len() as f64))
            })
            .collect();
        hour_avgs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        if let Some((best_hour, best_avg)) = hour_avgs.first() {
            recs.push(format!(
                "  🕐 {} best entry hour: {:02}:00 UTC (avg P&L={:.3})",
                cat, best_hour, best_avg
            ));
        }
        if let Some((worst_hour, worst_avg)) = hour_avgs.last() {
            if *worst_avg < -0.01 {
                recs.push(format!(
                    "  🕐 {} avoid entry at: {:02}:00 UTC (avg P&L={:.3})",
                    cat, worst_hour, worst_avg
                ));
            }
        }
    }

    // Signal quality filter suggestion
    if !outcomes.is_empty() {
        let low_q_trades: Vec<_> = outcomes.iter()
            .filter(|o| o.signal_quality_at_entry < 0.5)
            .collect();
        if low_q_trades.len() >= 5 {
            let wins = low_q_trades.iter().filter(|o| o.pnl_dollars > 0.0).count();
            let rate = wins as f64 / low_q_trades.len() as f64;
            if rate < 0.4 {
                recs.push(format!(
                    "  📈 Low-quality signals (q<0.5) win rate={:.0}% — consider SIGNAL_MIN_QUALITY=0.5",
                    rate * 100.0
                ));
            }
        }
    }

    // Entries vs rejections
    let entries  = events.iter().filter(|e| e.action == "entered").count();
    let rejected = events.iter().filter(|e| e.action == "rejected").count();
    if entries + rejected > 0 {
        let rej_rate = rejected as f64 / (entries + rejected) as f64 * 100.0;
        if rej_rate > 80.0 {
            recs.push(format!(
                "  ⚡ {:.0}% of signals rejected by risk manager — consider relaxing VAR limits or reducing concurrency",
                rej_rate
            ));
        }
    }

    // Check for missing market quality data
    if !obs.is_empty() {
        let mut quality_by_sub: HashMap<String, Vec<f64>> = HashMap::new();
        for e in events.iter().filter(|e| e.action == "entered") {
            quality_by_sub.entry(e.subcategory.clone()).or_default().push(e.signal_quality);
        }
        let mut subs: Vec<_> = quality_by_sub.keys().cloned().collect();
        subs.sort();
        for sub in &subs {
            let qs = &quality_by_sub[sub];
            let avg = qs.iter().sum::<f64>() / qs.len() as f64;
            recs.push(format!("  📊 {} avg entry quality = {:.3}", sub, avg));
        }
    }

    if recs.is_empty() {
        println!("  (not enough data for recommendations — need 5+ trades per subcategory)");
    } else {
        for rec in &recs {
            println!("{}", rec);
        }
    }
    println!();
}
