// src/diagnose.rs
// Diagnostic mode: connect, print a live market table, then exit.
//
// Activated via `--diagnose` CLI flag or `DIAGNOSE=1` env var.
// Duration: DIAGNOSE_SECS env var (default 60 s).
// Refresh: every 5 seconds.

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::config::wide_spread_threshold;
use crate::types::GlobalState;

/// Run the diagnostic display loop.
///
/// Prints a market table every 5 seconds.
/// Exits after `duration_secs`.
pub async fn run_diagnose(state: Arc<GlobalState>, duration_secs: u64) {
    info!(
        "[DIAGNOSE] Running for {} seconds (refresh every 5s)...",
        duration_secs
    );
    println!("\n══════════════════════════════════════════════════════════════════");
    println!("  AME BOT — DIAGNOSTIC MODE  ({}s)", duration_secs);
    println!("══════════════════════════════════════════════════════════════════\n");

    let start = std::time::Instant::now();
    let threshold = wide_spread_threshold();

    loop {
        if start.elapsed().as_secs() >= duration_secs {
            println!("\n[DIAGNOSE] Duration elapsed — exiting.");
            break;
        }

        print_table(&state, threshold);

        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

fn print_table(state: &GlobalState, wide_spread_threshold: u16) {
    let now = chrono::Utc::now().format("%H:%M:%S").to_string();
    let market_count = state.market_count();

    // Gather data
    struct Row {
        name: String,
        ticker: String,
        k_yes: u16,
        k_no: u16,
        p_yes: u16,
        p_no: u16,
        spread: i16,      // yes+no-100 (negative = arb)
        arb_profit: i16,  // meaningful when spread < 0
    }

    let mut rows: Vec<Row> = Vec::with_capacity(market_count);

    for i in 0..market_count {
        if let Some(market) = state.get_by_id(i as u16) {
            let (k_yes, k_no, _, _) = market.kalshi.load();
            let (p_yes, p_no, _, _) = market.poly.load();
            if k_yes == 0 && k_no == 0 { continue; }

            let name = market.pair
                .as_ref()
                .map(|p| p.description.to_string())
                .unwrap_or_else(|| format!("market_{}", i));
            let ticker = market.pair
                .as_ref()
                .map(|p| p.kalshi_market_ticker.to_string())
                .unwrap_or_default();

            let spread = k_yes as i16 + k_no as i16 - 100;
            let arb_profit = if spread < 0 {
                let yes_fee = crate::types::kalshi_fee_cents(k_yes) as i16;
                let no_fee = crate::types::kalshi_fee_cents(k_no) as i16;
                -spread - yes_fee - no_fee
            } else {
                0
            };

            rows.push(Row { name, ticker, k_yes, k_no, p_yes, p_no, spread, arb_profit });
        }
    }

    // Sort by spread ascending (tightest first = most interesting)
    rows.sort_by_key(|r| r.spread);

    println!("─────────────────────────────────────────────────────────────────");
    println!("  {} | {} markets tracked", now, market_count);
    println!("─────────────────────────────────────────────────────────────────");

    // Header
    println!("{:<36} {:>6} {:>6} {:>6} {:>6} {:>8}",
        "Market", "K-YES", "K-NO", "P-YES", "P-NO", "Spread");
    println!("{}", "─".repeat(70));

    // Top 20 rows
    let display_count = rows.len().min(20);
    for row in rows.iter().take(display_count) {
        let spread_str = if row.spread < 0 {
            format!("🎯{:+}¢", row.spread)
        } else if row.spread as u16 >= wide_spread_threshold {
            format!("📊{:+}¢", row.spread)
        } else {
            format!("{:+}¢", row.spread)
        };

        let name_truncated = if row.name.len() > 35 {
            format!("{}…", &row.name[..34])
        } else {
            row.name.clone()
        };

        let p_yes_str = if row.p_yes > 0 { format!("{}¢", row.p_yes) } else { "-".into() };
        let p_no_str = if row.p_no > 0 { format!("{}¢", row.p_no) } else { "-".into() };

        println!("{:<36} {:>5}¢ {:>5}¢ {:>6} {:>6} {:>8}",
            name_truncated,
            row.k_yes, row.k_no,
            p_yes_str, p_no_str,
            spread_str,
        );
    }

    // === Same-platform arbs ===
    let arbs: Vec<_> = rows.iter().filter(|r| r.arb_profit > 0).collect();
    if !arbs.is_empty() {
        println!("\n  🎯 SAME-PLATFORM ARBS DETECTED:");
        for r in &arbs {
            println!("    {} | YES={}¢ NO={}¢ → profit≈{}¢/contract",
                r.ticker, r.k_yes, r.k_no, r.arb_profit);
        }
    }

    // === Cross-platform arbs ===
    let cross_arbs: Vec<_> = rows.iter().filter(|r| {
        r.p_yes > 0 && r.k_no > 0 &&
        r.p_yes + r.k_no + crate::types::kalshi_fee_cents(r.k_no) < 100
    }).collect();
    if !cross_arbs.is_empty() {
        println!("\n  ⚡ CROSS-PLATFORM ARBS DETECTED:");
        for r in &cross_arbs {
            let fee = crate::types::kalshi_fee_cents(r.k_no);
            let cost = r.p_yes + r.k_no + fee;
            println!("    {} | P_yes={}¢ + K_no={}¢ + fee={}¢ = {}¢ → {}¢ profit",
                r.ticker, r.p_yes, r.k_no, fee, cost, 100u16.saturating_sub(cost));
        }
    }

    println!();
}
