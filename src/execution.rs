// src/execution.rs
// Execution Engine

use anyhow::{Result, anyhow};
use chrono::Utc;
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{info, warn, error};

use crate::kalshi::{KalshiApiClient, KalshiOrderResponse};
use crate::polymarket_clob::SharedAsyncClient;
use crate::polymarket_us::{PolymarketUsClient, LimitOrderRequest};
use crate::telegram::TelegramClient;
use crate::types::{
    ArbType, MarketPair,
    FastExecutionRequest, GlobalState,
    cents_to_price,
};
use crate::circuit_breaker::CircuitBreaker;
use crate::position_tracker::{FillRecord, PositionChannel};

// =============================================================================
// EXECUTION ENGINE
// =============================================================================

/// Monotonic nanosecond clock for latency measurement
pub struct NanoClock {
    start: Instant,
}

impl NanoClock {
    pub fn new() -> Self {
        Self { start: Instant::now() }
    }

    #[inline(always)]
    pub fn now_ns(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }
}

impl Default for NanoClock {
    fn default() -> Self {
        Self::new()
    }
}

/// Execution engine
pub struct ExecutionEngine {
    kalshi: Arc<KalshiApiClient>,
    poly_async: Option<Arc<SharedAsyncClient>>,
    /// Polymarket US Retail API client (Ed25519 auth). When present, used instead of
    /// the CLOB client for all Polymarket legs on cross-platform arb opportunities.
    poly_us: Option<Arc<PolymarketUsClient>>,
    state: Arc<GlobalState>,
    circuit_breaker: Arc<CircuitBreaker>,
    position_channel: PositionChannel,
    telegram: Option<Arc<TelegramClient>>,
    in_flight: Arc<[AtomicU64; 8]>,
    clock: NanoClock,
    pub dry_run: bool,
    test_mode: bool,
}

impl ExecutionEngine {
    pub fn new(
        kalshi: Arc<KalshiApiClient>,
        poly_async: Option<Arc<SharedAsyncClient>>,
        poly_us: Option<Arc<PolymarketUsClient>>,
        state: Arc<GlobalState>,
        circuit_breaker: Arc<CircuitBreaker>,
        position_channel: PositionChannel,
        telegram: Option<Arc<TelegramClient>>,
        dry_run: bool,
    ) -> Self {
        let test_mode = std::env::var("TEST_ARB")
            .map(|v| v == "1" || v == "true")
            .unwrap_or(false);

        Self {
            kalshi,
            poly_async,
            poly_us,
            state,
            circuit_breaker,
            position_channel,
            telegram,
            in_flight: Arc::new(std::array::from_fn(|_| AtomicU64::new(0))),
            clock: NanoClock::new(),
            dry_run,
            test_mode,
        }
    }

    /// Process an execution request
    #[inline]
    pub async fn process(&self, req: FastExecutionRequest) -> Result<ExecutionResult> {
        let market_id = req.market_id;

        // Deduplication check (512 markets via 8x u64 bitmask)
        if market_id < 512 {
            let slot = (market_id / 64) as usize;
            let bit = market_id % 64;
            let mask = 1u64 << bit;
            let prev = self.in_flight[slot].fetch_or(mask, Ordering::AcqRel);
            if prev & mask != 0 {
                return Ok(ExecutionResult {
                    market_id,
                    success: false,
                    profit_cents: 0,
                    latency_ns: self.clock.now_ns() - req.detected_ns,
                    error: Some("Already in-flight"),
                });
            }
        }

        // Get market pair 
        let market = self.state.get_by_id(market_id)
            .ok_or_else(|| anyhow!("Unknown market_id {}", market_id))?;

        let pair = market.pair.as_ref()
            .ok_or_else(|| anyhow!("No pair for market_id {}", market_id))?;

        // Calculate profit
        let profit_cents = req.profit_cents();
        if profit_cents < 1 {
            self.release_in_flight(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: false,
                profit_cents: 0,
                latency_ns: self.clock.now_ns() - req.detected_ns,
                error: Some("Profit below threshold"),
            });
        }

        // Calculate max contracts from size (min of both sides)
        let mut max_contracts = (req.yes_size.min(req.no_size) / 100) as i64;

        // SAFETY: In test mode, cap at 10 contracts
        // Note: Polymarket has $1 minimum spend, so at 40¢ price, 1 contract = $0.40 (rejected!)
        // 10 contracts ensures we meet the minimum at any reasonable price
        if self.test_mode && max_contracts > 10 {
            warn!("[EXEC] ⚠️ TEST_MODE: Capping contracts from {} to 10", max_contracts);
            max_contracts = 10;
        }

        if max_contracts < 1 {
            warn!(
                "[EXEC] Liquidity fail: {:?} | yes_size={}¢ no_size={}¢",
                req.arb_type, req.yes_size, req.no_size
            );
            self.release_in_flight(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: false,
                profit_cents: 0,
                latency_ns: self.clock.now_ns() - req.detected_ns,
                error: Some("Insufficient liquidity"),
            });
        }

        // Circuit breaker check
        if let Err(reason) = self.circuit_breaker.can_execute(&pair.pair_id, max_contracts).await {
            warn!("[EXEC] Circuit breaker blocked: {} (market={}, contracts={})", reason, &pair.pair_id, max_contracts);
            self.release_in_flight(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: false,
                profit_cents: 0,
                latency_ns: self.clock.now_ns() - req.detected_ns,
                error: Some("Circuit breaker"),
            });
        }

        let latency_to_exec = self.clock.now_ns() - req.detected_ns;
        info!(
            "[EXEC] 🎯 {} | {:?} y={}¢ n={}¢ | profit={}¢ | {}x | {}µs",
            pair.description,
            req.arb_type,
            req.yes_price,
            req.no_price,
            profit_cents,
            max_contracts,
            latency_to_exec / 1000
        );

        if self.dry_run {
            let (poly_side, poly_price) = match req.arb_type {
                ArbType::PolyYesKalshiNo => ("YES", req.yes_price),
                ArbType::KalshiYesPolyNo => ("NO", req.no_price),
            };
            let (kalshi_side, kalshi_price) = match req.arb_type {
                ArbType::PolyYesKalshiNo => ("NO", req.no_price),
                ArbType::KalshiYesPolyNo => ("YES", req.yes_price),
            };
            if self.poly_us.is_some() {
                info!(
                    "[EXEC] 🏃 DRY RUN — {}x | PolyUS-buy-{}@{}¢ ({}) + Kalshi-buy-{}@{}¢",
                    max_contracts, poly_side, poly_price, &*pair.poly_slug,
                    kalshi_side, kalshi_price,
                );
            } else {
                info!(
                    "[EXEC] 🏃 DRY RUN — {}x | PolyCLOB-buy-{}@{}¢ + Kalshi-buy-{}@{}¢",
                    max_contracts, poly_side, poly_price, kalshi_side, kalshi_price,
                );
            }
            self.release_in_flight_delayed(market_id);
            return Ok(ExecutionResult {
                market_id,
                success: true,
                profit_cents: profit_cents as i64,
                latency_ns: latency_to_exec,
                error: Some("DRY_RUN"),
            });
        }

        // Execute both legs concurrently 
        let result = self.execute_both_legs_async(&req, pair, max_contracts).await;

        // Release in-flight after delay
        self.release_in_flight_delayed(market_id);

        match result {
            Ok((yes_filled, no_filled, yes_cost, no_cost, yes_order_id, no_order_id)) => {
                let matched = yes_filled.min(no_filled);
                let success = matched > 0;
                let actual_profit = matched as i64 * 100 - (yes_cost + no_cost);

                // === AUTO-CLOSE MISMATCHED EXPOSURE (non-blocking) ===
                if yes_filled != no_filled && (yes_filled > 0 || no_filled > 0) {
                    let excess = (yes_filled - no_filled).abs();
                    let (leg1_name, leg2_name) = match req.arb_type {
                        ArbType::PolyYesKalshiNo => ("P_yes", "K_no"),
                        ArbType::KalshiYesPolyNo => ("K_yes", "P_no"),
                    };
                    warn!("[EXEC] ⚠️ Fill mismatch: {}={} {}={} (excess={})",
                        leg1_name, yes_filled, leg2_name, no_filled, excess);

                    // Spawn auto-close in background (don't block hot path with 2s sleep)
                    let kalshi = self.kalshi.clone();
                    let poly_async = self.poly_async.clone();
                    let arb_type = req.arb_type;
                    let yes_price = req.yes_price;
                    let no_price = req.no_price;
                    let poly_yes_token = pair.poly_yes_token.clone();
                    let poly_no_token = pair.poly_no_token.clone();
                    let kalshi_ticker = pair.kalshi_market_ticker.clone();
                    let original_cost_per_contract = if yes_filled > no_filled {
                        if yes_filled > 0 { yes_cost / yes_filled } else { 0 }
                    } else {
                        if no_filled > 0 { no_cost / no_filled } else { 0 }
                    };

                    let poly_us_bg = self.poly_us.clone();
                    let poly_slug_bg = pair.poly_slug.clone();
                    tokio::spawn(async move {
                        Self::auto_close_background(
                            kalshi, poly_async, poly_us_bg, arb_type, yes_filled, no_filled,
                            yes_price, no_price, poly_yes_token, poly_no_token,
                            poly_slug_bg, kalshi_ticker, original_cost_per_contract
                        ).await;
                    });
                }

                if success {
                    self.circuit_breaker.record_success(&pair.pair_id, matched, matched, actual_profit as f64 / 100.0, req.arb_type).await;
                }

                if matched > 0 {
                    let (platform1, side1, platform2, side2) = match req.arb_type {
                        ArbType::PolyYesKalshiNo => ("polymarket", "yes", "kalshi", "no"),
                        ArbType::KalshiYesPolyNo => ("kalshi", "yes", "polymarket", "no"),
                    };

                    self.position_channel.record_fill(FillRecord::new(
                        &pair.pair_id, &pair.description, platform1, side1,
                        matched as f64, yes_cost as f64 / 100.0 / yes_filled.max(1) as f64,
                        0.0, &yes_order_id,
                    ));
                    self.position_channel.record_fill(FillRecord::new(
                        &pair.pair_id, &pair.description, platform2, side2,
                        matched as f64, no_cost as f64 / 100.0 / no_filled.max(1) as f64,
                        0.0, &no_order_id,
                    ));

                    // Telegram trade notification
                    if let Some(ref tg) = self.telegram {
                        let arb_label = match req.arb_type {
                            ArbType::PolyYesKalshiNo => "PolyYES+KalshiNO",
                            ArbType::KalshiYesPolyNo => "KalshiYES+PolyNO",
                        };
                        // In both arb types: yes_filled=kalshi leg, no_filled=poly leg
                        tg.alert_trade_executed(
                            market_id,
                            &pair.description,
                            arb_label,
                            yes_filled,
                            no_filled,
                            yes_cost + no_cost,
                            actual_profit,
                            &pair.poly_slug,
                        );
                    }
                }

                let (kalshi_price_log, poly_price_log) = match req.arb_type {
                    ArbType::PolyYesKalshiNo => (req.no_price, req.yes_price),
                    ArbType::KalshiYesPolyNo => (req.yes_price, req.no_price),
                };
                let trade_status = if matched == max_contracts { "filled" }
                    else if matched > 0 { "partial" }
                    else { "failed" };
                write_trade_log(&TradeLogEntry {
                    timestamp: Utc::now().to_rfc3339(),
                    pair_id: pair.pair_id.to_string(),
                    description: pair.description.to_string(),
                    arb_type: match req.arb_type {
                        ArbType::PolyYesKalshiNo => "PolyYesKalshiNo".to_string(),
                        ArbType::KalshiYesPolyNo => "KalshiYesPolyNo".to_string(),
                    },
                    contracts: max_contracts,
                    kalshi_price: kalshi_price_log,
                    poly_price: poly_price_log,
                    kalshi_filled: yes_filled,
                    poly_filled: no_filled,
                    total_cost_cents: yes_cost + no_cost,
                    expected_profit_cents: profit_cents as i64,
                    status: trade_status.to_string(),
                }).await;

                Ok(ExecutionResult {
                    market_id,
                    success,
                    profit_cents: actual_profit,
                    latency_ns: self.clock.now_ns() - req.detected_ns,
                    error: if success { None } else { Some("Partial/no fill") },
                })
            }
            Err(e) => {
                error!("[EXEC] Both-legs execution failed for {}: {:#}", &pair.pair_id, e);
                self.circuit_breaker.record_error().await;
                let (kalshi_price_log, poly_price_log) = match req.arb_type {
                    ArbType::PolyYesKalshiNo => (req.no_price, req.yes_price),
                    ArbType::KalshiYesPolyNo => (req.yes_price, req.no_price),
                };
                write_trade_log(&TradeLogEntry {
                    timestamp: Utc::now().to_rfc3339(),
                    pair_id: pair.pair_id.to_string(),
                    description: pair.description.to_string(),
                    arb_type: match req.arb_type {
                        ArbType::PolyYesKalshiNo => "PolyYesKalshiNo".to_string(),
                        ArbType::KalshiYesPolyNo => "KalshiYesPolyNo".to_string(),
                    },
                    contracts: max_contracts,
                    kalshi_price: kalshi_price_log,
                    poly_price: poly_price_log,
                    kalshi_filled: 0,
                    poly_filled: 0,
                    total_cost_cents: 0,
                    expected_profit_cents: profit_cents as i64,
                    status: "error".to_string(),
                }).await;
                Ok(ExecutionResult {
                    market_id,
                    success: false,
                    profit_cents: 0,
                    latency_ns: self.clock.now_ns() - req.detected_ns,
                    error: Some("Execution failed"),
                })
            }
        }
    }

    async fn execute_both_legs_async(
        &self,
        req: &FastExecutionRequest,
        pair: &MarketPair,
        contracts: i64,
    ) -> Result<(i64, i64, i64, i64, String, String)> {
        match req.arb_type {
            // === CROSS-PLATFORM: Poly YES + Kalshi NO ===
            ArbType::PolyYesKalshiNo => {
                let kalshi_fut = self.kalshi.place_buy_order(
                    &pair.kalshi_market_ticker,
                    "no",
                    req.no_price as i64,
                    contracts,
                );
                if let Some(ref poly_us) = self.poly_us {
                    let poly_us = poly_us.clone();
                    let slug = pair.poly_slug.to_string();
                    let yes_price = req.yes_price;
                    let poly_fut = async move {
                        execute_poly_leg_us(&poly_us, &slug, ArbType::PolyYesKalshiNo, yes_price, contracts).await
                    };
                    let (kalshi_res, poly_res) = tokio::join!(kalshi_fut, poly_fut);

                    // For resting limit orders: poll up to 5s then cancel if still pending
                    let kalshi_res = maybe_poll_kalshi_limit(&self.kalshi, kalshi_res, Duration::from_secs(5)).await;

                    let (k_filled, k_cost, k_id) = self.extract_kalshi(kalshi_res);
                    let (p_filled, p_cost, p_id) = poly_res;

                    // Rollback one-sided fill
                    if k_filled > 0 && p_filled == 0 {
                        warn!("[EXEC] ⚠️ Unwinding one-sided fill: Kalshi NO filled={} Poly YES=0 | ticker={} slug={}",
                            k_filled, pair.kalshi_market_ticker, pair.poly_slug);
                        let kalshi = self.kalshi.clone();
                        let ticker = pair.kalshi_market_ticker.to_string();
                        let price = req.no_price as i64;
                        tokio::spawn(async move {
                            unwind_kalshi_fill(kalshi, ticker, "no", price, k_filled).await;
                        });
                    } else if p_filled > 0 && k_filled == 0 {
                        warn!("[EXEC] ⚠️ Unwinding one-sided fill: Poly YES filled={} Kalshi NO=0 | slug={} order={}",
                            p_filled, pair.poly_slug, p_id);
                        let poly_us_clone = self.poly_us.clone();
                        let order_id = p_id.clone();
                        let slug = pair.poly_slug.to_string();
                        let price = req.yes_price;
                        tokio::spawn(async move {
                            if let Some(ref us) = poly_us_clone {
                                unwind_poly_us_fill(us.clone(), order_id, slug, "yes", price, p_filled).await;
                            }
                        });
                    }

                    Ok((k_filled, p_filled, k_cost, p_cost, k_id, p_id))
                } else if let Some(ref poly_clob) = self.poly_async {
                    let poly_fut = poly_clob.buy_fak(
                        &pair.poly_yes_token,
                        cents_to_price(req.yes_price),
                        contracts as f64,
                    );
                    let (kalshi_res, poly_res) = tokio::join!(kalshi_fut, poly_fut);

                    // For resting limit orders: poll up to 5s then cancel if still pending
                    let kalshi_res = maybe_poll_kalshi_limit(&self.kalshi, kalshi_res, Duration::from_secs(5)).await;

                    let (k_filled, p_filled, k_cost, p_cost, k_id, p_id) =
                        self.extract_cross_results(kalshi_res, poly_res)?;

                    // Rollback one-sided fill
                    if k_filled > 0 && p_filled == 0 {
                        warn!("[EXEC] ⚠️ Unwinding one-sided fill: Kalshi NO filled={} Poly CLOB YES=0 | ticker={}",
                            k_filled, pair.kalshi_market_ticker);
                        let kalshi = self.kalshi.clone();
                        let ticker = pair.kalshi_market_ticker.to_string();
                        let price = req.no_price as i64;
                        tokio::spawn(async move {
                            unwind_kalshi_fill(kalshi, ticker, "no", price, k_filled).await;
                        });
                    } else if p_filled > 0 && k_filled == 0 {
                        warn!("[EXEC] ⚠️ Unwinding one-sided fill: Poly CLOB YES filled={} Kalshi NO=0 | token={}",
                            p_filled, pair.poly_yes_token);
                        let poly_clob_clone = poly_clob.clone();
                        let token = pair.poly_yes_token.to_string();
                        let price = req.yes_price;
                        tokio::spawn(async move {
                            unwind_poly_clob_fill(poly_clob_clone, token, price, p_filled).await;
                        });
                    }

                    Ok((k_filled, p_filled, k_cost, p_cost, k_id, p_id))
                } else {
                    warn!("[EXEC] No Polymarket client available for YES leg");
                    Ok((0, 0, 0, 0, String::new(), String::new()))
                }
            }

            // === CROSS-PLATFORM: Kalshi YES + Poly NO ===
            ArbType::KalshiYesPolyNo => {
                let kalshi_fut = self.kalshi.place_buy_order(
                    &pair.kalshi_market_ticker,
                    "yes",
                    req.yes_price as i64,
                    contracts,
                );
                if let Some(ref poly_us) = self.poly_us {
                    let poly_us = poly_us.clone();
                    let slug = pair.poly_slug.to_string();
                    let no_price = req.no_price;
                    let poly_fut = async move {
                        execute_poly_leg_us(&poly_us, &slug, ArbType::KalshiYesPolyNo, no_price, contracts).await
                    };
                    let (kalshi_res, poly_res) = tokio::join!(kalshi_fut, poly_fut);

                    // For resting limit orders: poll up to 5s then cancel if still pending
                    let kalshi_res = maybe_poll_kalshi_limit(&self.kalshi, kalshi_res, Duration::from_secs(5)).await;

                    let (k_filled, k_cost, k_id) = self.extract_kalshi(kalshi_res);
                    let (p_filled, p_cost, p_id) = poly_res;

                    // Rollback one-sided fill
                    if k_filled > 0 && p_filled == 0 {
                        warn!("[EXEC] ⚠️ Unwinding one-sided fill: Kalshi YES filled={} Poly NO=0 | ticker={} slug={}",
                            k_filled, pair.kalshi_market_ticker, pair.poly_slug);
                        let kalshi = self.kalshi.clone();
                        let ticker = pair.kalshi_market_ticker.to_string();
                        let price = req.yes_price as i64;
                        tokio::spawn(async move {
                            unwind_kalshi_fill(kalshi, ticker, "yes", price, k_filled).await;
                        });
                    } else if p_filled > 0 && k_filled == 0 {
                        warn!("[EXEC] ⚠️ Unwinding one-sided fill: Poly NO filled={} Kalshi YES=0 | slug={} order={}",
                            p_filled, pair.poly_slug, p_id);
                        let poly_us_clone = self.poly_us.clone();
                        let order_id = p_id.clone();
                        let slug = pair.poly_slug.to_string();
                        let price = req.no_price;
                        tokio::spawn(async move {
                            if let Some(ref us) = poly_us_clone {
                                unwind_poly_us_fill(us.clone(), order_id, slug, "no", price, p_filled).await;
                            }
                        });
                    }

                    Ok((k_filled, p_filled, k_cost, p_cost, k_id, p_id))
                } else if let Some(ref poly_clob) = self.poly_async {
                    let poly_fut = poly_clob.buy_fak(
                        &pair.poly_no_token,
                        cents_to_price(req.no_price),
                        contracts as f64,
                    );
                    let (kalshi_res, poly_res) = tokio::join!(kalshi_fut, poly_fut);

                    // For resting limit orders: poll up to 5s then cancel if still pending
                    let kalshi_res = maybe_poll_kalshi_limit(&self.kalshi, kalshi_res, Duration::from_secs(5)).await;

                    let (k_filled, p_filled, k_cost, p_cost, k_id, p_id) =
                        self.extract_cross_results(kalshi_res, poly_res)?;

                    // Rollback one-sided fill
                    if k_filled > 0 && p_filled == 0 {
                        warn!("[EXEC] ⚠️ Unwinding one-sided fill: Kalshi YES filled={} Poly CLOB NO=0 | ticker={}",
                            k_filled, pair.kalshi_market_ticker);
                        let kalshi = self.kalshi.clone();
                        let ticker = pair.kalshi_market_ticker.to_string();
                        let price = req.yes_price as i64;
                        tokio::spawn(async move {
                            unwind_kalshi_fill(kalshi, ticker, "yes", price, k_filled).await;
                        });
                    } else if p_filled > 0 && k_filled == 0 {
                        warn!("[EXEC] ⚠️ Unwinding one-sided fill: Poly CLOB NO filled={} Kalshi YES=0 | token={}",
                            p_filled, pair.poly_no_token);
                        let poly_clob_clone = poly_clob.clone();
                        let token = pair.poly_no_token.to_string();
                        let price = req.no_price;
                        tokio::spawn(async move {
                            unwind_poly_clob_fill(poly_clob_clone, token, price, p_filled).await;
                        });
                    }

                    Ok((k_filled, p_filled, k_cost, p_cost, k_id, p_id))
                } else {
                    warn!("[EXEC] No Polymarket client available for NO leg");
                    Ok((0, 0, 0, 0, String::new(), String::new()))
                }
            }
        }
    }

    /// Extract fills/cost/id from a Kalshi order response.
    fn extract_kalshi(
        &self,
        kalshi_res: Result<crate::kalshi::KalshiOrderResponse>,
    ) -> (i64, i64, String) {
        match kalshi_res {
            Ok(resp) => {
                let filled = resp.order.filled_count();
                let cost = resp.order.taker_fill_cost.unwrap_or(0) + resp.order.maker_fill_cost.unwrap_or(0);
                (filled, cost, resp.order.order_id)
            }
            Err(e) => {
                warn!("[EXEC] Kalshi failed: {}", e);
                (0, 0, String::new())
            }
        }
    }

    /// Extract results from cross-platform execution (CLOB path).
    fn extract_cross_results(
        &self,
        kalshi_res: Result<crate::kalshi::KalshiOrderResponse>,
        poly_res: Result<crate::polymarket_clob::PolyFillAsync>,
    ) -> Result<(i64, i64, i64, i64, String, String)> {
        let (kalshi_filled, kalshi_cost, kalshi_order_id) = self.extract_kalshi(kalshi_res);

        let (poly_filled, poly_cost, poly_order_id) = match poly_res {
            Ok(fill) => {
                ((fill.filled_size as i64), (fill.fill_cost * 100.0) as i64, fill.order_id)
            }
            Err(e) => {
                warn!("[EXEC] Poly CLOB failed: {}", e);
                (0, 0, String::new())
            }
        };

        Ok((kalshi_filled, poly_filled, kalshi_cost, poly_cost, kalshi_order_id, poly_order_id))
    }

    /// Background auto-close for mismatched fills.
    ///
    /// Prefers the Polymarket US client when available; falls back to the CLOB client.
    async fn auto_close_background(
        kalshi: Arc<KalshiApiClient>,
        poly_async: Option<Arc<SharedAsyncClient>>,
        poly_us: Option<Arc<PolymarketUsClient>>,
        arb_type: ArbType,
        yes_filled: i64,
        no_filled: i64,
        yes_price: u16,
        no_price: u16,
        poly_yes_token: Arc<str>,
        poly_no_token: Arc<str>,
        poly_slug: Arc<str>,
        kalshi_ticker: Arc<str>,
        original_cost_per_contract: i64,
    ) {
        let excess = (yes_filled - no_filled).abs();
        if excess == 0 {
            return;
        }

        // Helper to log P&L after close
        let log_close_pnl = |platform: &str, closed: i64, proceeds: i64| {
            if closed > 0 {
                let close_pnl = proceeds - (original_cost_per_contract * excess);
                info!("[EXEC] ✅ Closed {} {} contracts for {}¢ (P&L: {}¢)",
                    closed, platform, proceeds, close_pnl);
            } else {
                error!("[EXEC] Failed to close {} excess - 0 filled", platform);
            }
        };

        match arb_type {
            ArbType::PolyYesKalshiNo => {
                if yes_filled > no_filled {
                    // Poly YES excess — sell YES to flatten
                    let close_price_cents = (yes_price as i16).saturating_sub(10).max(1) as u16;
                    info!("[EXEC] 🔄 Waiting 2s for Poly settlement before auto-close ({} yes contracts)", excess);
                    tokio::time::sleep(Duration::from_secs(2)).await;

                    if let Some(ref us) = poly_us {
                        let order_req = LimitOrderRequest::sell_yes(
                            &poly_slug,
                            close_price_cents as f64 / 100.0,
                            excess as u64,
                        );
                        match us.place_limit_order(&order_req).await {
                            Ok(resp) => {
                                let proceeds = (close_price_cents as i64) * excess;
                                info!("[EXEC] PolyUS close order: id={:?}", resp.id);
                                log_close_pnl("PolyUS", excess, proceeds);
                            }
                            Err(e) => error!("[EXEC] Failed to close PolyUS YES excess: {}", e),
                        }
                    } else if let Some(ref poly_clob) = poly_async {
                        let close_price = cents_to_price(close_price_cents);
                        match poly_clob.sell_fak(&poly_yes_token, close_price, excess as f64).await {
                            Ok(fill) => log_close_pnl("Poly", fill.filled_size as i64, (fill.fill_cost * 100.0) as i64),
                            Err(e) => error!("[EXEC] Failed to close Poly YES excess: {}", e),
                        }
                    } else {
                        error!("[EXEC] No Poly client to close YES excess");
                    }
                } else {
                    // Kalshi NO excess
                    let close_price = (no_price as i64).saturating_sub(10).max(1);
                    match kalshi.sell_ioc(&kalshi_ticker, "no", close_price, excess).await {
                        Ok(resp) => {
                            let proceeds = resp.order.taker_fill_cost.unwrap_or(0) + resp.order.maker_fill_cost.unwrap_or(0);
                            log_close_pnl("Kalshi", resp.order.filled_count(), proceeds);
                        }
                        Err(e) => error!("[EXEC] Failed to close Kalshi NO excess: {}", e),
                    }
                }
            }

            ArbType::KalshiYesPolyNo => {
                if yes_filled > no_filled {
                    // Kalshi YES excess
                    let close_price = (yes_price as i64).saturating_sub(10).max(1);
                    match kalshi.sell_ioc(&kalshi_ticker, "yes", close_price, excess).await {
                        Ok(resp) => {
                            let proceeds = resp.order.taker_fill_cost.unwrap_or(0) + resp.order.maker_fill_cost.unwrap_or(0);
                            log_close_pnl("Kalshi", resp.order.filled_count(), proceeds);
                        }
                        Err(e) => error!("[EXEC] Failed to close Kalshi YES excess: {}", e),
                    }
                } else {
                    // Poly NO excess — sell NO to flatten
                    let close_price_cents = (no_price as i16).saturating_sub(10).max(1) as u16;
                    info!("[EXEC] 🔄 Waiting 2s for Poly settlement before auto-close ({} no contracts)", excess);
                    tokio::time::sleep(Duration::from_secs(2)).await;

                    if let Some(ref us) = poly_us {
                        let order_req = LimitOrderRequest::sell_no(
                            &poly_slug,
                            close_price_cents as f64 / 100.0,
                            excess as u64,
                        );
                        match us.place_limit_order(&order_req).await {
                            Ok(resp) => {
                                let proceeds = (close_price_cents as i64) * excess;
                                info!("[EXEC] PolyUS close order: id={:?}", resp.id);
                                log_close_pnl("PolyUS", excess, proceeds);
                            }
                            Err(e) => error!("[EXEC] Failed to close PolyUS NO excess: {}", e),
                        }
                    } else if let Some(ref poly_clob) = poly_async {
                        let close_price = cents_to_price(close_price_cents);
                        match poly_clob.sell_fak(&poly_no_token, close_price, excess as f64).await {
                            Ok(fill) => log_close_pnl("Poly", fill.filled_size as i64, (fill.fill_cost * 100.0) as i64),
                            Err(e) => error!("[EXEC] Failed to close Poly NO excess: {}", e),
                        }
                    } else {
                        error!("[EXEC] No Poly client to close NO excess");
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn release_in_flight(&self, market_id: u16) {
        if market_id < 512 {
            let slot = (market_id / 64) as usize;
            let bit = market_id % 64;
            let mask = !(1u64 << bit);
            self.in_flight[slot].fetch_and(mask, Ordering::Release);
        }
    }

    fn release_in_flight_delayed(&self, market_id: u16) {
        if market_id < 512 {
            let in_flight = self.in_flight.clone();
            let slot = (market_id / 64) as usize;
            let bit = market_id % 64;
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let mask = !(1u64 << bit);
                in_flight[slot].fetch_and(mask, Ordering::Release);
            });
        }
    }
}

// =============================================================================
// ROLLBACK HELPERS
// =============================================================================

/// For a resting Kalshi limit order with 0 fills: poll every 500ms for up to
/// `timeout`, then cancel and return the final order state if still unfilled.
/// For IOC orders or already-executed/canceled orders, returns immediately.
async fn maybe_poll_kalshi_limit(
    kalshi: &KalshiApiClient,
    kalshi_res: Result<KalshiOrderResponse>,
    timeout: Duration,
) -> Result<KalshiOrderResponse> {
    use crate::config::KalshiOrderType;
    if crate::config::kalshi_order_type() != KalshiOrderType::Limit {
        return kalshi_res;
    }
    let resp = match kalshi_res {
        Ok(r) => r,
        Err(e) => return Err(e),
    };
    // Nothing to poll if already settled
    if resp.order.filled_count() > 0
        || resp.order.status == "executed"
        || resp.order.status == "canceled"
        || resp.order.order_id.is_empty()
    {
        return Ok(resp);
    }

    let order_id = resp.order.order_id.clone();
    let poll_interval = Duration::from_millis(500);
    let deadline = tokio::time::Instant::now() + timeout;
    let mut current = resp;

    info!("[EXEC] Kalshi limit order {} resting, polling for up to {}s",
        order_id, timeout.as_secs());

    loop {
        tokio::time::sleep(poll_interval).await;
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        match kalshi.get_order(&order_id).await {
            Ok(updated) => {
                let filled = updated.order.filled_count();
                if filled > 0 || updated.order.status == "executed" || updated.order.status == "canceled" {
                    info!("[EXEC] Kalshi limit order {} filled={} status={}",
                        order_id, filled, updated.order.status);
                    return Ok(updated);
                }
                current = updated;
            }
            Err(e) => warn!("[EXEC] Failed to poll Kalshi order {}: {}", order_id, e),
        }
    }

    // Timed out — cancel the resting order
    warn!("[EXEC] Kalshi limit order {} still pending after {}s, canceling",
        order_id, timeout.as_secs());
    match kalshi.cancel_order(&order_id).await {
        Ok(canceled) => {
            info!("[EXEC] Canceled Kalshi order {}: filled={}", order_id, canceled.order.filled_count());
            Ok(canceled)
        }
        Err(e) => {
            warn!("[EXEC] Failed to cancel Kalshi limit order {}: {}", order_id, e);
            Ok(current)
        }
    }
}

/// Unwind a one-sided Kalshi fill by selling back with IOC at a slight discount.
async fn unwind_kalshi_fill(
    kalshi: Arc<KalshiApiClient>,
    ticker: String,
    side: &'static str,
    original_price_cents: i64,
    count: i64,
) {
    let sell_price = (original_price_cents - 10).max(1);
    info!("[EXEC] Kalshi unwind: {} {} @{}¢ x{}", side, ticker, sell_price, count);
    match kalshi.sell_ioc(&ticker, side, sell_price, count).await {
        Ok(resp) => {
            let filled = resp.order.filled_count();
            let proceeds = resp.order.taker_fill_cost.unwrap_or(0) + resp.order.maker_fill_cost.unwrap_or(0);
            if filled > 0 {
                info!("[EXEC] ✅ Kalshi unwind: sold={} proceeds={}¢", filled, proceeds);
            } else {
                error!("[EXEC] Kalshi unwind: 0 filled (market moved or no inventory)");
            }
        }
        Err(e) => error!("[EXEC] Kalshi unwind sell_ioc failed: {}", e),
    }
}

/// Unwind a one-sided Poly US fill: try cancel first (GTC may not have filled),
/// then place a sell-back limit order if cancel fails.
async fn unwind_poly_us_fill(
    poly_us: Arc<PolymarketUsClient>,
    order_id: String,
    slug: String,
    side: &'static str,
    price_cents: u16,
    count: i64,
) {
    info!("[EXEC] Poly US unwind: cancel order {}", order_id);
    match poly_us.cancel_order(&order_id).await {
        Ok(_) => {
            info!("[EXEC] ✅ Poly US order {} canceled", order_id);
            return;
        }
        Err(e) => warn!("[EXEC] Poly US cancel failed (may have filled): {}", e),
    }
    // Cancel failed — order may have executed; sell back at slight discount
    let sell_price = (price_cents as i16 - 10).max(1) as u16;
    let order_req = match side {
        "yes" => LimitOrderRequest::sell_yes(&slug, sell_price as f64 / 100.0, count as u64),
        _     => LimitOrderRequest::sell_no(&slug, sell_price as f64 / 100.0, count as u64),
    };
    info!("[EXEC] Poly US unwind sell: {} @{}¢ x{}", side, sell_price, count);
    match poly_us.place_limit_order(&order_req).await {
        Ok(resp) => info!("[EXEC] ✅ Poly US unwind sell placed: id={:?}", resp.id),
        Err(e)   => error!("[EXEC] Poly US unwind sell failed: {}", e),
    }
}

/// Unwind a one-sided Poly CLOB fill by selling back with FAK at a slight discount.
async fn unwind_poly_clob_fill(
    poly_clob: Arc<SharedAsyncClient>,
    token: String,
    original_price_cents: u16,
    count: i64,
) {
    let sell_price_cents = (original_price_cents as i16 - 10).max(1) as u16;
    let sell_price = cents_to_price(sell_price_cents);
    info!("[EXEC] Poly CLOB unwind: sell @{} x{}", sell_price, count);
    match poly_clob.sell_fak(&token, sell_price, count as f64).await {
        Ok(fill) => {
            let filled = fill.filled_size as i64;
            let proceeds = (fill.fill_cost * 100.0) as i64;
            if filled > 0 {
                info!("[EXEC] ✅ Poly CLOB unwind: sold={} proceeds={}¢", filled, proceeds);
            } else {
                error!("[EXEC] Poly CLOB unwind: 0 filled (market moved)");
            }
        }
        Err(e) => error!("[EXEC] Poly CLOB unwind sell_fak failed: {}", e),
    }
}

// =============================================================================
// POLYMARKET US LEG HELPER
// =============================================================================

/// Place a Polymarket US limit order for one leg of a cross-platform arb.
///
/// Returns `(filled, cost_cents, order_id)`.
///
/// Because the US API uses GTC limit orders (not FAK), we optimistically assume
/// the full quantity fills at the requested price when the order is accepted.
/// This is reasonable when placing at or better than the current best ask.
async fn execute_poly_leg_us(
    poly_us: &PolymarketUsClient,
    slug: &str,
    arb_type: ArbType,
    price_cents: u16,
    contracts: i64,
) -> (i64, i64, String) {
    let price_usd = price_cents as f64 / 100.0;
    let order_req = match arb_type {
        ArbType::PolyYesKalshiNo => LimitOrderRequest::buy_yes(slug, price_usd, contracts as u64),
        ArbType::KalshiYesPolyNo => LimitOrderRequest::buy_no(slug, price_usd, contracts as u64),
    };
    match poly_us.place_limit_order(&order_req).await {
        Ok(resp) => {
            let order_id = resp.id.unwrap_or_default();
            // TODO(CRITICAL): GTC orders are NOT immediately filled. This optimistically
            // reports full fill, which can cause incorrect position tracking and prevent
            // one-sided fill rollback from triggering. Need to implement fill polling
            // or switch to FOK order type on Polymarket US.
            let cost_cents = (price_usd * contracts as f64 * 100.0).round() as i64;
            warn!("[EXEC-US] PolyUS GTC order placed (optimistic fill assumed): id={} status={:?} contracts={}", order_id, resp.status, contracts);
            (contracts, cost_cents, order_id)
        }
        Err(e) => {
            warn!("[EXEC-US] PolyUS order failed: {}", e);
            (0, 0, String::new())
        }
    }
}

// =============================================================================
// EXECUTION RESULT / CHANNEL
// =============================================================================

/// Execution result
#[derive(Debug, Clone, Copy)]
pub struct ExecutionResult {
    pub market_id: u16,
    pub success: bool,
    pub profit_cents: i64,
    pub latency_ns: u64,
    pub error: Option<&'static str>,
}

/// Create execution channel
pub fn create_execution_channel() -> (mpsc::Sender<FastExecutionRequest>, mpsc::Receiver<FastExecutionRequest>) {
    mpsc::channel(256)
}

// =============================================================================
// TRADE LOG
// =============================================================================

#[derive(Serialize)]
struct TradeLogEntry {
    timestamp: String,
    pair_id: String,
    description: String,
    arb_type: String,
    contracts: i64,
    kalshi_price: u16,
    poly_price: u16,
    kalshi_filled: i64,
    poly_filled: i64,
    total_cost_cents: i64,
    expected_profit_cents: i64,
    status: String,
}

/// Append a single JSON line to state/trades.jsonl (async, non-blocking).
async fn write_trade_log(entry: &TradeLogEntry) {
    use tokio::io::AsyncWriteExt;
    match serde_json::to_string(entry) {
        Ok(json) => {
            match tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open("state/trades.jsonl")
                .await
            {
                Ok(mut f) => {
                    let line = format!("{}\n", json);
                    if let Err(e) = f.write_all(line.as_bytes()).await {
                        warn!("[EXEC] Failed to write trade log: {}", e);
                    }
                }
                Err(e) => warn!("[EXEC] Failed to open trade log: {}", e),
            }
        }
        Err(e) => warn!("[EXEC] Failed to serialize trade log entry: {}", e),
    }
}

/// Execution loop
pub async fn run_execution_loop(
    mut rx: mpsc::Receiver<FastExecutionRequest>,
    engine: Arc<ExecutionEngine>,
) {
    info!("[EXEC] Execution engine started (dry_run={})", engine.dry_run);

    while let Some(req) = rx.recv().await {
        let engine = engine.clone();

        // Process immediately in spawned task
        tokio::spawn(async move {
            match engine.process(req).await {
                Ok(result) if result.success => {
                    info!(
                        "[EXEC] ✅ market_id={} profit={}¢ latency={}µs",
                        result.market_id, result.profit_cents, result.latency_ns / 1000
                    );
                }
                Ok(result) => {
                    if result.error != Some("Already in-flight") {
                        warn!(
                            "[EXEC] ⚠️ market_id={}: {:?}",
                            result.market_id, result.error
                        );
                    }
                }
                Err(e) => {
                    error!("[EXEC] ❌ Error: {}", e);
                }
            }
        });
    }

    info!("[EXEC] Execution engine stopped");
}