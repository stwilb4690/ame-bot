// src/discovery.rs
// Market discovery - matches Kalshi events to Polymarket markets

use anyhow::Result;
use futures_util::{stream, StreamExt};
use governor::{Quota, RateLimiter, state::NotKeyed, clock::DefaultClock, middleware::NoOpMiddleware};
use serde::{Serialize, Deserialize};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::cache::TeamCache;
use crate::config::{LeagueConfig, get_league_configs, get_league_config};
use crate::kalshi::KalshiApiClient;
use crate::polymarket::GammaClient;
use crate::polymarket_us::{PolymarketUsClient, PolyUsMarket};
use crate::types::{MarketCategory, MarketPair, MarketType, DiscoveryResult, KalshiMarket, KalshiEvent};

/// A Kalshi-only market (no Polymarket pair) discovered from an extra series.
#[derive(Debug, Clone)]
pub struct KalshiOnlyMarket {
    pub ticker: String,
    pub description: String,
    pub series: String,
}

/// Max concurrent Gamma API requests
const GAMMA_CONCURRENCY: usize = 20;

/// Kalshi rate limit: 2 requests per second (very conservative - they rate limit aggressively)
/// Must be conservative because discovery runs many leagues/series in parallel
const KALSHI_RATE_LIMIT_PER_SEC: u32 = 2;

/// Max concurrent Kalshi API requests GLOBALLY across all leagues/series
/// This is the hard cap - prevents bursting even when rate limiter has tokens
const KALSHI_GLOBAL_CONCURRENCY: usize = 1;

/// Cache file path
const DISCOVERY_CACHE_PATH: &str = ".discovery_cache.json";

/// Cache TTL in seconds (2 hours - new markets appear every ~2 hours)
const CACHE_TTL_SECS: u64 = 2 * 60 * 60;

/// Task for parallel Gamma lookup
struct GammaLookupTask {
    event: Arc<KalshiEvent>,
    market: KalshiMarket,
    /// Guessed slug sent to the Gamma API for lookup (may differ from Poly US slug).
    poly_slug: String,
    /// Poly-side team name for home/first team (used for Poly US market matching).
    poly_team1: String,
    /// Poly-side team name for away/second team (used for Poly US market matching).
    poly_team2: String,
    /// ISO-8601 date derived from the Kalshi event ticker (e.g. "2026-02-22").
    /// Used to filter Poly US markets by date when matching.
    date_iso: String,
    market_type: MarketType,
    league: String,
}

/// Type alias for Kalshi rate limiter
type KalshiRateLimiter = RateLimiter<NotKeyed, governor::state::InMemoryState, DefaultClock, NoOpMiddleware>;

/// Persistent cache for discovered market pairs
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DiscoveryCache {
    /// Unix timestamp when cache was created
    timestamp_secs: u64,
    /// Cached market pairs
    pairs: Vec<MarketPair>,
    /// Set of known Kalshi market tickers (for incremental updates)
    known_kalshi_tickers: Vec<String>,
}

impl DiscoveryCache {
    fn new(pairs: Vec<MarketPair>) -> Self {
        let known_kalshi_tickers: Vec<String> = pairs.iter()
            .map(|p| p.kalshi_market_ticker.to_string())
            .collect();
        Self {
            timestamp_secs: current_unix_secs(),
            pairs,
            known_kalshi_tickers,
        }
    }

    fn is_expired(&self) -> bool {
        let now = current_unix_secs();
        now.saturating_sub(self.timestamp_secs) > CACHE_TTL_SECS
    }

    fn age_secs(&self) -> u64 {
        current_unix_secs().saturating_sub(self.timestamp_secs)
    }

    fn has_ticker(&self, ticker: &str) -> bool {
        self.known_kalshi_tickers.iter().any(|t| t == ticker)
    }
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Default max hours ahead for market expiration filter
const DEFAULT_MAX_EVENT_HOURS: u64 = 72;

/// Market discovery client
pub struct DiscoveryClient {
    kalshi: Arc<KalshiApiClient>,
    gamma: Arc<GammaClient>,
    pub team_cache: Arc<TeamCache>,
    kalshi_limiter: Arc<KalshiRateLimiter>,
    kalshi_semaphore: Arc<Semaphore>,  // Global concurrency limit for Kalshi
    gamma_semaphore: Arc<Semaphore>,
    max_event_hours: u64,
    /// Polymarket US client for slug validation — when present, only pairs whose
    /// slug exists on Poly US are returned, and the `poly_slug` field is set to
    /// the Poly US slug (not the Gamma slug).
    poly_us: Option<Arc<PolymarketUsClient>>,
}

impl DiscoveryClient {
    /// Create a new discovery client.
    ///
    /// `poly_us` — when provided, all discovered pairs are validated against the
    /// Polymarket US market catalog. Only pairs with a matching Poly US market are
    /// returned, and `MarketPair.poly_slug` is set to the Poly US slug.
    pub fn new(
        kalshi: KalshiApiClient,
        team_cache: TeamCache,
        poly_us: Option<Arc<PolymarketUsClient>>,
    ) -> Self {
        // Create token bucket rate limiter for Kalshi
        let quota = Quota::per_second(NonZeroU32::new(KALSHI_RATE_LIMIT_PER_SEC).unwrap());
        let kalshi_limiter = Arc::new(RateLimiter::direct(quota));

        let max_event_hours = std::env::var("MAX_EVENT_HOURS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_MAX_EVENT_HOURS);

        Self {
            kalshi: Arc::new(kalshi),
            gamma: Arc::new(GammaClient::new()),
            team_cache: Arc::new(team_cache),
            kalshi_limiter,
            kalshi_semaphore: Arc::new(Semaphore::new(KALSHI_GLOBAL_CONCURRENCY)),
            gamma_semaphore: Arc::new(Semaphore::new(GAMMA_CONCURRENCY)),
            max_event_hours,
            poly_us,
        }
    }

    /// Load cache from disk (async)
    async fn load_cache() -> Option<DiscoveryCache> {
        let data = tokio::fs::read_to_string(DISCOVERY_CACHE_PATH).await.ok()?;
        serde_json::from_str(&data).ok()
    }

    /// Save cache to disk (async)
    async fn save_cache(cache: &DiscoveryCache) -> Result<()> {
        let data = serde_json::to_string_pretty(cache)?;
        tokio::fs::write(DISCOVERY_CACHE_PATH, data).await?;
        Ok(())
    }
    
    /// Discover all market pairs with caching support
    ///
    /// Strategy:
    /// 1. Try to load cache from disk
    /// 2. If cache exists and is fresh (<2 hours), use it directly
    /// 3. If cache exists but is stale, load it + fetch incremental updates
    /// 4. If no cache, do full discovery
    pub async fn discover_all(&self, leagues: &[&str]) -> DiscoveryResult {
        // Try to load existing cache
        let cached = Self::load_cache().await;

        match cached {
            Some(cache) if !cache.is_expired() => {
                // Cache is fresh - use it directly
                info!("📂 Loaded {} pairs from cache (age: {}s)",
                      cache.pairs.len(), cache.age_secs());
                return DiscoveryResult {
                    pairs: cache.pairs,
                    kalshi_events_found: 0,  // From cache
                    poly_matches: 0,
                    poly_misses: 0,
                    errors: vec![],
                };
            }
            Some(cache) => {
                // Cache is stale - do incremental discovery
                info!("📂 Cache expired (age: {}s), doing incremental refresh...", cache.age_secs());
                return self.discover_incremental(leagues, cache).await;
            }
            None => {
                // No cache - do full discovery
                info!("📂 No cache found, doing full discovery...");
            }
        }

        // Full discovery (no cache)
        let result = self.discover_full(leagues).await;

        // Save to cache
        if !result.pairs.is_empty() {
            let cache = DiscoveryCache::new(result.pairs.clone());
            if let Err(e) = Self::save_cache(&cache).await {
                warn!("Failed to save discovery cache: {}", e);
            } else {
                info!("💾 Saved {} pairs to cache", result.pairs.len());
            }
        }

        result
    }

    /// Fetch all active open Poly US markets into an Arc'd Vec for use during discovery.
    ///
    /// Called once per discovery run so we minimise API calls to Poly US.
    /// Returns an empty Vec (gracefully) if the client is absent or the fetch fails.
    async fn fetch_poly_us_markets(&self) -> Arc<Vec<PolyUsMarket>> {
        let Some(ref us) = self.poly_us else {
            return Arc::new(vec![]);
        };
        match us.get_active_markets(500).await {
            Ok(markets) => {
                info!("[DISCOVERY] {} open Poly US markets available for matching:", markets.len());
                for m in &markets {
                    info!("[DISCOVERY]   slug={} | type={} | question={}",
                        m.slug,
                        m.sports_market_type_v2.as_deref().unwrap_or("?"),
                        m.question.as_deref().unwrap_or("?"));
                }
                Arc::new(markets)
            }
            Err(e) => {
                warn!("[DISCOVERY] Failed to fetch Poly US market catalog: {}", e);
                Arc::new(vec![])
            }
        }
    }

    /// Force full discovery (ignores cache)
    pub async fn discover_all_force(&self, leagues: &[&str]) -> DiscoveryResult {
        info!("🔄 Forced full discovery (ignoring cache)...");
        let result = self.discover_full(leagues).await;

        // Save to cache
        if !result.pairs.is_empty() {
            let cache = DiscoveryCache::new(result.pairs.clone());
            if let Err(e) = Self::save_cache(&cache).await {
                warn!("Failed to save discovery cache: {}", e);
            } else {
                info!("💾 Saved {} pairs to cache", result.pairs.len());
            }
        }

        result
    }

    /// Full discovery without cache
    async fn discover_full(&self, leagues: &[&str]) -> DiscoveryResult {
        let configs: Vec<_> = if leagues.is_empty() {
            get_league_configs()
        } else {
            leagues.iter()
                .filter_map(|l| get_league_config(l))
                .collect()
        };

        // Fetch Poly US market catalog once (used for slug matching across all leagues)
        let poly_us_markets = self.fetch_poly_us_markets().await;

        // Parallel discovery across all leagues
        let league_futures: Vec<_> = configs.iter()
            .map(|config| self.discover_league(config, None, poly_us_markets.clone()))
            .collect();

        let league_results = futures_util::future::join_all(league_futures).await;

        // Merge results
        let mut result = DiscoveryResult::default();
        for league_result in league_results {
            result.pairs.extend(league_result.pairs);
            result.poly_matches += league_result.poly_matches;
            result.errors.extend(league_result.errors);
        }
        result.kalshi_events_found = result.pairs.len();

        result
    }

    /// Incremental discovery - merge cached pairs with newly discovered ones
    async fn discover_incremental(&self, leagues: &[&str], cache: DiscoveryCache) -> DiscoveryResult {
        let configs: Vec<_> = if leagues.is_empty() {
            get_league_configs()
        } else {
            leagues.iter()
                .filter_map(|l| get_league_config(l))
                .collect()
        };

        // Fetch Poly US market catalog once for slug matching across all leagues
        let poly_us_markets = self.fetch_poly_us_markets().await;

        // Discover with filter for known tickers
        let league_futures: Vec<_> = configs.iter()
            .map(|config| self.discover_league(config, Some(&cache), poly_us_markets.clone()))
            .collect();

        let league_results = futures_util::future::join_all(league_futures).await;

        // Merge cached pairs with newly discovered ones
        let mut all_pairs = cache.pairs;
        let mut new_count = 0;
        let mut errors = Vec::new();

        for league_result in league_results {
            errors.extend(league_result.errors);
            for pair in league_result.pairs {
                if !all_pairs.iter().any(|p| *p.kalshi_market_ticker == *pair.kalshi_market_ticker) {
                    all_pairs.push(pair);
                    new_count += 1;
                }
            }
        }

        if new_count > 0 {
            info!("🆕 Found {} new market pairs", new_count);

            // Update cache
            let new_cache = DiscoveryCache::new(all_pairs.clone());
            if let Err(e) = Self::save_cache(&new_cache).await {
                warn!("Failed to update discovery cache: {}", e);
            } else {
                info!("💾 Updated cache with {} total pairs", all_pairs.len());
            }
        } else {
            info!("✅ No new markets found, using {} cached pairs", all_pairs.len());

            // Just update timestamp to extend TTL
            let refreshed_cache = DiscoveryCache::new(all_pairs.clone());
            let _ = Self::save_cache(&refreshed_cache).await;
        }

        DiscoveryResult {
            pairs: all_pairs,
            kalshi_events_found: new_count,
            poly_matches: new_count,
            poly_misses: 0,
            errors,
        }
    }

    /// Discover all market types for a single league (PARALLEL)
    /// If cache is provided, only discovers markets not already in cache
    async fn discover_league(
        &self,
        config: &LeagueConfig,
        cache: Option<&DiscoveryCache>,
        poly_us_markets: Arc<Vec<PolyUsMarket>>,
    ) -> DiscoveryResult {
        info!("🔍 Discovering {} markets...", config.league_code);

        // When Poly US is configured and has markets, only moneyline type exists on that platform.
        // Skip spread/total/btts discovery to avoid wasting Kalshi API quota.
        let poly_us_active = self.poly_us.is_some() && !poly_us_markets.is_empty();
        let candidate_types: Vec<MarketType> = if poly_us_active {
            vec![MarketType::Moneyline]
        } else {
            vec![MarketType::Moneyline, MarketType::Spread, MarketType::Total, MarketType::Btts]
        };

        // Build (type, future) pairs — skipping types that have no configured series.
        // Keep the type alongside the future so the zip below is always correct.
        let type_work: Vec<(MarketType, _)> = candidate_types.iter()
            .filter_map(|&market_type| {
                let series = self.get_series_for_type(config, market_type)?;
                Some((market_type, self.discover_series(config, series, market_type, cache, poly_us_markets.clone())))
            })
            .collect();

        let market_type_order: Vec<MarketType> = type_work.iter().map(|(t, _)| *t).collect();
        let futures: Vec<_> = type_work.into_iter().map(|(_, f)| f).collect();

        let results = futures_util::future::join_all(futures).await;

        let mut result = DiscoveryResult::default();
        for (pairs_result, market_type) in results.into_iter().zip(market_type_order.iter()) {
            match pairs_result {
                Ok(pairs) => {
                    let count = pairs.len();
                    if count > 0 {
                        info!("  ✅ {} {}: {} pairs", config.league_code, market_type, count);
                    }
                    result.poly_matches += count;
                    result.pairs.extend(pairs);
                }
                Err(e) => {
                    result.errors.push(format!("{} {}: {}", config.league_code, market_type, e));
                }
            }
        }

        result
    }
    
    fn get_series_for_type(&self, config: &LeagueConfig, market_type: MarketType) -> Option<&'static str> {
        match market_type {
            MarketType::Moneyline => Some(config.kalshi_series_game),
            MarketType::Spread => config.kalshi_series_spread,
            MarketType::Total => config.kalshi_series_total,
            MarketType::Btts => config.kalshi_series_btts,
        }
    }
    
    /// Discover markets for a specific series (PARALLEL Kalshi + Gamma lookups)
    /// If cache is provided, skips markets already in cache
    async fn discover_series(
        &self,
        config: &LeagueConfig,
        series: &str,
        market_type: MarketType,
        cache: Option<&DiscoveryCache>,
        poly_us_markets: Arc<Vec<PolyUsMarket>>,
    ) -> Result<Vec<MarketPair>> {
        // Fetch Kalshi events
        {
            let _permit = self.kalshi_semaphore.acquire().await.map_err(|e| anyhow::anyhow!("semaphore closed: {}", e))?;
            self.kalshi_limiter.until_ready().await;
        }
        let events = self.kalshi.get_events(series, 50).await?;

        // PHASE 2: Parallel market fetching 
        let kalshi = self.kalshi.clone();
        let limiter = self.kalshi_limiter.clone();
        let semaphore = self.kalshi_semaphore.clone();

        // Parse events first, filtering out unparseable ones
        let parsed_events: Vec<_> = events.into_iter()
            .filter_map(|event| {
                let parsed = match parse_kalshi_event_ticker(&event.event_ticker) {
                    Some(p) => p,
                    None => {
                        warn!("  ⚠️ Could not parse event ticker {}", event.event_ticker);
                        return None;
                    }
                };
                Some((parsed, event))
            })
            .collect();

        // Execute market fetches with GLOBAL concurrency limit
        let market_results: Vec<_> = stream::iter(parsed_events)
            .map(|(parsed, event)| {
                let kalshi = kalshi.clone();
                let limiter = limiter.clone();
                let semaphore = semaphore.clone();
                let event_ticker = event.event_ticker.clone();
                async move {
                    let _permit = semaphore.acquire().await.ok();
                    // rate limit
                    limiter.until_ready().await;
                    let markets_result = kalshi.get_markets(&event_ticker).await;
                    (parsed, Arc::new(event), markets_result)
                }
            })
            .buffer_unordered(KALSHI_GLOBAL_CONCURRENCY * 2)  // Allow some buffering, semaphore is the real limit
            .collect()
            .await;

        // Collect all (event, market) pairs
        let now_secs = current_unix_secs() as i64;
        let max_window_secs = self.max_event_hours as i64 * 3600;
        let mut event_markets = Vec::with_capacity(market_results.len() * 3);
        let mut filtered_count: usize = 0;
        for (parsed, event, markets_result) in market_results {
            match markets_result {
                Ok(markets) => {
                    for market in markets {
                        // Filter out markets expiring more than MAX_EVENT_HOURS from now
                        if let Some(exp_ts) = market.expiration_ts {
                            if exp_ts - now_secs > max_window_secs {
                                filtered_count += 1;
                                continue;
                            }
                        }
                        // Skip if already in cache
                        if let Some(c) = cache {
                            if c.has_ticker(&market.ticker) {
                                continue;
                            }
                        }
                        event_markets.push((parsed.clone(), event.clone(), market));
                    }
                }
                Err(e) => {
                    warn!("  ⚠️ Failed to get markets for {}: {}", event.event_ticker, e);
                }
            }
        }
        if filtered_count > 0 {
            info!("  🕐 Filtered {} markets expiring beyond {}h window", filtered_count, self.max_event_hours);
        }
        
        // Parallel Gamma lookups with semaphore
        let lookup_futures: Vec<_> = event_markets
            .into_iter()
            .map(|(parsed, event, market)| {
                let poly_slug = self.build_poly_slug(config.poly_prefix, &parsed, market_type, &market);
                let (poly_team1, poly_team2) = self.poly_teams(config.poly_prefix, &parsed);
                let date_iso = kalshi_date_to_iso(&parsed.date);

                GammaLookupTask {
                    event,
                    market,
                    poly_slug,
                    poly_team1,
                    poly_team2,
                    date_iso,
                    market_type,
                    league: config.league_code.to_string(),
                }
            })
            .collect();
        
        // Whether Poly US validation is active for this discovery run
        let poly_us_active = self.poly_us.is_some();

        // Execute lookups in parallel
        let pairs: Vec<MarketPair> = stream::iter(lookup_futures)
            .map(|task| {
                let gamma = self.gamma.clone();
                let semaphore = self.gamma_semaphore.clone();
                let poly_markets = poly_us_markets.clone();
                async move {
                    let _permit = semaphore.acquire().await.ok()?;
                    match gamma.lookup_market(&task.poly_slug).await {
                        Ok(Some((yes_token, no_token, gamma_slug))) => {
                            // Determine the slug to use for Polymarket execution:
                            // - When Poly US client is configured and the market catalog was
                            //   successfully fetched, find a matching Poly US market by team
                            //   names and date. If no match is found, skip this pair entirely.
                            // - When Poly US client is absent OR the catalog fetch failed
                            //   (empty slice), fall back to the Gamma slug (old behaviour).
                            let poly_slug = if poly_us_active && !poly_markets.is_empty() {
                                match find_poly_us_match(
                                    &poly_markets,
                                    &task.poly_team1,
                                    &task.poly_team2,
                                    &task.date_iso,
                                ) {
                                    Some(slug) => slug,
                                    None => {
                                        // No Poly US market found — skip this pair
                                        warn!("[DISCOVERY] No Poly US match: '{}' vs '{}' on {} (Kalshi: {})",
                                            task.poly_team1, task.poly_team2,
                                            task.date_iso, task.market.ticker);
                                        return None;
                                    }
                                }
                            } else {
                                gamma_slug.clone()
                            };

                            let team_suffix = extract_team_suffix(&task.market.ticker);
                            Some(MarketPair {
                                pair_id: format!("{}-{}", poly_slug, task.market.ticker).into(),
                                league: task.league.into(),
                                market_type: task.market_type,
                                description: format!("{} - {}", task.event.title, task.market.title).into(),
                                kalshi_event_ticker: task.event.event_ticker.clone().into(),
                                kalshi_market_ticker: task.market.ticker.into(),
                                // poly_slug is the Poly US slug (for order placement).
                                // poly_yes/no_token remain the Gamma CLOB token IDs
                                // (used for the international Polymarket WebSocket price feed).
                                poly_slug: poly_slug.into(),
                                poly_yes_token: yes_token.into(),
                                poly_no_token: no_token.into(),
                                line_value: task.market.floor_strike,
                                team_suffix: team_suffix.map(|s| s.into()),
                                category: MarketCategory::Sports,
                                expiry_ts: None,
                            })
                        }
                        Ok(None) => None,
                        Err(e) => {
                            warn!("  ⚠️ Gamma lookup failed for {}: {}", task.poly_slug, e);
                            None
                        }
                    }
                }
            })
            .buffer_unordered(GAMMA_CONCURRENCY)
            .filter_map(|x| async { x })
            .collect()
            .await;
        
        Ok(pairs)
    }
}

// ── Crypto / Economic Discovery ───────────────────────────────────────────────

/// Cache file for crypto/economic pairs (separate from sports cache)
const CRYPTO_ECO_CACHE_PATH: &str = ".crypto_eco_cache.json";

/// Cache TTL for crypto/economic pairs — 1 hour (markets update more frequently)
const CRYPTO_ECO_CACHE_TTL_SECS: u64 = 60 * 60;

/// A single crypto or economic Kalshi series and its Gamma search configuration.
struct CryptoEcoSeries {
    /// Kalshi series prefix (e.g. "KXBTC")
    series: &'static str,
    /// Market category
    category: MarketCategory,
    /// Gamma tag to search (e.g. "Bitcoin")
    gamma_tag: &'static str,
    /// For display / identification
    asset_label: &'static str,
}

const CRYPTO_ECO_SERIES: &[CryptoEcoSeries] = &[
    CryptoEcoSeries { series: "KXBTC",  category: MarketCategory::Crypto,   gamma_tag: "Bitcoin",       asset_label: "BTC"  },
    CryptoEcoSeries { series: "KXETH",  category: MarketCategory::Crypto,   gamma_tag: "Ethereum",      asset_label: "ETH"  },
    CryptoEcoSeries { series: "KXFED",  category: MarketCategory::Economic, gamma_tag: "Fed",           asset_label: "FED"  },
    CryptoEcoSeries { series: "KXCPI",  category: MarketCategory::Economic, gamma_tag: "CPI",           asset_label: "CPI"  },
    CryptoEcoSeries { series: "KXJOBS", category: MarketCategory::Economic, gamma_tag: "Jobs",          asset_label: "JOBS" },
    CryptoEcoSeries { series: "KXGDP",  category: MarketCategory::Economic, gamma_tag: "GDP",           asset_label: "GDP"  },
];

impl DiscoveryClient {
    /// Discover crypto and economic market pairs (Kalshi ↔ Polymarket Gamma).
    ///
    /// Strategy:
    /// 1. Load cache (1-hour TTL) if fresh
    /// 2. Otherwise: for each configured series, fetch Kalshi markets and
    ///    match against Gamma markets by threshold value + expiry date
    /// 3. Save results to `.crypto_eco_cache.json`
    ///
    /// Returns a `DiscoveryResult` with `category` set on each `MarketPair`.
    /// Unmatched Kalshi markets are silently skipped (logged at trace level).
    pub async fn discover_crypto_economic(&self) -> DiscoveryResult {
        // Try to load from cache
        if let Some(cached) = Self::load_crypto_eco_cache().await {
            if !cached.is_expired() {
                info!("[CRYPTO/ECO] 📂 {} pairs from cache (age: {}s)",
                      cached.pairs.len(), cached.age_secs());
                return DiscoveryResult {
                    pairs: cached.pairs,
                    kalshi_events_found: 0,
                    poly_matches: 0,
                    poly_misses: 0,
                    errors: vec![],
                };
            }
        }

        info!("[CRYPTO/ECO] 🔍 Discovering crypto & economic market pairs...");

        // Pre-fetch Gamma markets for each series (one API call per asset tag)
        let mut all_pairs = Vec::new();
        let mut errors = Vec::new();

        for series_cfg in CRYPTO_ECO_SERIES {
            match self.discover_one_crypto_eco_series(series_cfg).await {
                Ok(pairs) => {
                    if !pairs.is_empty() {
                        info!("[CRYPTO/ECO]   ✅ {}: {} pairs", series_cfg.asset_label, pairs.len());
                    }
                    all_pairs.extend(pairs);
                }
                Err(e) => {
                    let msg = format!("{}: {}", series_cfg.asset_label, e);
                    warn!("[CRYPTO/ECO]   ⚠️ {}", msg);
                    errors.push(msg);
                }
            }
        }

        info!("[CRYPTO/ECO] Total: {} pairs discovered", all_pairs.len());

        // Save to cache
        if !all_pairs.is_empty() {
            let cache = CryptoEcoCache {
                timestamp_secs: current_unix_secs(),
                pairs: all_pairs.clone(),
            };
            if let Ok(json) = serde_json::to_string_pretty(&cache) {
                let _ = tokio::fs::write(CRYPTO_ECO_CACHE_PATH, json).await;
            }
        }

        DiscoveryResult {
            pairs: all_pairs,
            kalshi_events_found: 0,
            poly_matches: 0,
            poly_misses: 0,
            errors,
        }
    }

    async fn discover_one_crypto_eco_series(
        &self,
        cfg: &CryptoEcoSeries,
    ) -> Result<Vec<MarketPair>> {
        // 1. Fetch Gamma markets for this asset tag
        let gamma_markets = self.gamma.search_by_tag(cfg.gamma_tag).await?;
        if gamma_markets.is_empty() {
            // Tag search returned nothing — Gamma API may not support this tag yet
            return Ok(vec![]);
        }

        // 2. Fetch Kalshi events for this series
        {
            let _permit = self.kalshi_semaphore.acquire().await.map_err(|e| anyhow::anyhow!("{}", e))?;
            self.kalshi_limiter.until_ready().await;
        }
        let events = self.kalshi.get_events(cfg.series, 100).await?;
        if events.is_empty() {
            return Ok(vec![]);
        }

        let mut pairs = Vec::new();

        for event in &events {
            // Rate-limit market fetches
            {
                let _permit = self.kalshi_semaphore.acquire().await.map_err(|e| anyhow::anyhow!("{}", e))?;
                self.kalshi_limiter.until_ready().await;
            }
            let markets = match self.kalshi.get_markets(&event.event_ticker).await {
                Ok(m) => m,
                Err(e) => {
                    warn!("[CRYPTO/ECO] Failed to get markets for {}: {}", event.event_ticker, e);
                    continue;
                }
            };

            for market in &markets {
                // Extract numeric threshold from market ticker (e.g. KXBTC-26FEB22-B100000 → 100000)
                let threshold = extract_threshold_from_ticker(&market.ticker);

                // Parse event date from the event ticker
                let event_date = parse_crypto_eco_event_date(&event.event_ticker);

                // Match against Gamma markets
                if let Some((yes_token, no_token, gamma_slug)) =
                    match_gamma_market(&gamma_markets, threshold, event_date.as_deref())
                {
                    let pair_id = format!("{}-{}", cfg.series, market.ticker);
                    let description = format!("{} — {}", event.title, market.title);
                    pairs.push(MarketPair {
                        pair_id:              pair_id.into(),
                        league:               cfg.series.into(),
                        market_type:          MarketType::Total, // Binary threshold market
                        description:          description.into(),
                        kalshi_event_ticker:  event.event_ticker.clone().into(),
                        kalshi_market_ticker: market.ticker.clone().into(),
                        poly_slug:            gamma_slug.clone().into(),
                        poly_yes_token:       yes_token.into(),
                        poly_no_token:        no_token.into(),
                        line_value:           threshold.map(|t| t as f64),
                        team_suffix:          None,
                        category:             cfg.category,
                        expiry_ts:            None,
                    });
                }
            }
        }

        Ok(pairs)
    }

    async fn load_crypto_eco_cache() -> Option<CryptoEcoCache> {
        let data = tokio::fs::read_to_string(CRYPTO_ECO_CACHE_PATH).await.ok()?;
        serde_json::from_str(&data).ok()
    }
}

/// Persistent cache for crypto/economic market pairs.
#[derive(Debug, Serialize, Deserialize)]
struct CryptoEcoCache {
    timestamp_secs: u64,
    pairs: Vec<MarketPair>,
}

impl CryptoEcoCache {
    fn is_expired(&self) -> bool {
        current_unix_secs().saturating_sub(self.timestamp_secs) > CRYPTO_ECO_CACHE_TTL_SECS
    }
    fn age_secs(&self) -> u64 {
        current_unix_secs().saturating_sub(self.timestamp_secs)
    }
}

/// Extract numeric threshold from a Kalshi market ticker.
/// e.g. "KXBTC-26FEB22-B100000" → Some(100000)
///      "KXFED-26MAR19-B450"    → Some(450)
///      "KXBTC-26FEB22"         → None
fn extract_threshold_from_ticker(ticker: &str) -> Option<u64> {
    let last = ticker.split('-').last()?;
    if last.starts_with('B') || last.starts_with('b') {
        last[1..].parse().ok()
    } else {
        None
    }
}

/// Parse event date from a crypto/economic Kalshi event ticker.
/// Crypto: "KXBTC-26FEB22" → Some("2026-02-22")
/// Economic: "KXFED-26MAR19" → Some("2026-03-19")
fn parse_crypto_eco_event_date(event_ticker: &str) -> Option<String> {
    let date_part = event_ticker.split('-').nth(1)?;
    if date_part.len() < 7 {
        return None;
    }
    Some(kalshi_date_to_iso(&date_part[..7]))
}

/// Try to match a Kalshi crypto/economic market to a Gamma market.
///
/// Matching criteria:
/// 1. Gamma market question/title contains the threshold value (exact or formatted)
/// 2. Gamma market end_date is within ±2 days of the Kalshi event date
///
/// Returns `Some((yes_token, no_token, slug))` for the best match, or `None`.
fn match_gamma_market(
    gamma_markets: &[crate::polymarket::GammaSearchResult],
    threshold: Option<u64>,
    event_date_iso: Option<&str>,
) -> Option<(String, String, String)> {
    let event_date = event_date_iso
        .and_then(|d| chrono::NaiveDate::parse_from_str(d, "%Y-%m-%d").ok());

    for gm in gamma_markets {
        // Filter: must have a slug and valid CLOB token IDs
        let slug = gm.slug.as_deref().unwrap_or("");
        if slug.is_empty() { continue; }

        let token_ids: Vec<String> = gm.clob_token_ids
            .as_ref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        if token_ids.len() < 2 { continue; }

        // Check date proximity (within ±2 days)
        if let (Some(ed), Some(end_date_str)) = (event_date, gm.end_date.as_deref()) {
            // Gamma end_date can be "2026-02-22T20:00:00Z" or "2026-02-22"
            let gamma_date_str = &end_date_str[..end_date_str.len().min(10)];
            if let Ok(gd) = chrono::NaiveDate::parse_from_str(gamma_date_str, "%Y-%m-%d") {
                let diff = (gd - ed).num_days().unsigned_abs();
                if diff > 2 { continue; }
            }
        }

        // Check threshold: Gamma question must contain the threshold value in some form
        if let Some(t) = threshold {
            let q = gm.question.as_deref().unwrap_or("").to_lowercase();
            let slug_lower = slug.to_lowercase();
            let combined = format!("{} {}", q, slug_lower);

            // Build threshold variants: 100000, 100,000, 100k
            let t_plain = t.to_string();
            let t_comma = format_with_commas(t);
            let t_k = if t >= 1_000 && t % 1_000 == 0 {
                Some(format!("{}k", t / 1_000))
            } else {
                None
            };
            // For economic basis-point thresholds (e.g. 450 bps = 4.50%)
            let t_pct = if t < 10_000 {
                Some(format!("{:.2}", t as f64 / 100.0))
            } else {
                None
            };

            let matched = combined.contains(&t_plain)
                || combined.contains(&t_comma)
                || t_k.as_deref().map(|k| combined.contains(k)).unwrap_or(false)
                || t_pct.as_deref().map(|p| combined.contains(p)).unwrap_or(false);

            if !matched { continue; }
        }

        return Some((
            token_ids[0].clone(),
            token_ids[1].clone(),
            slug.to_string(),
        ));
    }

    None
}

/// Format a number with comma separators (e.g. 100000 → "100,000").
fn format_with_commas(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

impl DiscoveryClient {
    /// Discover Kalshi-only markets from a list of series tickers.
    ///
    /// These markets have no Polymarket counterpart and are used for:
    ///   - Same-platform arb detection
    ///   - Wide spread / momentum alerts
    ///
    /// Configured via `KALSHI_EXTRA_SERIES=KXFED,KXCPI,KXBTC` (comma-separated).
    pub async fn discover_kalshi_only_series(&self, series: &[&str]) -> Vec<KalshiOnlyMarket> {
        let mut results = Vec::new();

        for &s in series {
            // Rate-limit Kalshi requests
            {
                let _permit = match self.kalshi_semaphore.acquire().await {
                    Ok(p) => p,
                    Err(_) => { continue; }
                };
                self.kalshi_limiter.until_ready().await;
            }

            let events = match self.kalshi.get_events(s, 100).await {
                Ok(e) => e,
                Err(err) => {
                    warn!("[DISCOVERY] Kalshi-only series {}: {}", s, err);
                    continue;
                }
            };

            info!("[DISCOVERY] Kalshi-only '{}': {} events", s, events.len());

            for event in events {
                // Rate-limit market fetches
                {
                    let _permit = match self.kalshi_semaphore.acquire().await {
                        Ok(p) => p,
                        Err(_) => { continue; }
                    };
                    self.kalshi_limiter.until_ready().await;
                }

                let markets = match self.kalshi.get_markets(&event.event_ticker).await {
                    Ok(m) => m,
                    Err(err) => {
                        warn!("[DISCOVERY] Kalshi-only markets for {}: {}", event.event_ticker, err);
                        continue;
                    }
                };

                for market in markets {
                    let description = format!("{} — {}", event.title, market.title);
                    results.push(KalshiOnlyMarket {
                        ticker: market.ticker,
                        description,
                        series: s.to_string(),
                    });
                }
            }
        }

        results
    }

    /// Return the Poly-side team names for a parsed Kalshi ticker.
    ///
    /// Uses the team cache to convert Kalshi codes (e.g. "CHIEFS") to Poly codes
    /// (e.g. "chiefs"), falling back to a lowercase version of the Kalshi code.
    fn poly_teams(&self, poly_prefix: &str, parsed: &ParsedKalshiTicker) -> (String, String) {
        let team1 = self.team_cache
            .kalshi_to_poly(poly_prefix, &parsed.team1)
            .unwrap_or_else(|| parsed.team1.to_lowercase());
        let team2 = self.team_cache
            .kalshi_to_poly(poly_prefix, &parsed.team2)
            .unwrap_or_else(|| parsed.team2.to_lowercase());
        (team1, team2)
    }

    /// Build Polymarket slug from Kalshi event data
    fn build_poly_slug(
        &self,
        poly_prefix: &str,
        parsed: &ParsedKalshiTicker,
        market_type: MarketType,
        market: &KalshiMarket,
    ) -> String {
        // Convert Kalshi team codes to Polymarket codes using cache
        let poly_team1 = self.team_cache
            .kalshi_to_poly(poly_prefix, &parsed.team1)
            .unwrap_or_else(|| parsed.team1.to_lowercase());
        let poly_team2 = self.team_cache
            .kalshi_to_poly(poly_prefix, &parsed.team2)
            .unwrap_or_else(|| parsed.team2.to_lowercase());
        
        // Convert date from "25DEC27" to "2025-12-27"
        let date_str = kalshi_date_to_iso(&parsed.date);
        
        // Base slug: league-team1-team2-date
        let base = format!("{}-{}-{}-{}", poly_prefix, poly_team1, poly_team2, date_str);
        
        match market_type {
            MarketType::Moneyline => {
                if let Some(suffix) = extract_team_suffix(&market.ticker) {
                    if suffix.to_lowercase() == "tie" {
                        format!("{}-draw", base)
                    } else {
                        let poly_suffix = self.team_cache
                            .kalshi_to_poly(poly_prefix, &suffix)
                            .unwrap_or_else(|| suffix.to_lowercase());
                        format!("{}-{}", base, poly_suffix)
                    }
                } else {
                    base
                }
            }
            MarketType::Spread => {
                if let Some(floor) = market.floor_strike {
                    let floor_str = format!("{:.1}", floor).replace(".", "pt");
                    format!("{}-spread-{}", base, floor_str)
                } else {
                    format!("{}-spread", base)
                }
            }
            MarketType::Total => {
                if let Some(floor) = market.floor_strike {
                    let floor_str = format!("{:.1}", floor).replace(".", "pt");
                    format!("{}-total-{}", base, floor_str)
                } else {
                    format!("{}-total", base)
                }
            }
            MarketType::Btts => {
                format!("{}-btts", base)
            }
        }
    }
}

// === Helpers ===

/// Find the best-matching Poly US market for a game between `team1` and `team2` on `date_iso`.
///
/// Matching criteria (all must pass):
/// 1. Market must be open: `closed != true` AND `active != false`
/// 2. Market must be moneyline type (`sportsMarketTypeV2 == "SPORTS_MARKET_TYPE_MONEYLINE"`,
///    or the field is absent for forward-compatibility)
/// 3. Both team names must appear somewhere in the slug or question (case-insensitive)
/// 4. The ISO date (e.g. "2026-02-22") must appear in the slug
///
/// The Poly US slug format is `aec-{sport}-{team1abbrev}-{team2abbrev}-{YYYY-MM-DD}` so a
/// date like `2026-02-22` and abbreviated team codes like `cle`/`okc` will match cleanly.
///
/// Returns the Poly US slug of the first match, or `None` if no match is found.
fn find_poly_us_match(
    markets: &[PolyUsMarket],
    team1: &str,
    team2: &str,
    date_iso: &str,
) -> Option<String> {
    let t1 = team1.to_lowercase();
    let t2 = team2.to_lowercase();

    markets.iter()
        // Must not be closed or inactive
        .filter(|m| m.closed != Some(true))
        .filter(|m| m.active != Some(false))
        // Must be moneyline (only type that exists on Poly US currently)
        .filter(|m| {
            m.sports_market_type_v2.as_deref()
                .map(|t| t == "SPORTS_MARKET_TYPE_MONEYLINE")
                .unwrap_or(true) // allow through if field is absent
        })
        .find(|m| {
            let s = m.slug.to_lowercase();
            let q = m.question.as_deref().unwrap_or("").to_lowercase();

            // Both team names must appear in the slug or the question
            let teams_match = (s.contains(&t1) || q.contains(&t1))
                && (s.contains(&t2) || q.contains(&t2));

            // Date must appear in the slug (e.g. "2026-02-22" in "aec-nba-cle-okc-2026-02-22").
            // Only enforce when we have a well-formed ISO date (length 10, contains dashes).
            let date_match = date_iso.len() == 10 && s.contains(date_iso);

            teams_match && date_match
        })
        .map(|m| m.slug.clone())
}

#[derive(Debug, Clone)]
struct ParsedKalshiTicker {
    date: String,  // "25DEC27"
    team1: String, // "CFC"
    team2: String, // "AVL"
}

/// Parse Kalshi event ticker like "KXEPLGAME-25DEC27CFCAVL" or "KXNCAAFGAME-25DEC27M-OHFRES"
fn parse_kalshi_event_ticker(ticker: &str) -> Option<ParsedKalshiTicker> {
    let parts: Vec<&str> = ticker.split('-').collect();
    if parts.len() < 2 {
        return None;
    }

    // Handle two formats:
    // 1. "KXEPLGAME-25DEC27CFCAVL" - date+teams in parts[1]
    // 2. "KXNCAAFGAME-25DEC27M-OHFRES" - date in parts[1], teams in parts[2]
    let (date, teams_part) = if parts.len() >= 3 && parts[2].len() >= 4 {
        // Format 2: 3-part ticker with separate teams section
        // parts[1] is like "25DEC27M" (date + optional suffix)
        let date_part = parts[1];
        let date = if date_part.len() >= 7 {
            date_part[..7].to_uppercase()
        } else {
            return None;
        };
        (date, parts[2])
    } else {
        // Format 1: 2-part ticker with combined date+teams
        let date_teams = parts[1];
        // Minimum: 7 (date) + 2 + 2 (min team codes) = 11
        if date_teams.len() < 11 {
            return None;
        }
        let date = date_teams[..7].to_uppercase();
        let teams = &date_teams[7..];
        (date, teams)
    };

    // Split team codes - try to find the best split point
    // Team codes range from 2-4 chars (e.g., OM, CFC, FRES)
    let (team1, team2) = split_team_codes(teams_part);

    Some(ParsedKalshiTicker { date, team1, team2 })
}

/// Split a combined team string into two team codes
/// Tries multiple split strategies based on string length
fn split_team_codes(teams: &str) -> (String, String) {
    let len = teams.len();

    // For 6 chars, could be 3+3, 2+4, or 4+2
    // For 5 chars, could be 2+3 or 3+2
    // For 4 chars, must be 2+2
    // For 7 chars, could be 3+4 or 4+3
    // For 8 chars, could be 4+4, 3+5, 5+3

    match len {
        4 => (teams[..2].to_uppercase(), teams[2..].to_uppercase()),
        5 => {
            // Prefer 2+3 (common for OM+ASM, OL+PSG)
            (teams[..2].to_uppercase(), teams[2..].to_uppercase())
        }
        6 => {
            // Check if it looks like 2+4 pattern (e.g., OHFRES = OH+FRES)
            // Common 2-letter codes: OM, OL, OH, SF, LA, NY, KC, TB, etc.
            let first_two = &teams[..2].to_uppercase();
            if is_likely_two_letter_code(first_two) {
                (first_two.clone(), teams[2..].to_uppercase())
            } else {
                // Default to 3+3
                (teams[..3].to_uppercase(), teams[3..].to_uppercase())
            }
        }
        7 => {
            // Could be 3+4 or 4+3 - prefer 3+4
            (teams[..3].to_uppercase(), teams[3..].to_uppercase())
        }
        _ if len >= 8 => {
            // 4+4 or longer
            (teams[..4].to_uppercase(), teams[4..].to_uppercase())
        }
        _ => {
            let mid = len / 2;
            (teams[..mid].to_uppercase(), teams[mid..].to_uppercase())
        }
    }
}

/// Check if a 2-letter code is a known/likely team abbreviation
fn is_likely_two_letter_code(code: &str) -> bool {
    matches!(
        code,
        // European football (Ligue 1, etc.)
        "OM" | "OL" | "FC" |
        // US sports common abbreviations
        "OH" | "SF" | "LA" | "NY" | "KC" | "TB" | "GB" | "NE" | "NO" | "LV" |
        // Generic short codes
        "BC" | "SC" | "AC" | "AS" | "US"
    )
}

/// Convert Kalshi date "25DEC27" to ISO "2025-12-27"
fn kalshi_date_to_iso(kalshi_date: &str) -> String {
    if kalshi_date.len() != 7 {
        return kalshi_date.to_string();
    }
    
    let year = format!("20{}", &kalshi_date[..2]);
    let month = match &kalshi_date[2..5].to_uppercase()[..] {
        "JAN" => "01", "FEB" => "02", "MAR" => "03", "APR" => "04",
        "MAY" => "05", "JUN" => "06", "JUL" => "07", "AUG" => "08",
        "SEP" => "09", "OCT" => "10", "NOV" => "11", "DEC" => "12",
        _ => "01",
    };
    let day = &kalshi_date[5..7];
    
    format!("{}-{}-{}", year, month, day)
}

/// Extract team suffix from market ticker (e.g., "KXEPLGAME-25DEC27CFCAVL-CFC" -> "CFC")
fn extract_team_suffix(ticker: &str) -> Option<String> {
    let mut splits = ticker.splitn(3, '-');
    splits.next()?; // series
    splits.next()?; // event
    splits.next().map(|s| s.to_uppercase())
}

// ── Short-Duration Crypto Discovery ───────────────────────────────────────────

/// Short-duration series to attempt (series_ticker, asset_label, duration_minutes)
const SHORT_DURATION_SERIES: &[(&str, &str, u32)] = &[
    ("KXBTC15M",  "BTC", 15),
    ("KXETH15M",  "ETH", 15),
    ("KXSOL15M",  "SOL", 15),
    ("KXBTCUD",   "BTC", 15),  // "Up/Down" variant
    ("KXETHUD",   "ETH", 15),
    ("KXSOLUD",   "SOL", 15),
    ("KXBTC1H",   "BTC", 60),
    ("KXETH1H",   "ETH", 60),
    ("KXSOL1H",   "SOL", 60),
];

/// Build the list of Polymarket slug patterns to try for a short-duration crypto market.
///
/// Polymarket uses slugs like `btc-updown-15m-{UNIX_TS}` where the timestamp is the
/// **start** of the window, not the end/resolution time.
///
/// Kalshi `close_time` is the **end** of the window (resolution time), so we subtract
/// `duration_mins * 60` to convert to Poly's start-time convention before building slugs.
///
/// Example: KXBTC15M-26FEB231130 → close_time=11:30 (end) → poly_start_ts = 11:30 - 15min = 11:15
fn short_duration_slug_patterns(asset: &str, duration_mins: u32, kalshi_close_ts: i64) -> Vec<String> {
    let long = match asset {
        "BTC" => "bitcoin",
        "ETH" => "ethereum",
        "SOL" => "solana",
        _     => asset,
    };
    let short = asset.to_lowercase();

    // Poly slug timestamp = window start = Kalshi close_time - window duration
    let poly_start_ts = kalshi_close_ts - (duration_mins as i64 * 60);

    if duration_mins <= 15 {
        vec![
            format!("{}-updown-15m-{}", short, poly_start_ts),
            format!("{}-up-or-down-15m-{}", long, poly_start_ts),
            format!("{}-up-or-down-15-minutes-{}", long, poly_start_ts),
            format!("{}-up-down-15m-{}", short, poly_start_ts),
        ]
    } else {
        // hourly
        vec![
            format!("{}-updown-1h-{}", short, poly_start_ts),
            format!("{}-updown-60m-{}", short, poly_start_ts),
            format!("{}-up-or-down-1h-{}", long, poly_start_ts),
            format!("{}-up-or-down-hourly-{}", long, poly_start_ts),
        ]
    }
}

impl DiscoveryClient {
    /// Discover short-duration (15-minute and hourly) crypto markets on Kalshi,
    /// with best-effort matching against Polymarket Gamma.
    ///
    /// This runs at startup and every 10 minutes. Unlike the daily crypto series
    /// (KXBTC, KXETH), these markets expire every 15-60 minutes and resolve on
    /// simple "will price go UP or DOWN?" questions — the highest-frequency arb
    /// opportunity on the platform.
    ///
    /// Discovery is resilient: if a series ticker doesn't exist on Kalshi, the error
    /// is logged as info (not a warning) since this is expected. Whatever is found
    /// is logged clearly so we know exactly what tickers to target.
    pub async fn discover_short_duration_crypto(&self) -> DiscoveryResult {
        info!("[SHORT-CRYPTO] 🔍 Discovering short-duration crypto markets...");
        info!("[SHORT-CRYPTO] Trying series: {}",
              SHORT_DURATION_SERIES.iter().map(|(s, _, _)| *s).collect::<Vec<_>>().join(", "));

        let mut all_pairs   = Vec::new();
        let mut errors      = Vec::new();
        let mut kalshi_only = Vec::new();

        for (series, asset, duration_mins) in SHORT_DURATION_SERIES {
            match self.discover_short_series(series, asset, *duration_mins).await {
                Ok((pairs, kalshi_tickers)) => {
                    let matched = pairs.len();
                    let total   = kalshi_tickers.len();
                    if total > 0 {
                        info!("[SHORT-CRYPTO] {} [{}-{}m]: {} Kalshi markets, {} slug-matched",
                              series, asset, duration_mins, total, matched);
                        for t in &kalshi_tickers {
                            if !pairs.iter().any(|p: &MarketPair| &*p.kalshi_market_ticker == t) {
                                kalshi_only.push(t.clone());
                            }
                        }
                    } else {
                        info!("[SHORT-CRYPTO] {} [{}-{}m]: series not found or empty",
                              series, asset, duration_mins);
                    }
                    all_pairs.extend(pairs);
                }
                Err(e) => {
                    // Expected for series that don't exist on Kalshi
                    info!("[SHORT-CRYPTO] {} [{}-{}m]: {}", series, asset, duration_mins, e);
                    errors.push(format!("{}: {}", series, e));
                }
            }
        }

        if !kalshi_only.is_empty() {
            info!("[SHORT-CRYPTO] {} Kalshi-only tickers (no Poly slug match): {}",
                  kalshi_only.len(),
                  kalshi_only.iter().take(10).cloned().collect::<Vec<_>>().join(", "));
        }

        let btc15 = all_pairs.iter().filter(|p| p.kalshi_market_ticker.contains("BTC") && p.kalshi_market_ticker.contains("15M")).count();
        let eth15 = all_pairs.iter().filter(|p| p.kalshi_market_ticker.contains("ETH") && p.kalshi_market_ticker.contains("15M")).count();
        let other = all_pairs.iter().filter(|p| !p.kalshi_market_ticker.contains("15M")).count();
        info!("[SHORT-CRYPTO] 🔄 Short-duration: {} BTC-15m, {} ETH-15m, {} other matched | {} Kalshi-only (no Poly match)",
              btc15, eth15, other, kalshi_only.len());

        DiscoveryResult {
            pairs: all_pairs,
            kalshi_events_found: 0,
            poly_matches: 0,
            poly_misses: kalshi_only.len(),
            errors,
        }
    }

    /// Try to discover markets for a single short-duration series.
    ///
    /// Instead of searching Gamma by tag, constructs the Polymarket slug directly from
    /// the Kalshi market's close_time (e.g. `btc-updown-15m-1771806600` = close_ts minus 15min)
    /// and fetches it via `GammaClient::lookup_market()`. Tries multiple slug variations in
    /// order and uses the first that returns token IDs.
    ///
    /// Returns `(matched_pairs, all_kalshi_tickers)` so the caller can log
    /// how many Kalshi markets were found vs how many got a Poly match.
    async fn discover_short_series(
        &self,
        series:        &str,
        asset:         &str,
        duration_mins: u32,
    ) -> Result<(Vec<MarketPair>, Vec<String>)> {
        // Rate-limited Kalshi call
        {
            let _permit = self.kalshi_semaphore.acquire().await
                .map_err(|e| anyhow::anyhow!("semaphore: {}", e))?;
            self.kalshi_limiter.until_ready().await;
        }

        let events = self.kalshi.get_events(series, 20).await?;
        if events.is_empty() {
            return Ok((vec![], vec![]));
        }

        info!("[SHORT-CRYPTO]   {} events in series {}", events.len(), series);

        let now_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        let mut pairs       = Vec::new();
        let mut all_tickers = Vec::new();

        for event in &events {
            {
                let _permit = self.kalshi_semaphore.acquire().await
                    .map_err(|e| anyhow::anyhow!("semaphore: {}", e))?;
                self.kalshi_limiter.until_ready().await;
            }
            let markets = match self.kalshi.get_markets(&event.event_ticker).await {
                Ok(m)  => m,
                Err(e) => {
                    warn!("[SHORT-CRYPTO] get_markets {}: {}", event.event_ticker, e);
                    continue;
                }
            };

            info!("[SHORT-CRYPTO]   {} → {} markets | e.g. {:?}",
                  event.event_ticker, markets.len(),
                  markets.first().map(|m| &m.ticker));

            for market in &markets {
                all_tickers.push(market.ticker.clone());

                // Resolve close_time → unix timestamp for slug construction.
                // Prefer the close_time string returned by the REST API (RFC3339).
                // Fall back to expiration_ts if close_time is absent.
                let exp_ts: i64 = if let Some(ct) = market.close_time.as_deref() {
                    match chrono::DateTime::parse_from_rfc3339(ct) {
                        Ok(dt) => dt.timestamp(),
                        Err(e) => {
                            info!("[SHORT-CRYPTO] {} bad close_time {:?}: {}", market.ticker, ct, e);
                            continue;
                        }
                    }
                } else if let Some(ts) = market.expiration_ts {
                    ts
                } else {
                    info!("[SHORT-CRYPTO] {} has no close_time or expiration_ts, skipping", market.ticker);
                    continue;
                };

                // Skip markets closing in less than 5 minutes — not worth tracking
                let secs_remaining = exp_ts - now_ts;
                if secs_remaining < 5 * 60 {
                    info!("[SHORT-CRYPTO] Skipping {} — closes in {}s (< 5min)", market.ticker, secs_remaining);
                    continue;
                }
                info!("[SHORT-CRYPTO] {} closes at ts={} (~{}min remaining)",
                      market.ticker, exp_ts, secs_remaining / 60);

                // Try Polymarket slug patterns in order; use the first that resolves.
                // Poly slug timestamp = window START; Kalshi exp_ts = window END.
                let duration_secs = duration_mins as i64 * 60;
                let poly_start_ts = exp_ts - duration_secs;
                let slugs = short_duration_slug_patterns(asset, duration_mins, exp_ts);
                let mut matched = false;

                for slug in &slugs {
                    info!("[SHORT-CRYPTO] Trying slug: {}", slug);
                    match self.gamma.lookup_market(slug).await {
                        Ok(Some((yes_token, no_token, actual_slug))) => {
                            // Format window boundaries for visual confirmation
                            let win_start = chrono::DateTime::from_timestamp(poly_start_ts, 0)
                                .map(|dt| dt.format("%H:%M UTC").to_string())
                                .unwrap_or_else(|| poly_start_ts.to_string());
                            let win_end = chrono::DateTime::from_timestamp(exp_ts, 0)
                                .map(|dt| dt.format("%H:%M UTC").to_string())
                                .unwrap_or_else(|| exp_ts.to_string());
                            info!("[SHORT-CRYPTO] ✅ Matched {} [{}-{}] ↔ {} [{}-{}] (exp_ts={})",
                                  market.ticker, win_start, win_end,
                                  actual_slug, win_start, win_end,
                                  exp_ts);
                            let desc = format!("[{}-{}m] {}", asset, duration_mins, market.ticker);
                            pairs.push(MarketPair {
                                pair_id:              format!("{}-{}", series, market.ticker).into(),
                                league:               series.into(),
                                market_type:          MarketType::Total,
                                description:          desc.into(),
                                kalshi_event_ticker:  event.event_ticker.clone().into(),
                                kalshi_market_ticker: market.ticker.clone().into(),
                                poly_slug:            actual_slug.into(),
                                poly_yes_token:       yes_token.into(),
                                poly_no_token:        no_token.into(),
                                line_value:           None,
                                team_suffix:          None,
                                category:             MarketCategory::Crypto,
                                expiry_ts:            Some(exp_ts),
                            });
                            matched = true;
                            break;
                        }
                        Ok(None) => {
                            info!("[SHORT-CRYPTO] No result for slug: {}", slug);
                        }
                        Err(e) => {
                            warn!("[SHORT-CRYPTO] Slug lookup error for {}: {}", slug, e);
                        }
                    }
                }

                if !matched {
                    info!("[SHORT-CRYPTO] No Poly match for {} (close_ts={}, poly_start_ts={}, tried {} slugs)",
                          market.ticker, exp_ts, poly_start_ts, slugs.len());
                }
            }
        }

        Ok((pairs, all_tickers))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_kalshi_ticker() {
        let parsed = parse_kalshi_event_ticker("KXEPLGAME-25DEC27CFCAVL").unwrap();
        assert_eq!(parsed.date, "25DEC27");
        assert_eq!(parsed.team1, "CFC");
        assert_eq!(parsed.team2, "AVL");
    }
    
    #[test]
    fn test_kalshi_date_to_iso() {
        assert_eq!(kalshi_date_to_iso("25DEC27"), "2025-12-27");
        assert_eq!(kalshi_date_to_iso("25JAN01"), "2025-01-01");
    }
}
