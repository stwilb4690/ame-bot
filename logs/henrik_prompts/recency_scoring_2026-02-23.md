# Prompt: Add Recency Scoring and Trade Volume Filtering to AME Market Maker

## Goal
Modify the market maker bot to prioritize markets that have recent trading activity and filter out illiquid markets.

## Required Changes

### 1. Add Trade History Tracking
**File:** `src/market_maker.rs`

Add a new field to the `MarketMaker` struct to track recent trades by market:
```rust
/// Recent trade history per market: ticker → VecDeque of (timestamp, trade_count) in last hour
pub trade_history: Mutex<HashMap<String, VecDeque<(Instant, u32)>>>,
```

### 2. Add Configuration Parameters
**File:** `src/market_maker.rs` in `MmConfig`

Add new env var config options:
- `MM_MIN_TRADES_PER_HOUR` (default: 5) - Filter: minimum trades in last hour to consider market
- `MM_RECENCY_WINDOW_SECS` (default: 1800) - 30 min window for recency score calculation
- `MM_RECENCY_SCORE_WEIGHT` (default: 0.3) - Weight to apply to recency in composite score

### 3. Track Trades from Market Data
The Kalshi order book delta messages implicitly show trades when levels change. 

In the `on_delta` method, detect significant level changes that indicate trading activity. When a delta shows a meaningful reduction in best bid depth or best ask depth, record a "trade event":

```rust
// In on_delta() - after applying the delta, check if top-of-book changed significantly
// If best bid decreased by > threshold or best ask increased by > threshold, 
// record as probable trade activity
```

Add a helper method:
```rust
async fn record_trade_activity(&self, ticker: &str, trade_count: u32) {
    let mut history = self.trade_history.lock().await;
    let window = history.entry(ticker.to_string()).or_insert_with(VecDeque::new);
    
    // Add current timestamp
    window.push_back((Instant::now(), trade_count));
    
    // Remove entries older than 1 hour
    let cutoff = Instant::now() - Duration::from_secs(3600);
    while window.front().map_or(false, |(t, _)| *t < cutoff) {
        window.pop_front();
    }
}
```

### 4. Calculate Recency Score
Add a helper method to compute the recency score:
```rust
fn calculate_recency_score(&self, ticker: &str, history: &HashMap<String, VecDeque<(Instant, u32)>>) -> Decimal {
    let now = Instant::now();
    let window = Duration::from_secs(self.config.recency_window_secs);
    
    let trades_in_window = history.get(ticker)
        .map(|deque| {
            deque.iter()
                .filter(|(t, _)| now.duration_since(*t) < window)
                .map(|(_, count)| count)
                .sum::<u32>()
        })
        .unwrap_or(0);
    
    // Normalize: more trades = higher score (max boost at 20 trades in 30 min)
    let normalized = (trades_in_window as f64 / 20.0).min(1.0);
    Decimal::from_f64(normalized).unwrap_or(Decimal::ONE)
}
```

### 5. Filter in find_opportunities()
**File:** `src/market_maker.rs` in `find_opportunities()` method

Add two new filters after the existing ones:

```rust
// Filter: minimum trades per hour
let trade_history = self.trade_history.lock().await;
let trades_last_hour: u32 = trade_history.get(ticker.as_str())
    .map(|deque| deque.iter().map(|(_, count)| count).sum())
    .unwrap_or(0);
if trades_last_hour < self.config.min_trades_per_hour { return None; }

// Calculate recency score for composite
let recency_score = calculate_recency_score(ticker, &trade_history);
```

### 6. Update Composite Score
Modify the score calculation in `find_opportunities()`:
```rust
// Original score: min(bid_depth, ask_depth) × (spread / mid)
let base_score = if mid > Decimal::ZERO {
    bid_depth.min(ask_depth) * (spread / mid)
} else {
    Decimal::ZERO
};

// Apply recency weighting: blend base score with recency bonus
let final_score = base_score * (Decimal::ONE - self.config.recency_score_weight 
    + self.config.recency_score_weight * recency_score);
```

### 7. Update SpreadOpportunity struct
```rust
pub struct SpreadOpportunity {
    pub ticker: String,
    pub best_yes_bid: Decimal,
    pub best_yes_ask: Decimal,
    pub spread: Decimal,
    pub bid_depth: Decimal,
    pub ask_depth: Decimal,
    pub mid: Decimal,
    pub score: Decimal,
    pub trades_last_hour: u32,     // NEW
    pub recency_score: Decimal,     // NEW
}
```

### 8. init() method update
Ensure `trade_history` is initialized in `MarketMaker::new()`.

## Implementation Notes

1. Trade detection from order book deltas is heuristic - we're inferring trades from depth changes. This is acceptable for filtering but won't be perfectly accurate.

2. The recency score should boost markets that traded recently, even if they have slightly lower base scores. This prevents the bot from cycling through dead markets.

3. Consider adding a decay factor to recency - trades in the last 5 minutes matter more than trades 25 minutes ago.

4. Debug logging: add trace logs showing recency_score + trades_last_hour per candidate market.

5. Config validation: ensure MM_MIN_TRADES_PER_HOUR is reasonable (2-10 range). Too high and no markets qualify; too low and the filter is useless.

## Testing Checklist
- [ ] Markets with 0-4 trades/hour are filtered out
- [ ] Markets with 5+ trades/hour are considered
- [ ] Recency score correctly boosts recently-active markets in ranking
- [ ] Bot stops cycling through dead markets (timeout loop)
- [ ] Debug logs show recency info

## Files to Modify
- `src/market_maker.rs` - Main logic
- `.env` - Add new config vars (document only, don't change defaults yet)

Do NOT change:
- Existing market selection logic beyond what's specified
- Risk limits or position sizing
- ws connection handling
