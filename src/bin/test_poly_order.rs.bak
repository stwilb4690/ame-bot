// src/bin/test_poly_order.rs
//
// Manual integration test for Polymarket US order placement.
// Loads credentials from env vars, checks balance, lists open orders,
// places a tiny GTC limit buy, then immediately cancels it.
//
// Usage:
//   POLY_US_API_KEY=<key> POLY_US_API_SECRET=<b64-secret> \
//     cargo run --release --bin test_poly_order

use anyhow::Result;
use ame_bot::polymarket_us::{LimitOrderRequest, PolymarketUsClient};

#[tokio::main]
async fn main() -> Result<()> {
    // Load credentials
    let api_key = std::env::var("POLY_US_API_KEY")
        .map_err(|_| anyhow::anyhow!("POLY_US_API_KEY env var not set"))?;
    let api_secret = std::env::var("POLY_US_API_SECRET")
        .map_err(|_| anyhow::anyhow!("POLY_US_API_SECRET env var not set"))?;

    println!("=== Polymarket US Order Test ===");
    println!("API key: {}…", &api_key[..api_key.len().min(8)]);

    let client = PolymarketUsClient::new(&api_key, &api_secret)?;

    // -------------------------------------------------------------------------
    // 1. Balances
    // -------------------------------------------------------------------------
    println!("\n--- GET /v1/account/balances ---");
    match client.get_balances().await {
        Ok(resp) => println!("{:#}", resp.data),
        Err(e) => println!("ERROR: {e}"),
    }

    // -------------------------------------------------------------------------
    // 2. Open orders
    // -------------------------------------------------------------------------
    println!("\n--- GET /v1/orders/open ---");
    match client.get_open_orders().await {
        Ok(orders) => println!("Open order count: {}", orders.len()),
        Err(e) => println!("ERROR: {e}"),
    }

    // -------------------------------------------------------------------------
    // 3. Place a tiny limit buy-YES order
    //    market: nba-mil-det-2026-02-22 | quantity: 1 contract | price: $0.05
    // -------------------------------------------------------------------------
    let market_slug = "nba-mil-det-2026-02-22";
    let price_usd = 0.05_f64; // 5 cents (far below market — safe resting order)
    let quantity = 1_u64; // 1 contract

    println!("\n--- POST /v1/orders (buy YES @ ${price_usd:.2} x {quantity}) ---");
    println!("Market slug: {market_slug}");

    let req = LimitOrderRequest::buy_yes(market_slug, price_usd, quantity);
    println!("Request body: {}", serde_json::to_string_pretty(&req)?);

    match client.place_limit_order(&req).await {
        Ok(resp) => {
            println!("\nOrder response:");
            println!("  id:     {:?}", resp.id);
            println!("  status: {:?}", resp.status);
            println!("  extra:  {:#}", resp.extra);

            // ----------------------------------------------------------------
            // 4. Cancel the order immediately if we got an ID back
            // ----------------------------------------------------------------
            if let Some(order_id) = &resp.id {
                println!("\n--- POST /v1/order/{order_id}/cancel ---");
                match client.cancel_order(order_id).await {
                    Ok(cancel_resp) => println!("Cancel response: {:#}", cancel_resp.data),
                    Err(e) => println!("Cancel ERROR: {e}"),
                }
            } else {
                println!("\nNo order ID in response — skipping cancel.");
            }
        }
        Err(e) => {
            println!("\nplace_limit_order ERROR: {e}");
        }
    }

    println!("\nDone.");
    Ok(())
}
