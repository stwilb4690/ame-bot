use anyhow::Result;
use ame_bot::polymarket_us::PolymarketUsClient;

#[tokio::main]
async fn main() -> Result<()> {
    let client = PolymarketUsClient::new(
        &std::env::var("POLY_US_API_KEY")?,
        &std::env::var("POLY_US_API_SECRET")?,
    )?;

    println!("=== ALL open markets (limit 200) ===");
    match client.get_raw("/v1/markets?active=true&closed=false&limit=200").await {
        Ok(body) => {
            let v: serde_json::Value = serde_json::from_str(&body)?;
            if let Some(markets) = v.get("markets").and_then(|m| m.as_array()) {
                let open: Vec<_> = markets.iter().filter(|m| m.get("closed") == Some(&serde_json::Value::Bool(false))).collect();
                println!("Total returned: {}  Actually open: {}", markets.len(), open.len());
                for m in &open {
                    println!("  {} | {} | {}", 
                        m.get("slug").and_then(|s| s.as_str()).unwrap_or("?"),
                        m.get("sportsMarketTypeV2").and_then(|s| s.as_str()).unwrap_or("?"),
                        m.get("question").and_then(|s| s.as_str()).unwrap_or("?"));
                }
            }
        }
        Err(e) => println!("ERROR: {e}"),
    }
    Ok(())
}
