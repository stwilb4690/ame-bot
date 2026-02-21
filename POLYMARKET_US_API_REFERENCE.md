# Polymarket US Retail API Reference
Base URL: https://api.polymarket.us

## Auth Headers
X-PM-Access-Key: <api-key-id> (UUID)
X-PM-Timestamp: <timestamp-ms> (Unix ms, within 30s)
X-PM-Signature: <base64-ed25519-sig>

## Signature
GET: sign(timestamp + method + path)
POST: sign(timestamp + method + path + body)
Secret is base64 64-byte Ed25519 keypair. First 32 bytes = seed.

## Order Endpoints
POST /v1/orders - Create order
POST /v1/order/preview - Preview
POST /v1/order/close-position - Close position
GET /v1/orders/open - Open orders
GET /v1/order/{orderId} - Get order
POST /v1/order/{orderId}/cancel - Cancel
POST /v1/orders/open/cancel - Cancel all

## Other Endpoints
GET /v1/account/balances
GET /v1/markets
GET /v1/market/{slug}/book
GET /v1/market/{slug}/bbo

## Create Order Body
{"marketSlug":"slug","type":"ORDER_TYPE_LIMIT","price":{"value":"0.55","currency":"USD"},"quantity":100,"tif":"TIME_IN_FORCE_GOOD_TILL_CANCEL","intent":"ORDER_INTENT_BUY_LONG","manualOrderIndicator":"MANUAL_ORDER_INDICATOR_AUTOMATIC"}

## Intents
ORDER_INTENT_BUY_LONG=Buy YES, ORDER_INTENT_BUY_SHORT=Buy NO
ORDER_INTENT_SELL_LONG=Sell YES, ORDER_INTENT_SELL_SHORT=Sell NO
CRITICAL: price.value ALWAYS = YES side price. To buy NO at X, set price=1.00-X

## Rust Ed25519
Use ed25519-dalek crate:
let secret_bytes = BASE64.decode(secret_b64).unwrap();
let signing_key = SigningKey::from_bytes(&secret_bytes[..32].try_into().unwrap());
let msg = format!("{}{}{}", timestamp, method, path);
let sig = signing_key.sign(msg.as_bytes());
BASE64.encode(sig.to_bytes())

## Differences from old CLOB
No wallet needed. Ed25519 not HMAC. Market slugs not token IDs. USD not USDC. api.polymarket.us not clob.polymarket.com
