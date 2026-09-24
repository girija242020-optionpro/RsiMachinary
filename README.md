# Bharati Unique Backend V4.1.2 — RSI DEMA VOLUME State-Machine Compatible

This backend is adapted specifically for `RSI_DEMA_VOLUME_StateMachine_PWA_v1.zip`. The PWA itself does not need to be changed for the normal WebSocket connection flow.

## What this backend fixes

- Correct Dhan v2 binary header parsing.
- Correct exchange-segment mapping (`IDX_I`, `NSE_FNO`, etc.).
- Sends the PWA-compatible WebSocket messages: `hello`, `state`, and `tick` with `close`, `volume`, `timestamp`, `rsi5`, `rsiEma14`, `dema14`, and `volSma250`.
- Accepts the PWA message `{type:"subscribe",symbol:"NIFTY",timeframe:"1"}`.
- Keeps the selected index feed alive.
- Maintains bounded tick and candle history.
- Exposes feed freshness as `feedLastMessage`, `feedLastMessageAgeMs`, and `feedStale`.
- Parses Dhan Full Packet (5-level quote/depth) and Dhan 20-level depth packets (codes 41/51).
- Keeps option-chain, OI, IV, Greeks, hidden Greeks, PCR and Max Pain endpoints from the older backend.
- Adds push/VAPID endpoints for future PWA push integration.
- Root page shows Dhan feed, L20 feed, feed stale state, last message time and counters.

## Dhan configuration

Dhan's current v2 docs use the v2 market-feed WebSocket at `wss://api-feed.dhan.co?...`, with RequestCode 21 for Full Packet. Full Market Depth uses `wss://depth-api-feed.dhan.co/twentydepth?...` and RequestCode 23. Dhan documents 20 levels of depth and up to 50 instruments per 20-level depth connection. The backend follows these protocols.

Use either:

1. `DHAN_ACCESS_TOKEN` (manual 24-hour access token), OR
2. `DHAN_CLIENT_ID` + `DHAN_PIN` + `DHAN_TOTP_SECRET` for automatic token generation.

Do not set a stale manual token if you expect automatic renewal.

## Render

Root Directory: `.`

Build Command:
`npm install`

Start Command:
`npm start`

The included `render.yaml` contains the environment variable names.

## Required environment variables

### Dhan
- `DHAN_CLIENT_ID`
- `DHAN_PIN`
- `DHAN_TOTP_SECRET`
- OR `DHAN_ACCESS_TOKEN`

### VAPID
- `VAPID_PUBLIC_KEY`
- `VAPID_PRIVATE_KEY`
- `VAPID_SUBJECT`

### Data / storage
- `MAX_TICKS_PER_INSTRUMENT=3000`
- `MAX_CANDLES_PER_INSTRUMENT=2000`
- `TICK_STALE_MS=15000`
- `LIVE_HISTORY_REFRESH_MS=10000`
- `CANDLE_TIMEFRAME_MS=60000`
- `OPTION_CHAIN_REFRESH_MS=3200`
- `DEPTH_MAX_INSTRUMENTS=50`
- `DEFAULT_INDEX=NIFTY`
- `NIFTY_SECURITY_ID=13`
- `BANKNIFTY_SECURITY_ID=25`
- `FINNIFTY_SECURITY_ID=27`
- `MIDCPNIFTY_SECURITY_ID=442`
- `SENSEX_SECURITY_ID=51`

`PORT` is supplied by Render automatically; do not hard-code it.

## PWA WebSocket URL

Paste the Render HTTPS URL into the PWA, for example:
`https://YOUR-SERVICE.onrender.com`

The PWA converts it to `wss://YOUR-SERVICE.onrender.com/ws` automatically.

## Health / verification

- `/` — mobile-friendly backend status page
- `/health` — plain health
- `/api/health` — JSON health, counters and feed-stale status
- `/api/state` — public state
- `/api/ticks` — current tick cache
- `/api/history` — candle history
- `/api/depth` — current 20-level depth book when available
- `/api/option-chain` — option chain
- `/api/analytics` — PCR / Max Pain / IV / Greek analytics

Before connecting the PWA, open `/api/health`. You want `dhanConnected:true`, `depthConnected:true`, and `feedStale:false` while the market feed is live.

## VAPID keys

Generate a fresh pair for your deployment and store them only in Render Environment Variables. Never commit the private key to GitHub.


## V4.1.2 index-feed fix
Dhan v2 response code `1` is the Index Packet. The decoder now parses the index LTP from byte offset 8 for IDX_I instruments (including NIFTY SecurityId 13), allowing `state.spot`, `lastTick`, candles, and the unchanged PWA live fields to receive the index price. Dhan documents response code 1 as Index Packet and IDX_I as segment enum 0.
