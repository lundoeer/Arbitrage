Decide your output schema

Required columns:

slug

window_start_utc

window_end_utc

open_price

close_price

fetched_at_utc

Prepare your input list

Create slugs.txt or slugs.csv containing one btc-updown-15m-... slug per line.

This is your authoritative list for the run.

Fetch the current Next.js BUILD_ID once

Make a GET request to one normal page URL:

https://polymarket.com/en/event/<slug>

Extract the build id:

either parse **NEXT_DATA** JSON and read buildId

or regex the HTML for /\_next/data/<BUILD_ID>/

Store it as build_id in memory (and log it).

Construct the JSON URL for each slug

For each slug:

https://polymarket.com/_next/data/{build_id}/en/event/{slug}.json?slug={slug}

Download the JSON payload

Use a normal browser-like header set:

Accept: _/_

Accept-Encoding: gzip, deflate, br

Accept-Language: en-US,en;q=0.9

User-Agent: <something standard>

Add retry logic for:

429 (rate limit) → exponential backoff

5xx → retry a few times

If you get 404s widely: refresh build_id (step 3) and retry.

Locate the right dehydrated query entry

Navigate: pageProps.dehydratedState.queries

Find the object where:

state.data.openPrice exists AND state.data.closePrice exists

(optionally also confirm queryKey looks like: ["crypto-prices","price","BTC",<start>,"fifteen",<end>])

Do not use a fixed index like [7].

Extract the window timestamps

From that matched entry:

window_start_utc = queryKey[3]

window_end_utc = queryKey[5]

Also extract:

open_price = state.data.openPrice

close_price = state.data.closePrice

Compute derived fields

computed_outcome = "UP" if close_price >= open_price else "DOWN"

fetched_at_utc = now() in UTC

source_build_id = build_id (useful for debugging)

Write results incrementally

Append each parsed row immediately to:

a CSV (easy), or

SQLite (better for dedupe + reruns)

Also write a failure log file with:

slug

HTTP status / exception

whether missing open/close query was found

Add deduplication

Use a unique key:

(slug) or (window_start_utc, window_end_utc) (better if slug format ever changes)

If a row already exists, skip or overwrite (your choice).

Run a validation batch

Pick ~20 slugs known to be resolved.

Compare your computed_outcome vs the market’s resolved outcome (if you also extract it from the same JSON or from the page).

If mismatches appear, confirm you’re reading the correct query entry and the timezone boundaries.

Turn it into a rolling collector (optional but recommended)

Daily job:

fetch new slugs for the last day (from your own slug generator or by listing markets)

repeat steps 3–10

This overcomes the ~20-day retention limitation because you store the data yourself.

Hardening (minimum practical)

Rate limit (e.g. 2–5 req/sec).

Backoff on 429.

Refresh build id automatically when \_next/data 404s.

Save raw JSON for a small sample of failures for debugging.
