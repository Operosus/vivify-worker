# Vivify venue-hirers worker

Finds external groups that book/hire a specific school venue (for Vivify sales to poach to nearby customer schools).

Pipeline: web search → read pages + 1-level crawl to "where we meet" pages → charity register by postcode →
exact-postcode DB → Facebook posts → gpt-4o venue-tie gate → write via `process_venue_hirer_results` RPC →
trigger shared enrichment → status complete.

Deploy: Render web service (Docker). POST `/webhook/vivify-venue-hirers` `{"search_id": <id>}`.

Required env vars: DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD, SUPABASE_URL, SUPABASE_KEY, APIFY_TOKEN, OPENAI_API_KEY.
