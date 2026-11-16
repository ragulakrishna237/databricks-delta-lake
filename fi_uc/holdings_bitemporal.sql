-- Bitemporal Holding Master: business time (as_of_date) and system time (recorded_at).
-- Point-in-time as-of: filter as_of_date <= :biz AND recorded_at <= :sys.

CREATE TABLE IF NOT EXISTS fi_invest.gold.holdings_bitemporal (
  account_id STRING NOT NULL,
  instrument_id STRING NOT NULL,
  position_qty DOUBLE,
  market_value DOUBLE,
  as_of_date DATE NOT NULL,
  recorded_at TIMESTAMP NOT NULL,
  source_txn_id STRING
)
USING DELTA;

-- Example as-of query (business 2024-06-03, knowledge as of 2024-06-04 12:00Z).
SELECT account_id, instrument_id, position_qty, market_value, as_of_date, recorded_at
FROM fi_invest.gold.holdings_bitemporal
WHERE as_of_date <= DATE '2024-06-03'
  AND recorded_at <= TIMESTAMP '2024-06-04 12:00:00'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY account_id, instrument_id
  ORDER BY as_of_date DESC, recorded_at DESC
) = 1;
