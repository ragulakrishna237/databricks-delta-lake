-- Row filter: non-ops identities only see their own account_id.

CREATE OR REPLACE FUNCTION fi_invest.gold.account_row_filter(account_id STRING)
RETURN IS_ACCOUNT_GROUP_MEMBER('fi-ops') OR account_id = CURRENT_USER();

ALTER TABLE fi_invest.gold.holdings_bitemporal
  SET ROW FILTER fi_invest.gold.account_row_filter ON (account_id);
