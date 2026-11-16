-- Column mask: hide CUSIP from non-ops readers.

CREATE OR REPLACE FUNCTION fi_invest.gold.mask_cusip(cusip STRING)
RETURN CASE
  WHEN IS_ACCOUNT_GROUP_MEMBER('fi-ops') THEN cusip
  ELSE CONCAT(LEFT(cusip, 3), '*****', RIGHT(cusip, 1))
END;

ALTER TABLE fi_invest.gold.security_master
  ALTER COLUMN cusip SET MASK fi_invest.gold.mask_cusip;
