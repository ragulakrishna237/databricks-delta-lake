-- Cost per Lakeflow job: usage x list price x job timeline.
-- Verify table names in Catalog Explorer before running in a workspace.

SELECT
  j.job_id,
  j.name AS job_name,
  u.workspace_id,
  DATE(u.usage_start_time) AS usage_date,
  u.sku_name,
  SUM(u.usage_quantity) AS dbus,
  SUM(u.usage_quantity * p.pricing.effective_list.default) AS list_cost_usd
FROM system.billing.usage AS u
INNER JOIN system.billing.list_prices AS p
  ON u.sku_name = p.sku_name
 AND u.usage_unit = p.usage_unit
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
LEFT JOIN system.lakeflow.job_run_timeline AS r
  ON TRY_CAST(u.usage_metadata.job_id AS BIGINT) = r.job_id
 AND u.usage_start_time < COALESCE(r.period_end_time, CURRENT_TIMESTAMP())
 AND COALESCE(u.usage_end_time, u.usage_start_time) >= r.period_start_time
LEFT JOIN system.lakeflow.jobs AS j
  ON r.job_id = j.job_id
WHERE u.usage_start_time >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY ALL
ORDER BY list_cost_usd DESC;
