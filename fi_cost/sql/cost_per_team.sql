-- Cost per team: usage custom_tags.team (or cluster tags) x list price.

SELECT
  COALESCE(u.custom_tags.team, u.identity_metadata.created_by, 'unattributed') AS team,
  u.workspace_id,
  DATE(u.usage_start_time) AS usage_date,
  SUM(u.usage_quantity) AS dbus,
  SUM(u.usage_quantity * p.pricing.effective_list.default) AS list_cost_usd
FROM system.billing.usage AS u
INNER JOIN system.billing.list_prices AS p
  ON u.sku_name = p.sku_name
 AND u.usage_unit = p.usage_unit
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
WHERE u.usage_start_time >= DATEADD(DAY, -30, CURRENT_DATE())
GROUP BY ALL
ORDER BY list_cost_usd DESC;
