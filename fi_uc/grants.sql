-- Unity Catalog grants for the synthetic FI investment catalog.
-- Replace `fi-readers` / `fi-ops` with real groups in a workspace.

CREATE CATALOG IF NOT EXISTS fi_invest;
CREATE SCHEMA IF NOT EXISTS fi_invest.gold;
CREATE SCHEMA IF NOT EXISTS fi_invest.silver;

GRANT USE CATALOG ON CATALOG fi_invest TO `fi-readers`;
GRANT USE SCHEMA ON SCHEMA fi_invest.gold TO `fi-readers`;
GRANT SELECT ON SCHEMA fi_invest.gold TO `fi-readers`;

GRANT USE CATALOG ON CATALOG fi_invest TO `fi-ops`;
GRANT ALL PRIVILEGES ON SCHEMA fi_invest.gold TO `fi-ops`;
GRANT ALL PRIVILEGES ON SCHEMA fi_invest.silver TO `fi-ops`;
