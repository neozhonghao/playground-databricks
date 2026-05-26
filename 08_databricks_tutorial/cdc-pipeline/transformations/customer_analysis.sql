CREATE OR REPLACE MATERIALIZED VIEW customers_history_agg AS
SELECT
  id,
  count(distinct address) as address_count,
  count(distinct email) AS email_count,
  count(distinct firstname) AS firstname_count,
  count(distinct lastname) AS lastname_count
FROM customers_history
GROUP BY id