CREATE OR REFRESH STREAMING TABLE customers_history;

CREATE FLOW customers_history_cdc
AS AUTO CDC INTO
  customers_history
FROM stream(customers_cdc_clean)
KEYS (id)
APPLY AS DELETE WHEN
operation = "DELETE"
SEQUENCE BY operation_date
COLUMNS * EXCEPT (operation, operation_date, _rescued_data)
STORED AS SCD TYPE 2;