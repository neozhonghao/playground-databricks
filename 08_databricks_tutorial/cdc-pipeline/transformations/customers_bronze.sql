CREATE OR REFRESH STREAMING TABLE customers_cdc_bronze
COMMENT "New customer data incrementally ingested from cloud object storage landing zone";

CREATE FLOW customers_bronze_ingest_flow AS
INSERT INTO customers_cdc_bronze BY NAME
  SELECT *
  FROM STREAM read_files(
    -- replace with the catalog/schema you are using:
    "/Volumes/playground/cdc-pipeline/raw_data/customers",
    format => "json",
    inferColumnTypes => "true"
  )