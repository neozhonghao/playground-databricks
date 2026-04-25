# Databricks notebook source

# MAGIC %md
# MAGIC # Section 6 — Permissions, Row-Level Security & Column Masking
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Identify grant of permissions to users and groups within UC
# MAGIC - Configure access controls: GRANT, REVOKE, DENY
# MAGIC - Column-level masking and row-level security
# MAGIC - Unity Catalog ABAC policies

# COMMAND ----------

# MAGIC %md
# MAGIC ## UC Privilege Hierarchy
# MAGIC
# MAGIC Privileges are inherited downward. Granting `USE CATALOG` on a catalog does NOT
# MAGIC automatically grant `USE SCHEMA` or `SELECT` on objects inside — each must be granted.
# MAGIC
# MAGIC ```
# MAGIC METASTORE
# MAGIC  └── CATALOG          → USE CATALOG, CREATE SCHEMA, ...
# MAGIC       └── SCHEMA      → USE SCHEMA, CREATE TABLE, CREATE VIEW, ...
# MAGIC            └── TABLE  → SELECT, MODIFY, ...
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Exam Q2 answer: grant SELECT on a schema to a group
# MAGIC -- Prerequisite: analysts already have USE CATALOG + USE SCHEMA
# MAGIC GRANT SELECT ON SCHEMA sales_data TO analysts;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full pattern: grant minimal access for read-only analysts
# MAGIC GRANT USE CATALOG ON CATALOG main TO `analysts`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA main.silver TO `analysts`;
# MAGIC GRANT SELECT      ON ALL TABLES IN SCHEMA main.silver TO `analysts`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant write access to a pipeline service principal
# MAGIC GRANT USE CATALOG ON CATALOG main TO `sp-billing-pipeline`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA main.bronze TO `sp-billing-pipeline`;
# MAGIC GRANT MODIFY      ON TABLE main.bronze.patient_billing TO `sp-billing-pipeline`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant table creation in a schema
# MAGIC GRANT CREATE TABLE, CREATE VIEW ON SCHEMA main.gold TO `data-engineers`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revoke a previously granted privilege
# MAGIC REVOKE SELECT ON TABLE main.silver.patient_billing FROM `analysts`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show current grants on an object
# MAGIC SHOW GRANTS ON TABLE main.silver.patient_billing;
# MAGIC SHOW GRANTS ON SCHEMA main.silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row-Level Security (RLS)
# MAGIC
# MAGIC Control which rows each user/group sees by registering a **row filter function**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Define the row filter function (returns BOOLEAN)
# MAGIC CREATE OR REPLACE FUNCTION main.security.filter_by_region(region STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC LANGUAGE SQL
# MAGIC RETURN
# MAGIC     is_account_group_member('apac-analysts') AND region = 'APAC'
# MAGIC     OR is_account_group_member('emea-analysts') AND region = 'EMEA'
# MAGIC     OR is_account_group_member('data-engineers');  -- full access

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply the row filter to a table
# MAGIC ALTER TABLE main.silver.sales_data
# MAGIC SET ROW FILTER main.security.filter_by_region ON (region);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove the row filter
# MAGIC ALTER TABLE main.silver.sales_data DROP ROW FILTER;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column-Level Masking
# MAGIC
# MAGIC Hide or transform sensitive column values based on the current user's group.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Define a masking function
# MAGIC CREATE OR REPLACE FUNCTION main.security.mask_ssn(ssn STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE SQL
# MAGIC RETURN
# MAGIC     CASE
# MAGIC         WHEN is_account_group_member('pii-access') THEN ssn
# MAGIC         ELSE CONCAT('XXX-XX-', RIGHT(ssn, 4))     -- last 4 digits only
# MAGIC     END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apply the mask to a column
# MAGIC ALTER TABLE main.silver.patients
# MAGIC ALTER COLUMN ssn SET MASK main.security.mask_ssn;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove the mask
# MAGIC ALTER TABLE main.silver.patients
# MAGIC ALTER COLUMN ssn DROP MASK;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ABAC (Attribute-Based Access Control) Policies
# MAGIC
# MAGIC ABAC in Unity Catalog provides **centralised** row-filter and column-masking policies
# MAGIC that apply automatically to all tables tagged with a given attribute — no need to
# MAGIC alter each table individually.
# MAGIC
# MAGIC **Key concepts:**
# MAGIC
# MAGIC | Concept | Description |
# MAGIC |---|---|
# MAGIC | **Tag** | Metadata label on a table/column (e.g., `pii = true`) |
# MAGIC | **Policy** | A ABAC rule that applies a row filter or column mask to objects with a specific tag |
# MAGIC | **Centralized** | One policy protects all tagged objects — no per-table DDL changes |
# MAGIC
# MAGIC ```sql
# MAGIC -- Tag a column as PII
# MAGIC ALTER TABLE main.silver.patients
# MAGIC ALTER COLUMN ssn SET TAGS ('pii' = 'true');
# MAGIC
# MAGIC -- In the UC ABAC policy (configured in Account Console UI):
# MAGIC -- "IF column has tag pii=true THEN apply mask function main.security.mask_ssn"
# MAGIC ```
# MAGIC
# MAGIC **Exam tip:** ABAC vs per-table masking:
# MAGIC - Per-table `SET MASK`: applied to one specific table column
# MAGIC - ABAC policy: applies to ALL columns with a matching tag across any table
