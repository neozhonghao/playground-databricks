# Databricks notebook source

# MAGIC %md
# MAGIC # Section 6 — Delta Sharing
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Use the Delta Sharing feature available with Unity Catalog
# MAGIC - Identify the advantages and limitations of Delta Sharing
# MAGIC - Identify types: Databricks-to-Databricks vs external system (open protocol)
# MAGIC - Analyze cost considerations of data sharing across clouds

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Delta Sharing?
# MAGIC
# MAGIC Delta Sharing is an **open protocol** for securely sharing Delta Lake data with any
# MAGIC recipient — no data copy required; the recipient reads directly from the provider's
# MAGIC cloud storage via a pre-signed URL mechanism.
# MAGIC
# MAGIC | | Databricks-to-Databricks sharing | External / Open Protocol sharing |
# MAGIC |---|---|---|
# MAGIC | **Recipient type** | Another Databricks workspace (any cloud) | Any system with a Delta Sharing client (Python, Spark, pandas, Power BI, etc.) |
# MAGIC | **Setup** | Recipient authenticates via Unity Catalog | Recipient gets an `activation_link` / token file |
# MAGIC | **Data format** | Delta (retains all Delta features) | Parquet (Delta features like time travel not exposed) |
# MAGIC | **UC required** | Provider: yes. Recipient: yes | Provider: yes. Recipient: no |
# MAGIC | **Access control** | Full UC privilege model | Read-only; token-based |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exam Q3 — Sharing Config Pattern
# MAGIC
# MAGIC **Question:** Internal teams need READ/WRITE; external partners need READ only.
# MAGIC
# MAGIC **Answer A (correct):** Grant `READ` to external partners through the Delta Share;
# MAGIC internal teams get `READ/WRITE` directly on the **Unity Catalog tables** (not through the Share).
# MAGIC
# MAGIC **Why:** Delta Sharing only supports READ access. Internal teams should be granted
# MAGIC UC privileges directly; Delta Shares are for external / cross-organisation sharing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Provider Side — Create and Populate a Share

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the share object
# MAGIC CREATE SHARE IF NOT EXISTS billing_share
# MAGIC COMMENT 'Aggregated billing data shared with finance partners';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add a table to the share
# MAGIC ALTER SHARE billing_share
# MAGIC ADD TABLE main.gold.daily_revenue
# MAGIC    COMMENT 'Daily revenue by department'
# MAGIC    PARTITIONS (billing_date >= '2024-01-01');   -- optional partition filter

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add a view to the share (applies row filters at sharing layer)
# MAGIC ALTER SHARE billing_share
# MAGIC ADD TABLE main.gold.v_cardiology_revenue;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant READ access to a Databricks recipient (another UC metastore)
# MAGIC CREATE RECIPIENT IF NOT EXISTS finance_team_recipient
# MAGIC USING ID 'databricks_metastore_id_here';   -- recipient's metastore sharing ID

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grant access to the share
# MAGIC GRANT SELECT ON SHARE billing_share TO RECIPIENT finance_team_recipient;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- For an external (non-Databricks) recipient: generate an activation link
# MAGIC CREATE RECIPIENT IF NOT EXISTS external_partner
# MAGIC COMMENT 'External analytics partner';
# MAGIC -- Databricks generates a one-time activation_link — distribute securely

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect the share
# MAGIC SHOW ALL IN SHARE billing_share;
# MAGIC SHOW GRANTS ON SHARE billing_share;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recipient Side (Databricks) — Read a Shared Dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List shares made available to this metastore by a provider
# MAGIC SHOW SHARES IN PROVIDER acme_corp_provider;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a local catalog pointing at the share
# MAGIC CREATE CATALOG IF NOT EXISTS acme_billing_share
# MAGIC USING SHARE acme_corp_provider.billing_share;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the shared table as if it were a local table
# MAGIC SELECT * FROM acme_billing_share.gold.daily_revenue
# MAGIC WHERE billing_date >= '2024-01-01';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advantages of Delta Sharing
# MAGIC
# MAGIC - **No data copy:** reads directly from provider's storage — no storage duplication cost
# MAGIC - **Open protocol:** non-Databricks consumers (Pandas, Spark, Power BI) can read via REST
# MAGIC - **Live data:** consumers always see the latest version (or a partitioned subset)
# MAGIC - **Governance stays with the provider:** provider controls what is shared and can revoke at any time
# MAGIC - **Cross-cloud and cross-region:** share from AWS → Azure without data movement

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limitations and Cost Considerations
# MAGIC
# MAGIC | Limitation | Detail |
# MAGIC |---|---|
# MAGIC | **Read-only for external recipients** | Delta Sharing does not support writes from the recipient |
# MAGIC | **No Delta-specific features for external** | External consumers get Parquet; no time travel, Change Data Feed |
# MAGIC | **Egress costs** | When recipients are in a different cloud region or provider, cloud egress charges apply on the **provider's** cloud bill |
# MAGIC | **No real-time push** | Consumers query on demand; there is no push/subscription model |
# MAGIC | **Provider must have UC** | Delta Sharing requires Unity Catalog on the provider side |
# MAGIC
# MAGIC **Cross-cloud cost consideration:**
# MAGIC - Sharing within the **same region and cloud**: minimal egress cost
# MAGIC - Sharing **cross-region (same cloud)**: regional data transfer charges
# MAGIC - Sharing **cross-cloud** (AWS → Azure): internet egress costs (typically $0.08–0.09 / GB)
# MAGIC - Design shares to minimise the data volume exposed (use partition filters, row filters)
