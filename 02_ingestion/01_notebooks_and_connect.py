# Databricks notebook source

# MAGIC %md
# MAGIC # Section 2 — Notebooks & Databricks Connect
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Determine the capabilities of Notebooks functionality
# MAGIC - Use Databricks Connect in a data engineering workflow
# MAGIC - Use Databricks' built-in debugging tools

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Notebook Capabilities
# MAGIC
# MAGIC | Feature | Details |
# MAGIC |---|---|
# MAGIC | **Magic commands** | `%sql`, `%python`, `%scala`, `%r`, `%sh`, `%fs`, `%md` |
# MAGIC | **dbutils** | File system (`dbutils.fs`), secrets (`dbutils.secrets`), widgets (`dbutils.widgets`), notebook control (`dbutils.notebook`) |
# MAGIC | **Notebook workflows** | `dbutils.notebook.run()` calls another notebook and returns a value |
# MAGIC | **Multi-language** | Each cell can override the default language |
# MAGIC | **Revision history** | Built-in Git-like versioning per notebook |
# MAGIC | **Collaborative editing** | Real-time co-authoring |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets — parameterised notebooks

# COMMAND ----------

# Create a text widget with a default value
dbutils.widgets.text("catalog", "main", "Catalog name")
dbutils.widgets.dropdown("environment", "dev", ["dev", "test", "prod"], "Environment")

catalog = dbutils.widgets.get("catalog")
env = dbutils.widgets.get("environment")
print(f"Running against catalog={catalog}  env={env}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dbutils.notebook.run — call another notebook as a task

# COMMAND ----------

# result = dbutils.notebook.run(
#     "./02_autoloader",          # relative path
#     timeout_seconds=600,
#     arguments={"catalog": catalog, "env": env}
# )
# print(f"Child notebook returned: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### dbutils.fs — browse the file system

# COMMAND ----------

# List DBFS root (always available)
display(dbutils.fs.ls("dbfs:/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Built-in Debugging Tools
# MAGIC
# MAGIC | Tool | How to access | Purpose |
# MAGIC |---|---|---|
# MAGIC | **Cluster UI** | Compute → cluster → Spark UI | DAG, stage metrics, executor stats |
# MAGIC | **Event log** | Cluster UI → Event Log | Cluster start/stop, errors |
# MAGIC | **Driver logs** | Compute → cluster → Driver logs | stderr/stdout from the driver |
# MAGIC | **`display(df)`** | In notebook | Inspect DataFrame with sorting/filtering |
# MAGIC | **`df.explain()`** | In notebook cell | Show the physical query plan |
# MAGIC | **Exception stack traces** | Notebook cell output | Python/Spark exceptions |

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.range(1000).selectExpr("id", "id * 2 AS doubled")

# explain() shows the query plan — look for Exchange (shuffle) and BroadcastExchange
df.filter("id > 500").explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Databricks Connect
# MAGIC
# MAGIC Databricks Connect lets you run PySpark code from your **local IDE** against a remote
# MAGIC Databricks cluster or serverless environment — no need to upload notebooks.
# MAGIC
# MAGIC ### Install (local machine)
# MAGIC ```bash
# MAGIC pip install databricks-connect==15.*   # match your cluster DBR version
# MAGIC ```
# MAGIC
# MAGIC ### Configure
# MAGIC ```bash
# MAGIC databricks configure --token   # stores profile in ~/.databrickscfg
# MAGIC ```
# MAGIC
# MAGIC ### Use in Python code
# MAGIC ```python
# MAGIC from databricks.connect import DatabricksSession
# MAGIC
# MAGIC spark = DatabricksSession.builder.serverless().getOrCreate()
# MAGIC # or: .remote(host="...", token="...", cluster_id="...").getOrCreate()
# MAGIC
# MAGIC df = spark.read.table("main.default.sensor_readings")
# MAGIC df.show()
# MAGIC ```
# MAGIC
# MAGIC **Key points for the exam:**
# MAGIC - Databricks Connect v2 (DBR 13+) replaces the older v1 API
# MAGIC - Supports serverless compute — no cluster ID required with `.serverless()`
# MAGIC - Local libraries installed via `pip` are available in the remote execution context
# MAGIC - `dbutils` is NOT available via Databricks Connect (it's a notebook-only API)
