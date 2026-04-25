# Databricks notebook source

# MAGIC %md
# MAGIC # Section 4 — Lakeflow Jobs: Orchestration
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Configure common tasks and their dependencies (DAG-based task graph)
# MAGIC - Implement control flows: retries, conditional tasks (branching/looping)
# MAGIC - Deploy a workflow, repair and rerun a task on failure
# MAGIC - Implement job schedules with trigger types

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakeflow Jobs Overview
# MAGIC
# MAGIC Lakeflow Jobs (formerly Databricks Workflows) is the native orchestration service.
# MAGIC
# MAGIC | Concept | Description |
# MAGIC |---|---|
# MAGIC | **Job** | A named workflow made up of one or more tasks |
# MAGIC | **Task** | A unit of work: notebook, SQL query, Python script, pipeline run, etc. |
# MAGIC | **DAG** | The dependency graph between tasks |
# MAGIC | **Run** | A single execution of a job |
# MAGIC | **Repair run** | Re-execute only the failed tasks (and their dependants) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task Types
# MAGIC
# MAGIC | Task Type | Use case |
# MAGIC |---|---|
# MAGIC | **Notebook** | PySpark, SQL, or mixed-language notebooks |
# MAGIC | **Python script** | Standalone `.py` file (serverless or cluster) |
# MAGIC | **SQL query** | Named query in Databricks SQL |
# MAGIC | **Dashboard refresh** | Refresh an AI/BI dashboard |
# MAGIC | **Pipeline (LDP)** | Trigger a Lakeflow Spark Declarative Pipeline |
# MAGIC | **dbt** | Run a dbt project |
# MAGIC | **Spark submit** | Submit a JAR or Python egg |
# MAGIC | **Condition task** | Branch based on an expression |
# MAGIC | **For each** | Loop over a collection |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Job YAML (databricks.yml representation)
# MAGIC
# MAGIC See `resources/bundles/databricks.yml` for a full working example.
# MAGIC The structure below shows the key fields:
# MAGIC
# MAGIC ```yaml
# MAGIC jobs:
# MAGIC   - name: billing_pipeline_job
# MAGIC     schedule:
# MAGIC       quartz_cron_expression: "0 0 6 * * ?"   # 06:00 daily
# MAGIC       timezone_id: "Asia/Singapore"
# MAGIC     max_concurrent_runs: 1
# MAGIC     tasks:
# MAGIC       - task_key: ingest
# MAGIC         notebook_task:
# MAGIC           notebook_path: ./02_ingestion/02_autoloader.py
# MAGIC         new_cluster: ...
# MAGIC
# MAGIC       - task_key: transform
# MAGIC         depends_on:
# MAGIC           - task_key: ingest
# MAGIC         notebook_task:
# MAGIC           notebook_path: ./03_transformation/01_medallion_architecture.py
# MAGIC
# MAGIC       - task_key: quality_check
# MAGIC         condition_task:
# MAGIC           op: EQUAL_TO
# MAGIC           left: "{{tasks.transform.values.row_count}}"
# MAGIC           right: "0"
# MAGIC         depends_on:
# MAGIC           - task_key: transform
# MAGIC
# MAGIC       - task_key: alert_on_empty
# MAGIC         depends_on:
# MAGIC           - task_key: quality_check
# MAGIC             outcome: "true"
# MAGIC         notebook_task:
# MAGIC           notebook_path: ./04_lakeflow_jobs/alert_empty.py
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task Value Passing
# MAGIC
# MAGIC Tasks can share values via **task values** (key-value store per run).

# COMMAND ----------

# In the upstream task (e.g., ingest):
row_count = spark.table("main.bronze.patient_billing").count()
dbutils.jobs.taskValues.set(key="row_count", value=row_count)

# In the downstream task (e.g., quality_check):
row_count_from_ingest = dbutils.jobs.taskValues.get(
    taskKey="ingest",
    key="row_count",
    default=0,
    debugValue=0,   # returned when running the notebook interactively (not in a job)
)
print(f"Rows ingested: {row_count_from_ingest}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retries and Timeout

# COMMAND ----------

# In databricks.yml task definition:
# max_retries: 3
# min_retry_interval_millis: 60000   # 1 minute between retries
# timeout_seconds: 3600              # fail task after 1 hour

# COMMAND ----------

# MAGIC %md
# MAGIC ## Repairing a Failed Run
# MAGIC
# MAGIC When a job run fails, you can **repair** it instead of re-running the entire job:
# MAGIC
# MAGIC 1. Go to **Lakeflow Jobs** → select the job → **Runs** tab
# MAGIC 2. Click the failed run → **Repair run**
# MAGIC 3. Choose whether to re-run only failed tasks or all tasks from a specific point
# MAGIC
# MAGIC **Via CLI:**
# MAGIC ```bash
# MAGIC databricks jobs repair-run --run-id 12345 --rerun-tasks ingest,transform
# MAGIC ```
# MAGIC
# MAGIC **Key point:** Repair runs use the same `run_id`; task outputs from successful tasks
# MAGIC are preserved — you don't re-ingest data that was already processed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger Types
# MAGIC
# MAGIC | Trigger | When it fires | Use case |
# MAGIC |---|---|---|
# MAGIC | **Scheduled** (cron) | Fixed time interval | Daily/hourly batch pipelines |
# MAGIC | **File arrival** | New file lands in a location | Event-driven ingestion |
# MAGIC | **Table update** | A Delta table is updated | Downstream transformation after upstream writes |
# MAGIC | **Manual** | User or API call | Ad-hoc runs, testing |
# MAGIC | **Continuous** (LDP only) | Always running | Near-real-time streaming |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time-based vs Data-driven Triggers
# MAGIC
# MAGIC **Choose time-based when:**
# MAGIC - Source data arrives on a known schedule (e.g., nightly extract)
# MAGIC - Pipeline SLA is clock-based ("report ready by 07:00")
# MAGIC - Upstream system pushes on a schedule
# MAGIC
# MAGIC **Choose data-driven (file arrival / table update) when:**
# MAGIC - Source data arrives irregularly
# MAGIC - You want to minimise latency (process as soon as data is ready)
# MAGIC - Multiple upstream tables must all be ready before proceeding

# COMMAND ----------

# MAGIC %md
# MAGIC ## For Each Task (Loop)
# MAGIC
# MAGIC Process a dynamic list of items by iterating in a job:
# MAGIC
# MAGIC ```yaml
# MAGIC tasks:
# MAGIC   - task_key: for_each_region
# MAGIC     for_each_task:
# MAGIC       inputs: '["APAC", "EMEA", "AMER"]'
# MAGIC       concurrency: 3      # run 3 iterations in parallel
# MAGIC       task:
# MAGIC         task_key: process_region
# MAGIC         notebook_task:
# MAGIC           notebook_path: ./process_region.py
# MAGIC           base_parameters:
# MAGIC             region: "{{input}}"
# MAGIC ```
