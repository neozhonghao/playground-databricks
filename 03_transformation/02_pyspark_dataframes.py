# Databricks notebook source

# MAGIC %md
# MAGIC # Section 3 — PySpark DataFrames
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Compute complex aggregations and metrics with PySpark DataFrames
# MAGIC - Combine DataFrames: inner join, left join, broadcast join, cross join, union
# MAGIC - Manipulate columns: add, drop, split, rename, filter, explode arrays
# MAGIC - Data deduplication and aggregate operations

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Sample billing data (matches the exam sample question)
billing_data = [
    (401, "p001", "Cardiology", "2024-03-01", 1500.0, 1),
    (402, "p002", "Radiology",  "2024-03-02", 3000.0, 1),
    (403, "p001", "Cardiology", "2024-03-01", 6500.0, 1),
    (404, "p003", "Radiology",  "2024-03-03",  500.0, 1),
    (404, "p003", "Radiology",  "2024-03-03",  500.0, 1),   # duplicate
]

billing_schema = StructType([
    StructField("billing_id",   IntegerType()),
    StructField("patient_id",   StringType()),
    StructField("department",   StringType()),
    StructField("billing_date", StringType()),
    StructField("amount_billed",DoubleType()),
    StructField("quantity",     IntegerType()),
])

billing_df = spark.createDataFrame(billing_data, billing_schema)
display(billing_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregations — Exam Question Pattern

# COMMAND ----------

# Correct answer (Q1 from exam): sum(amount_billed) + count_distinct(billing_id)
daily_revenue_df = billing_df.groupBy("billing_date").agg(
    F.sum("amount_billed").alias("total_revenue"),
    F.count_distinct("billing_id").alias("total_invoices"),
    F.count_distinct("patient_id").alias("unique_patients"),
    F.avg("amount_billed").alias("avg_invoice"),
    F.max("amount_billed").alias("max_invoice"),
)
display(daily_revenue_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Approximate Count Distinct (scalable for big data)

# COMMAND ----------

# approx_count_distinct is much faster on large datasets; uses HyperLogLog
approx_df = billing_df.agg(
    F.approx_count_distinct("patient_id", rsd=0.05).alias("approx_unique_patients")
)
display(approx_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## summary() and describe()

# COMMAND ----------

# summary returns count, mean, stddev, min, 25%, 50%, 75%, max
billing_df.select("amount_billed", "quantity").summary().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column Manipulation

# COMMAND ----------

transformed_df = (
    billing_df
    # Add computed column
    .withColumn("total_amount", F.col("amount_billed") * F.col("quantity"))
    # Rename column
    .withColumnRenamed("billing_date", "invoice_date")
    # Drop column
    .drop("quantity")
    # Conditional column
    .withColumn("tier",
        F.when(F.col("amount_billed") >= 3000, "high")
         .when(F.col("amount_billed") >= 1000, "medium")
         .otherwise("low")
    )
    # Cast column type
    .withColumn("invoice_date", F.to_date(F.col("invoice_date"), "yyyy-MM-dd"))
    # Filter
    .filter(F.col("total_amount") > 1000)
)
display(transformed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split a String Column

# COMMAND ----------

from pyspark.sql.functions import split, element_at

names_df = spark.createDataFrame([("John Doe",), ("Jane Smith",)], ["full_name"])

split_df = (
    names_df
    .withColumn("parts",      split(F.col("full_name"), " "))
    .withColumn("first_name", element_at(F.col("parts"), 1))
    .withColumn("last_name",  element_at(F.col("parts"), 2))
    .drop("parts")
)
display(split_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explode Arrays

# COMMAND ----------

orders_data = [
    ("o1", ["item_a", "item_b", "item_c"]),
    ("o2", ["item_d"]),
]
orders_df = spark.createDataFrame(orders_data, ["order_id", "items"])

exploded_df = orders_df.select("order_id", F.explode("items").alias("item"))
display(exploded_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joins

# COMMAND ----------

customers_data = [("p001", "Alice"), ("p002", "Bob"), ("p004", "Charlie")]
customers_df = spark.createDataFrame(customers_data, ["patient_id", "name"])

# Inner join — only matching rows
inner_df = billing_df.join(customers_df, on="patient_id", how="inner")

# Left join — all billing rows, NULLs for unmatched customers
left_df = billing_df.join(customers_df, on="patient_id", how="left")

# Broadcast join — small table broadcast to all executors (avoids shuffle)
broadcast_df = billing_df.join(
    F.broadcast(customers_df), on="patient_id", how="inner"
)

# Cross join — Cartesian product (use with caution!)
cross_df = billing_df.crossJoin(customers_df)
print(f"Cross join row count: {cross_df.count()}")  # billing_rows × customer_rows

# Multiple key join
multi_key_df = billing_df.join(
    customers_df,
    on=["patient_id"],   # list for multiple keys
    how="inner"
)

display(broadcast_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union vs Union All

# COMMAND ----------

df_a = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
df_b = spark.createDataFrame([(2, "b"), (3, "c")], ["id", "val"])

# union: removes duplicates (like SQL UNION)
unioned = df_a.union(df_b).distinct()

# unionAll (alias of union without .distinct()): keeps duplicates (like SQL UNION ALL)
union_all = df_a.unionAll(df_b)

print(f"union distinct rows: {unioned.count()}")       # 3
print(f"unionAll total rows: {union_all.count()}")     # 4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplication

# COMMAND ----------

# Full deduplication
deduped_df = billing_df.dropDuplicates()

# Deduplicate on specific columns (keep first occurrence)
deduped_key_df = billing_df.dropDuplicates(["billing_id", "patient_id"])

# MAGIC %md
# MAGIC ### Window-based deduplication (keep the latest record per key)

# COMMAND ----------

w = Window.partitionBy("billing_id").orderBy(F.col("billing_date").desc())

deduped_latest = (
    billing_df
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
display(deduped_latest)
