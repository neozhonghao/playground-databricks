# Databricks Data Engineer Associate — Tutorial Repository

Practice notebooks and configs covering every objective in the **Databricks Certified Data Engineer Associate** exam (both the current guide and the new guide effective **May 4, 2026**).

## Repository Layout

```
playground-databricks/
├── 01_platform/            # Platform overview, compute types, Delta Lake, Unity Catalog
├── 02_ingestion/           # Auto Loader, COPY INTO, Lakeflow Connect, JDBC
├── 03_transformation/      # Medallion Architecture, PySpark, DDL/DML, Gold layer
├── 04_lakeflow_jobs/       # Job orchestration, control flow, triggers
├── 05_cicd/                # Git integration, Databricks Asset Bundles, CLI
├── 06_governance/          # Unity Catalog, permissions, Delta Sharing, Lakehouse Federation
├── 07_troubleshooting/     # Spark UI, Liquid Clustering, performance tuning
├── sample_data/            # CSV/JSON fixtures used by notebooks
└── resources/bundles/      # Example DAB (databricks.yml) for dev/test/prod
```

Each `.py` file is a **Databricks notebook** (source format). Import into your workspace via **Repos** or upload directly.

## Exam Section Coverage

| Section | Notebooks |
|---|---|
| Databricks Intelligence Platform | `01_platform/` |
| Data Ingestion & Loading | `02_ingestion/` |
| Data Transformation & Modeling | `03_transformation/` |
| Working with Lakeflow Jobs | `04_lakeflow_jobs/` |
| CI/CD | `05_cicd/` |
| Troubleshooting, Monitoring & Optimization | `07_troubleshooting/` |
| Governance & Security | `06_governance/` |

## Prerequisites

- Databricks workspace (Community Edition works for most notebooks)
- Unity Catalog enabled (required for `06_governance/` notebooks)
- Python 3.10+, PySpark 3.5+

## Quick Start

```bash
# Clone and push to your Databricks Repo
git clone <this-repo>
# In Databricks UI: Repos > Add Repo > paste URL
```
