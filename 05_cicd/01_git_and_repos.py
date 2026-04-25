# Databricks notebook source

# MAGIC %md
# MAGIC # Section 5 — CI/CD: Git Integration & Databricks Repos
# MAGIC
# MAGIC **Exam objectives covered:**
# MAGIC - Manage code development workflow: create/switch branches, commit/push, create PRs
# MAGIC - Identify the difference between DAB and traditional deployment methods
# MAGIC - Identify the structure of Asset Bundles
# MAGIC - Use the Databricks CLI to validate, deploy, and manage bundles

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Git Integration (Repos)
# MAGIC
# MAGIC Databricks Repos connects your workspace to a Git provider (GitHub, GitLab, Azure DevOps, Bitbucket).
# MAGIC
# MAGIC ### Workflow
# MAGIC
# MAGIC | Action | UI location |
# MAGIC |---|---|
# MAGIC | Clone a repo | **Repos** → **Add repo** → paste Git URL |
# MAGIC | Create branch | Repo header → branch name → **Create branch** |
# MAGIC | Switch branch | Repo header → branch name dropdown |
# MAGIC | Commit & push | Repo header → **Git** → stage files → **Commit & push** |
# MAGIC | Create PR | Repo header → **Git** → **Create pull request** (opens Git provider) |
# MAGIC | Pull latest | Repo header → **Git** → **Pull** |
# MAGIC
# MAGIC **Key exam point:** Repos support both **notebook files** and **arbitrary files** (YAML, Python modules, requirements.txt).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Traditional Deployment vs Databricks Asset Bundles (DAB)
# MAGIC
# MAGIC | Aspect | Traditional (manual) | DAB (Declarative Automation Bundles) |
# MAGIC |---|---|---|
# MAGIC | Deployment method | Export/import notebooks via UI or REST API | `databricks bundle deploy` CLI command |
# MAGIC | Config management | Separate scripts per environment | `targets:` block in `databricks.yml` |
# MAGIC | Version control | Notebooks exported as JSON | Source code + YAML in Git |
# MAGIC | Reproducibility | Error-prone, manual | Deterministic, idempotent |
# MAGIC | Environment promotion | Copy/paste, manually edit job configs | Override variables per target |
# MAGIC | Asset types supported | Notebooks, jobs | Jobs, pipelines, dashboards, alerts, models, permissions |

# COMMAND ----------

# MAGIC %md
# MAGIC ## DAB Structure
# MAGIC
# MAGIC A bundle is a directory containing:
# MAGIC
# MAGIC ```
# MAGIC my_bundle/
# MAGIC ├── databricks.yml          ← root bundle config (required)
# MAGIC ├── resources/
# MAGIC │   ├── my_job.yml          ← job definition (can be split from root)
# MAGIC │   └── my_pipeline.yml     ← pipeline definition
# MAGIC ├── src/
# MAGIC │   ├── ingest.py           ← notebook / Python files
# MAGIC │   └── transform.py
# MAGIC └── .databricks/            ← generated; add to .gitignore
# MAGIC ```
# MAGIC
# MAGIC ### Root `databricks.yml` skeleton
# MAGIC
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: billing_pipeline
# MAGIC
# MAGIC variables:
# MAGIC   catalog:
# MAGIC     default: main
# MAGIC   env:
# MAGIC     default: dev
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development        # prefixes resources with username
# MAGIC     default: true
# MAGIC     variables:
# MAGIC       catalog: dev_catalog
# MAGIC       env: dev
# MAGIC   test:
# MAGIC     variables:
# MAGIC       catalog: test_catalog
# MAGIC       env: test
# MAGIC   prod:
# MAGIC     mode: production
# MAGIC     variables:
# MAGIC       catalog: main
# MAGIC       env: prod
# MAGIC     run_as:
# MAGIC       service_principal_name: sp-data-eng-prod
# MAGIC
# MAGIC include:
# MAGIC   - resources/*.yml
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks CLI — Common Bundle Commands
# MAGIC
# MAGIC ```bash
# MAGIC # Install CLI
# MAGIC pip install databricks-cli
# MAGIC # or: brew install databricks (macOS)
# MAGIC
# MAGIC # Authenticate
# MAGIC databricks configure --token
# MAGIC # or: databricks auth login --host https://adb-xxxx.azuredatabricks.net
# MAGIC
# MAGIC # Initialise a new bundle from template
# MAGIC databricks bundle init
# MAGIC
# MAGIC # Validate bundle YAML (catches config errors without deploying)
# MAGIC databricks bundle validate
# MAGIC
# MAGIC # Deploy to the default (dev) target
# MAGIC databricks bundle deploy
# MAGIC
# MAGIC # Deploy to a specific target
# MAGIC databricks bundle deploy --target prod
# MAGIC
# MAGIC # Run a job defined in the bundle
# MAGIC databricks bundle run billing_pipeline_job
# MAGIC
# MAGIC # Destroy all deployed resources for a target
# MAGIC databricks bundle destroy --target dev
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## CI/CD Pipeline Pattern (GitHub Actions)
# MAGIC
# MAGIC ```yaml
# MAGIC # .github/workflows/deploy.yml
# MAGIC name: Deploy Databricks Bundle
# MAGIC on:
# MAGIC   push:
# MAGIC     branches: [main]
# MAGIC
# MAGIC jobs:
# MAGIC   deploy:
# MAGIC     runs-on: ubuntu-latest
# MAGIC     steps:
# MAGIC       - uses: actions/checkout@v4
# MAGIC
# MAGIC       - name: Install Databricks CLI
# MAGIC         run: pip install databricks-cli
# MAGIC
# MAGIC       - name: Validate bundle
# MAGIC         env:
# MAGIC           DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST_PROD }}
# MAGIC           DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_PROD }}
# MAGIC         run: databricks bundle validate --target prod
# MAGIC
# MAGIC       - name: Deploy to prod
# MAGIC         env:
# MAGIC           DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST_PROD }}
# MAGIC           DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN_PROD }}
# MAGIC         run: databricks bundle deploy --target prod
# MAGIC ```
