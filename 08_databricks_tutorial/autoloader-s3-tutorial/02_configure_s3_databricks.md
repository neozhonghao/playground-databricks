# S3 + Databricks Configuration Guide

This guide covers two approaches to connect Databricks to S3, then shows you
how to verify the connection before running Autoloader.

---

## Architecture Overview

```
Local Machine                 AWS S3                    Databricks
──────────────      ──────────────────────────      ─────────────────────
01_generate*.py  →  s3://your-bucket/orders-raw/  →  Autoloader stream
(boto3 upload)      (NDJSON files land here)          (cloudFiles format)
```

---

## Step 1: Create an S3 Bucket

```bash
# Via AWS CLI
aws s3 mb s3://your-databricks-autoloader-demo --region us-east-1

# Enable versioning (optional but good practice)
aws s3api put-bucket-versioning \
  --bucket your-databricks-autoloader-demo \
  --versioning-configuration Status=Enabled
```

Or do it in the AWS Console: **S3 → Create bucket → uncheck "Block all public
access" is NOT required** (keep it blocked, Databricks uses IAM, not public
URLs).

---

## Step 2: IAM — Picking Your Auth Strategy

### Option A: IAM Role via Instance Profile (Recommended for Production)

This is the zero-credential approach. The Databricks cluster assumes an IAM
role with S3 permissions automatically.

**2A-1. Create an IAM Role**

In the AWS Console → IAM → Roles → Create Role:
- Trusted entity: **EC2**
- Add the following inline policy (or attach `AmazonS3FullAccess` for a demo):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::your-databricks-autoloader-demo",
        "arn:aws:s3:::your-databricks-autoloader-demo/*"
      ]
    }
  ]
}
```

Name the role: `databricks-s3-demo-role`

**2A-2. Create an Instance Profile**

In the Databricks workspace:

1. Go to **Settings → Security → Instance Profiles → Add**
2. Paste the ARN of `databricks-s3-demo-role`
3. Enable the checkbox "IAM Role has an associated instance profile"

**2A-3. Attach the Instance Profile to a Cluster**

When creating/editing a cluster:
- **Advanced Options → IAM Role** → select `databricks-s3-demo-role`

No credentials needed in code — S3 access works automatically.

---

### Option B: Access Keys via Spark Config (Quick Dev/Lab Setup)

Use this only for personal dev/sandbox clusters. Never commit keys to Git.

**2B-1. Add Spark config to your cluster:**

In Databricks cluster → **Advanced Options → Spark → Spark Config**, add:

```
spark.hadoop.fs.s3a.access.key  YOUR_ACCESS_KEY_ID
spark.hadoop.fs.s3a.secret.key  YOUR_SECRET_ACCESS_KEY
```

Or pass them at runtime in a notebook (load from Databricks Secrets):

```python
# In Databricks notebook — retrieve from Databricks Secret Scope
access_key = dbutils.secrets.get(scope="aws", key="s3_access_key")
secret_key = dbutils.secrets.get(scope="aws", key="s3_secret_key")

spark.conf.set("fs.s3a.access.key", access_key)
spark.conf.set("fs.s3a.secret.key", secret_key)
```

**2B-2. Create a Databricks Secret Scope (to avoid hardcoding keys)**

```bash
# Using Databricks CLI
databricks secrets create-scope aws
databricks secrets put-secret aws s3_access_key --string-value "AKIAIOSFODNN7EXAMPLE"
databricks secrets put-secret aws s3_secret_key --string-value "wJalrXUtnFEMI/..."
```

---

## Step 3: Unity Catalog External Location (Recommended Modern Approach)

If your workspace uses Unity Catalog, the cleanest approach is to define an
**External Location** pointing to your S3 bucket, then reference it with
`s3://` paths directly.

1. **Storage Credential** — wraps your IAM Role ARN
   - Unity Catalog → External Data → Storage Credentials → Create
   - Paste the IAM Role ARN

2. **External Location** — maps a UC path to S3
   - Unity Catalog → External Data → External Locations → Create
   - URL: `s3://your-databricks-autoloader-demo/`
   - Credential: the one you just created

3. In your notebook, access files with:
   ```python
   s3_path = "s3://your-databricks-autoloader-demo/autoloader-tutorial/orders-raw/"
   ```

---

## Step 4: Verify the Connection

Run this in a Databricks notebook cell to confirm access before running
Autoloader:

```python
# List files in S3 prefix
files = dbutils.fs.ls("s3://your-databricks-autoloader-demo/autoloader-tutorial/orders-raw/")
display(files)
```

If you see your uploaded `.json` files, you're good to go.

---

## Quick Reference: S3 Path Formats in Databricks

| Format          | When to Use                                      |
|-----------------|--------------------------------------------------|
| `s3://bucket/`  | Unity Catalog External Location or IAM Role     |
| `s3a://bucket/` | Legacy — Spark's native Hadoop S3A connector    |
| `dbfs:/mnt/…`   | Legacy — mounted S3 bucket via `dbutils.fs.mount` |

For new workspaces on Unity Catalog, always use `s3://`.

---

## Troubleshooting

| Error | Fix |
|-------|-----|
| `AmazonS3Exception: Access Denied` | IAM role missing S3 permissions or not attached to cluster |
| `Path does not exist` | Wrong S3 prefix, or files not yet uploaded |
| `Schema mismatch` | Delete checkpoint location and restart stream |
| `No files to process` | Autoloader checks every trigger interval — wait or add files |
