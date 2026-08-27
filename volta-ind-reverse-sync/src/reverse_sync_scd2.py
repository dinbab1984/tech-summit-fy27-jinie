# Databricks notebook source
# DBTITLE 1,Reverse Sync: Lakebase work_orders_app → Delta SCD-2
# Reverse Sync: Lakebase Postgres work_orders_app → Delta SCD-2
# Scheduled by Declarative Automation Bundle (volta-ind-reverse-sync).
# Reads current state from production Postgres, applies SCD Type 2
# MERGE into Unity Catalog Delta table.
#
# Parameters (passed by the DAB job):
#   lakebase_project_id, catalog, schema, postgres_database

import psycopg
import pandas as pd
from databricks.sdk import WorkspaceClient

# --- Parameters ---
dbutils.widgets.text("lakebase_project_id", "volta-ind")
dbutils.widgets.text("catalog", "tech_summit_jinie_group_catalog")
dbutils.widgets.text("schema", "default")
dbutils.widgets.text("postgres_database", "databricks_postgres")

PROJECT_ID = dbutils.widgets.get("lakebase_project_id")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DATABASE = dbutils.widgets.get("postgres_database")
SCD2_TABLE = f"{CATALOG}.{SCHEMA}.work_orders_app_scd2"
PROD_ENDPOINT = f"projects/{PROJECT_ID}/branches/production/endpoints/primary"

w = WorkspaceClient()

# --- 1. Ensure SCD-2 target exists ---
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SCD2_TABLE} (
    id STRING,
    line_id STRING,
    action_type STRING,
    part_id STRING,
    drafted_wo STRING,
    predicted_downtime_cost_avoided_usd DOUBLE,
    status STRING,
    approved_by STRING,
    audit_trail STRING,
    created_at TIMESTAMP,
    decided_at TIMESTAMP,
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN NOT NULL
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
print(f"\u2713 SCD-2 table: {SCD2_TABLE}")

# --- 2. Read from Postgres ---
ep = w.postgres.get_endpoint(name=PROD_ENDPOINT)
host = ep.status.hosts.host
user = w.current_user.me().user_name
token = w.postgres.generate_database_credential(endpoint=PROD_ENDPOINT).token

with psycopg.connect(host=host, dbname=DATABASE, user=user,
                     password=token, sslmode="require") as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, line_id, action_type, part_id, drafted_wo,
                   predicted_downtime_cost_avoided_usd, status, approved_by,
                   audit_trail::text AS audit_trail, created_at, decided_at
            FROM app.work_orders_app
        """)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]

if not rows:
    print("\u2139 No work orders — nothing to sync")
    dbutils.notebook.exit("NO_DATA")

pdf = pd.DataFrame(rows, columns=columns)
staging_df = spark.createDataFrame(pdf)
staging_df.createOrReplaceTempView("wo_staging")
print(f"  Staging: {len(pdf)} rows")

# --- 3. SCD-2 MERGE ---
# Step A: Close changed records
spark.sql(f"""
MERGE INTO {SCD2_TABLE} t
USING wo_staging s
ON t.id = s.id AND t.is_current = true
WHEN MATCHED AND (
    t.status       IS DISTINCT FROM s.status
    OR t.approved_by IS DISTINCT FROM s.approved_by
    OR t.decided_at  IS DISTINCT FROM s.decided_at
    OR t.drafted_wo  IS DISTINCT FROM s.drafted_wo
    OR t.audit_trail IS DISTINCT FROM s.audit_trail
)
THEN UPDATE SET
    effective_to = current_timestamp(),
    is_current   = false
""")

# Step B: Insert new/changed records as current
spark.sql(f"""
INSERT INTO {SCD2_TABLE}
SELECT
    s.id, s.line_id, s.action_type, s.part_id, s.drafted_wo,
    s.predicted_downtime_cost_avoided_usd, s.status, s.approved_by,
    s.audit_trail, s.created_at, s.decided_at,
    current_timestamp() AS effective_from,
    CAST(NULL AS TIMESTAMP) AS effective_to,
    true AS is_current
FROM wo_staging s
WHERE NOT EXISTS (
    SELECT 1 FROM {SCD2_TABLE} t
    WHERE t.id = s.id AND t.is_current = true
)
""")

# --- 4. Summary ---
stats = spark.sql(f"""
SELECT count(*) AS total,
       sum(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows,
       sum(CASE WHEN NOT is_current THEN 1 ELSE 0 END) AS history_rows
FROM {SCD2_TABLE}
""").collect()[0]

result = f"total={stats.total}, current={stats.current_rows}, history={stats.history_rows}"
print(f"\u2705 SCD-2 sync complete: {result}")
dbutils.notebook.exit(result)

# COMMAND ----------

