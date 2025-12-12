import os
import pandas as pd

from google.cloud import bigquery
from google.cloud import bigquery_storage

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "keys/key_bq_githubpj-54196716a587.json"

# Load org IDs
org_ids = pd.read_csv("org_id_only.csv", dtype={"org_id": "Int64"})["org_id"].tolist()

# Build query
query = """
-- Create a virtual table with months to ensure all months are represented
WITH months AS (
  SELECT
    FORMAT_DATE('%Y-%m', month) AS month
  FROM UNNEST(
    GENERATE_DATE_ARRAY(
      DATE '2015-01-01',
      DATE '2025-11-01',
      INTERVAL 1 MONTH
    )
  ) AS month
),

-- Extract relevant fields from raw GitHub Archive data
events AS (
  SELECT
    org.id AS org_id,
    actor.id AS actor_id,
    repo.id AS repo_id,
    type,
    created_at,
    payload,
    JSON_VALUE(payload, '$.ref_type') AS ref_type,
    JSON_VALUE(payload, '$.action') AS action,
    JSON_VALUE(payload, '$.pull_request.merged') AS merged,
    SAFE_CAST(JSON_VALUE(payload, '$.distinct_size') AS INT64) AS distinct_size
  FROM `githubarchive.month.*`
  WHERE _TABLE_SUFFIX BETWEEN '201501' AND '202511'
    AND org.id IN UNNEST(@org_ids)
),

-- Base table to avoid repeating transformations
base AS (
  SELECT
    org_id,
    FORMAT_DATE('%Y-%m', DATE(created_at)) AS month,
    type,
    actor_id,
    repo_id,
    ref_type,
    action,
    merged,
    distinct_size
  FROM events
),

-- Push events per month - ok
pushes AS (
  SELECT
    org_id,
    month,
    COUNT(*) AS push_requests
  FROM base
  WHERE type = 'PushEvent'
  GROUP BY org_id, month
),

-- Commits per month - ok
commits AS (
  SELECT
    org_id,
    month,
    SUM(IFNULL(distinct_size, 0)) AS commit_count
  FROM base
  WHERE type = 'PushEvent'
  GROUP BY org_id, month
),

-- Active developers per month - ok
active_devs AS (
  SELECT
    org_id,
    month,
    COUNT(DISTINCT actor_id) AS active_developers
  FROM base
  GROUP BY org_id, month
),

-- Fork events per month - ok
forks AS (
  SELECT
    org_id,
    month,
    COUNT(*) AS forks
  FROM base
  WHERE type = 'ForkEvent'
  GROUP BY org_id, month
),

-- Forks per repository per month - ok
forks_per_repo AS (
  SELECT
    org_id,
    month,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT repo_id)) AS forks_per_repo
  FROM base
  WHERE type = 'ForkEvent'
  GROUP BY org_id, month
),

-- Stars per month - ok
stars AS (
  SELECT
    org_id,
    month,
    COUNT(*) AS stars
  FROM base
  WHERE type = 'WatchEvent'
  GROUP BY org_id, month
),

-- Active repositories per month - ok
active_repos AS (
  SELECT
    org_id,
    month,
    COUNT(DISTINCT repo_id) AS active_repos
  FROM base
  GROUP BY org_id, month
),

-- Public repositories created per month - ok
public_repos AS (
  SELECT
    org_id,
    month,
    COUNT(*) AS public_repos
  FROM base
  WHERE type = 'CreateEvent'
    AND ref_type = 'repository'
  GROUP BY org_id, month
),

-- Pull requests created per month - ok
pr_created AS (
  SELECT
    org_id,
    month,
    COUNT(*) AS pull_requests_created
  FROM base
  WHERE type = 'PullRequestEvent'
    AND action = 'opened'
  GROUP BY org_id, month
),

-- Pull requests merged per month - ok
pr_merged AS (
  SELECT
    org_id,
    month,
    COUNT(*) AS pull_requests_merged
  FROM base
  WHERE type = 'PullRequestEvent'
    AND merged = 'true'
  GROUP BY org_id, month
)

-- Final monthly metrics per organization
SELECT
  org_id,
  m.month,
  IFNULL(p.push_requests, 0) AS push_requests_per_month,
  IFNULL(c.commit_count, 0) AS commits_per_month,
  IFNULL(ad.active_developers, 0) AS active_developers_per_month,
  IFNULL(f.forks, 0) AS forks_per_month,
  IFNULL(fpr.forks_per_repo, 0) AS forks_per_repo_per_month,
  IFNULL(s.stars, 0) AS total_stars_per_month,
  IFNULL(ar.active_repos, 0) AS active_repos_per_month,
  IFNULL(prp.public_repos, 0) AS public_repos_per_month,
  IFNULL(prc.pull_requests_created, 0) AS pull_requests_created_per_month,
  IFNULL(prm.pull_requests_merged, 0) AS pull_requests_merged_per_month
FROM UNNEST(@org_ids) AS org_id
CROSS JOIN months AS m
LEFT JOIN pushes p
  ON p.org_id = org_id AND p.month = m.month
LEFT JOIN commits c
  ON c.org_id = org_id AND c.month = m.month
LEFT JOIN active_devs ad
  ON ad.org_id = org_id AND ad.month = m.month
LEFT JOIN forks f
  ON f.org_id = org_id AND f.month = m.month
LEFT JOIN forks_per_repo fpr
  ON fpr.org_id = org_id AND fpr.month = m.month
LEFT JOIN stars s
  ON s.org_id = org_id AND s.month = m.month
LEFT JOIN active_repos ar
  ON ar.org_id = org_id AND ar.month = m.month
LEFT JOIN public_repos prp
  ON prp.org_id = org_id AND prp.month = m.month
LEFT JOIN pr_created prc
  ON prc.org_id = org_id AND prc.month = m.month
LEFT JOIN pr_merged prm
  ON prm.org_id = org_id AND prm.month = m.month
ORDER BY org_id, m.month;
"""

# Configure parameters
job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ArrayQueryParameter("org_ids", "INT64", org_ids)
    ]
)

# Run
client = bigquery.Client()

bqstorage_client = bigquery_storage.BigQueryReadClient()

df = client.query(query, job_config=job_config).to_dataframe(
    bqstorage_client=bqstorage_client)

df.to_parquet("gitarchive_resultsjan2015_nov2025.parquet")
print(df)