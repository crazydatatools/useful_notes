SELECT
--  SPLIT(TRIM(SPLIT(query, '*/')[OFFSET(0)],'/*'))[OFFSET(1)] AS query_id,
 SPLIT(SPLIT(TRIM(SPLIT(query, '*/')[OFFSET(0)],'/*'))[OFFSET(1)], '_')[OFFSET(0)] AS complexity,
 COUNT(1)
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  DATE(creation_time) = CURRENT_DATE()  -- Partitioning column
  AND project_id = 'YOUR_PROJECT'       -- Clustering column
  AND SPLIT(TRIM(SPLIT(query, '*/')[OFFSET(0)],'/*'))[OFFSET(0)] = 'jmeter_jdbc_test'
GROUP BY 1


SELECT
  SPLIT(labels[OFFSET(1)].value, '_')[OFFSET(0)] AS complexity,
  COUNT(1)
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  DATE(creation_time) = CURRENT_DATE()  -- Partitioning column
  AND project_id = 'YOUR_PROJECT'       -- Clustering column
  AND ARRAY_LENGTH(labels) > 0
  AND EXISTS (
    SELECT *
    FROM UNNEST(labels) AS labels
    WHERE
      labels.key = 'run_id'
      AND labels.value = 'jmeter_http_test'
  )
GROUP BY 1



WITH
  test_run_queries AS (
  SELECT
    *,
    JSON_EXTRACT_ARRAY(REGEXP_EXTRACT(query, r"^/\*(.*)\*/"),
      '$') AS jmeter_metadata_rows
  FROM
    `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  WHERE
    DATE(creation_time) = CURRENT_DATE() AND
    REGEXP_CONTAINS(query, r'^/\*.*\*/')
    AND query LIKE '%"test_plan": "BigQuery-BI-and-ELT.jmx"%'),
  test_run_queries_with_metadata_columns AS (
  SELECT
    -- Extract specific fields from the metadata JSON passed above.
    JSON_EXTRACT_SCALAR(jmeter_metadata,
      '$.run_id') AS jmeter_run_id,
    JSON_EXTRACT_SCALAR(jmeter_metadata,
      '$.query_id') AS jmeter_query_id,
    JSON_EXTRACT_SCALAR(jmeter_metadata,
      '$.num_users') AS jmeter_users,
    SAFE_CAST(JSON_EXTRACT_SCALAR(jmeter_metadata,
        '$.num_slots') AS INT64) AS num_slots,
    JSON_EXTRACT_SCALAR(jmeter_metadata,
      '$.test_plan') AS test_plan,
    JSON_EXTRACT_SCALAR(jmeter_metadata,
      '$.thread_group_name') AS thread_group_name,
    JSON_EXTRACT_SCALAR(jmeter_metadata,
      '$.thread_num') AS thread_number,
    * EXCEPT(jmeter_metadata)
  FROM
    test_run_queries,
    UNNEST(jmeter_metadata_rows) AS jmeter_metadata
  WHERE
    jmeter_metadata_rows IS NOT NULL)

SELECT
  jmeter_run_id, thread_group_name, jmeter_users,
  TIMESTAMP_DIFF(MAX(end_time), MIN(creation_time), MILLISECOND) AS test_run_elapsed_millis
FROM
  test_run_queries_with_metadata_columns
GROUP BY
  jmeter_run_id, jmeter_users, thread_group_name
ORDER BY
  jmeter_run_id, jmeter_users, thread_group_name

 prolific_users	SELECT username, COUNT(*) AS c FROM `bigquery-public-data.geo_openstreetmap.planet_relations` AS relations GROUP BY username ORDER BY c DESC LIMIT 10
fire_hydrants	SELECT id, version, all_tags FROM `bigquery-public-data.geo_openstreetmap.history_nodes` AS node JOIN UNNEST(all_tags) AS tags WHERE (tags.key = 'emergency' AND tags.value = 'fire_hydrant') AND id NOT IN ( SELECT id FROM `bigquery-public-data.geo_openstreetmap.planet_nodes` AS node JOIN UNNEST(all_tags) AS tags WHERE (tags.key = 'emergency' AND tags.value = 'fire_hydrant') )
singapore_edits	WITH singapore AS ( SELECT ST_MAKEPOLYGON(ST_MAKELINE( [ST_GEOGPOINT(103.6920359,1.1304753),ST_GEOGPOINT(104.0120359,1.1304753), ST_GEOGPOINT(104.0120359,1.4504753),ST_GEOGPOINT(103.6920359,1.4504753) ])) AS boundingbox) SELECT DISTINCT changeset.id FROM `bigquery-public-data.geo_openstreetmap.history_changesets` AS changeset JOIN UNNEST(nodes) AS cnode, `bigquery-public-data.geo_openstreetmap.history_nodes` AS nodes, singapore WHERE cnode = nodes.id AND ST_INTERSECTS(singapore.boundingbox, nodes.geometry)

SELECT
   job_id,
   job_type,
   start_time,
   end_time,
   query,
   total_bytes_processed,
   total_slot_ms,
   destination_table
   state,
   error_result,
   (SELECT value FROM UNNEST(labels) WHERE key = "component") as component,
   (SELECT value FROM UNNEST(labels) WHERE key = "cloud-function-name") as cloud_function_name,
   (SELECT value FROM UNNEST(labels) WHERE key = "batch-id") as batch_id,
FROM
   `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
   (SELECT value FROM UNNEST(labels) WHERE key = "component") = "event-based-gcs-ingest"


##cancel_running_jobs.py
from argparse import ArgumentParser
from google.cloud import bigquery


def cancel_jobs(client):
    for job in client.list_jobs(all_users=True, state_filter="RUNNING"):
        client.cancel_job(job.job_id, location='us')


def get_cmd_line_args():
    parser = ArgumentParser()
    parser.add_argument(
        '--project_id',
        help='Project in which all running BigQuery jobs will be cancelled.')
    return parser.parse_args()


def main():
    args = get_cmd_line_args()
    cancel_jobs(bigquery.Client(project=args.project_id))


if __name__ == '__main__':
    main()
