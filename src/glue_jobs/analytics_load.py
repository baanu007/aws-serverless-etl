"""AWS Glue PySpark job: analytics_load.

Reads NDJSON files from the ``processed/`` prefix on S3, applies a few
analytics-friendly transformations, and writes the result as
partitioned Parquet to the curated zone. A second writer pushes the
same DataFrame into Redshift via a Glue JDBC connection if the
``--redshift_connection`` argument is provided.

The job is parameterized so it can be reused for any source/table
combination without code changes.

Job arguments
-------------
--JOB_NAME                  (required, set by Glue)
--source_bucket             S3 bucket containing the NDJSON input
--source_key                Specific NDJSON key, OR
--source_prefix             A prefix to read recursively
--target_bucket             Bucket to write Parquet to
--target_prefix             Prefix for Parquet output (default: curated/)
--redshift_connection       (optional) Glue connection name
--redshift_table            (optional) target table, e.g. analytics.fact_events
--partition_keys            Comma-separated partition columns (default: dt)
"""

from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


REQUIRED_ARGS = ["JOB_NAME", "source_bucket", "target_bucket"]
OPTIONAL_ARGS = [
    "source_key",
    "source_prefix",
    "target_prefix",
    "redshift_connection",
    "redshift_table",
    "partition_keys",
]


def _resolve_args() -> dict:
    """Resolve required + optional args.

    Glue's ``getResolvedOptions`` raises if any listed argument is missing,
    so optional args are checked individually via ``sys.argv``.
    """
    parsed = getResolvedOptions(sys.argv, REQUIRED_ARGS)
    raw_argv = " ".join(sys.argv)
    for opt in OPTIONAL_ARGS:
        flag = f"--{opt}"
        if flag in raw_argv:
            parsed.update(getResolvedOptions(sys.argv, [opt]))
    return parsed


def _build_source_paths(args: dict) -> list[str]:
    bucket = args["source_bucket"]
    if "source_key" in args:
        return [f"s3://{bucket}/{args['source_key']}"]
    prefix = args.get("source_prefix", "processed/").lstrip("/")
    return [f"s3://{bucket}/{prefix}"]


def _apply_transformations(df):
    """Project, cast, and enrich the DataFrame for analytics consumption."""
    enriched = df.withColumn("_loaded_at", F.current_timestamp())
    if "dt" not in df.columns and "_processed_at" in df.columns:
        enriched = enriched.withColumn(
            "dt", F.to_date(F.col("_processed_at"))
        )
    # Drop fully-null columns to keep the curated schema tight.
    non_null_cols = [
        c for c in enriched.columns
        if enriched.filter(F.col(c).isNotNull()).limit(1).count() > 0
    ]
    return enriched.select(*non_null_cols)


def main() -> None:
    args = _resolve_args()

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    source_paths = _build_source_paths(args)
    print(f"[analytics_load] reading from {source_paths}")
    df = spark.read.json(source_paths)
    print(f"[analytics_load] input row count: {df.count()}")

    transformed = _apply_transformations(df)

    partition_keys = [
        p.strip() for p in args.get("partition_keys", "dt").split(",") if p.strip()
    ]
    partition_keys = [p for p in partition_keys if p in transformed.columns]

    target_prefix = args.get("target_prefix", "curated/").strip("/")
    target_path = f"s3://{args['target_bucket']}/{target_prefix}/"
    print(f"[analytics_load] writing parquet to {target_path}")

    dyf = DynamicFrame.fromDF(transformed, glue_context, "curated")
    glue_context.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="s3",
        connection_options={
            "path": target_path,
            "partitionKeys": partition_keys,
        },
        format="glueparquet",
        format_options={"compression": "snappy"},
        transformation_ctx="write_curated",
    )

    # Optional Redshift load via a pre-configured Glue connection.
    if "redshift_connection" in args and "redshift_table" in args:
        print(
            f"[analytics_load] loading into Redshift table {args['redshift_table']} "
            f"via connection {args['redshift_connection']}"
        )
        glue_context.write_dynamic_frame.from_jdbc_conf(
            frame=dyf,
            catalog_connection=args["redshift_connection"],
            connection_options={
                "dbtable": args["redshift_table"],
                "database": "analytics",
            },
            redshift_tmp_dir=f"s3://{args['target_bucket']}/_tmp/redshift/",
            transformation_ctx="write_redshift",
        )

    job.commit()
    print("[analytics_load] job complete")


if __name__ == "__main__":
    main()
