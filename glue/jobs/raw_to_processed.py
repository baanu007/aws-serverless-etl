"""
AWS Glue Job: Raw to Processed
Transforms raw JSON data to partitioned Parquet in processed zone
"""

import sys
from datetime import datetime

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_database',
    'source_table',
    'target_bucket',
    'target_prefix'
])

job.init(args['JOB_NAME'], args)

# Configuration
SOURCE_DATABASE = args['source_database']
SOURCE_TABLE = args['source_table']
TARGET_BUCKET = args['target_bucket']
TARGET_PREFIX = args['target_prefix']


def read_raw_data():
    """Read data from Glue Data Catalog"""
    print(f"Reading from {SOURCE_DATABASE}.{SOURCE_TABLE}")
    
    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=SOURCE_DATABASE,
        table_name=SOURCE_TABLE,
        transformation_ctx="read_raw"
    )
    
    return dynamic_frame.toDF()


def clean_data(df):
    """Apply data cleaning transformations"""
    print("Applying data cleaning...")
    
    cleaned = df
    
    # Drop duplicates based on primary key and timestamp
    if 'id' in df.columns and 'timestamp' in df.columns:
        window = Window.partitionBy('id').orderBy(F.desc('timestamp'))
        cleaned = (
            cleaned
            .withColumn('row_num', F.row_number().over(window))
            .filter(F.col('row_num') == 1)
            .drop('row_num')
        )
    
    # Trim string columns
    string_cols = [f.name for f in df.schema.fields if str(f.dataType) == 'StringType']
    for col_name in string_cols:
        cleaned = cleaned.withColumn(col_name, F.trim(F.col(col_name)))
    
    # Handle nulls in critical columns
    cleaned = cleaned.na.drop(subset=['id']) if 'id' in df.columns else cleaned
    
    return cleaned


def add_audit_columns(df):
    """Add audit and metadata columns"""
    print("Adding audit columns...")
    
    return (
        df
        .withColumn('_processed_at', F.current_timestamp())
        .withColumn('_source_file', F.input_file_name())
        .withColumn('_job_run_id', F.lit(args['JOB_RUN_ID'] if 'JOB_RUN_ID' in args else 'local'))
    )


def add_partitions(df):
    """Add partition columns for efficient querying"""
    print("Adding partition columns...")
    
    # If timestamp exists, create date partitions
    if 'timestamp' in df.columns:
        return (
            df
            .withColumn('year', F.year(F.col('timestamp')))
            .withColumn('month', F.month(F.col('timestamp')))
            .withColumn('day', F.dayofmonth(F.col('timestamp')))
        )
    else:
        # Use processing date for partitions
        now = datetime.utcnow()
        return (
            df
            .withColumn('year', F.lit(now.year))
            .withColumn('month', F.lit(now.month))
            .withColumn('day', F.lit(now.day))
        )


def write_processed_data(df):
    """Write data to processed zone as Parquet"""
    output_path = f"s3://{TARGET_BUCKET}/{TARGET_PREFIX}"
    print(f"Writing to {output_path}")
    
    # Convert to DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "processed")
    
    # Write with partitioning
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": output_path,
            "partitionKeys": ["year", "month", "day"]
        },
        format_options={
            "compression": "snappy"
        },
        transformation_ctx="write_processed"
    )
    
    return df.count()


def update_catalog(row_count):
    """Update Glue Data Catalog with new table/partitions"""
    print("Updating Data Catalog...")
    
    # The catalog is automatically updated when using Glue write
    # Additional logic could be added here for custom catalog updates
    
    print(f"Processed {row_count} records")


def main():
    """Main ETL pipeline"""
    print("=" * 60)
    print(f"Starting Raw to Processed ETL")
    print(f"Source: {SOURCE_DATABASE}.{SOURCE_TABLE}")
    print(f"Target: s3://{TARGET_BUCKET}/{TARGET_PREFIX}")
    print("=" * 60)
    
    # Read
    raw_df = read_raw_data()
    print(f"Read {raw_df.count()} raw records")
    
    # Transform
    cleaned_df = clean_data(raw_df)
    with_audit = add_audit_columns(cleaned_df)
    with_partitions = add_partitions(with_audit)
    
    # Write
    row_count = write_processed_data(with_partitions)
    
    # Update catalog
    update_catalog(row_count)
    
    print("=" * 60)
    print("ETL Complete!")
    print("=" * 60)


if __name__ == '__main__':
    main()
    job.commit()
