"""
Common Spark / S3 helpers used by every ETL job in this pipeline.

PATH CONVENTION: We always use s3:// (NOT s3a://). Inside Spark we configure
fs.s3.impl = S3AFileSystem so the S3A driver handles s3:// URLs; this keeps the
URL scheme compatible with Trino's native S3 filesystem.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# Single source of truth for zone roots — every job imports these.
RAW_BASE       = "s3://raw"
PROCESSED_BASE = "s3://processed"
CURATED_BASE   = "s3://curated"
DLQ_BASE       = "s3://dlq"


def get_spark(app_name="DataLakeJob"):
    """
    Return a SparkSession configured for MinIO (S3) + Hive Metastore.
    Most settings come from /opt/bitnami/spark/conf/spark-defaults.conf;
    we set a few extras here in case the conf file isn't mounted.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        # Map s3:// to S3A driver (matches spark-defaults.conf)
        .config("spark.hadoop.fs.s3.impl",                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint",               "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",             "admin")
        .config("spark.hadoop.fs.s3a.secret.key",             "admin123456")
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Hive Metastore
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.sql.warehouse.dir", "s3://warehouse/")
        # Performance
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .enableHiveSupport()
        .getOrCreate()
    )


def add_lineage_columns(df, source_file: str):
    """Add lineage columns to every row so we can trace back to raw source."""
    return (df
            .withColumn("_source_file",      F.lit(source_file))
            .withColumn("_ingested_at",      F.current_timestamp())
            .withColumn("_pipeline_version", F.lit("1.1.0")))


def write_parquet(df, path, mode="overwrite", partition_by=None):
    """Write a DataFrame to Parquet at the given s3:// path."""
    writer = df.write.mode(mode).format("parquet")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    print(f"  ✓ wrote {df.count():,} rows to {path}")


def null_pct_per_column(df, exclude_internal=True):
    """
    Compute null percentage for every column in ONE pass over the data,
    not N passes. Returns a dict {col_name: pct_null}.
    """
    cols = [c for c in df.columns if not (exclude_internal and c.startswith("_"))]
    if not cols:
        return {}
    total = df.count()
    if total == 0:
        return {c: 0.0 for c in cols}
    aggs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in cols]
    row = df.agg(*aggs).collect()[0].asDict()
    return {c: round((row[c] or 0) / total * 100, 2) for c in cols}
