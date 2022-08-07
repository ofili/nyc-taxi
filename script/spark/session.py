from loguru import logger
import os

from pyspark.sql import SparkSession

from script.config import get_settings

# setting up logging
logger.add('../log/errors.log', format='{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}',
           level='INFO', retention='30 days', rotation='10 MB')


def create_spark_session() -> SparkSession:
    """
    Create spark session.

    The entry point to programming Spark with the Dataset and DataFrame API.

    Returns:
        spark session object
    """
    try:
        aws_access_key_id = get_settings().aws_access_key_id
        aws_secret_access_key = get_settings().aws_secret_access_key
        aws_region = get_settings().aws_region

        spark = SparkSession.builder.config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").config(
            "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").config(
            "spark.hadoop.fs.s3a.access.key", aws_access_key_id).config(
            "spark.hadoop.fs.s3a.secret.key", aws_secret_access_key).config(
            "spark.hadoop.fs.s3a.endpoint", aws_region).config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled", "false").config(
            "spark.hadoop.fs.s3a.path.style.access", "true").config(
            "spark.hadoop.fs.s3a.implicit.path.style.external-location", "true").config(
            "spark.hadoop.fs.s3a.fast.upload", "true").config(
            "spark.hadoop.fs.s3a.buffer.dir", "/tmp").config(
            "spark.hadoop.fs.s3a.multipart.size", "64MB").config(
            "spark.hadoop.fs.s3a.enable.sse.algorithm", "AES256").enableHiveSupport().getOrCreate()
        logger.success(f"Spark session created successfully with {spark}")
        return spark
    except Exception as spark_err:
        logger.error("Error creating spark session", str(spark_err))
        raise f"Error creating spark session {str(spark_err)}"
