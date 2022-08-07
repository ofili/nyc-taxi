#!/usr/bin python3
# -*- coding: utf-8 -*-


from loguru import logger
from pyspark.sql import functions as f
from pyspark.sql.types import *

from script.config import get_settings
from script.spark.session import create_spark_session


class Transform:

    def __init__(self):
        self.spark = create_spark_session()
        self.bucket = 's3a://{}/'.format(get_settings().bucket)
        self.key = '{}'.format(get_settings().new_key)
        self.prefix = get_settings().key

    def transform_data(self, bucket, key, prefix):
        """
        Rename columns.
        Rename columns in the dataframe.

        Returns:
            dataframe with renamed columns
        """
        green_schema = StructType([
            StructField("VendorID", IntegerType(), True),
            StructField("lpep_pickup_datetime",
                        TimestampType(), True),
            StructField("lpep_dropoff_datetime",
                        TimestampType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("RatecodeID", IntegerType(), True),
            StructField("PULocationID", IntegerType(), True),
            StructField("DOLocationID", IntegerType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("ehail_fee", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("payment_type", IntegerType(), True),
            StructField("trip_type", IntegerType(), True),
            StructField("congestion_surcharge", DoubleType(), True)
        ])

        yellow_schema = StructType([
            StructField("VendorID", IntegerType(), True),
            StructField("tpep_pickup_datetime",
                        TimestampType(), True),
            StructField("tpep_dropoff_datetime",
                        TimestampType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("RatecodeID", IntegerType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("PULocationID", IntegerType(), True),
            StructField("DOLocationID", IntegerType(), True),
            StructField("payment_type", IntegerType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge",
                        DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("congestion_surcharge",
                        DoubleType(), True),
            StructField("airport_fee", DoubleType(), True)
        ])

        files = [file.key for file in self.s3.bucket.objects.filter(self.prefix)]
        # get the file name
        file_name = files[0]
        # get the file from s3
        file = self.s3.bucket.Object(file_name)
        # read all parquet files from s3 if taxi is green
        if 'green' in file_name:
            df = self.spark.read.parquet(file.get()['Body'].read(), schema=green_schema)
            df = df.withColumnRenamed('VendorID', 'vendor_id') \
                .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
                .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime') \
                .withColumnRenamed('store_and_fwd_flag', 'store_and_fwd_flag') \
                .withColumnRenamed('RatecodeID', 'ratecode_id') \
                .withColumnRenamed('PULocationID', 'pickup_location_id') \
                .withColumnRenamed('DOLocationID', 'dropoff_location_id') \
                .withColumnRenamed('passenger_count', 'passenger_count') \
                .withColumnRenamed('trip_distance', 'trip_distance') \
                .withColumnRenamed('fare_amount', 'fare_amount') \
                .withColumnRenamed('extra', 'extra') \
                .withColumnRenamed('mta_tax', 'mta_tax') \
                .withColumnRenamed('tip_amount', 'tip_amount') \
                .withColumnRenamed('tolls_amount', 'tolls_amount') \
                .withColumnRenamed('ehail_fee', 'ehail_fee') \
                .withColumnRenamed('improvement_surcharge', 'improvement_surcharge') \
                .withColumnRenamed('total_amount', 'total_amount') \
                .withColumnRenamed('payment_type', 'payment_type') \
                .withColumnRenamed('trip_type', 'trip_type') \
                .withColumnRenamed('congestion_surcharge', 'congestion_surcharge')
            df = df.withColumn('pickup_datetime',
                               f.from_unixtime(f.unix_timestamp(
                                   f.col('pickup_datetime'), "yyyy-MM-dd HH:mm:ss"))) \
                .withColumn('dropoff_datetime',
                            f.from_unixtime(f.unix_timestamp(
                                f.col('dropoff_datetime'), "yyyy-MM-dd HH:mm:ss")))
            # add new columns
            logger.info("Adding service_type column...")
            df = df.withColumn("service_type", f.lit("green"))
            logger.info("Added service_type column.")
            # write parquet to s3 bucket
            df.write.parquet("s3a://{}/{}/parquet/".format(self.bucket, self.key), mode="append")
            logger.info(f"{file_name} was transformed to parquet")
        else:
            df = self.spark.read.parquet(file.get()['Body'].read(), schema=yellow_schema)
            df = df.withColumnRenamed('VendorID', 'vendor_id') \
                .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
                .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime') \
                .withColumnRenamed('passenger_count', 'passenger_count') \
                .withColumnRenamed('trip_distance', 'trip_distance') \
                .withColumnRenamed('RatecodeID', 'ratecode_id') \
                .withColumnRenamed('store_and_fwd_flag', 'store_and_fwd_flag') \
                .withColumnRenamed('PULocationID', 'pickup_location_id') \
                .withColumnRenamed('DOLocationID', 'dropoff_location_id') \
                .withColumnRenamed('payment_type', 'payment_type') \
                .withColumnRenamed('fare_amount', 'fare_amount') \
                .withColumnRenamed('extra', 'extra') \
                .withColumnRenamed('mta_tax', 'mta_tax') \
                .withColumnRenamed('tip_amount', 'tip_amount') \
                .withColumnRenamed('tolls_amount', 'tolls_amount') \
                .withColumnRenamed('improvement_surcharge', 'improvement_surcharge') \
                .withColumnRenamed('total_amount', 'total_amount') \
                .withColumnRenamed('congestion_surcharge', 'congestion_surcharge') \
                .withColumnRenamed('airport_fee', 'airport_fee')
            df = df.withColumn('pickup_datetime',
                               f.from_unixtime(f.unix_timestamp(
                                   f.col('pickup_datetime'), "yyyy-MM-dd HH:mm:ss"))) \
                .withColumn('dropoff_datetime',
                            f.from_unixtime(f.unix_timestamp(
                                f.col('dropoff_datetime'), "yyyy-MM-dd HH:mm:ss")))
            # add new columns
            logger.info("Adding service_type column...")
            df = df.withColumn("service_type", f.lit("yellow"))
            logger.info("Added service_type column.")
            # write parquet to s3 bucket
            df.write.parquet("s3a://{}/{}/parquet/".format(self.bucket, self.key), mode="append")
            logger.info(f"{file_name} was transformed to parquet")
        return df


''' def main():
    logger.info('Transforming data...')
    t = Transform()
    transformed_data = t.transform_data()
    logger.info('Pipeline transform phase completed.')
    return transformed_data
'''
