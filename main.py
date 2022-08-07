"""Script entry point"""
from loguru import logger

from script.etl import init_pipeline
from script.spark.session import create_spark_session

if __name__ == 'main':
    init_pipeline()

    # stop spark session
    create_spark_session().stop()
    logger.info("Spark session stopped")
    logger.info("Script finished")
    logger.remove()
