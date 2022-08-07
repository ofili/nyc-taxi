from loguru import logger

from script.etl.extract import Extract
from script.etl.transform import Transform

logger.add('../log/errors.log', format='{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}',
           level='INFO', rotation='10 MB', retention='30 days', enqueue=True)


@logger.catch
def init_pipeline():
    """
    Initialize pipeline.
    """
    files = extract_data()
    transformed_data = transform_data()
    logger.info(transformed_data)
    logger.info('Pipeline processing completed.')


def extract_data():
    """
    Extract data to S3.
    """
    logger.info('Fetching urls...')
    e = Extract()
    urls = e.get_urls()
    for url in urls:
        file = e.download_file(url)
        e.put_object_s3(file)
        logger.info("File %s uploaded to S3", url)
    logger.info("Pipeline extract phase completed.")
    return urls


def transform_data():
    """
    Transform data.
    """
    logger.info('Transforming data...')
    t = Transform()
    transformed_data = t.transform_data()
    logger.info('Pipeline transform phase completed.')
    return transformed_data
