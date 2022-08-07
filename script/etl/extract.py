import logging

import boto3
import requests
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ParamValidationError, ClientError

# setting up logging
from script.config import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class Extract:
    """
    Extract class.
    """

    def __init__(self):
        self.s3 = boto3.resource('s3')
        self.bucket = get_settings().bucket
        self.key = get_settings().key

    @staticmethod
    def get_urls():
        """Get url list"""
        taxi = 'yellow green'.split()
        year = [str(n) for n in range(2019, 2022)]
        month = [str(n).zfill(2) for n in range(1, 13)]
        url_prefix = 'http://d37ci6vzurychx.cloudfront.net/trip-data/'
        url = [url_prefix + t + '_tripdata_' + y + '-' + m + '.parquet' for t in taxi for y in year for m in month]
        return url

    @staticmethod
    def download_file(url):
        """Download files from urls"""
        file = requests.get(url).content
        return file

    def put_object_s3(self, file):
        """Put object to s3"""
        try:
            self.s3.Bucket(self.bucket).put_object(Key=self.key, Body=file)
            logger.info("File %s uploaded to S3", self.key)
        except ParamValidationError as err:
            logger.error("Invalid parameter: %s", str(err))
            raise err
        except ClientError as err:
            raise S3UploadFailedError(
                f"Failed to upload {file} to {self.bucket}/{self.key}: {err}") from err
        return True


# url = url.append('https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.parquet')


''' def main():
    extract = Extract()
    urls = extract.get_urls()
    for url in urls:
        file = extract.download_file(url)
        extract.put_object_s3(file)
        logger.info("File %s uploaded to S3", url)
    logger.info("Pipeline extract phase completed.") '''
