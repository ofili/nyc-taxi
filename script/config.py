import os
from functools import lru_cache

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

from loguru import logger
from pydantic import BaseSettings


class Settings(BaseSettings):
    """Settings for the application."""
    env_name: str = "New York Taxi Data"
    base_url: str = "http://localhost:5000"
    bucket: str = os.getenv("BUCKET")
    key: str = os.getenv("KEY")
    new_key: str = os.getenv("NEW_KEY")

    aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region: str = os.getenv("AWS_REGION")

    class Config:
        """Config for the settings."""
        env_file = ".env"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Get the settings.
    """
    settings = Settings()
    logger.info(f"Loading settings for {settings.env_name} from environment variables")
    return settings
