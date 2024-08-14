import os
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote_plus
from decouple import config

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)


class Settings(BaseSettings):

    # App
    APP_NAME:  str = config("APP_NAME", default="FastAPI")
    DEBUG: bool = config("DEBUG", default=False, cast=bool)
    
    # FrontEnd Application
    FRONTEND_HOST: str = config("FRONTEND_HOST", "http://localhost:4200")

    # MySql Database Config
    MYSQL_HOST: str = config("MYSQL_HOST", default='localhost')
    MYSQL_USER: str = config("MYSQL_USER", default='root')
    MYSQL_PASS: str = config("MYSQL_PASSWORD", default='secret')
    MYSQL_PORT: int = config("MYSQL_PORT", default=3306, cast=int)
    MYSQL_DB: str = config("MYSQL_DB", 'fastapi')
    DATABASE_URI: str = f"mysql+pymysql://{MYSQL_USER}:%s@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}" % quote_plus(MYSQL_PASS)

    # JWT Secret Key
    JWT_SECRET: str = config("JWT_SECRET", "649fb93ef34e4fdf4187709c84d643dd61ce730d91856418fdcf563f895ea40f")
    JWT_ALGORITHM: str = config("ACCESS_TOKEN_ALGORITHM", "HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = config("ACCESS_TOKEN_EXPIRE_MINUTES", default=10, cast=int)
    REFRESH_TOKEN_EXPIRE_MINUTES: int = config("REFRESH_TOKEN_EXPIRE_MINUTES", default=1440, cast=int)

    # App Secret Key
    SECRET_KEY: str = config("SECRET_KEY", default="8deadce9449770680910741063cd0a3fe0acb62a8978661f421bbcbb66dc41f1")


@lru_cache()
def get_settings() -> Settings:
    return Settings()
