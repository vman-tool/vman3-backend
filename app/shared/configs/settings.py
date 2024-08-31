from functools import lru_cache
from pathlib import Path

from decouple import config
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)


class Settings(BaseSettings):

    # App
    APP_NAME:  str = config("APP_NAME", default="VMan")
    DEBUG: bool = config("DEBUG", default=False, cast=bool)
    
    # FrontEnd Application
    FRONTEND_HOST: str = config("FRONTEND_HOST", "https://vman3.vatools.net")

  
    # JWT Secret Key
    JWT_SECRET: str = config("JWT_SECRET", "649fb93ef34e4fdf4187709c84d643dd61ce730d91856418fdcf563f895ea40f")
    JWT_ALGORITHM: str = config("ACCESS_TOKEN_ALGORITHM", "HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: float = config("ACCESS_TOKEN_EXPIRE_MINUTES", default=10, cast=float)
    REFRESH_TOKEN_EXPIRE_MINUTES: float = config("REFRESH_TOKEN_EXPIRE_MINUTES", default=1440, cast=float)

    # App Secret Key
    SECRET_KEY: str = config("SECRET_KEY", default="8deadce9449770680910741063cd0a3fe0acb62a8978661f421bbcbb66dc41f1")


@lru_cache()
def get_settings() -> Settings:
    return Settings()
