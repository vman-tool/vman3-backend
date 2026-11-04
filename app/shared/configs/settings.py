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

  
    # JWT Secret Key - no hardcoded fallback: this signs every auth token, so a
    # known default would let anyone forge one. config() with no default raises
    # UndefinedValueError at import time, failing the app loudly instead of
    # silently running with a predictable secret.
    JWT_SECRET: str = config("JWT_SECRET")
    JWT_ALGORITHM: str = config("ACCESS_TOKEN_ALGORITHM", "HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: float = config("ACCESS_TOKEN_EXPIRE_MINUTES", default=10, cast=float)
    REFRESH_TOKEN_EXPIRE_MINUTES: float = config("REFRESH_TOKEN_EXPIRE_MINUTES", default=1440, cast=float)

    # App Secret Key - same reasoning as JWT_SECRET above.
    SECRET_KEY: str = config("SECRET_KEY")


@lru_cache()
def get_settings() -> Settings:
    return Settings()
