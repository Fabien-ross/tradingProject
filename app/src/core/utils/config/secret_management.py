import os
from dotenv import load_dotenv
from src.core.logging.loggers import logger_structure
from src.core.exceptions.exceptions import MissingEnvKeyError

load_dotenv()

REQUIRED_ENV_VARS = [
    "SECRET_API_KEY_BINANCE",
    "API_KEY_BINANCE",
    "DATABASE_URL",
]

ENV_VAR = {key: os.getenv(key, "") for key in REQUIRED_ENV_VARS}

def check_secrets():
    missing_keys = [key for key, value in ENV_VAR.items() if not value]
    if missing_keys:
        raise MissingEnvKeyError(missing_keys=missing_keys)


SECRET_API_KEY_BINANCE = ENV_VAR["SECRET_API_KEY_BINANCE"]
API_KEY_BINANCE        = ENV_VAR["API_KEY_BINANCE"]
DATABASE_URL           = ENV_VAR["DATABASE_URL"]

