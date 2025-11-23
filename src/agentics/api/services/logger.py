import sys
import json
from loguru import logger
from agentics.api.config import settings, Environment


def serialize(record):
    subset = {
        "timestamp": record["time"].timestamp(),
        "level": record["level"].name,
        "message": record["message"],
        "module": record["name"],
        "function": record["function"],
        "extra": record["extra"],
    }
    return json.dumps(subset)


def configure_logging():
    logger.remove()  # Remove default handler

    if settings.ENVIRONMENT == Environment.PRODUCTION:
        # JSON logging for production
        logger.add(sys.stdout, format="{message}", serialize=True, level="INFO")
    else:
        # Pretty logging for local dev
        logger.add(
            sys.stdout,
            colorize=True,
            format="<green>{time}</green> <level>{message}</level>",
            level="DEBUG",
        )

    logger.info(f"Logging initialized in {settings.ENVIRONMENT} mode")
