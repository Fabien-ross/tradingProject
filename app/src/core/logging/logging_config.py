import os
import sys
import logging
import traceback
from logging.handlers import RotatingFileHandler
from typing import cast

from src.core.utils.config.paths import ROOT_PATH, LOG_DIR

class ColorFormatter(logging.Formatter):
    """
    Formatter that applies ANSI colors based on log level.
    Intended for console output only.
    """
    COLOR_MAP = {
        logging.DEBUG: "\033[94m",    # Blue
        logging.INFO: "\033[92m",     # Green
        logging.WARNING: "\033[93m",  # Yellow
        logging.ERROR: "\033[91m",    # Red
        logging.CRITICAL: "\033[41m", # Red background
    }
    COLOR_RESET = "\033[0m"

    def format(self, record):
        color = self.COLOR_MAP.get(record.levelno, "")
        message = super().format(record)
        return f"{color}{message}{self.COLOR_RESET}"


class ColoredLogger(logging.Logger):
    """
    Extended logger with rooted_exception() method.
    Logs only the final line of the traceback that originates from project code.
    """

    def rooted_exception(self, msg: str = "Error detected") -> None:
        """
        Logs only the final traceback line from user code,
        with exception name, relative path, line number, and function name.
        """
        exc_type, _, tb_obj = sys.exc_info()
        tb = traceback.extract_tb(tb_obj)
        project_root = ROOT_PATH

        # Keep only traceback frames from inside the project
        user_frames = [f for f in tb if f.filename.startswith(project_root)]
        last = user_frames[-1] if user_frames else tb[-1]

        rel_path = os.path.relpath(last.filename, project_root)
        exc_name = exc_type.__name__ if exc_type else "UnknownError"

        self.error(
            f"{msg} — [{exc_name}] {rel_path}:{last.lineno} → {last.name}()"
        )


class ColoredLoggerManager:
    """
    Manages the creation of loggers with:
    - File rotation
    - Console color formatting
    - Custom Logger subclass (ColoredLogger)
    """

    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    MAX_SIZE_MB = 2
    BACKUP_COUNT = 5
    DEFAULT_LEVEL = logging.DEBUG

    def __init__(self, log_dir=LOG_DIR):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        logging.setLoggerClass(ColoredLogger)

    def get_logger(self, name: str, filename: str, level=None, to_console=False) -> ColoredLogger:
        """
        Creates and configures a logger with optional console output.
        :param name: name of the logger
        :param filename: log file name (stored in log_dir)
        :param level: logging level
        :param to_console: if True, adds a colorized StreamHandler
        """
        level = level or self.DEFAULT_LEVEL
        path = os.path.join(self.log_dir, filename)

        # File handler (non-colored)
        file_handler = RotatingFileHandler(
            path,
            maxBytes=self.MAX_SIZE_MB * 1024 * 1024,
            backupCount=self.BACKUP_COUNT
        )
        file_handler.setFormatter(logging.Formatter(self.LOG_FORMAT))

        logger = cast(ColoredLogger,logging.getLogger(name))
        logger.setLevel(level)
        logger.addHandler(file_handler)

        # Console handler (colored)
        if to_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(ColorFormatter(self.LOG_FORMAT))
            console_handler.setLevel(level)
            logger.addHandler(console_handler)

        logger.propagate = False
        return logger
