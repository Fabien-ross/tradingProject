from src.core.logging.logging_config import ColoredLoggerManager, ColoredLogger
from logging import DEBUG, INFO, ERROR, CRITICAL

log_manager = ColoredLoggerManager()
standard_level = DEBUG

logger_database : ColoredLogger     = log_manager.get_logger("Database", "database.log", level=standard_level, to_console=True)
logger_structure : ColoredLogger    = log_manager.get_logger("Structure", "structure.log", level=standard_level, to_console=True)
logger_data_ret: ColoredLogger      = log_manager.get_logger("Retriever", "retriever.log", level=standard_level, to_console=True)
logger_spo : ColoredLogger          = log_manager.get_logger("SPO", "spo.log", level=standard_level, to_console=True)