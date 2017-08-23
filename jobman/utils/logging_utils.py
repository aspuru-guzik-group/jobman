from pathlib import Path
import logging


def generate_logger(logging_cfg=None):
    logging_cfg = logging_cfg or {}
    logger = (
        logging_cfg.get('logger') or
        logging.getLogger(logging_cfg.get('name', __name__))
    )
    log_file = logging_cfg.get('log_file')
    if log_file:
        file_handler = logging.FileHandler(Path(log_file).expanduser())
        logger.addHandler(file_handler)
    if not logging_cfg.get('no_stream_handler'):
        logger.addHandler(logging.StreamHandler())
    level = logging_cfg.get('level') or 'INFO'
    if level:
        logger.setLevel(getattr(logging, level))
    formatter = logging.Formatter(
        logging_cfg.get('format') or
        '| %(asctime)s | %(name)s | %(levelname)s |\n%(message)s\n'
    )
    for handler in logger.handlers:
        handler.setFormatter(formatter)
    return logger
