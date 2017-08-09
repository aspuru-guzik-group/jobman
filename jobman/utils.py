from pathlib import Path
import logging


def get_key_or_attr(src=None, key=None, default=...):
    try: return src[key]  # noqa
    except: pass  # noqa
    try: return getattr(src, key)  # noqa
    except: pass  # noqa
    if default is not ...: return default  # noqa
    raise KeyError(key)


def get_keys_or_attrs(src=None, keys=None):
    result = {}
    for key in keys:
        try: result[key] = get_key_or_attr(src=src, key=key)  # noqa
        except: pass  # noqa
    return result


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
    if logging_cfg.get('stream'):
        logger.addHandler(logging.StreamHandler())
    level = logging_cfg.get('level')
    if level:
        logger.setLevel(getattr(logging, level))
    formatter = logging.Formatter(
        logging_cfg.get('format') or
        '| %(asctime)s | %(name)s | %(levelname)s |\n%(message)s\n'
    )
    for handler in logger.handlers:
        handler.setFormatter(formatter)
    return logger
