import logging
import os


def get_log_level(name: str = None):
    if name is None:
        level_name = os.environ.get("DPF_LOG_LEVEL", "INFO")
    else:
        name = name.upper()
        name = "DPF_" + name + "_LOG_LEVEL"
        level_name = os.environ.get(name, "INFO")
    return level_name


default_level = get_log_level()


def get_logger(name: str, level=None, file=None):
    logger = logging.getLogger(name)
    if level is None:
        level = get_log_level(name)
    logger.setLevel(level)
    c_handler = logging.StreamHandler()
    # msgfmt = '[%(asctime)s] p%(process)s {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'
    # msgfmt = '%(asctime)s p%(process)s %(levelname)s %(filename)s:%(lineno)d - %(message)s'
    if level is "DEBUG":
        msgfmt = "%(asctime)s %(levelname)s %(filename)s:%(lineno)d - %(message)s"
    else:
        msgfmt = "%(asctime)s %(levelname)s - %(message)s"
    timefmt = "%H:%M:%S"

    c_format = logging.Formatter(msgfmt, timefmt)
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)

    if file is not None:
        f_handler = logging.FileHandler(file)
        f_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        f_handler.setFormatter(f_format)
        logger.addHandler(f_handler)

    # Add handlers to the logger
    return logger


# logger = get_logger("main")
# logger.info("info message")
# logger.warning("info message")
# logger.error("info message")
