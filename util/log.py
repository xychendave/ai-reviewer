import time
import pathlib
import logging


def get_logger(name: str = "project", level: str = "info", save_file: bool = False) -> logging.Logger:
    # set logger
    logger = logging.getLogger(name)
    level_dict = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL
    }
    logger.setLevel(level_dict[level])

    if not logger.handlers:
        sh = logging.StreamHandler()
        sh.setLevel(logging.DEBUG)
        stream_fmt = logging.Formatter("%(asctime)-15s [%(filename)s] %(levelname)s %(lineno)d: %(message)s")
        sh.setFormatter(stream_fmt)
        logger.addHandler(sh)

        if save_file:
            log_path = log_path_util(name)
            fh = logging.FileHandler(log_path)
            fh.setLevel(logging.INFO)
            fh_fmt = logging.Formatter("%(asctime)-15s [%(filename)s] %(levelname)s %(lineno)d: %(message)s")
            fh.setFormatter(fh_fmt)
            logger.addHandler(fh)

    return logger


def log_path_util(name: str) -> str:
    day = time.strftime("%Y-%m-%d", time.localtime())
    log_path = pathlib.Path(f"./log/{day}")
    if not log_path.exists():
        log_path.mkdir(parents=True)
    return f"{str(log_path)}/{name}.log"
