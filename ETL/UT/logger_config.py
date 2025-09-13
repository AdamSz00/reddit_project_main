import logging


def init_logger():
    """loger configuration"""
    logging.basicConfig(
        filename="/Users/adam/Documents/Python/reddit_project_main/logs/etl.log",
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] (%(filename)s: %(lineno)d) - %(message)s",
        datefmt="%d.%m.%Y %H:%M:%S",
    )
