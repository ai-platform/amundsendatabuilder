import logging
from typing import Union, Optional


def init(level: Optional[Union[str, int]] = logging.WARN) -> None:
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=level)
