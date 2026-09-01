import sys

from loguru import logger as logger

logger.remove()
logger.configure(extra={"node": "", "role": "", "term": ""})
logger.add(
    sys.stderr, format="<green>{extra[node]}</green>:<cyan>{extra[role]}</cyan>:<red>{extra[term]}</red> {message}"
)
