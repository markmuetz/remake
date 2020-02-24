import hashlib
from logging import getLogger
from pathlib import Path

logger = getLogger(__name__)

# BUF_SIZE is totally arbitrary, change for your app!
SHA1_BUF_SIZE = 65536  # lets read stuff in 64kb chunks!


def sha1sum(path: Path, buf_size: int = SHA1_BUF_SIZE) -> str:
    logger.debug(f'calc sha1sum for {path}')
    sha1 = hashlib.sha1()
    with path.open('rb') as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()
