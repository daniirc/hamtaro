import logging
from pathlib import Path

from aiofile import AIOFile

logger = logging.getLogger(__name__)


async def create_file(outfile: str | Path, mode: str, size: int | None = None) -> AIOFile:
    fp = await AIOFile(outfile, mode)
    try:
        if size is not None:
            logger.info('Allocating file space')
            # as of python 3.12, this may take a while if it's executed on windows, as it
            # actually writes zeros to the file instead of making the equivalent syscalls to
            # truncate the file like it already does on *nix.
            # more info: https://github.com/python/cpython/issues/84091
            await fp.truncate(size)
    except OSError:
        await fp.close()
        raise
    return fp