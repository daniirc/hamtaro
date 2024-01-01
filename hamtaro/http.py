import asyncio
import itertools
import logging
from dataclasses import dataclass, field

import httpx

from aiofile import AIOFile

MAX_CONN_RETRIES = 5
MAX_HTTP_RETRIES = 2
MIN_RETRY_INTERVAL = 0.2

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ResponseInfo:
    resp: httpx.Response
    conn_retries: int = field(default=0)
    http_retries: int = field(default=0)


@dataclass(slots=True)
class DownloadInfo:
    offset: int = field(default=0)
    length: int | None = field(default=None)
    num_bytes_downloaded: int = field(default=0)
    done: asyncio.Event = field(default_factory=asyncio.Event)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def update(self, size: int):
        self.num_bytes_downloaded += size

    def reset(self, offset: int = 0, length: int | None = None):
        self.offset = offset
        self.length = length
        self.num_bytes_downloaded = 0
        self.done.clear()


@dataclass(slots=True)
class ProgressInfo:
    num_bytes_downloaded: int = field(default=0)
    done: asyncio.Event = field(default_factory=asyncio.Event)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def update(self, size: int):
        async with self.lock:
            self.num_bytes_downloaded += size

    async def reset(self):
        async with self.lock:
            self.num_bytes_downloaded = 0
        self.done.clear()


class Downloader:
    CHUNK_SIZE = 131072

    def __init__(
        self, client: httpx.AsyncClient | None = None,
        max_conn_retries: int = MAX_CONN_RETRIES, max_http_retries: int = MAX_HTTP_RETRIES
    ):
        self.client = client or httpx.AsyncClient()
        self.max_conn_retries = max_conn_retries
        self.max_http_retries = max_http_retries

    def __repr__(self) -> str:
        return "Downloader({!r}, max_conn_retries={}, max_http_retries={})".format(
            self.client, self.max_conn_retries, self.max_http_retries
        )

    async def __aenter__(self) -> 'Downloader':
        return self

    async def __aexit__(self, *_):
        await self.close()

    async def send_request(
        self, request: httpx.Request, *send_args,
        conn_retries: int = 0, http_retries: int = 0, **kwargs,
    ) -> ResponseInfo:
        for retry in itertools.count():
            if retry > 0:
                await asyncio.sleep(MIN_RETRY_INTERVAL * retry)
            try:
                resp = await self.client.send(request, *send_args, **kwargs)
                resp.raise_for_status()
                return ResponseInfo(resp, conn_retries, http_retries)
            except httpx.RequestError as exc:
                if isinstance(exc, httpx.UnsupportedProtocol):
                    raise
                logger.error(f'Connection error while attempting to make request: {exc!r}')
                if conn_retries >= self.max_conn_retries:
                    raise
                conn_retries += 1
                continue
            except httpx.HTTPStatusError as exc:
                logger.error(f'HTTP status error while attempting to make request: {exc!r}')
                if http_retries >= self.max_http_retries:
                    raise
                http_retries += 1
                continue
        assert False

    async def download(
        self, request: httpx.Request, afp: AIOFile,
        download_info: DownloadInfo | None = None, progress_info: ProgressInfo | None = None,
        response_info: ResponseInfo | None = None, conn_retries: int = 0, http_retries: int = 0,
    ):
        if not download_info:
            download_info = DownloadInfo()

        offset = download_info.offset
        for retry in itertools.count():
            if retry > 0:
                await asyncio.sleep(MIN_RETRY_INTERVAL * retry)

            if not response_info:
                if (length := download_info.length):
                    request.headers['Range'] = f'bytes={download_info.offset}-{length-1}'
                response_info = await self.send_request(
                    request, stream=True,
                    conn_retries=conn_retries, http_retries=http_retries,
                )
            resp = response_info.resp
            conn_retries += response_info.conn_retries
            http_retries += response_info.http_retries

            downloaded = 0
            try:
                async for chunk in resp.aiter_bytes(self.CHUNK_SIZE):
                    downloaded += len(chunk)
                    async with download_info.lock:
                        if (
                            (length := download_info.length)
                            and downloaded > (current_length := length-download_info.offset)
                        ):
                            chunk_length = len(chunk) - (downloaded-current_length)
                            if chunk_length <= 0:
                                break
                            await afp.write(chunk[:chunk_length], offset)
                            offset += chunk_length
                            download_info.update(chunk_length)
                            if progress_info:
                                await progress_info.update(chunk_length)
                            break

                        await afp.write(chunk, offset)
                        offset += len(chunk)
                        download_info.update(len(chunk))
                        if progress_info:
                            await progress_info.update(len(chunk))
            except httpx.RequestError as exc:
                logger.error(f'Connection error while reading HTTP response: {exc!r}')
                length = download_info.length
                if length is None or conn_retries >= self.max_conn_retries:
                    raise
                if downloaded >= length-download_info.offset:
                    break
                response_info = None
                conn_retries += 1
                continue
            finally:
                await resp.aclose()

            if (length := download_info.length) and downloaded < length-download_info.offset:
                logger.error('Server ended the connection with an incomplete stream length')
                if conn_retries >= self.max_conn_retries:
                    raise ValueError('IncompleteRead')  # TODO: raise 'proper' exception
                response_info = None
                conn_retries += 1
                continue
            break
        download_info.done.set()

    async def close(self):
        await self.client.aclose()
