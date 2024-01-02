#!/usr/bin/python3

import argparse
import asyncio
import logging
import math
import sys
from pathlib import Path

from .http import Downloader, DownloadInfo, ProgressInfo, ResponseInfo
from .util import create_file

import httpx

HTTP_TIMEOUT = 5
WORKERS = 5

MIN_WORKER_RANGE_SIZE = 6_000_000

WorkerDownloadInfoList = list[DownloadInfo]

logger = logging.getLogger(__name__)


def run():
    try:
        asyncio.run(main())
    except (asyncio.CancelledError, KeyboardInterrupt):
        sys.exit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog='hamtaro', description='A smol & cute HTTP downloader',
    )
    parser.add_argument('url')
    parser.add_argument(
        '-o', '--outfile', type=Path, required=True, help='path of output file')
    parser.add_argument(
        '-w', '--workers', type=int, default=WORKERS, help='number of download workers')
    parser.add_argument(
        '--http2', action=argparse.BooleanOptionalAction, help='use HTTP/2 if supported')
    parser.add_argument(
        '-d', '--debug', action='store_const', dest='loglevel',
        const=logging.DEBUG, default=logging.INFO, help='show debug logs')
    return parser.parse_args()


async def main():
    # signal.signal(signal.SIGINT, lambda *_: sys.exit(130))

    args = parse_args()
    logging.basicConfig(level=args.loglevel)
    if args.workers < 1:
        raise ValueError('sai fora gracinha')

    async with httpx.AsyncClient(
        timeout=HTTP_TIMEOUT, follow_redirects=True, http2=args.http2,
        limits=httpx.Limits(
            max_keepalive_connections=max(20, args.workers),
            max_connections=max(100, args.workers),
            keepalive_expiry=HTTP_TIMEOUT
        ),
    ) as client:
        downloader = Downloader(client)
        supports_ranges = True
        total_length = None

        download_req_info = await downloader.send_request(
            client.build_request('GET', args.url), stream=True)
        if download_req_info.resp.headers.get('Accept-Ranges') != 'bytes':
            logger.info("Server doesn't support partial requests")
            supports_ranges = False
        if (
            'Content-Length' not in download_req_info.resp.headers
            or not download_req_info.resp.headers['Content-Length'].isdecimal()
        ):
            if supports_ranges:
                logger.info("content-length header invalid or unavailable")
        else:
            total_length = int(download_req_info.resp.headers['Content-Length'])

        workers = 1
        worker_range_size = total_length or 0
        if supports_ranges and total_length:
            workers = min(args.workers, total_length)
            worker_range_size = min(math.ceil(total_length/workers), total_length)

        outfile = args.outfile  # TODO: get filename from the url or content-disposition header
        progress_info = ProgressInfo()

        logger.info(f'Downloading file with {workers} worker{"s" if workers != 1 else ""}')
        logger.debug(f'{total_length=}')

        worker_download_info_list: WorkerDownloadInfoList = [
            DownloadInfo(length=worker_range_size if total_length else total_length)
        ]
        worker_download_info_list.extend(
            DownloadInfo(offset=offset, length=min(offset + worker_range_size, total_length))
            for offset in range(worker_range_size, total_length or 0, worker_range_size))

        afp = await create_file(outfile, 'wb', total_length)
        progress_task = asyncio.create_task(progress_printer(progress_info))
        try:
            search_lock = asyncio.Lock()
            async with asyncio.TaskGroup() as tg:
                tg.create_task(download_and_find_jobs(
                    downloader, worker_download_info_list, search_lock,
                    client.build_request('GET', args.url), afp,
                    download_info=worker_download_info_list[0], progress_info=progress_info,
                    response_info=download_req_info))

                for download_info in worker_download_info_list[1:]:
                    tg.create_task(download_and_find_jobs(
                        downloader, worker_download_info_list, search_lock,
                        client.build_request('GET', args.url), afp,
                        download_info=download_info, progress_info=progress_info))
        finally:
            progress_info.done.set()
            await afp.close()
            await progress_task


async def download_and_find_jobs(
    downloader: Downloader, worker_download_info_list: WorkerDownloadInfoList,
    search_lock: asyncio.Lock, *args, download_info: DownloadInfo,
    response_info: ResponseInfo | None = None, **kwargs
):
    try:
        await downloader.download(
            *args, download_info=download_info, response_info=response_info, **kwargs)
    finally:
        if response_info:
            await response_info.resp.aclose()

    if len(worker_download_info_list) <= 1:
        return
    while (await job_hunter(download_info, worker_download_info_list, search_lock)):
        await downloader.download(*args, download_info=download_info, **kwargs)


async def job_hunter(
    download_info: DownloadInfo, worker_download_info_list: WorkerDownloadInfoList,
    search_lock: asyncio.Lock
) -> bool:
    async with search_lock:
        for other_download_info in worker_download_info_list:
            async with other_download_info.lock:
                if other_download_info.done.is_set() or not other_download_info.length:
                    continue

                offset = other_download_info.offset
                length = other_download_info.length
                actual_length = length - offset
                bytes_remaining = actual_length - other_download_info.num_bytes_downloaded
                if (bytes_remaining_half := bytes_remaining//2) > MIN_WORKER_RANGE_SIZE:
                    logger.debug(
                        'Reusing worker and changing target length of ongoing download: '
                        f'{offset=} {length=} {bytes_remaining=}')
                    download_info.reset(
                        offset=other_download_info.length - bytes_remaining_half,
                        length=other_download_info.length)
                    other_download_info.length = download_info.offset
                    return True
    return False


async def progress_printer(progress_info: ProgressInfo, interval: float = 0.5):
    done = False
    while True:
        print(progress_info.num_bytes_downloaded)  # TODO: progress bar
        if done:
            break
        try:
            await asyncio.wait_for(progress_info.done.wait(), timeout=interval)
            done = True
        except TimeoutError:
            pass


if __name__ == '__main__':
    run()
