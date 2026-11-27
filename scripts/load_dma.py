#!/usr/bin/env python3
"""Load Danish Maritime Authority (DMA) daily AIS zips into the HDFS landing zone as plain CSV.

For every day from --from to --to the script reads aisdk-YYYY-MM-DD.zip, checks that it holds
one CSV with the 26-column DMA header, and streams that CSV to

    /lake/landing/dma/day=YYYY-MM-DD/aisdk-YYYY-MM-DD.csv

The pod has a read-only root filesystem, so the script writes nothing to local disk. It reads
the zip over HTTP range requests (zipfile seeks to the central directory near the end, then
streams the member), and sends the unzipped bytes straight to WebHDFS in 8 MiB chunks. A day is
first written to a hidden .tmp file in its day folder and then renamed over the final name in
one step, so a re-run replaces the day and never duplicates rows. Spark skips names that start
with "." so it never sees a half-written day.

Exit codes: 0 every day loaded; 3 the only problem was days not published yet (HTTP 404 or no
local zip); 1 at least one day failed for another reason (wins over 3); 2 bad arguments.

It runs in the spark-jobs image (Python 3.10, standard library only) from a ConfigMap. The Job
manifest is infra/k8s/spark/school/one-off/job-load-dma.yaml. For development, read local zips
and write to a local folder:

    python3 -B scripts/load_dma.py --from 2026-10-01 --src-dir data/dma/zips --local-dest out
"""

from __future__ import annotations

import argparse
import contextlib
import functools
import http.client
import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
from collections.abc import Callable, Iterable, Iterator
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import BinaryIO, Protocol

# The DMA host resets HTTPS connections, so the default is plain HTTP.
DEFAULT_URL_TEMPLATE = "http://aisdata.ais.dk/aisdk-{date}.zip"
DEFAULT_WEBHDFS = "http://hdfs-namenode:9870"
DEFAULT_HDFS_USER = "spark"
DEFAULT_LANDING = "/lake/landing/dma"

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_NOT_PUBLISHED = 3

CHUNK = 8 << 20  # bytes per write to the destination
READ_BUFFER = 1 << 20  # buffer between zipfile and the HTTP range reader
TIMEOUT = 60  # seconds, for every HTTP request
RETRIES = 5  # reconnects after a dropped download
HEADER_PREFIX = "# Timestamp"
HEADER_COLUMNS = 26
LOG = "[load-dma]"


class NotPublished(Exception):
    """The zip for a day does not exist yet (HTTP 404 or no local file)."""


class LoadError(RuntimeError):
    """A day cannot be loaded: bad zip, bad header, HTTP or WebHDFS error."""


def log(message: str) -> None:
    print(f"{LOG} {message}", flush=True)


def daterange(start: date, end: date) -> Iterator[date]:
    """Yield every date from start to end, both inclusive."""
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


# --- reading the zip --------------------------------------------------------------------------

# Errors after which a download is reopened at the current position.
RETRYABLE = (http.client.HTTPException, ConnectionError, TimeoutError, urllib.error.URLError)


class HttpRangeFile(io.RawIOBase):
    """A read-only, seekable file over HTTP range requests.

    It keeps one open-ended range response and reads it in order. Only a read at another
    position closes it and opens a new range there, so zipfile needs a handful of requests.
    """

    def __init__(
        self, url: str, timeout: float = TIMEOUT, retries: int = RETRIES, backoff: float = 1.0
    ) -> None:
        super().__init__()
        self.url = url
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.requests = 0
        self._pos = 0
        self._response: http.client.HTTPResponse | None = None
        self._response_pos = 0
        self.size = self._head()

    def _urlopen(self, request: urllib.request.Request) -> http.client.HTTPResponse:
        self.requests += 1
        try:
            return urllib.request.urlopen(request, timeout=self.timeout)
        except urllib.error.HTTPError as exc:
            exc.close()
            if exc.code == 404:
                raise NotPublished(f"404 {request.full_url}") from None
            if exc.code < 500:
                raise LoadError(f"HTTP {exc.code} from {request.full_url}") from None
            raise

    def _head(self) -> int:
        with self._urlopen(urllib.request.Request(self.url, method="HEAD")) as response:
            length = response.headers.get("Content-Length")
            self.url = response.geturl()  # after a redirect, ask the final host directly
        if length is None:
            raise LoadError(f"HEAD {self.url} gave no Content-Length")
        return int(length)

    def _open(self) -> None:
        self._close_response()
        request = urllib.request.Request(self.url, headers={"Range": f"bytes={self._pos}-"})
        response = self._urlopen(request)
        if response.status != 206:
            response.close()
            raise LoadError(
                f"{self.url} answered {response.status} to a range request, expected 206"
            )
        self._response = response
        self._response_pos = self._pos

    def _close_response(self) -> None:
        if self._response is not None:
            self._response.close()
            self._response = None

    def _read_once(self, buffer: memoryview) -> int:
        if self._response is None or self._response_pos != self._pos:
            self._open()
        assert self._response is not None
        count = self._response.readinto(buffer)
        if not count:
            raise ConnectionError(f"connection closed at byte {self._pos:,} of {self.size:,}")
        return count

    def readinto(self, buffer: memoryview) -> int:  # type: ignore[override]
        if self._pos >= self.size or not len(buffer):
            return 0
        for attempt in range(self.retries + 1):
            try:
                count = self._read_once(buffer)
            except RETRYABLE as exc:
                self._close_response()
                if attempt == self.retries:
                    raise LoadError(f"{self.url}: {exc} (after {self.retries} retries)") from exc
                time.sleep(self.backoff * 2**attempt)
                continue
            self._pos += count
            self._response_pos += count
            return count
        raise AssertionError("unreachable")

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            pos = offset
        elif whence == io.SEEK_CUR:
            pos = self._pos + offset
        elif whence == io.SEEK_END:
            pos = self.size + offset
        else:
            raise ValueError(f"invalid whence {whence}")
        if pos < 0:
            raise OSError(f"negative seek position {pos}")
        self._pos = pos  # the next read opens a new range only if this moved
        return pos

    def tell(self) -> int:
        return self._pos

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def close(self) -> None:
        self._close_response()
        super().close()


def local_zip(src_dir: Path, day: date) -> BinaryIO:
    path = src_dir / f"aisdk-{day.isoformat()}.zip"
    if not path.is_file():
        raise NotPublished(f"no zip at {path}")
    return path.open("rb")


def remote_zip(url_template: str, day: date) -> BinaryIO:
    url = url_template.format(date=day.isoformat(), year=day.year)
    return io.BufferedReader(HttpRangeFile(url), buffer_size=READ_BUFFER)


def csv_member(archive: zipfile.ZipFile) -> zipfile.ZipInfo:
    members = [m for m in archive.infolist() if m.filename.lower().endswith(".csv")]
    if len(members) != 1:
        names = [m.filename for m in archive.infolist()]
        raise LoadError(f"expected exactly one .csv member, found {names}")
    return members[0]


def check_header(first_chunk: bytes) -> None:
    line = first_chunk.split(b"\n", 1)[0].rstrip(b"\r")
    header = line.decode("utf-8", errors="replace")
    columns = header.split(",")
    if not header.startswith(HEADER_PREFIX) or len(columns) != HEADER_COLUMNS:
        raise LoadError(
            f"unexpected header ({len(columns)} columns, want {HEADER_COLUMNS} starting with "
            f"{HEADER_PREFIX!r}): {header[:120]!r}"
        )


# --- counting ---------------------------------------------------------------------------------


@dataclass
class Counter:
    bytes: int = 0
    newlines: int = 0
    last: bytes = b""

    def add(self, chunk: bytes) -> None:
        self.bytes += len(chunk)
        self.newlines += chunk.count(b"\n")
        self.last = chunk[-1:]

    @property
    def rows(self) -> int:
        """Lines minus the header; a last line without a newline still counts."""
        lines = self.newlines + (1 if self.last not in (b"", b"\n") else 0)
        return max(lines - 1, 0)


def counted(first: bytes, stream: BinaryIO, counter: Counter) -> Iterator[bytes]:
    chunk = first
    while chunk:
        counter.add(chunk)
        yield chunk
        chunk = stream.read(CHUNK)


# --- destinations -----------------------------------------------------------------------------


class Sink(Protocol):
    def create(self, path: str, chunks: Iterable[bytes]) -> None: ...

    def rename_overwrite(self, src: str, dst: str) -> None: ...

    def length(self, path: str) -> int | None: ...

    def delete(self, path: str) -> None: ...


class LocalDir:
    """Writes to local paths, for the tests and for development."""

    def create(self, path: str, chunks: Iterable[bytes]) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("wb") as handle:
            for chunk in chunks:
                handle.write(chunk)

    def rename_overwrite(self, src: str, dst: str) -> None:
        os.replace(src, dst)

    def length(self, path: str) -> int | None:
        target = Path(path)
        return target.stat().st_size if target.is_file() else None

    def delete(self, path: str) -> None:
        Path(path).unlink(missing_ok=True)


def http_request(
    method: str, url: str, body: Iterable[bytes] | None = None, timeout: float = TIMEOUT
) -> tuple[int, http.client.HTTPMessage, bytes]:
    """One request on a fresh connection; an iterable body is sent chunked."""
    parts = urllib.parse.urlsplit(url)
    connection_class = (
        http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
    )
    connection = connection_class(parts.netloc, timeout=timeout)
    target = parts.path + (f"?{parts.query}" if parts.query else "")
    headers = {} if body is None else {"Content-Type": "application/octet-stream"}
    try:
        connection.request(method, target, body=body, headers=headers, encode_chunked=bool(body))
        response = connection.getresponse()
        return response.status, response.headers, response.read()
    finally:
        connection.close()


def remote_error(what: str, status: int, body: bytes) -> LoadError:
    try:
        remote = json.loads(body)["RemoteException"]
        detail = f"{remote.get('exception')}: {remote.get('message')}"
    except (ValueError, KeyError, TypeError):
        detail = body[:200].decode("utf-8", errors="replace") or "(empty body)"
    return LoadError(f"WebHDFS {what} answered {status}: {detail}")


class WebHdfs:
    """The WebHDFS REST API with simple auth (user.name on every request)."""

    def __init__(self, base_url: str, user: str, timeout: float = TIMEOUT) -> None:
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.timeout = timeout

    def _url(self, path: str, op: str, **params: str) -> str:
        query = urllib.parse.urlencode({"op": op, **params, "user.name": self.user})
        return f"{self.base_url}/webhdfs/v1{urllib.parse.quote(path, safe='/=')}?{query}"

    def _call(
        self, method: str, url: str, body: Iterable[bytes] | None = None
    ) -> tuple[int, http.client.HTTPMessage, bytes]:
        return http_request(method, url, body=body, timeout=self.timeout)

    def create(self, path: str, chunks: Iterable[bytes]) -> None:
        # The namenode answers 307 with the datanode that takes the data.
        status, headers, body = self._call("PUT", self._url(path, "CREATE", overwrite="true"))
        location = headers.get("Location")
        if status != 307 or not location:
            raise remote_error(f"CREATE {path}", status, body)
        status, _, body = self._call("PUT", location, body=chunks)
        if status != 201:
            raise remote_error(f"CREATE {path} (datanode)", status, body)

    def rename_overwrite(self, src: str, dst: str) -> None:
        url = self._url(src, "RENAME", destination=dst, renameoptions="OVERWRITE")
        status, _, body = self._call("PUT", url)
        if status != 200:
            raise remote_error(f"RENAME {src}", status, body)
        if body.strip() and json.loads(body).get("boolean") is False:
            raise LoadError(f"WebHDFS RENAME {src} -> {dst} returned false")

    def length(self, path: str) -> int | None:
        status, _, body = self._call("GET", self._url(path, "GETFILESTATUS"))
        if status == 404:
            return None
        if status != 200:
            raise remote_error(f"GETFILESTATUS {path}", status, body)
        return int(json.loads(body)["FileStatus"]["length"])

    def delete(self, path: str) -> None:
        status, _, body = self._call("DELETE", self._url(path, "DELETE", recursive="false"))
        if status != 200:
            raise remote_error(f"DELETE {path}", status, body)


# --- one day ----------------------------------------------------------------------------------


@dataclass
class DayResult:
    day: str
    status: str  # ok, not_published or failed
    bytes: int = 0
    rows: int = 0
    seconds: float = 0.0
    path: str = ""
    error: str = ""


def day_paths(landing: str, day: date, run_id: str) -> tuple[str, str]:
    """Return (final path, hidden temp path in the same folder)."""
    name = f"aisdk-{day.isoformat()}.csv"
    folder = f"{landing.rstrip('/')}/day={day.isoformat()}"
    return f"{folder}/{name}", f"{folder}/.{name}.{run_id}.tmp"


def write_atomically(
    sink: Sink, chunks: Iterable[bytes], counter: Counter, expected: int, final: str, tmp: str
) -> None:
    """Write to tmp, rename onto final; on failure drop tmp and leave final as it was."""
    try:
        sink.create(tmp, chunks)
        if counter.bytes != expected:
            raise LoadError(f"wrote {counter.bytes:,} bytes, the zip says {expected:,}")
        sink.rename_overwrite(tmp, final)
    except BaseException:
        with contextlib.suppress(Exception):
            sink.delete(tmp)
        raise
    length = sink.length(final)
    if length != counter.bytes:
        raise LoadError(f"{final} is {length} bytes after the rename, wrote {counter.bytes:,}")


def load_day(
    day: date, open_zip: Callable[[date], BinaryIO], sink: Sink, landing: str, run_id: str
) -> tuple[Counter, str]:
    final, tmp = day_paths(landing, day, run_id)
    counter = Counter()
    with open_zip(day) as handle, zipfile.ZipFile(handle) as archive:
        member = csv_member(archive)
        with archive.open(member) as stream:
            first = stream.read(CHUNK)
            check_header(first)  # before anything is written
            chunks = counted(first, stream, counter)
            write_atomically(sink, chunks, counter, member.file_size, final, tmp)
    return counter, final


def run_day(
    day: date, open_zip: Callable[[date], BinaryIO], sink: Sink, landing: str, run_id: str
) -> DayResult:
    """Load one day and log one line; never raises."""
    result = DayResult(day=day.isoformat(), status="failed")
    started = time.monotonic()
    try:
        counter, final = load_day(day, open_zip, sink, landing, run_id)
    except NotPublished as exc:
        result.status, result.error = "not_published", str(exc)
        log(f"{day} not published yet ({exc})")
    except Exception as exc:
        result.error = f"{type(exc).__name__}: {exc}"
        log(f"{day} FAILED {result.error}")
    else:
        seconds = time.monotonic() - started
        result.status, result.bytes, result.rows, result.path = (
            "ok",
            counter.bytes,
            counter.rows,
            final,
        )
        rate = counter.bytes / 1e6 / seconds if seconds > 0 else 0.0
        log(
            f"{day} ok  bytes={counter.bytes:,} rows={counter.rows:,} seconds={seconds:.1f} "
            f"MB/s={rate:.1f} -> {final}"
        )
    result.seconds = round(time.monotonic() - started, 1)
    return result


def exit_code(results: list[DayResult]) -> int:
    statuses = {r.status for r in results}
    if "failed" in statuses:
        return EXIT_FAILED
    if "not_published" in statuses:
        return EXIT_NOT_PUBLISHED
    return EXIT_OK


# --- command line -----------------------------------------------------------------------------


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n")[0])
    parser.add_argument(
        "--from",
        dest="start",
        type=date.fromisoformat,
        required=True,
        metavar="YYYY-MM-DD",
        help="first day",
    )
    parser.add_argument(
        "--to",
        dest="end",
        type=date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="last day, inclusive (default: --from)",
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--url-template",
        default=os.environ.get("DMA_URL_TEMPLATE", DEFAULT_URL_TEMPLATE),
        help="zip URL with {date} (and {year}); must be http, the DMA host resets https "
        "(env DMA_URL_TEMPLATE, default %(default)s)",
    )
    source.add_argument("--src-dir", type=Path, help="read local zips aisdk-YYYY-MM-DD.zip")
    dest = parser.add_mutually_exclusive_group()
    dest.add_argument(
        "--webhdfs",
        default=os.environ.get("WEBHDFS_URL", DEFAULT_WEBHDFS),
        help="WebHDFS namenode URL (env WEBHDFS_URL, default %(default)s)",
    )
    dest.add_argument(
        "--local-dest", type=Path, help="write day=YYYY-MM-DD/ folders here instead of HDFS"
    )
    parser.add_argument(
        "--hdfs-user",
        default=os.environ.get("HDFS_USER", DEFAULT_HDFS_USER),
        help="HDFS user (env HDFS_USER, default %(default)s)",
    )
    parser.add_argument(
        "--landing", default=DEFAULT_LANDING, help="HDFS landing root (default %(default)s)"
    )
    args = parser.parse_args(argv)
    if args.end is None:
        args.end = args.start
    if args.end < args.start:
        parser.error("--to must not be before --from")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    if args.src_dir is not None:
        open_zip = functools.partial(local_zip, args.src_dir)
    else:
        open_zip = functools.partial(remote_zip, args.url_template)
    if args.local_dest is not None:
        sink: Sink = LocalDir()
        landing = str(args.local_dest)
    else:
        sink = WebHdfs(args.webhdfs, args.hdfs_user)
        landing = args.landing

    results = [
        run_day(day, open_zip, sink, landing, run_id) for day in daterange(args.start, args.end)
    ]
    code = exit_code(results)
    summary = {"run_id": run_id, "exit_code": code, "days": [asdict(r) for r in results]}
    log(f"summary {json.dumps(summary)}")
    return code


if __name__ == "__main__":
    sys.exit(main())
