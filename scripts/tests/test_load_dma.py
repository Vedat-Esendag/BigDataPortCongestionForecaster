"""load_dma: local and HTTP sources, local and WebHDFS destinations, exit codes."""

from __future__ import annotations

import contextlib
import http.server
import io
import json
import threading
import urllib.parse
import zipfile
from collections.abc import Iterator
from pathlib import Path

import pytest

import load_dma

DAY = "2026-10-01"
COLUMNS = [
    "# Timestamp", "Type of mobile", "MMSI", "Latitude", "Longitude", "Navigational status",
    "ROT", "SOG", "COG", "Heading", "IMO", "Callsign", "Name", "Ship type", "Cargo type",
    "Width", "Length", "Type of position fixing device", "Draught", "Destination", "ETA",
    "Data source type", "A", "B", "C", "D",
]  # fmt: skip
HEADER = ",".join(COLUMNS)


@pytest.fixture(autouse=True)
def no_proxy_no_env(monkeypatch):
    """Keep the local test servers off any proxy and the defaults off the shell env."""
    for name in ("http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("no_proxy", "*")
    for name in ("DMA_URL_TEMPLATE", "WEBHDFS_URL", "HDFS_USER"):
        monkeypatch.delenv(name, raising=False)


def row(i: int, name: str = "MAERSK, ONE") -> str:
    return (
        f"01/10/2026 00:00:{i % 60:02d},Class A,2190000{i:02d},55.6,12.6,Under way using engine,"
        f'0.0,10.2,180.0,181,Unknown,OXAB2,"{name}",Cargo,,32,200,GPS,10.5,"DK AAR",'
        "02/10/2026 08:00:00,AIS,10,190,16,16"
    )


def csv_bytes(rows: int, name: str = "MAERSK, ONE", trailing_newline: bool = True) -> bytes:
    text = "\n".join([HEADER, *(row(i, name) for i in range(rows))])
    return (text + ("\n" if trailing_newline else "")).encode()


def make_zip(path: Path, content: bytes, member: str | None = None, stored: bool = False) -> Path:
    member = member or path.name.replace(".zip", ".csv")
    compression = zipfile.ZIP_STORED if stored else zipfile.ZIP_DEFLATED
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", compression=compression) as archive:
        archive.writestr(member, content)
    return path


def zip_bytes(content: bytes, stored: bool = False) -> bytes:
    member = f"aisdk-{DAY}.csv"
    compression = zipfile.ZIP_STORED if stored else zipfile.ZIP_DEFLATED
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=compression) as archive:
        archive.writestr(member, content)
    return buffer.getvalue()


def summary(capsys) -> dict:
    lines = capsys.readouterr().out.splitlines()
    last = [line for line in lines if line.startswith("[load-dma] summary ")][-1]
    return json.loads(last.removeprefix("[load-dma] summary "))


def run_local(src: Path, dest: Path, start: str = DAY, end: str | None = None) -> int:
    argv = ["--from", start, "--to", end or start, "--src-dir", str(src)]
    return load_dma.main([*argv, "--local-dest", str(dest)])


def day_dir(dest: Path, day: str = DAY) -> Path:
    return dest / f"day={day}"


# --- local source, local destination ----------------------------------------------------------


def test_local_load_is_byte_identical_and_counted(tmp_path, capsys):
    content = csv_bytes(3)
    make_zip(tmp_path / "src" / f"aisdk-{DAY}.zip", content)

    assert run_local(tmp_path / "src", tmp_path / "out") == 0

    folder = day_dir(tmp_path / "out")
    assert sorted(p.name for p in folder.iterdir()) == [f"aisdk-{DAY}.csv"]  # no .tmp left
    assert (folder / f"aisdk-{DAY}.csv").read_bytes() == content
    [day] = summary(capsys)["days"]
    assert (day["status"], day["rows"], day["bytes"]) == ("ok", 3, len(content))


def test_rerun_replaces_the_day(tmp_path, capsys):
    src = tmp_path / "src" / f"aisdk-{DAY}.zip"
    make_zip(src, csv_bytes(3, name="OLD"))
    assert run_local(tmp_path / "src", tmp_path / "out") == 0
    new = csv_bytes(5, name="NEW")
    make_zip(src, new)

    assert run_local(tmp_path / "src", tmp_path / "out") == 0

    folder = day_dir(tmp_path / "out")
    assert [p.name for p in folder.iterdir()] == [f"aisdk-{DAY}.csv"]
    assert (folder / f"aisdk-{DAY}.csv").read_bytes() == new
    assert summary(capsys)["days"][0]["rows"] == 5


def test_missing_day_exits_3_and_other_days_load(tmp_path, capsys):
    make_zip(tmp_path / "src" / "aisdk-2026-10-01.zip", csv_bytes(2))
    make_zip(tmp_path / "src" / "aisdk-2026-10-03.zip", csv_bytes(4))

    code = run_local(tmp_path / "src", tmp_path / "out", "2026-10-01", "2026-10-03")

    assert code == load_dma.EXIT_NOT_PUBLISHED == 3
    statuses = [d["status"] for d in summary(capsys)["days"]]
    assert statuses == ["ok", "not_published", "ok"]
    assert (day_dir(tmp_path / "out", "2026-10-03") / "aisdk-2026-10-03.csv").exists()
    assert not day_dir(tmp_path / "out", "2026-10-02").exists()


def test_corrupt_zip_exits_1(tmp_path, capsys):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / f"aisdk-{DAY}.zip").write_bytes(b"this is not a zip file at all")

    assert run_local(tmp_path / "src", tmp_path / "out") == 1
    out = capsys.readouterr().out
    assert f"[load-dma] {DAY} FAILED BadZipFile" in out


def test_wrong_header_exits_1_and_keeps_the_old_file(tmp_path, capsys):
    src = tmp_path / "src" / f"aisdk-{DAY}.zip"
    good = csv_bytes(2)
    make_zip(src, good)
    assert run_local(tmp_path / "src", tmp_path / "out") == 0
    make_zip(src, b"a,b,c\n1,2,3\n")

    assert run_local(tmp_path / "src", tmp_path / "out") == 1

    folder = day_dir(tmp_path / "out")
    assert [p.name for p in folder.iterdir()] == [f"aisdk-{DAY}.csv"]
    assert (folder / f"aisdk-{DAY}.csv").read_bytes() == good
    assert "unexpected header" in summary(capsys)["days"][0]["error"]


def test_two_csv_members_fail(tmp_path):
    path = tmp_path / "src" / f"aisdk-{DAY}.zip"
    path.parent.mkdir()
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("a.csv", csv_bytes(1))
        archive.writestr("b.csv", csv_bytes(1))

    assert run_local(tmp_path / "src", tmp_path / "out") == 1


def test_crc_error_mid_stream_removes_the_tmp_file(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(load_dma, "CHUNK", 1024)  # so the bad byte comes after the first write
    content = csv_bytes(50)
    path = make_zip(tmp_path / "src" / f"aisdk-{DAY}.zip", content, stored=True)
    raw = bytearray(path.read_bytes())
    at = raw.index(content[-40:])  # a byte inside the stored member, after the header
    raw[at] ^= 0xFF
    path.write_bytes(bytes(raw))

    assert run_local(tmp_path / "src", tmp_path / "out") == 1

    folder = day_dir(tmp_path / "out")
    assert list(folder.iterdir()) == []  # tmp removed, no final file
    assert "CRC" in summary(capsys)["days"][0]["error"]


def test_failure_wins_over_not_published(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "aisdk-2026-10-01.zip").write_bytes(b"broken")

    assert run_local(tmp_path / "src", tmp_path / "out", "2026-10-01", "2026-10-02") == 1


def test_rows_without_a_trailing_newline(tmp_path, capsys):
    content = csv_bytes(4, trailing_newline=False)
    make_zip(tmp_path / "src" / f"aisdk-{DAY}.zip", content)

    assert run_local(tmp_path / "src", tmp_path / "out") == 0

    day = summary(capsys)["days"][0]
    assert (day["rows"], day["bytes"]) == (4, len(content))


def test_counter_edge_cases():
    header_only = load_dma.Counter()
    header_only.add(HEADER.encode() + b"\n")
    no_newline = load_dma.Counter()
    no_newline.add(HEADER.encode())

    assert header_only.rows == 0
    assert no_newline.rows == 0


def test_to_before_from_exits_2(tmp_path):
    with pytest.raises(SystemExit) as exc:
        load_dma.main(["--from", "2026-10-02", "--to", "2026-10-01", "--src-dir", str(tmp_path)])
    assert exc.value.code == 2


# --- test servers -----------------------------------------------------------------------------


@contextlib.contextmanager
def serve(handler: type, **attrs) -> Iterator[http.server.ThreadingHTTPServer]:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    server.daemon_threads = True
    for name, value in attrs.items():
        setattr(server, name, value)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def base_url(server: http.server.ThreadingHTTPServer) -> str:
    return f"http://127.0.0.1:{server.server_address[1]}"


class Quiet(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args) -> None:
        pass


class RangeHandler(Quiet):
    """Serves server.files with HEAD and open-ended Range GETs; can drop or ignore ranges."""

    def do_HEAD(self) -> None:
        data = self.server.files.get(self.path)
        if data is None:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()

    def do_GET(self) -> None:
        data = self.server.files.get(self.path)
        ranges = self.headers.get("Range")
        self.server.gets.append(ranges)
        if data is None:
            self.send_error(404)
            return
        if not ranges or self.server.ignore_range:
            self._send(200, data, len(data))
            return
        start = int(ranges.removeprefix("bytes=").rstrip("-"))
        body = data[start:]
        self.send_response_only(206)
        self.send_header("Content-Range", f"bytes {start}-{len(data) - 1}/{len(data)}")
        if start == 0 and self.server.drop_at:  # send part of the body, then hang up
            self._send(None, body[: self.server.drop_at], len(body))
            self.server.drop_at = 0
            return
        self._send(None, body, len(body))

    def _send(self, status: int | None, body: bytes, length: int) -> None:
        if status is not None:
            self.send_response_only(status)
        self.send_header("Content-Length", str(length))
        self.end_headers()
        # The client may close a range early; that is allowed.
        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            self.wfile.write(body)


@pytest.fixture
def range_server():
    with serve(RangeHandler, files={}, gets=[], ignore_range=False, drop_at=0) as server:
        yield server


def test_http_range_file_reads_the_zip_with_few_requests(range_server):
    content = csv_bytes(3000)
    range_server.files["/aisdk.zip"] = zip_bytes(content)

    raw = load_dma.HttpRangeFile(f"{base_url(range_server)}/aisdk.zip")
    with zipfile.ZipFile(io.BufferedReader(raw, buffer_size=1 << 20)) as archive:
        data = archive.read(load_dma.csv_member(archive))

    assert data == content
    assert len(range_server.gets) <= 6
    assert raw.requests == len(range_server.gets) + 1  # plus the HEAD


def test_http_range_file_reconnects_after_a_dropped_connection(range_server):
    content = csv_bytes(3000)
    range_server.files["/aisdk.zip"] = zip_bytes(content, stored=True)  # bigger than drop_at
    range_server.drop_at = 50_000

    raw = load_dma.HttpRangeFile(f"{base_url(range_server)}/aisdk.zip", backoff=0)
    with zipfile.ZipFile(io.BufferedReader(raw, buffer_size=1 << 20)) as archive:
        data = archive.read(load_dma.csv_member(archive))

    assert data == content
    assert "bytes=50000-" in range_server.gets


def test_http_range_file_needs_206(range_server):
    range_server.files["/aisdk.zip"] = zip_bytes(csv_bytes(3))
    range_server.ignore_range = True

    raw = load_dma.HttpRangeFile(f"{base_url(range_server)}/aisdk.zip")
    with pytest.raises(load_dma.LoadError, match="expected 206"):
        raw.read(10)


def test_url_source_404_is_not_published(range_server, tmp_path, capsys):
    content = csv_bytes(3)
    range_server.files["/aisdk-2026-10-01.zip"] = zip_bytes(content)
    template = f"{base_url(range_server)}/aisdk-{{date}}.zip"

    code = load_dma.main(
        ["--from", "2026-10-01", "--to", "2026-10-02", "--url-template", template,
         "--local-dest", str(tmp_path / "out")]
    )  # fmt: skip

    assert code == 3
    assert (day_dir(tmp_path / "out") / f"aisdk-{DAY}.csv").read_bytes() == content
    out = capsys.readouterr().out
    assert "[load-dma] 2026-10-02 not published yet (404 http://127.0.0.1:" in out


# --- WebHDFS ----------------------------------------------------------------------------------


class FakeWebHdfs(Quiet):
    """Namenode and datanode in one: an in-memory dict of path -> bytes."""

    def _parse(self) -> tuple[str, dict[str, str]]:
        parts = urllib.parse.urlsplit(self.path)
        assert parts.path.startswith("/webhdfs/v1/")
        path = urllib.parse.unquote(parts.path.removeprefix("/webhdfs/v1"))
        query = dict(urllib.parse.parse_qsl(parts.query))
        self.server.calls.append((self.command, path, query))
        return path, query

    def _json(self, status: int, payload: dict | None = None, **headers: str) -> None:
        body = json.dumps(payload).encode() if payload is not None else b""
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _remote(self, status: int, exception: str, message: str) -> None:
        self._json(status, {"RemoteException": {"exception": exception, "message": message}})

    def _read_chunked(self) -> bytes:
        body = bytearray()
        while size := int(self.rfile.readline().split(b";")[0].strip(), 16):
            body += self.rfile.read(size)
            self.rfile.readline()
        self.rfile.readline()  # the empty line after the last chunk
        return bytes(body)

    def do_PUT(self) -> None:
        path, query = self._parse()
        files = self.server.files
        if query["op"] == "CREATE" and "datanode" not in query:
            location = (
                f"{base_url(self.server)}/webhdfs/v1{urllib.parse.quote(path)}"
                f"?op=CREATE&datanode=true&user.name={query['user.name']}&overwrite=true"
            )
            self._json(307, Location=location)
        elif query["op"] == "CREATE":
            self.server.encodings.append(self.headers.get("Transfer-Encoding"))
            data = self._read_chunked()
            if self.server.fail_datanode:
                self._remote(500, "DiskFullException", "no space left")
                return
            files[path] = data
            self._json(201, Location=f"hdfs://namenode{path}")
        elif query["op"] == "RENAME":
            destination = query["destination"]
            if path not in files:
                self._remote(404, "FileNotFoundException", path)
            elif destination in files and query.get("renameoptions") != "OVERWRITE":
                self._remote(409, "FileAlreadyExistsException", destination)
            else:
                files[destination] = files.pop(path)
                self._json(200)
        else:
            self._remote(400, "IllegalArgumentException", query["op"])

    def do_GET(self) -> None:
        path, query = self._parse()
        assert query["op"] == "GETFILESTATUS"
        if path not in self.server.files:
            self._remote(404, "FileNotFoundException", f"File does not exist: {path}")
            return
        length = len(self.server.files[path])
        self._json(200, {"FileStatus": {"length": length, "type": "FILE"}})

    def do_DELETE(self) -> None:
        path, query = self._parse()
        assert query == {"op": "DELETE", "recursive": "false", "user.name": query["user.name"]}
        self._json(200, {"boolean": self.server.files.pop(path, None) is not None})


@pytest.fixture
def webhdfs():
    with serve(FakeWebHdfs, files={}, calls=[], encodings=[], fail_datanode=False) as server:
        yield server


def test_webhdfs_sink_operations(webhdfs):
    sink = load_dma.WebHdfs(base_url(webhdfs), "spark")
    tmp, final = "/lake/x/day=2026-10-01/.a.csv.run.tmp", "/lake/x/day=2026-10-01/a.csv"

    sink.create(final, iter([b"old"]))
    sink.create(tmp, iter([b"new ", b"data"]))
    sink.rename_overwrite(tmp, final)

    assert webhdfs.files == {final: b"new data"}
    assert sink.length(final) == 8
    assert sink.length(tmp) is None
    sink.delete(final)
    assert webhdfs.files == {}
    assert webhdfs.encodings == ["chunked", "chunked"]
    assert {query["user.name"] for _, _, query in webhdfs.calls} == {"spark"}
    renames = [query for _, _, query in webhdfs.calls if query["op"] == "RENAME"]
    assert renames == [
        {"op": "RENAME", "destination": final, "renameoptions": "OVERWRITE", "user.name": "spark"}
    ]


def test_webhdfs_errors_carry_the_remote_message(webhdfs):
    webhdfs.fail_datanode = True
    sink = load_dma.WebHdfs(base_url(webhdfs), "spark")

    with pytest.raises(load_dma.LoadError, match="DiskFullException: no space left"):
        sink.create("/lake/x/a.csv", iter([b"data"]))


def test_main_end_to_end_into_webhdfs(webhdfs, tmp_path):
    content = csv_bytes(7)
    make_zip(tmp_path / "src" / f"aisdk-{DAY}.zip", content)
    final = f"/lake/landing/dma/day={DAY}/aisdk-{DAY}.csv"
    webhdfs.files[final] = b"an older load"

    code = load_dma.main(
        ["--from", DAY, "--src-dir", str(tmp_path / "src"), "--webhdfs", base_url(webhdfs)]
    )

    assert code == 0
    assert webhdfs.files == {final: content}  # replaced, tmp gone
    created = [path for method, path, q in webhdfs.calls if q["op"] == "CREATE"]
    assert all(Path(p).name.startswith(f".aisdk-{DAY}.csv.") for p in created)
    assert all(p.endswith(".tmp") for p in created)
    assert {query["user.name"] for _, _, query in webhdfs.calls} == {"spark"}


def test_main_webhdfs_failure_leaves_no_tmp(webhdfs, tmp_path):
    make_zip(tmp_path / "src" / f"aisdk-{DAY}.zip", csv_bytes(2))
    webhdfs.fail_datanode = True

    code = load_dma.main(
        ["--from", DAY, "--src-dir", str(tmp_path / "src"), "--webhdfs", base_url(webhdfs)]
    )

    assert code == 1
    assert webhdfs.files == {}
    assert [q["op"] for _, _, q in webhdfs.calls][-1] == "DELETE"
