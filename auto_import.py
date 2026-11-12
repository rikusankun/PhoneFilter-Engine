#!/usr/bin/env python3
"""把文件夹里的 CSV / XLSX 底库并发、流式地灌进 ClickHouse。

用法:
    python auto_import.py                        # 默认扫 ./data_base
    python auto_import.py --probe                # 只看硬件探测和并发决策，不连库
    python auto_import.py --limit 5              # 冒烟测试，只跑 5 个文件
    python auto_import.py D:/xxx/data --workers 6

依赖:
    pip install psutil clickhouse-connect openpyxl
    （openpyxl 只在目录里出现 .xlsx 时才需要；全程不用 pandas）

稳定性设计:
  * 线程数 = clamp(物理核心 x 80%, 4, 8)，再按可用内存折算硬上限；
  * 每个工作线程独享一条 ClickHouse 连接，逐行流式读取，绝不整表读进内存；
  * imported_files.txt 用 threading.Lock 串行写，只有整文件落库成功才记断点；
  * 只有主线程刷新进度行，工作线程把日志/异常丢进队列，终端不会被刷爆；
  * 遇到 Too many parts / 断连等瞬时故障，全局退避 + 指数重试 + 重建连接。
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from queue import Empty, Queue

try:
    import psutil
except ImportError:  # 没装也能跑，只是拿不到 CPU / 内存读数
    psutil = None

from import_data import (
    COLUMNS,
    CONTAINER,
    DATABASE,
    GENERIC_NULLS,
    HOST,
    NAME_NULLS,
    PASSWORD,
    PHONE_NULLS,
    PORT,
    SNAPSHOT_LIMIT,
    TABLE,
    USER,
    DockerInserter,
    cell,
    configure_stdio,
    connect,
    ensure_schema,
    is_retriable,
    load_phone_snapshot,
    parse_age,
    parse_money,
    parse_string,
    resolve_indexes,
    say,
    select_changed_records,
)

ROOT = Path(__file__).resolve().parent
LEDGER_PATH = ROOT / "imported_files.txt"
DEFAULT_FOLDER = ROOT / "data_base"

BATCH_SIZE = 200_000
MIN_WORKERS = 4
MAX_WORKERS = 8
CPU_TARGET = 0.80
EST_BYTES_PER_ROW = 700  # 一行 11 列在 CPython 里的大致内存占用
MEM_BUDGET_RATIO = 0.60  # 最多拿 60% 可用内存放并发批次
PER_WORKER_OVERHEAD = 128 * 1024**2
INSERT_ATTEMPTS = 5

# 工作线程永远不直接 print，只把消息丢进队列，由主线程统一输出。
EVENTS: Queue[str] = Queue()


def emit(message: str) -> None:
    EVENTS.put(message)


class Cancelled(Exception):
    """Ctrl+C 时让正在跑的文件体面退出，且不记断点。"""


# ---------------------------------------------------------------------------
# 1. 硬件探测 + 自适应并发
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Hardware:
    logical: int
    physical: int
    mem_total: int
    mem_available: int
    source: str


def _windows_memory() -> tuple[int, int]:
    """没装 psutil 时，用 Win32 API 兜底拿物理内存（不引入额外依赖）。"""
    if sys.platform != "win32":
        return 0, 0
    try:
        import ctypes

        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        stat = MEMORYSTATUSEX()
        stat.dwLength = ctypes.sizeof(stat)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
            return 0, 0
        return stat.ullTotalPhys, stat.ullAvailPhys
    except Exception:
        return 0, 0


def probe_hardware() -> Hardware:
    logical = os.cpu_count() or 4
    physical = max(1, logical // 2)
    mem_total = mem_available = 0
    source = "os.cpu_count + Win32 API"

    if psutil is not None:
        logical = psutil.cpu_count(logical=True) or logical
        physical = psutil.cpu_count(logical=False) or max(1, logical // 2)
        vm = psutil.virtual_memory()
        mem_total, mem_available = vm.total, vm.available
        source = "psutil"
    else:
        mem_total, mem_available = _windows_memory()

    return Hardware(logical, physical, mem_total, mem_available, source)


def per_worker_bytes(batch_size: int) -> int:
    """行数 x 单行占用 x 2（行列表 + 序列化缓冲）+ 固定开销。"""
    return batch_size * EST_BYTES_PER_ROW * 2 + PER_WORKER_OVERHEAD


def choose_workers(hw: Hardware, batch_size: int, override: int | None) -> tuple[int, str]:
    # 按物理核心算，超线程不白送算力；0.8 留出系统 / ClickHouse 服务端的余量。
    base = max(1, round(hw.physical * CPU_TARGET))
    base = max(MIN_WORKERS, min(MAX_WORKERS, base))
    notes = [f"物理核心 {hw.physical} x {CPU_TARGET:.0%} → 基准 {base} 线程（{hw.source}）"]

    need = per_worker_bytes(batch_size)
    if hw.mem_available > 0:
        mem_cap = max(1, int(hw.mem_available * MEM_BUDGET_RATIO) // need)
        notes.append(
            f"可用内存 {hw.mem_available / 1024**3:.1f} GB，单线程预留 {need / 1024**3:.2f} GB → 内存上限 {mem_cap} 线程"
        )
        base = min(base, mem_cap)  # 内存是硬约束，稳定性优先于跑满
    else:
        notes.append("拿不到内存读数，跳过内存限流")

    if override is not None:
        base = max(1, min(override, 32))
        notes.append(f"--workers 覆盖 → {base} 线程")

    return base, "；".join(notes)


# ---------------------------------------------------------------------------
# 2. 每线程独立连接 + 全局退避重试
# ---------------------------------------------------------------------------


class ClickHousePool:
    """每个工作线程一条独立 client.insert 连接，主线程统一登记 / 关闭。

    HTTP 后端：thread-local 的 clickhouse_connect client，注册表供主线程收尾。
    Docker 后端：clickhouse-client 本来就每次一个子进程，天然隔离，但用信号量限流。
    """

    def __init__(self, backend: str, password: str, max_inserts: int, async_insert: bool) -> None:
        self.backend = backend
        self._password = password
        self._async_insert = async_insert
        self._docker = DockerInserter(CONTAINER) if backend == "docker" else None
        self._clients: dict[int, object] = {}
        self._clients_lock = threading.Lock()
        self._insert_gate = threading.BoundedSemaphore(max_inserts) if max_inserts > 0 else None
        self._pause_until = 0.0
        self._pause_lock = threading.Lock()

    def _client(self):
        try:
            import clickhouse_connect
        except ImportError as exc:
            raise RuntimeError("缺少依赖，请执行 pip install clickhouse-connect") from exc

        tid = threading.get_ident()
        with self._clients_lock:
            client = self._clients.get(tid)
        if client is None:
            client = clickhouse_connect.get_client(
                host=HOST,
                port=PORT,
                username=USER,
                password="" if self._password == "" else self._password,
                connect_timeout=20,
                send_receive_timeout=3600,
                compress=True,
            )
            with self._clients_lock:
                self._clients[tid] = client
        return client

    def command(self, sql: str) -> str:
        if self._docker is not None:
            return self._docker.command(sql)
        result = self._client().command(sql)
        return "" if result is None else str(result).strip()

    def query_rows(self, sql: str) -> list[tuple]:
        if self._docker is not None:
            return self._docker.query_rows(sql)
        return list(self._client().query(sql).result_rows)

    def reset_thread(self) -> None:
        """当前线程连接作废，下次 insert 会重新建连。"""
        if self._docker is not None:
            return
        with self._clients_lock:
            client = self._clients.pop(threading.get_ident(), None)
        if client is not None:
            try:
                client.close()
            except Exception:
                pass

    def close(self) -> None:
        with self._clients_lock:
            clients = list(self._clients.values())
            self._clients.clear()
        for client in clients:
            try:
                client.close()
            except Exception:
                pass

    @staticmethod
    def _transient(exc: BaseException) -> bool:
        if is_retriable(exc):
            return True
        text = str(exc).lower()
        extra = (
            "code: 252",
            "too many simultaneous queries",
            "memory limit exceeded",
            "cannot allocate",
            "server is overloaded",
            "503",
            "429",
        )
        return any(needle in text for needle in extra)

    @staticmethod
    def _is_parts_error(exc: BaseException) -> bool:
        text = str(exc).lower()
        return "too many parts" in text or "code: 252" in text

    def _cool_down(self, seconds: float) -> None:
        """一个线程撞到瞬时故障，所有线程一起短暂歇口气，给 CH 合并 part 的时间。"""
        cap = 20.0
        with self._pause_lock:
            self._pause_until = min(
                max(self._pause_until, time.monotonic()) + min(seconds, cap),
                time.monotonic() + cap,
            )

    def _wait_gate(self) -> None:
        while True:
            with self._pause_lock:
                left = self._pause_until - time.monotonic()
            if left <= 0:
                return
            time.sleep(min(left, 0.25))

    def _raw_insert(self, rows: list[tuple]) -> None:
        if self._docker is not None:
            self._docker.insert(rows)
            return
        settings = {"async_insert": 0}
        if self._async_insert:
            settings = {"async_insert": 1, "wait_for_async_insert": 1}
        self._client().insert(
            TABLE,
            rows,
            column_names=list(COLUMNS),
            database=DATABASE,
            settings=settings,
        )

    def insert(self, rows: list[tuple]) -> None:
        delay = 1.0
        for attempt in range(1, INSERT_ATTEMPTS + 1):
            self._wait_gate()
            try:
                if self._insert_gate is None:
                    self._raw_insert(rows)
                else:
                    with self._insert_gate:
                        self._raw_insert(rows)
                return
            except Exception as exc:
                if attempt >= INSERT_ATTEMPTS or not self._transient(exc):
                    raise
                # Too many parts 是 CH 后台 merge 跟不上，退避要更狠一点。
                if self._is_parts_error(exc):
                    delay = max(delay, 8.0)
                self._cool_down(delay)
                self.reset_thread()
                emit(
                    f"   ⏳ 瞬时故障（第 {attempt}/{INSERT_ATTEMPTS} 次重试，全局退避 {delay:.0f}s）："
                    f"{type(exc).__name__}: {str(exc)[:120]}"
                )
                time.sleep(delay + random.uniform(0, 0.6))
                delay = min(delay * 2, 20.0)
        raise RuntimeError("插入重试次数耗尽")


# ---------------------------------------------------------------------------
# 3. 去重：共享快照、整批只加一次锁
# ---------------------------------------------------------------------------


class DedupIndex:
    """auto  = 表小就装内存快照；表太大就交给 ReplacingMergeTree 合并
    off    = 完全不去重，最快
    query  = 逐批回库比对（原始行为，很慢，并发下必须限流）
    snapshot = 强制内存快照
    """

    def __init__(self, mode: str, pool: ClickHousePool, query_concurrency: int) -> None:
        self.mode = mode
        self._pool = pool
        self._index: dict[str, tuple] | None = None
        self._lock = threading.Lock()
        self._frozen = False
        self._query_gate = (
            threading.BoundedSemaphore(max(1, query_concurrency)) if mode == "query" else None
        )

        if mode in {"auto", "snapshot"}:
            self._index = load_phone_snapshot(pool)
            if self._index is None:
                if mode == "snapshot":
                    emit(f"   ⚠️ 表超过 {SNAPSHOT_LIMIT:,} 行，装不下快照，本次改为不去重")
                else:
                    emit(
                        f"   ⚠️ 表超过 {SNAPSHOT_LIMIT:,} 行，跳过回库比对，"
                        "重复号码交给 ReplacingMergeTree 合并（查询记得加 FINAL）"
                    )
                self.mode = "off"
            else:
                self.mode = "snapshot"
                emit(f"   🧠 已把 {len(self._index):,} 行库内快照装进内存（共享 + 批次级加锁）")

    def filter(self, rows: list[tuple]) -> tuple[list[tuple], int]:
        """批内同号码只留最后一条，再和已知状态比对。整批只持锁一次。"""
        latest: dict[str, tuple] = {}
        order: list[str] = []
        for row in rows:
            if row[0] not in latest:
                order.append(row[0])
            latest[row[0]] = row
        batch = [latest[phone] for phone in order]
        skipped = len(rows) - len(batch)

        if self.mode == "off":
            return batch, skipped

        if self.mode == "query":
            assert self._query_gate is not None
            with self._query_gate:
                keep, more = select_changed_records(batch, self._pool.query_rows)
            return keep, skipped + more

        assert self._index is not None
        with self._lock:
            keep = []
            for row in batch:
                if self._index.get(row[0]) == row:
                    skipped += 1
                else:
                    keep.append(row)
        # 注意：这里故意不改快照，等 insert 真的成功后再 commit。
        # 否则一次失败重试会被快照误判成"已入库"，造成静默丢数据。
        return keep, skipped

    def commit(self, rows: list[tuple]) -> None:
        if self.mode != "snapshot" or self._frozen or not rows:
            return
        assert self._index is not None
        with self._lock:
            for row in rows:
                if len(self._index) >= SNAPSHOT_LIMIT:
                    self._frozen = True
                    emit(
                        f"   ⚠️ 内存快照到达 {SNAPSHOT_LIMIT:,} 行上限，不再增长；"
                        "之后的跨文件重复由 ReplacingMergeTree 兜底"
                    )
                    break
                self._index[row[0]] = row


# ---------------------------------------------------------------------------
# 4. 断点日志：一把锁，串行追加
# ---------------------------------------------------------------------------


class Ledger:
    def __init__(self, path: Path) -> None:
        self._lock = threading.Lock()
        self._handle = path.open("a", encoding="utf-8", newline="\n")
        self._since_fsync = 0

    @staticmethod
    def load(path: Path) -> set[str]:
        if not path.exists():
            return set()
        return {
            line.strip()
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }

    def mark(self, key: str) -> None:
        # 只有整文件所有批次都 insert 成功后，调用方才会走到这里。
        with self._lock:
            self._handle.write(key + "\n")
            self._handle.flush()
            self._since_fsync += 1
            if self._since_fsync >= 20:
                try:
                    os.fsync(self._handle.fileno())
                except OSError:
                    pass
                self._since_fsync = 0

    def close(self) -> None:
        with self._lock:
            try:
                self._handle.flush()
                os.fsync(self._handle.fileno())
            except OSError:
                pass
            finally:
                self._handle.close()


# ---------------------------------------------------------------------------
# 5. 进度计数（主线程读，工作线程写，全程加锁）
# ---------------------------------------------------------------------------


@dataclass
class Progress:
    total: int
    workers: int
    done: int = 0
    failed: int = 0
    cancelled: int = 0
    active: int = 0
    rows_read: int = 0
    rows_written: int = 0
    rows_skipped: int = 0
    bad_rows: int = 0
    peak_cpu: float = 0.0
    start: float = field(default_factory=time.monotonic)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _samples: deque = field(default_factory=lambda: deque(maxlen=60), repr=False)

    @property
    def finished(self) -> int:
        with self._lock:
            return self.done + self.failed + self.cancelled

    def started(self) -> None:
        with self._lock:
            self.active += 1

    def bump(self, read: int = 0, written: int = 0, skipped: int = 0, bad: int = 0) -> None:
        with self._lock:
            self.rows_read += read
            self.rows_written += written
            self.rows_skipped += skipped
            self.bad_rows += bad
            self._samples.append((time.monotonic(), self.rows_written))

    def file_done(self) -> None:
        with self._lock:
            self.done += 1
            self.active -= 1

    def file_failed(self) -> None:
        with self._lock:
            self.failed += 1
            self.active -= 1

    def file_cancelled(self) -> None:
        with self._lock:
            self.cancelled += 1
            self.active -= 1

    def note_cpu(self, cpu: float) -> None:
        with self._lock:
            self.peak_cpu = max(self.peak_cpu, cpu)

    def snapshot(self) -> dict[str, float | int]:
        with self._lock:
            data = {
                "done": self.done,
                "failed": self.failed,
                "cancelled": self.cancelled,
                "active": self.active,
                "rows_read": self.rows_read,
                "rows_written": self.rows_written,
                "rows_skipped": self.rows_skipped,
                "bad_rows": self.bad_rows,
                "peak_cpu": self.peak_cpu,
                "elapsed": time.monotonic() - self.start,
            }
            samples = list(self._samples)
        speed = 0.0
        if len(samples) >= 2 and samples[-1][0] > samples[0][0]:
            speed = (samples[-1][1] - samples[0][1]) / (samples[-1][0] - samples[0][0])
        data["speed"] = speed
        return data


# ---------------------------------------------------------------------------
# 6. 流式逐行读取（没有 pandas，没有 read_csv，没有整表加载）
# ---------------------------------------------------------------------------


@dataclass
class RowStats:
    bad: int = 0


def detect_encoding(path: Path) -> str:
    with path.open("rb") as handle:
        sample = handle.read(65536)
    for encoding in ("utf-8-sig", "utf-8", "gb18030", "cp1252"):
        try:
            sample.decode(encoding)
        except UnicodeDecodeError:
            continue
        return encoding
    return "utf-8"


def as_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer() and abs(value) < 10**15:
            return str(int(value))
        return str(value)
    if isinstance(value, datetime):
        return value.strftime("%Y/%m/%d")
    if isinstance(value, date):
        return value.strftime("%Y/%m/%d")
    return str(value).strip()


def clean_row_quiet(raw: list[str], indexes: dict[str, int]) -> tuple:
    """和 import_data.clean_row 同一套 11 列清洗，但不逐行刷终端。

    parse_* 已经返回 str / int，和 ClickHouse 返回的字段类型一致，
    所以这里直接产出最终元组，省掉原版每行一次 coerce_record 的二次拷贝。
    """
    phone, _ = parse_string(cell(raw, indexes.get("phone")), PHONE_NULLS)
    if "name" in indexes:
        name, _ = parse_string(cell(raw, indexes.get("name")), NAME_NULLS)
    else:
        first, _ = parse_string(cell(raw, indexes.get("first_name")), NAME_NULLS)
        last, _ = parse_string(cell(raw, indexes.get("last_name")), NAME_NULLS)
        name = " ".join(part for part in (first, last) if part)
    age, _, _ = parse_age(cell(raw, indexes.get("age")))
    gender, _ = parse_string(cell(raw, indexes.get("gender")), GENERIC_NULLS)
    estimated_value, _ = parse_money(cell(raw, indexes.get("estimated_value")))
    estimated_equity, _ = parse_money(cell(raw, indexes.get("estimated_equity")))
    last_sale_amount, _ = parse_money(cell(raw, indexes.get("last_sale_amount")))
    last_sale_date, _ = parse_string(cell(raw, indexes.get("last_sale_date")), GENERIC_NULLS)
    email, _ = parse_string(cell(raw, indexes.get("email")), GENERIC_NULLS)
    ownership_type, _ = parse_string(cell(raw, indexes.get("ownership_type")), GENERIC_NULLS)
    occupancy_type, _ = parse_string(cell(raw, indexes.get("occupancy_type")), GENERIC_NULLS)
    return (
        phone,
        name,
        age,
        gender,
        estimated_value,
        estimated_equity,
        last_sale_amount,
        last_sale_date,
        email,
        ownership_type,
        occupancy_type,
    )


def iter_csv_rows(path: Path, stats: RowStats):
    # errors="replace"：31 GB 的异构文件里偶尔一两个坏字节不该让整个文件失败。
    with path.open("r", encoding=detect_encoding(path), errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        header = None
        while header is None:
            try:
                first = next(reader)
            except StopIteration:
                raise ValueError("文件是空的") from None
            except csv.Error:
                stats.bad += 1
                continue
            if len(first) == 1 and first[0].strip().lower().startswith("sep="):
                continue
            header = first
        indexes = resolve_indexes(header)  # 表头不对 -> SystemExit，整文件不记断点
        while True:
            try:
                raw = next(reader)
            except StopIteration:
                return
            except csv.Error:
                stats.bad += 1
                continue
            if not raw or not any(item.strip() for item in raw):
                continue
            try:
                yield clean_row_quiet(raw, indexes)
            except (ValueError, IndexError, TypeError):
                stats.bad += 1


def iter_xlsx_rows(path: Path, stats: RowStats):
    try:
        import openpyxl
    except ImportError as exc:
        raise RuntimeError("目录里有 .xlsx，需要 pip install openpyxl") from exc

    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        rows = workbook.active.iter_rows(values_only=True)
        try:
            header = next(rows)
        except StopIteration:
            raise ValueError("文件是空的") from None
        indexes = resolve_indexes([as_text(item) for item in header])
        for raw in rows:
            values = [as_text(item) for item in raw]
            if not any(item.strip() for item in values):
                continue
            try:
                yield clean_row_quiet(values, indexes)
            except (ValueError, IndexError, TypeError):
                stats.bad += 1
    finally:
        workbook.close()


def iter_rows(path: Path, stats: RowStats):
    if path.suffix.lower() == ".xlsx":
        yield from iter_xlsx_rows(path, stats)
    else:
        yield from iter_csv_rows(path, stats)


def list_files(folder: Path) -> list[Path]:
    found = [
        path
        for path in folder.rglob("*")
        if path.is_file() and path.suffix.lower() in {".csv", ".xlsx"}
    ]
    found.sort(key=lambda path: path.relative_to(folder).as_posix().lower())
    return found


def file_key(folder: Path, path: Path) -> str:
    return path.relative_to(folder).as_posix()


# ---------------------------------------------------------------------------
# 7. 单个文件的工作单元
# ---------------------------------------------------------------------------


@dataclass
class Context:
    pool: ClickHousePool
    dedup: DedupIndex
    ledger: Ledger
    progress: Progress
    batch_size: int
    stop: threading.Event
    verbose: bool = False


def process_file(path: Path, key: str, ctx: Context) -> None:
    ctx.progress.started()
    stats = RowStats()
    batch: list[tuple] = []
    read = written = skipped = 0
    pending_written = pending_skipped = pending_bad = 0

    def flush() -> None:
        nonlocal written, skipped, pending_written, pending_skipped
        if not batch:
            return
        keep, batch_skipped = ctx.dedup.filter(batch)
        skipped += batch_skipped
        pending_skipped += batch_skipped
        if keep:
            ctx.pool.insert(keep)
            ctx.dedup.commit(keep)  # 插入成功后才更新共享快照
            written += len(keep)
            pending_written += len(keep)
        batch.clear()

    def bump() -> None:
        """按批次增量上报，避免大文件跑很久时进度条一动不动。"""
        nonlocal pending_written, pending_skipped, pending_bad
        ctx.progress.bump(written=pending_written, skipped=pending_skipped, bad=pending_bad)
        pending_written = pending_skipped = pending_bad = 0

    try:
        for row in iter_rows(path, stats):
            if ctx.stop.is_set():
                raise Cancelled()
            batch.append(row)
            read += 1
            if len(batch) >= ctx.batch_size:
                flush()
                pending_bad += stats.bad
                stats.bad = 0
                bump()
        flush()
        pending_bad += stats.bad
        bump()
        ctx.progress.bump(read=read)
        ctx.ledger.mark(key)
        ctx.progress.file_done()
        # 成功不逐文件打印（3744 个文件会把终端刷爆），进度都在监控行里。
        if ctx.verbose:
            emit(f"   ✅ {key} 完成：读 {read:,} 行，写 {written:,} 行，跳重复 {skipped:,} 行")
    except (Cancelled, KeyboardInterrupt):
        ctx.progress.file_cancelled()
    except (Exception, SystemExit) as exc:
        # resolve_indexes 抛的是 SystemExit，必须接住，否则表头不对的文件会反复重试。
        ctx.progress.file_failed()
        emit(f"   ❌ {key} 失败（不记断点，下次会重试）：{type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# 8. 主线程进度显示
# ---------------------------------------------------------------------------


def drain_events() -> None:
    """主线程独占终端输出；异常风暴时截断，避免反而把屏幕刷爆。"""
    messages: list[str] = []
    while True:
        try:
            messages.append(EVENTS.get_nowait())
        except Empty:
            break
    if not messages:
        return
    if len(messages) > 20:
        messages = messages[:15] + [f"   … 另有 {len(messages) - 15} 条同类消息已折叠"]
    for message in messages:
        sys.stdout.write("\n" + message + "\n")


def fmt_eta(seconds: float) -> str:
    if seconds <= 0 or seconds != seconds:  # NaN
        return "--:--"
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours:d}:{minutes:02d}:{secs:02d}" if hours else f"{minutes:02d}:{secs:02d}"


def render(progress: Progress) -> str:
    snap = progress.snapshot()
    cpu = psutil.cpu_percent(interval=None) if psutil is not None else None
    mem = psutil.virtual_memory().percent if psutil is not None else None
    if cpu is not None:
        progress.note_cpu(cpu)

    finished = int(snap["done"]) + int(snap["failed"]) + int(snap["cancelled"])
    remaining = max(0, progress.total - finished)
    elapsed = float(snap["elapsed"])
    eta_text = fmt_eta(remaining * elapsed / finished) if finished else "--:--"

    cpu_text = f"{cpu:.0f}%（峰值 {snap['peak_cpu']:.0f}%）" if cpu is not None else "n/a"
    mem_text = f"{mem:.0f}%" if mem is not None else "n/a"

    return (
        f"🚀 并发线程: {progress.workers} | 进度: {finished}/{progress.total} | "
        f"当前 CPU 占用: {cpu_text} | 内存: {mem_text} | "
        f"写入 {int(snap['rows_written']):,} 行 · {float(snap['speed']):,.0f} 行/s | "
        f"成功 {int(snap['done'])} 失败 {int(snap['failed'])} 活跃 {int(snap['active'])} | "
        f"ETA {eta_text}"
    )


# ---------------------------------------------------------------------------
# 9. 入口
# ---------------------------------------------------------------------------


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="并发、流式地把底库 CSV / XLSX 导入 ClickHouse")
    parser.add_argument("folder", nargs="?", default=str(DEFAULT_FOLDER), help="要扫描的文件夹")
    parser.add_argument(
        "--password",
        default=os.environ.get("CLICKHOUSE_PASSWORD", PASSWORD),
        help="ClickHouse 密码，默认读环境变量 CLICKHOUSE_PASSWORD，再退回空字符串",
    )
    parser.add_argument("--workers", type=int, default=None, help="覆盖自适应线程数")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help=f"每批行数，默认 {BATCH_SIZE:,}")
    parser.add_argument(
        "--max-inserts", type=int, default=0,
        help="同时落库的最大批次数，0=不限制；CH 报 Too many parts 时调到 2~3",
    )
    parser.add_argument(
        "--dedup-mode", choices=("auto", "snapshot", "off", "query"), default="auto",
        help="auto=小表装快照/大表交给合并；off=不去重最快；query=逐批回库比对（很慢）",
    )
    parser.add_argument("--query-concurrency", type=int, default=2, help="query 模式下同时回库查询的线程数")
    parser.add_argument("--async-insert", action="store_true", help="打开服务端 async_insert，减少小 part")
    parser.add_argument("--interval", type=float, default=0.5, help="进度刷新间隔（秒）")
    parser.add_argument("--limit", type=int, default=0, help="只处理前 N 个待导入文件，用于冒烟测试")
    parser.add_argument("--verbose", action="store_true", help="逐文件打印完成日志（默认关闭，避免刷屏）")
    parser.add_argument("--probe", action="store_true", help="只打印硬件探测与并发决策，不连库")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_stdio()
    try:
        csv.field_size_limit(min(sys.maxsize, 2**31 - 1))
    except OverflowError:
        csv.field_size_limit(2**31 - 1)

    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.batch_size < 1000:
        print("--batch-size 不能小于 1000", file=sys.stderr)
        return 2

    hw = probe_hardware()
    workers, why = choose_workers(hw, args.batch_size, args.workers)
    folder = Path(args.folder).expanduser().resolve()
    if not folder.is_dir():
        print(f"找不到文件夹：{folder}", file=sys.stderr)
        return 1

    files = list_files(folder)
    done = Ledger.load(LEDGER_PATH)
    pending_all = [(p, file_key(folder, p)) for p in files if file_key(folder, p) not in done]
    pending = pending_all[: args.limit] if args.limit > 0 else pending_all

    say("=" * 72)
    say(f"硬件探测（{hw.source}）：逻辑 {hw.logical} 核 / 物理 {hw.physical} 核 / "
        f"内存 {hw.mem_total / 1024**3:.1f} GB（可用 {hw.mem_available / 1024**3:.1f} GB）")
    say(f"并发决策：{why}")
    say(f"批次大小 {args.batch_size:,} 行，单线程预留 {per_worker_bytes(args.batch_size) / 1024**3:.2f} GB")
    say(f"发现文件 {len(files):,} 个，已导入 {len(files) - len(pending_all):,} 个（断点 {len(done):,} 条），"
        f"本次待导入 {len(pending):,} 个")
    say("=" * 72)

    if args.probe:
        say("--probe 模式：不连接 ClickHouse，退出。")
        return 0
    if not pending:
        say("没有新文件。")
        return 0
    if psutil is None:
        say("提示：pip install psutil 后进度条才能显示实时 CPU / 内存占用。")

    # 用 import_data.connect 探测后端（8123 空密码被拒时会自动退回 docker exec）
    try:
        probe = connect(args.password if args.password is not None else PASSWORD)
    except Exception as exc:
        print(f"连不上 ClickHouse：{exc}", file=sys.stderr)
        return 1
    backend = "docker" if isinstance(probe, DockerInserter) else "http"
    try:
        probe.reset()  # 探测用的那条连接关掉，工作线程会各自建新的
    except Exception:
        pass
    if backend == "http":
        say("入库后端：http 8123（每个工作线程一条独立长连接）")
    else:
        say("入库后端：docker exec 子进程（8123 拒绝了空密码，已自动回退）")
        say("   ⚠️ 这是本机能跑的最慢后端：每次落库都要起一个 clickhouse-client 进程。")
        say("   给 default 用户配好密码后，用 set CLICKHOUSE_PASSWORD=xxx 或 --password xxx")
        say("   即可切到原生 HTTP 长连接，通常还能再快一截。")

    pool = ClickHousePool(backend, args.password or "", args.max_inserts, args.async_insert)
    ledger = Ledger(LEDGER_PATH)
    progress = Progress(total=len(pending), workers=workers)
    stop = threading.Event()
    executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="import")
    futures = []
    startup_error = False

    try:
        ensure_schema(pool)
        dedup = DedupIndex(args.dedup_mode, pool, args.query_concurrency)
        ctx = Context(pool, dedup, ledger, progress, args.batch_size, stop, args.verbose)
        drain_events()

        futures = [executor.submit(process_file, path, key, ctx) for path, key in pending]
        if psutil is not None:
            psutil.cpu_percent(interval=None)  # 第一次调用返回值无意义，先预热

        # ---- 主线程监控：唯一允许写终端的地方 ----
        while True:
            drain_events()
            line = render(progress)
            sys.stdout.write("\r" + line.ljust(165))
            sys.stdout.flush()
            # 用 future 状态判断收尾，比依赖计数器更稳（极端异常也不会死循环）。
            if stop.is_set() or all(future.done() for future in futures):
                break
            stop.wait(max(0.1, args.interval))
        drain_events()
        sys.stdout.write("\n")
        sys.stdout.flush()
    except KeyboardInterrupt:
        startup_error = True
        stop.set()
        print("\n收到 Ctrl+C，正在收尾：未完成的文件不记断点，下次会自动重试。", flush=True)
    except (Exception, SystemExit) as exc:
        # ensure_schema 校验不通过时抛的是 SystemExit，也必须接住。
        startup_error = True
        stop.set()
        print(f"\n启动阶段失败：{type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
    finally:
        executor.shutdown(wait=True, cancel_futures=stop.is_set())
        for future in futures:
            if future.cancelled():
                # 排队中就被取消，压根没开跑，不用扣 active，也不计入断点。
                continue
            try:
                future.result()
            except BaseException as exc:  # 兜底，正常情况 process_file 已吞掉
                progress.file_failed()
                emit(f"   ❌ 线程异常：{type(exc).__name__}: {exc}")
        drain_events()
        ledger.close()
        pool.close()

    snap = progress.snapshot()
    say("=" * 72)
    say(
        f"扫盘结束：成功 {int(snap['done']):,} / 失败 {int(snap['failed']):,} / "
        f"中断 {int(snap['cancelled']):,}，跳过已导入 {len(files) - len(pending_all):,} 个"
    )
    say(
        f"读取 {int(snap['rows_read']):,} 行，写入 {int(snap['rows_written']):,} 行，"
        f"跳重复 {int(snap['rows_skipped']):,} 行，坏行 {int(snap['bad_rows']):,} 行"
    )
    say(
        f"耗时 {float(snap['elapsed']):.1f}s，平均 {int(snap['rows_written']) / max(float(snap['elapsed']), 1):,.0f} 行/s，"
        f"峰值 CPU {float(snap['peak_cpu']):.0f}%"
    )
    say("=" * 72)
    return 1 if (startup_error or int(snap["failed"]) > 0 or int(snap["cancelled"]) > 0) else 0


if __name__ == "__main__":
    sys.exit(main())
