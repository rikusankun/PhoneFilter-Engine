#!/usr/bin/env python3
"""把 Alaska_1.csv 清洗后批量写入 ClickHouse。

依赖（走 8123 时）: pip install clickhouse-connect

体检报告:
  成功 = 字段都没被改动、原样入库的行
  清洗 = 至少改过一处（金额符号、空值、出生年份、超过 120 岁等）的行
  总共入库 = 成功 + 清洗

同一个 phone 会在 ReplacingMergeTree 合并后只留一行。查询最新结果请加 FINAL。
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

try:
    import clickhouse_connect
except ImportError:  # 没装包时仍可走容器内客户端
    clickhouse_connect = None


BATCH_SIZE = 100_000
MAX_WORKERS = 8
CURRENT_YEAR = 2026
MAX_AGE = 120
UINT64_MAX = (1 << 64) - 1

DATABASE = "phone_filter_engine"
TABLE = "phone_records"
HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
USER = os.environ.get("CLICKHOUSE_USER", "default")
PASSWORD = ""
CONTAINER = os.environ.get("CLICKHOUSE_CONTAINER", "PhoneFilter_Engine")
ROOT = Path(__file__).resolve().parent
CSV_PATH = ROOT / "Alaska_1.csv"

COLUMNS = (
    "phone",
    "name",
    "age",
    "gender",
    "estimated_value",
    "estimated_equity",
    "last_sale_amount",
    "last_sale_date",
    "email",
    "ownership_type",
    "occupancy_type",
)

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {DATABASE}.{TABLE}
(
    phone String,
    name String,
    age UInt16,
    gender LowCardinality(String),
    estimated_value UInt64,
    estimated_equity UInt64,
    last_sale_amount UInt64,
    last_sale_date String,
    email String,
    ownership_type LowCardinality(String),
    occupancy_type LowCardinality(String)
)
ENGINE = ReplacingMergeTree()
ORDER BY (phone)
"""

INSERT_SQL = (
    f"INSERT INTO {DATABASE}.{TABLE} ({', '.join(COLUMNS)}) FORMAT TabSeparated"
)

# 名字里会出现 Nan / Na，不能当成空值清掉。
NAME_NULLS = {"n/a", "null", "#n/a"}
PHONE_NULLS = {"n/a", "null", "#n/a", "none", "nan", "-", "--", "undefined"}
GENERIC_NULLS = PHONE_NULLS

ALIASES: dict[str, tuple[str, ...]] = {
    "phone": (
        "phone",
        "phone_number",
        "phonenumber",
        "phone1",
        "phone_1",
        "primary_phone",
        "mobile",
        "mobile_phone",
        "cell",
        "cell_phone",
        "tel",
        "telephone",
    ),
    "name": ("name", "full_name", "fullname", "owner_name", "owner", "contact_name"),
    "first_name": ("first_name", "firstname", "owner_first_name"),
    "last_name": ("last_name", "lastname", "owner_last_name"),
    "age": ("age", "owner_age"),
    "gender": ("gender", "sex"),
    "estimated_value": (
        "estimated_value",
        "est_value",
        "estimatedvalue",
        "property_value",
        "market_value",
    ),
    "estimated_equity": (
        "estimated_equity",
        "est_equity",
        "estimatedequity",
        "equity",
    ),
    "last_sale_amount": (
        "last_sale_amount",
        "last_sale_price",
        "sale_amount",
        "sale_price",
        "lastsaleamount",
    ),
    "last_sale_date": (
        "last_sale_date",
        "sale_date",
        "lastsaledate",
        "last_sale_price_date",
    ),
    "email": ("email", "e_mail", "email_address", "email1", "email_1", "owner_email"),
    "ownership_type": ("ownership_type", "owner_type", "ownershiptype"),
    "occupancy_type": ("occupancy_type", "occupancy", "occupancytype"),
}

CN_HEADERS = {
    "电话": "phone",
    "手机": "phone",
    "手机号": "phone",
    "手机号码": "phone",
    "号码": "phone",
    "姓名": "name",
    "名字": "name",
    "名": "first_name",
    "姓": "last_name",
    "年龄": "age",
    "性别": "gender",
    "预估价值": "estimated_value",
    "估值": "estimated_value",
    "预估权益": "estimated_equity",
    "权益": "estimated_equity",
    "最近成交额": "last_sale_amount",
    "上次成交金额": "last_sale_amount",
    "最近成交日期": "last_sale_date",
    "上次成交日期": "last_sale_date",
    "邮箱": "email",
    "邮箱地址": "email",
    "产权类型": "ownership_type",
    "所有权类型": "ownership_type",
    "入住类型": "occupancy_type",
    "占用类型": "occupancy_type",
}

_NUMBER_RE = re.compile(r"\d+(?:\.\d+)?")
_CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def configure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def say(message: str) -> None:
    print(message, flush=True)


def parse_decimal(text: str) -> Decimal | None:
    negative = False
    body = text.strip()
    if body.startswith("(") and body.endswith(")"):
        negative = True
        body = body[1:-1]
    body = body.replace("$", "").replace(",", "").replace(" ", "")
    body = re.sub(r"(?i)usd", "", body)
    if body.startswith("+"):
        body = body[1:]
    if body.startswith("-"):
        negative = True
        body = body[1:]
    try:
        number = Decimal(body)
    except InvalidOperation:
        match = _NUMBER_RE.search(body)
        if match is None:
            return None
        number = Decimal(match.group())
    if negative:
        number = -number
    if not number.is_finite():
        return None
    return number


def parse_money(raw: object) -> tuple[int, bool]:
    original = "" if raw is None else str(raw)
    text = original.strip()
    if text == "" or text.lower() in GENERIC_NULLS:
        return 0, original != "0"
    number = parse_decimal(text)
    if number is None or number < 0:
        return 0, True
    if number > UINT64_MAX:
        return UINT64_MAX, True
    value = int(number.to_integral_value(rounding=ROUND_HALF_UP))
    if value > UINT64_MAX:
        value = UINT64_MAX
    return value, original != str(value)


def parse_age(raw: object) -> tuple[int, bool, str | None]:
    """返回 (年龄, 是否清洗过, 事件)。事件为 year / immortal / None。"""
    original = "" if raw is None else str(raw)
    text = original.strip()
    if text == "" or text.lower() in GENERIC_NULLS:
        return 0, original != "0", None
    number = parse_decimal(text)
    if number is None:
        return 0, True, None
    if Decimal(1900) <= number <= Decimal(CURRENT_YEAR):
        age = int((Decimal(CURRENT_YEAR) - number).to_integral_value(rounding=ROUND_HALF_UP))
        if age < 0 or age > MAX_AGE:
            return 0, True, "immortal"
        return age, True, "year"
    if number < 0:
        return 0, True, None
    if number > MAX_AGE:
        return 0, True, "immortal"
    age = int(number.to_integral_value(rounding=ROUND_HALF_UP))
    if age > MAX_AGE:
        return 0, True, "immortal"
    return age, original != str(age), None


def parse_string(raw: object, nulls: set[str]) -> tuple[str, bool]:
    if raw is None:
        return "", True
    original = str(raw)
    text = original.strip()
    if text.lower() in nulls:
        return "", True
    if text == "":
        return "", original != ""
    return text, text != original


def apply_age(raw: object) -> tuple[int, bool]:
    age, cleaned, event = parse_age(raw)
    if event == "year":
        say("🕰️ 逮到个填出生年份的，已换算！")
    elif event == "immortal":
        say("👻 逮到一个修仙的（>120岁），直接打回 0！")
    return age, cleaned


def normalize_token(value: str) -> str:
    token = value.strip().lower().replace("-", "_").replace(" ", "_")
    token = re.sub(r"[^0-9a-z_]+", "", token)
    token = re.sub(r"_+", "_", token).strip("_")
    return token


def resolve_indexes(header: list[str]) -> dict[str, int]:
    alias_owner: dict[str, str] = {}
    for field, names in ALIASES.items():
        for name in names:
            alias_owner[name] = field

    found: dict[str, int] = {}
    for idx, raw in enumerate(header):
        label = raw.strip()
        if not label:
            continue
        field = CN_HEADERS.get(label) or alias_owner.get(normalize_token(label))
        if field and field not in found:
            found[field] = idx

    required = [
        "phone",
        "age",
        "gender",
        "estimated_value",
        "estimated_equity",
        "last_sale_amount",
        "last_sale_date",
        "email",
        "ownership_type",
        "occupancy_type",
    ]
    missing = [name for name in required if name not in found]
    if "name" not in found and "first_name" not in found and "last_name" not in found:
        missing.append("name")
    if missing:
        seen = ", ".join(cell.strip() or "<空>" for cell in header)
        raise SystemExit(
            "CSV 表头对不上，缺这些列: "
            + ", ".join(missing)
            + f"\n文件里的表头是: {seen}"
        )
    return found


def cell(raw: list[str], index: int | None) -> str:
    if index is None or index >= len(raw):
        return ""
    return raw[index]


def clean_row(raw: list[str], indexes: dict[str, int]) -> tuple[tuple, int]:
    cleaned = False

    phone, changed = parse_string(cell(raw, indexes.get("phone")), PHONE_NULLS)
    cleaned |= changed

    if "name" in indexes:
        name, changed = parse_string(cell(raw, indexes.get("name")), NAME_NULLS)
        cleaned |= changed
    else:
        first, changed_first = parse_string(cell(raw, indexes.get("first_name")), NAME_NULLS)
        last, changed_last = parse_string(cell(raw, indexes.get("last_name")), NAME_NULLS)
        name = " ".join(part for part in (first, last) if part)
        cleaned |= changed_first or changed_last

    age, changed = apply_age(cell(raw, indexes.get("age")))
    cleaned |= changed

    gender, changed = parse_string(cell(raw, indexes.get("gender")), GENERIC_NULLS)
    cleaned |= changed

    estimated_value, changed = parse_money(cell(raw, indexes.get("estimated_value")))
    cleaned |= changed
    estimated_equity, changed = parse_money(cell(raw, indexes.get("estimated_equity")))
    cleaned |= changed
    last_sale_amount, changed = parse_money(cell(raw, indexes.get("last_sale_amount")))
    cleaned |= changed

    last_sale_date, changed = parse_string(cell(raw, indexes.get("last_sale_date")), GENERIC_NULLS)
    cleaned |= changed
    email, changed = parse_string(cell(raw, indexes.get("email")), GENERIC_NULLS)
    cleaned |= changed
    ownership_type, changed = parse_string(cell(raw, indexes.get("ownership_type")), GENERIC_NULLS)
    cleaned |= changed
    occupancy_type, changed = parse_string(cell(raw, indexes.get("occupancy_type")), GENERIC_NULLS)
    cleaned |= changed

    row = (
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
    return row, int(cleaned)


SNAPSHOT_LIMIT = 3_000_000
LOOKUP_CHUNK = 1000


def coerce_record(raw: tuple) -> tuple:
    def text(value: object) -> str:
        return "" if value is None else str(value)

    def number(value: object) -> int:
        if value is None or value == "":
            return 0
        return int(value)

    return (
        text(raw[0]),
        text(raw[1]),
        number(raw[2]),
        text(raw[3]),
        number(raw[4]),
        number(raw[5]),
        number(raw[6]),
        text(raw[7]),
        text(raw[8]),
        text(raw[9]),
        text(raw[10]),
    )


def sql_quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def load_phone_snapshot(runner) -> dict[str, tuple] | None:
    """表不大时一次性装进内存。太大就返回 None，改成按批查询。"""
    count = int(runner.command(f"SELECT count() FROM {DATABASE}.{TABLE}"))
    if count > SNAPSHOT_LIMIT:
        return None
    rows = runner.query_rows(
        f"SELECT {', '.join(COLUMNS)} FROM {DATABASE}.{TABLE} FINAL"
    )
    index: dict[str, tuple] = {}
    for raw in rows:
        row = coerce_record(raw)
        index[row[0]] = row
    return index


def select_changed_records(rows: list[tuple], query) -> tuple[list[tuple], int]:
    """同一批里完全相同的号码记为跳过，和库里不一致的留下。"""
    skipped = 0
    latest: dict[str, tuple] = {}
    order: list[str] = []
    for raw in rows:
        row = coerce_record(raw)
        previous = latest.get(row[0])
        if previous == row:
            skipped += 1
            continue
        if previous is None:
            order.append(row[0])
        latest[row[0]] = row
    existing: dict[str, tuple] = {}
    phones = list(order)
    for start in range(0, len(phones), LOOKUP_CHUNK):
        chunk = phones[start:start + LOOKUP_CHUNK]
        listed = ", ".join(sql_quote(phone) for phone in chunk)
        found = query(
            f"SELECT {', '.join(COLUMNS)} FROM {DATABASE}.{TABLE} FINAL "
            f"WHERE phone IN ({listed})"
        )
        for raw in found:
            item = coerce_record(raw)
            existing[item[0]] = item
    keep: list[tuple] = []
    for phone in order:
        row = latest[phone]
        if existing.get(phone) == row:
            skipped += 1
            continue
        keep.append(row)
    return keep, skipped


def dedup_report(read: int, written: int, skipped: int) -> None:
    say(
        f"📦 本次读取 {read:,} 条，成功插入/更新 {written:,} 条，"
        f"跳过完全重复 {skipped:,} 条。"
    )


def escape_tsv(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\0", "\\0")
        .replace("\b", "\\b")
        .replace("\f", "\\f")
    )


def render_tsv(rows: list[tuple]) -> bytes:
    lines = []
    for row in rows:
        lines.append(
            "\t".join(
                (
                    escape_tsv(row[0]),
                    escape_tsv(row[1]),
                    str(row[2]),
                    escape_tsv(row[3]),
                    str(row[4]),
                    str(row[5]),
                    str(row[6]),
                    escape_tsv(row[7]),
                    escape_tsv(row[8]),
                    escape_tsv(row[9]),
                    escape_tsv(row[10]),
                )
            )
        )
    return ("\n".join(lines) + "\n").encode("utf-8")


def is_auth_error(exc: BaseException) -> bool:
    text = str(exc)
    return "Authentication failed" in text or "REQUIRED_PASSWORD" in text or "code: 194" in text


def is_retriable(exc: BaseException) -> bool:
    text = str(exc).lower()
    needles = (
        "timeout",
        "connection",
        "reset",
        "broken pipe",
        "eof",
        "temporarily",
        "too many parts",
        "socket",
    )
    return any(needle in text for needle in needles)


class HttpInserter:
    def __init__(self, password: str) -> None:
        if clickhouse_connect is None:
            raise SystemExit("缺少依赖，请先执行: pip install clickhouse-connect")
        self._password = password
        self._local = threading.local()

    def _client(self):
        client = getattr(self._local, "client", None)
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
            self._local.client = client
        return client

    def reset(self) -> None:
        client = getattr(self._local, "client", None)
        self._local.client = None
        if client is not None:
            try:
                client.close()
            except Exception:
                pass

    def command(self, sql: str) -> str:
        result = self._client().command(sql)
        return "" if result is None else str(result).strip()

    def query_rows(self, sql: str) -> list[tuple]:
        return list(self._client().query(sql).result_rows)

    def insert(self, rows: list[tuple]) -> None:
        self._client().insert(
            TABLE,
            rows,
            column_names=list(COLUMNS),
            database=DATABASE,
            settings={"async_insert": 0},
        )

    def insert_job(self, rows: list[tuple], cleaned: int) -> tuple[int, int]:
        self._insert_with_retry(rows)
        return len(rows), cleaned

    def _insert_with_retry(self, rows: list[tuple]) -> None:
        delay = 1.0
        for attempt in range(3):
            try:
                self.insert(rows)
                return
            except Exception as exc:
                if attempt == 2 or not is_retriable(exc):
                    raise
                self.reset()
                time.sleep(delay)
                delay *= 2


class DockerInserter:
    """容器外访问 8123 要密码时，走容器里的 clickhouse-client。"""

    def __init__(self, container: str) -> None:
        self.container = container

    def reset(self) -> None:
        return None

    def _run(self, args: list[str], data: bytes | None = None) -> str:
        proc = subprocess.run(
            ["docker", "exec", "-i", self.container, "clickhouse-client", *args],
            input=data,
            capture_output=True,
            check=False,
            creationflags=_CREATE_NO_WINDOW,
        )
        if proc.returncode != 0:
            err = proc.stderr.decode("utf-8", errors="replace").strip()
            out = proc.stdout.decode("utf-8", errors="replace").strip()
            raise RuntimeError(err or out or f"clickhouse-client 退出码 {proc.returncode}")
        return proc.stdout.decode("utf-8", errors="replace")

    def command(self, sql: str) -> str:
        return self._run(["--query", sql]).strip()

    def query_rows(self, sql: str) -> list[tuple]:
        text = self.command(sql.rstrip() + "\nFORMAT TabSeparated")
        if not text:
            return []
        rows = []
        for line in text.splitlines():
            rows.append(tuple(line.split("\t")))
        return rows

    def insert(self, rows: list[tuple]) -> None:
        self._run(["--query", INSERT_SQL], render_tsv(rows))

    def insert_job(self, rows: list[tuple], cleaned: int) -> tuple[int, int]:
        delay = 1.0
        for attempt in range(3):
            try:
                self.insert(rows)
                return len(rows), cleaned
            except Exception as exc:
                if attempt == 2 or not is_retriable(exc):
                    raise
                time.sleep(delay)
                delay *= 2
        raise RuntimeError("插入失败")


def connect(password: str = ""):
    """默认 password=''。8123 若拒绝空密码，再改走容器内客户端。"""
    if clickhouse_connect is not None and password == "":
        try:
            inserter = HttpInserter(password)
            inserter.command("SELECT 1")
            return inserter
        except Exception as exc:
            if not is_auth_error(exc):
                raise
            print(
                f"8123 拒绝了空密码，改为容器 {CONTAINER} 里的 clickhouse-client 入库。",
                file=sys.stderr,
                flush=True,
            )
            return DockerInserter(CONTAINER)
    if clickhouse_connect is None and password == "":
        print(
            f"未安装 clickhouse-connect，改为容器 {CONTAINER} 里的 clickhouse-client 入库。",
            file=sys.stderr,
            flush=True,
        )
        return DockerInserter(CONTAINER)
    inserter = HttpInserter(password)
    inserter.command("SELECT 1")
    return inserter


def ensure_schema(inserter) -> None:
    inserter.command(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
    rows = inserter.query_rows(
        "SELECT engine, sorting_key FROM system.tables "
        f"WHERE database = '{DATABASE}' AND name = '{TABLE}'"
    )
    if not rows:
        inserter.command(CREATE_TABLE_SQL)
        return
    engine, sorting_key = rows[0][0], rows[0][1]
    if engine != "ReplacingMergeTree" or sorting_key not in {"phone", "(phone)"}:
        raise SystemExit(
            f"表 {DATABASE}.{TABLE} 已经存在，引擎是 {engine}，排序键是 {sorting_key}。"
            "这里要求 ReplacingMergeTree 且 ORDER BY (phone)。"
            "请先删掉或换掉这张旧表再导入。"
        )


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


def choose_dialect(sample: str) -> csv.Dialect:
    try:
        return csv.Sniffer().sniff(sample, delimiters=",\t;|")
    except csv.Error:
        return csv.get_dialect("excel")


def iter_clean_rows(path: Path):
    encoding = detect_encoding(path)
    with path.open("r", encoding=encoding, newline="") as handle:
        sample = handle.read(65536)
        handle.seek(0)
        reader = csv.reader(handle, choose_dialect(sample))
        try:
            header = next(reader)
        except StopIteration:
            raise SystemExit(f"文件是空的: {path}") from None
        if len(header) == 1 and header[0].strip().lower().startswith("sep="):
            try:
                header = next(reader)
            except StopIteration:
                raise SystemExit(f"文件只有分隔符声明: {path}") from None
        indexes = resolve_indexes(header)
        for raw in reader:
            if not raw or not any(item.strip() for item in raw):
                continue
            yield clean_row(raw, indexes)


def import_file(path: Path, inserter) -> tuple[int, int]:
    inserted = 0
    cleaned = 0
    pending: set = set()

    def account(done) -> None:
        nonlocal inserted, cleaned
        for future in done:
            batch_inserted, batch_cleaned = future.result()
            inserted += batch_inserted
            cleaned += batch_cleaned
            say(f"📦 咚！又塞进 {batch_inserted:,} 条...")

    def drain_ready() -> None:
        nonlocal pending
        done = {future for future in pending if future.done()}
        if not done:
            return
        pending -= done
        account(done)

    def wait_for_slot() -> None:
        nonlocal pending
        while len(pending) >= MAX_WORKERS:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            account(done)

    say("🚀 老板，管道已接好，准备抽水...")
    seen = 0
    read = 0
    skipped = 0
    index = load_phone_snapshot(inserter)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="insert") as executor:
        try:
            batch: list[tuple] = []
            batch_cleaned = 0
            for row, row_cleaned in iter_clean_rows(path):
                read += 1
                row = coerce_record(row)
                if index is not None and index.get(row[0]) == row:
                    skipped += 1
                    continue
                if index is not None:
                    index[row[0]] = row
                batch.append(row)
                batch_cleaned += row_cleaned
                seen += 1
                if seen % 1000 == 0:
                    drain_ready()
                if len(batch) < BATCH_SIZE:
                    continue
                wait_for_slot()
                pending.add(executor.submit(inserter.insert_job, batch, batch_cleaned))
                batch = []
                batch_cleaned = 0
            if batch:
                wait_for_slot()
                pending.add(executor.submit(inserter.insert_job, batch, batch_cleaned))
            if pending:
                done, _pending = wait(pending)
                pending.clear()
                account(done)
        except KeyboardInterrupt as exc:
            raise KeyboardInterrupt(
                f"手动停了。已确认入库 {inserted:,} 条，还在飞的批次可能也会落库。"
            ) from exc
        except Exception as exc:
            raise RuntimeError(
                f"导入中断，已确认入库 {inserted:,} 条。还在飞的批次可能也会落库。原因：{exc}"
            ) from exc

    success = inserted - cleaned
    say(
        f"🎉 收工！总共入库 {inserted:,} 条，数据体检报告：成功 {success:,} 条，清洗 {cleaned:,} 条。"
    )
    if index is None:
        say("表太大，内存里放不下全表，这次改为全量写入，交给 ClickHouse 合并。")
        skipped = 0
    dedup_report(read, inserted, skipped)
    return inserted, cleaned


def default_csv_path() -> Path:
    return CSV_PATH


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="把 Alaska CSV 清洗后导入 ClickHouse")
    parser.add_argument("csv", nargs="?", default=str(CSV_PATH), help="CSV 路径，默认项目根目录的 Alaska_1.csv")
    parser.add_argument(
        "--password",
        default="",
        help="ClickHouse 密码，默认空字符串",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_stdio()
    try:
        csv.field_size_limit(min(sys.maxsize, 2**31 - 1))
    except OverflowError:
        csv.field_size_limit(2**31 - 1)

    args = parse_args(sys.argv[1:] if argv is None else argv)
    path = Path(args.csv).expanduser().resolve()
    if not path.is_file():
        print(
            f"找不到文件：{path}\n请把 Alaska_1.csv 放到脚本同目录，或执行: python import_data.py 你的文件.csv",
            file=sys.stderr,
        )
        return 1

    try:
        inserter = connect(args.password if args.password is not None else PASSWORD)
        ensure_schema(inserter)
        import_file(path, inserter)
    except KeyboardInterrupt as exc:
        print(f"\n{exc}", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"失败：{exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
