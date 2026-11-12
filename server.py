#!/usr/bin/env python3
"""号码查询服务。页面在 / ，接口是 /lookup 和 /export。"""

import asyncio
import csv
import io
import re
import subprocess
import threading
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

import clickhouse_connect
import openpyxl
import pandas as pd
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.concurrency import iterate_in_threadpool

from import_data import (
    BATCH_SIZE,
    CONTAINER,
    DATABASE,
    GENERIC_NULLS,
    HOST,
    INSERT_SQL,
    NAME_NULLS,
    PHONE_NULLS,
    PORT,
    TABLE,
    USER,
    _CREATE_NO_WINDOW,
    cell,
    coerce_record,
    connect,
    dedup_report,
    escape_tsv,
    is_auth_error,
    load_phone_snapshot,
    parse_age,
    parse_money,
    parse_string,
    render_tsv,
    resolve_indexes,
    select_changed_records,
)

WEB_DIR = Path(__file__).resolve().parent / "web"
ROW_LIMIT = 100
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
INT_COLUMNS = {"age", "estimated_value", "estimated_equity", "last_sale_amount"}

app = FastAPI(title="Phone Filter Lookup")
app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")
db = connect("")
http_client = None
try:
    http_client = clickhouse_connect.get_client(
        host=HOST,
        port=PORT,
        username=USER,
        password="",
        connect_timeout=10,
        send_receive_timeout=3600,
    )
    http_client.command("SELECT 1")
except Exception as exc:
    http_client = None
    if not is_auth_error(exc):
        raise

CSV_BATCH = 1000
_TSV_ESCAPES = {
    "n": "\n",
    "r": "\r",
    "t": "\t",
    "0": "\0",
    "b": "\b",
    "f": "\f",
    "'": "'",
    "\\": "\\",
}


def quote_string(value: str) -> str:
    escaped = (
        value.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
        .replace("\0", "")
    )
    return f"'{escaped}'"


def build_where(phone: str, gender: str, age_min: int | None, age_max: int | None) -> str:
    clauses = ["1"]
    if phone:
        clauses.append(f"startsWith(phone, {quote_string(phone)})")
    if gender:
        clauses.append(f"lowerUTF8(gender) = lowerUTF8({quote_string(gender)})")
    if age_min is not None:
        clauses.append(f"age >= {age_min}")
    if age_max is not None:
        clauses.append(f"age <= {age_max}")
    return " AND ".join(clauses)


def read_filters(
    phone: str,
    gender: str,
    age_min: int | None,
    age_max: int | None,
) -> tuple[str, str, int | None, int | None]:
    phone = phone.strip()
    gender = gender.strip()
    if len(phone) > 32:
        raise HTTPException(status_code=400, detail="号码最长 32 位")
    if len(gender) > 32:
        raise HTTPException(status_code=400, detail="性别最长 32 位")
    if age_min is not None and age_max is not None and age_min > age_max:
        raise HTTPException(status_code=400, detail="最小年龄不能大于最大年龄")
    return phone, gender, age_min, age_max


def select_sql(where: str) -> str:
    return (
        f"SELECT {', '.join(COLUMNS)} FROM {DATABASE}.{TABLE} FINAL "
        f"WHERE {where} ORDER BY phone"
    )


def table_columns(table: str) -> set[str]:
    rows = db.query_rows(
        "SELECT name FROM system.columns "
        f"WHERE database = {quote_string(DATABASE)} "
        f"AND `table` = {quote_string(table)}"
    )
    return {str(row[0]) for row in rows}


def touch_join_sql() -> str | None:
    """没有触达表时不联查。有版本时间就取最新一条 category。"""
    try:
        if not table_exists("touch_results"):
            return None
        names = table_columns("touch_results")
    except Exception:
        return None
    if "phone" not in names or "category" not in names:
        return None
    picked = "argMax(category, updated_at)" if "updated_at" in names else "any(category)"
    return (
        f"SELECT phone, {picked} AS category "
        f"FROM {DATABASE}.touch_results GROUP BY phone"
    )


def lookup_sql(where: str) -> str:
    fields = ", ".join(COLUMNS)
    unmatched = quote_string("未触达")
    limited = (
        f"SELECT {fields} FROM {DATABASE}.{TABLE} FINAL "
        f"WHERE {where} ORDER BY phone LIMIT {ROW_LIMIT}"
    )
    joined = touch_join_sql()
    if joined is None:
        return f"SELECT {fields}, {unmatched} AS status FROM ({limited})"
    picked = ", ".join(f"p.{column}" for column in COLUMNS)
    return (
        f"SELECT {picked}, "
        f"if(ifNull(t.category, '') = '', {unmatched}, t.category) AS status "
        f"FROM ({limited}) AS p "
        f"LEFT JOIN ({joined}) AS t ON p.phone = t.phone "
        f"ORDER BY p.phone"
    )


def unescape_tsv(value: str) -> str:
    if "\\" not in value:
        return value
    pieces: list[str] = []
    index = 0
    while index < len(value):
        if value[index] == "\\" and index + 1 < len(value):
            pieces.append(_TSV_ESCAPES.get(value[index + 1], value[index + 1]))
            index += 2
            continue
        pieces.append(value[index])
        index += 1
    return "".join(pieces)


def docker_row_cursor(sql: str) -> Iterator[tuple]:
    """容器内 clickhouse-client 的 stdout 游标，读一行吐一行。"""
    proc = subprocess.Popen(
        [
            "docker",
            "exec",
            "-i",
            CONTAINER,
            "clickhouse-client",
            "--query",
            sql + "\nFORMAT TabSeparated",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=_CREATE_NO_WINDOW,
    )
    assert proc.stdout is not None
    assert proc.stderr is not None
    errors: list[bytes] = []

    def drain_stderr() -> None:
        errors.append(proc.stderr.read())

    thread = threading.Thread(target=drain_stderr, daemon=True)
    thread.start()
    try:
        for raw_line in proc.stdout:
            line = raw_line.decode("utf-8").rstrip("\r\n")
            if not line:
                continue
            yield tuple(unescape_tsv(field) for field in line.split("\t"))
        proc.wait()
        thread.join(timeout=5)
        if proc.returncode != 0:
            message = errors[0].decode("utf-8", "replace").strip() if errors else ""
            raise RuntimeError(message or f"clickhouse-client 退出码 {proc.returncode}")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        if thread.is_alive():
            thread.join(timeout=2)


def stream_rows(sql: str) -> Iterator[tuple]:
    """ClickHouse 流式游标。禁止把整次查询结果装进列表。"""
    if http_client is not None:
        with http_client.query_rows_stream(sql) as cursor:
            yield from cursor
        return
    yield from docker_row_cursor(sql)


def encode_csv(rows: list[tuple]) -> bytes:
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="\n").writerows(rows)
    return buffer.getvalue().encode("utf-8")


def csv_stream(sql: str) -> Iterator[bytes]:
    cursor = stream_rows(sql)
    try:
        batch: list[tuple] = []
        wrote_header = False
        for row in cursor:
            if not wrote_header:
                yield encode_csv([COLUMNS])
                wrote_header = True
            batch.append(tuple("" if value is None else value for value in row))
            if len(batch) >= CSV_BATCH:
                yield encode_csv(batch)
                batch.clear()
        if not wrote_header:
            yield encode_csv([COLUMNS])
        elif batch:
            yield encode_csv(batch)
    finally:
        close = getattr(cursor, "close", None)
        if close is not None:
            close()


def ident(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(name)
    return f"`{name}`"


def table_exists(name: str) -> bool:
    count = int(db.command(
        "SELECT count() FROM system.tables "
        f"WHERE database = {quote_string(DATABASE)} AND name = {quote_string(name)}"
    ))
    return count > 0


def status_column(table: str) -> str | None:
    rows = db.query_rows(
        "SELECT name FROM system.columns "
        f"WHERE database = {quote_string(DATABASE)} AND `table` = {quote_string(table)}"
    )
    names = {str(row[0]) for row in rows}
    for candidate in ("status", "state", "pool_status", "touch_status", "result_status", "stage"):
        if candidate in names:
            return candidate
    return None


def count_status(primary: str, fallback: str, label: str) -> int:
    """表不存在就当 0。存在则按 status 字段统计对应状态。"""
    try:
        for table in (primary, fallback):
            if not table_exists(table):
                continue
            column = status_column(table)
            if column is None:
                continue
            return int(db.command(
                f"SELECT count() FROM {DATABASE}.{ident(table)} "
                f"WHERE {ident(column)} = {quote_string(label)}"
            ))
    except Exception:
        return 0
    return 0


def as_record(raw: tuple) -> dict:
    record = {}
    for column, value in zip(COLUMNS, raw):
        if column in INT_COLUMNS:
            record[column] = int(value or 0)
        else:
            record[column] = "" if value is None else str(value)
    raw_status = raw[len(COLUMNS)] if len(raw) > len(COLUMNS) else "未触达"
    status = "" if raw_status is None else str(raw_status).strip()
    record["status"] = status or "未触达"
    return record


@app.get("/stats")
def stats() -> dict:
    total_records = int(db.command(f"SELECT count() FROM {DATABASE}.{TABLE}"))
    return {
        "total_records": total_records,
        "pending_assign": count_status("number_pool", "touch_results", "待分配"),
        "pending_touch": count_status("number_pool", "touch_results", "待触达"),
        "touch_valid": count_status("touch_results", "number_pool", "触达有效"),
        "touch_opened": count_status("touch_results", "number_pool", "触达已开客"),
    }


@app.get("/")
def index() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")


@app.get("/lookup")
def lookup(
    phone: str = "",
    gender: str = "",
    age_min: int | None = Query(default=None, ge=0, le=65535),
    age_max: int | None = Query(default=None, ge=0, le=65535),
) -> dict:
    phone, gender, age_min, age_max = read_filters(phone, gender, age_min, age_max)
    where = build_where(phone, gender, age_min, age_max)
    source = f"{DATABASE}.{TABLE} FINAL"
    total = int(db.command(f"SELECT count() FROM {source} WHERE {where}"))
    rows = db.query_rows(lookup_sql(where))
    return {
        "total": total,
        "limit": ROW_LIMIT,
        "rows": [as_record(row) for row in rows],
    }


@app.get("/export")
def export(
    phone: str = "",
    gender: str = "",
    age_min: int | None = Query(default=None, ge=0, le=65535),
    age_max: int | None = Query(default=None, ge=0, le=65535),
) -> StreamingResponse:
    phone, gender, age_min, age_max = read_filters(phone, gender, age_min, age_max)
    sql = select_sql(build_where(phone, gender, age_min, age_max))
    return StreamingResponse(
        iterate_in_threadpool(csv_stream(sql)),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="phone_records.csv"'},
    )


UPLOAD_BATCH = 100_000
PHONE_HEADERS = {
    "phone",
    "phones",
    "phone_number",
    "phonenumber",
    "mobile",
    "tel",
    "telephone",
    "number",
    "号码",
    "手机号",
    "手机号码",
}
MARK_DDL = {
    "number_pool": f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.number_pool
        (
            phone String,
            status LowCardinality(String),
            updated_at DateTime64(3)
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY phone
    """,
    "touch_results": f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.touch_results
        (
            phone String,
            category LowCardinality(String),
            updated_at DateTime64(3)
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY phone
    """,
}
MARK_COLUMNS = {
    "number_pool": {"phone", "status", "updated_at"},
    "touch_results": {"phone", "category", "updated_at"},
}


def table_engine(name: str) -> str:
    return db.command(
        "SELECT engine FROM system.tables "
        f"WHERE database = {quote_string(DATABASE)} AND name = {quote_string(name)}"
    ).strip()


def ensure_mark_table(table: str) -> None:
    """没有表就建可覆盖的 ReplacingMergeTree。旧的测试表行数很少时换掉。"""
    ddl = MARK_DDL[table]
    if not table_exists(table):
        db.command(ddl)
        return
    ready = (
        table_engine(table) == "ReplacingMergeTree"
        and MARK_COLUMNS[table] <= table_columns(table)
    )
    if ready:
        return
    count = int(db.command(f"SELECT count() FROM {DATABASE}.{ident(table)}"))
    if count > 20:
        raise RuntimeError(f"{table} 里已有数据，结构却不能按号码覆盖")
    db.command(f"DROP TABLE {DATABASE}.{ident(table)}")
    db.command(ddl)


PHONE_MIN_DIGITS = 7
PHONE_MAX_DIGITS = 15
PHONE_NAME_HINTS = ("phone", "mobile", "tel", "cell", "号码", "手机")


def digits_only(value: object) -> str | None:
    """去掉符号和小数尾巴，只留 7 到 15 位纯数字号码。"""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        return None
    text = str(value).strip().lstrip("\ufeff")
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".", 1)[0]
    digits = re.sub(r"\D", "", text)
    if PHONE_MIN_DIGITS <= len(digits) <= PHONE_MAX_DIGITS:
        return digits
    return None


def remember_phone(seen: dict[str, None], value: object) -> None:
    phone = digits_only(value)
    if phone:
        seen[phone] = None


def collect_phones_text(stream: io.BufferedIOBase) -> list[str]:
    stream.seek(0)
    wrapper = io.TextIOWrapper(stream, encoding="utf-8-sig", errors="replace", newline="")
    seen: dict[str, None] = {}
    try:
        for line in wrapper:
            for part in line.split(","):
                remember_phone(seen, part)
    finally:
        wrapper.detach()
    return list(seen)


def header_phone_index(header: list[str]) -> int | None:
    for index, label in enumerate(header):
        folded = label.casefold()
        if folded in PHONE_HEADERS or any(hint in folded for hint in PHONE_NAME_HINTS):
            return index
    return None


def looks_like_header(header: list[str]) -> bool:
    if header_phone_index(header) is not None:
        return True
    return bool(header) and all(digits_only(label) is None for label in header)


def busiest_phone_column(frame: pd.DataFrame) -> int:
    best_index = 0
    best_score = -1
    sample = frame.head(50)
    for index in range(sample.shape[1]):
        score = sum(1 for value in sample.iloc[:, index].tolist() if digits_only(value))
        if score > best_score:
            best_index = index
            best_score = score
    return best_index


def collect_phones_xlsx(stream: io.BufferedIOBase) -> list[str]:
    stream.seek(0)
    frame = pd.read_excel(
        io.BytesIO(stream.read()),
        engine=openpyxl.__name__,
        header=None,
        dtype=str,
    )
    frame = frame.dropna(axis=0, how="all").dropna(axis=1, how="all")
    if frame.empty:
        return []
    header = ["" if pd.isna(value) else str(value).strip() for value in frame.iloc[0].tolist()]
    body = frame.iloc[1:] if looks_like_header(header) else frame
    if body.empty:
        body = frame
    named = header_phone_index(header)
    index = named if named is not None else busiest_phone_column(body)
    seen: dict[str, None] = {}
    if named is None and digits_only(header[index] if index < len(header) else None):
        remember_phone(seen, header[index])
    for value in body.iloc[:, index].tolist():
        remember_phone(seen, value)
    return list(seen)


def render_mark_tsv(rows: list[tuple]) -> bytes:
    lines = []
    for phone, label, moment in rows:
        stamp = moment.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        lines.append(f"{escape_tsv(str(phone))}\t{escape_tsv(str(label))}\t{stamp}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def write_rows(table: str, columns: list[str], rows: list[tuple]) -> None:
    if http_client is not None:
        http_client.insert(table, rows, column_names=columns, database=DATABASE)
        return
    column_list = ", ".join(ident(column) for column in columns)
    sql = f"INSERT INTO {DATABASE}.{ident(table)} ({column_list}) FORMAT TabSeparated"
    proc = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "clickhouse-client", "--query", sql],
        input=render_mark_tsv(rows),
        capture_output=True,
        check=False,
        creationflags=_CREATE_NO_WINDOW,
    )
    if proc.returncode != 0:
        message = proc.stderr.decode("utf-8", "replace").strip()
        raise RuntimeError(message or f"clickhouse-client 退出码 {proc.returncode}")


def import_marked(
    stream: io.BufferedIOBase,
    suffix: str,
    table: str,
    column: str,
    label: str,
) -> int:
    if suffix == ".xlsx":
        phones = collect_phones_xlsx(stream)
    else:
        phones = collect_phones_text(stream)
    if not phones:
        raise HTTPException(status_code=400, detail="文件里没有可导入的号码")
    ensure_mark_table(table)
    moment = datetime.now(timezone.utc).replace(tzinfo=None)
    columns = ["phone", column, "updated_at"]
    for start in range(0, len(phones), UPLOAD_BATCH):
        chunk = [(phone, label, moment) for phone in phones[start:start + UPLOAD_BATCH]]
        write_rows(table, columns, chunk)
    return len(phones)


async def receive_marked(file: UploadFile, table: str, column: str, label: str) -> dict:
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in {".txt", ".csv", ".xlsx"}:
        raise HTTPException(status_code=400, detail="只接收 TXT、CSV 或 XLSX 文件")
    try:
        accepted = await asyncio.to_thread(
            import_marked, file.file, suffix, table, column, label
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail="导入失败") from exc
    return {"ok": True, "accepted": accepted}


def cleaned_record(raw: list[str], indexes: dict[str, int]) -> tuple:
    """和 import_data.clean_row 同一套 11 列清洗，上传时不往终端刷出生年份。"""
    phone, _changed = parse_string(cell(raw, indexes.get("phone")), PHONE_NULLS)
    if "name" in indexes:
        name, _changed = parse_string(cell(raw, indexes.get("name")), NAME_NULLS)
    else:
        first, _changed = parse_string(cell(raw, indexes.get("first_name")), NAME_NULLS)
        last, _changed = parse_string(cell(raw, indexes.get("last_name")), NAME_NULLS)
        name = " ".join(part for part in (first, last) if part)
    age, _changed, _event = parse_age(cell(raw, indexes.get("age")))
    gender, _changed = parse_string(cell(raw, indexes.get("gender")), GENERIC_NULLS)
    estimated_value, _changed = parse_money(cell(raw, indexes.get("estimated_value")))
    estimated_equity, _changed = parse_money(cell(raw, indexes.get("estimated_equity")))
    last_sale_amount, _changed = parse_money(cell(raw, indexes.get("last_sale_amount")))
    last_sale_date, _changed = parse_string(cell(raw, indexes.get("last_sale_date")), GENERIC_NULLS)
    email, _changed = parse_string(cell(raw, indexes.get("email")), GENERIC_NULLS)
    ownership_type, _changed = parse_string(cell(raw, indexes.get("ownership_type")), GENERIC_NULLS)
    occupancy_type, _changed = parse_string(cell(raw, indexes.get("occupancy_type")), GENERIC_NULLS)
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


def insert_phone_rows(rows: list[tuple]) -> None:
    if http_client is not None:
        http_client.insert(TABLE, rows, column_names=list(COLUMNS), database=DATABASE)
        return
    proc = subprocess.run(
        ["docker", "exec", "-i", CONTAINER, "clickhouse-client", "--query", INSERT_SQL],
        input=render_tsv(rows),
        capture_output=True,
        check=False,
        creationflags=_CREATE_NO_WINDOW,
    )
    if proc.returncode != 0:
        message = proc.stderr.decode("utf-8", "replace").strip()
        raise RuntimeError(message or f"clickhouse-client 退出码 {proc.returncode}")


def flush_base_batch(
    batch: list[tuple],
    index: dict[str, tuple] | None,
) -> tuple[int, int]:
    if not batch:
        return 0, 0
    if index is None:
        keep, skipped = select_changed_records(batch, db.query_rows)
    else:
        keep = list(batch)
        skipped = 0
    if keep:
        insert_phone_rows(keep)
    batch.clear()
    return len(keep), skipped


def import_base(stream: io.BufferedIOBase) -> int:
    stream.seek(0)
    wrapper = io.TextIOWrapper(stream, encoding="utf-8-sig", errors="replace", newline="")
    reader = csv.reader(wrapper)
    read = 0
    written = 0
    skipped = 0
    index = load_phone_snapshot(db)
    try:
        try:
            header = next(reader)
        except StopIteration as exc:
            raise HTTPException(status_code=400, detail="文件是空的") from exc
        try:
            indexes = resolve_indexes(header)
        except SystemExit as exc:
            detail = exc.code if isinstance(exc.code, str) else "CSV 表头对不上"
            raise HTTPException(status_code=400, detail=detail) from exc
        batch: list[tuple] = []
        for raw in reader:
            if not any(part.strip() for part in raw):
                continue
            row = coerce_record(cleaned_record(raw, indexes))
            read += 1
            if index is not None and index.get(row[0]) == row:
                skipped += 1
                continue
            if index is not None:
                index[row[0]] = row
            batch.append(row)
            if len(batch) < BATCH_SIZE:
                continue
            batch_written, batch_skipped = flush_base_batch(batch, index)
            written += batch_written
            skipped += batch_skipped
        batch_written, batch_skipped = flush_base_batch(batch, index)
        written += batch_written
        skipped += batch_skipped
    finally:
        wrapper.detach()
    if read == 0:
        raise HTTPException(status_code=400, detail="文件里没有可导入的号码")
    dedup_report(read, written, skipped)
    return written


@app.post("/upload_base")
async def upload_base(file: UploadFile = File(...)) -> dict:
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in {".csv", ".txt"}:
        raise HTTPException(status_code=400, detail="底库只接收 CSV 或 TXT，按行导入")
    try:
        accepted = await asyncio.to_thread(import_base, file.file)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail="底库导入失败") from exc
    return {"ok": True, "accepted": accepted}


def load_pool_snapshot() -> dict[str, str] | None:
    if not table_exists("number_pool"):
        return {}
    count = int(db.command(f"SELECT count() FROM {DATABASE}.number_pool"))
    if count > 3_000_000:
        return None
    rows = db.query_rows(
        "SELECT phone, argMax(status, updated_at) "
        f"FROM {DATABASE}.number_pool GROUP BY phone"
    )
    return {str(phone): str(status) for phone, status in rows}


def pool_statuses(phones: list[str]) -> dict[str, str]:
    found: dict[str, str] = {}
    for start in range(0, len(phones), 1000):
        chunk = phones[start:start + 1000]
        listed = ", ".join(quote_string(phone) for phone in chunk)
        rows = db.query_rows(
            "SELECT phone, argMax(status, updated_at) "
            f"FROM {DATABASE}.number_pool "
            f"WHERE phone IN ({listed}) GROUP BY phone"
        )
        for phone, status in rows:
            found[str(phone)] = str(status)
    return found


def iter_uploaded_phones(stream: io.BufferedIOBase, suffix: str) -> Iterator[str]:
    if suffix == ".xlsx":
        yield from collect_phones_xlsx(stream)
        return
    stream.seek(0)
    wrapper = io.TextIOWrapper(stream, encoding="utf-8-sig", errors="replace", newline="")
    try:
        for line in wrapper:
            for part in line.split(","):
                phone = digits_only(part)
                if phone:
                    yield phone
    finally:
        wrapper.detach()


def import_pool(stream: io.BufferedIOBase, suffix: str) -> int:
    label = "已触达"
    ensure_mark_table("number_pool")
    index = load_pool_snapshot()
    read = 0
    written = 0
    skipped = 0
    batch: list[str] = []

    def flush() -> None:
        nonlocal written, skipped
        if not batch:
            return
        moment = datetime.now(timezone.utc).replace(tzinfo=None)
        phones = batch
        if index is None:
            current = pool_statuses(batch)
            phones = []
            seen: set[str] = set()
            for phone in batch:
                if phone in seen or current.get(phone) == label:
                    skipped += 1
                    continue
                seen.add(phone)
                phones.append(phone)
        rows = [(phone, label, moment) for phone in phones]
        if rows:
            write_rows("number_pool", ["phone", "status", "updated_at"], rows)
            written += len(rows)
        batch.clear()

    for phone in iter_uploaded_phones(stream, suffix):
        read += 1
        if index is not None and index.get(phone) == label:
            skipped += 1
            continue
        if index is not None:
            index[phone] = label
        batch.append(phone)
        if len(batch) >= BATCH_SIZE:
            flush()
    flush()
    if read == 0:
        raise HTTPException(status_code=400, detail="文件里没有可导入的号码")
    dedup_report(read, written, skipped)
    return written


@app.post("/upload_pool")
async def upload_pool(file: UploadFile = File(...)) -> dict:
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in {".txt", ".csv", ".xlsx"}:
        raise HTTPException(status_code=400, detail="只接收 TXT、CSV 或 XLSX 文件")
    try:
        accepted = await asyncio.to_thread(import_pool, file.file, suffix)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail="导入失败") from exc
    return {"ok": True, "accepted": accepted}


@app.post("/upload_invalid")
async def upload_invalid(file: UploadFile = File(...)) -> dict:
    return await receive_marked(file, "touch_results", "category", "触达无效")


@app.post("/upload_valid")
async def upload_valid(file: UploadFile = File(...)) -> dict:
    return await receive_marked(file, "touch_results", "category", "触达有效")


@app.post("/upload_customer")
async def upload_customer(file: UploadFile = File(...)) -> dict:
    return await receive_marked(file, "touch_results", "category", "触达已开客")
