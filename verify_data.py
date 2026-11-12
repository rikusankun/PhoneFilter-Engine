#!/usr/bin/env python3
"""只读核对 phone_records：条数、样例号码、年龄、金额。"""

import os
import sys

from import_data import (
    CONTAINER,
    DATABASE,
    TABLE,
    DockerInserter,
    HttpInserter,
    configure_stdio,
)

PHONE = "19072000000"
FULL = f"{DATABASE}.{TABLE}"


def main() -> int:
    configure_stdio()
    password = os.environ.get("CLICKHOUSE_PASSWORD")
    db = HttpInserter(password) if password else DockerInserter(CONTAINER)

    total = db.command(f"SELECT count() FROM {FULL}")
    print(f"总共入库: {int(total):,} 条")

    rows = db.query_rows(
        f"SELECT name, age FROM {FULL} WHERE phone = '{PHONE}'"
    )
    print(f"号码 {PHONE}:")
    if not rows:
        print("  没有这条号码")
    else:
        for name, age in rows:
            print(f"  name={name}  age={age}")

    over = db.command(f"SELECT count() FROM {FULL} WHERE age > 120")
    oldest = db.command(f"SELECT max(age) FROM {FULL}")
    print(f"age > 120: {int(over):,} 条，最大年龄 {oldest}")

    dollar = db.command(
        f"SELECT count() FROM {FULL} WHERE toString(estimated_value) LIKE '%$%'"
    )
    blank = db.command(
        f"SELECT count() FROM {FULL} WHERE toString(estimated_value) = ''"
    )
    samples = db.query_rows(f"SELECT estimated_value FROM {FULL} LIMIT 5")
    shown = ", ".join(str(row[0]) for row in samples) if samples else "（空表）"
    print(f"金额带 $: {int(dollar):,} 条")
    print(f"金额是空字符串: {int(blank):,} 条")
    print(f"金额抽样: {shown}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
