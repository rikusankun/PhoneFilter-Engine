#!/usr/bin/env python3
"""从 phone_records 打出 10 条，核对名字和年龄是不是空的。"""

import sys

from import_data import CONTAINER, DATABASE, TABLE, DockerInserter, configure_stdio

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


def main() -> int:
    configure_stdio()
    db = DockerInserter(CONTAINER)
    full = f"{DATABASE}.{TABLE}"
    rows = db.query_rows(
        f"SELECT {', '.join(COLUMNS)} FROM {full} LIMIT 10"
    )
    empty_name = db.command(f"SELECT count() FROM {full} WHERE name = ''")
    age_zero = db.command(f"SELECT count() FROM {full} WHERE age = 0")
    total = db.command(f"SELECT count() FROM {full}")

    print(f"表里共 {int(total):,} 条，名字为空 {int(empty_name):,} 条，年龄为 0 {int(age_zero):,} 条")
    print(f"下面是前 10 条：")
    for index, row in enumerate(rows, start=1):
        print(f"\n--- 第 {index} 条 ---")
        for column, value in zip(COLUMNS, row):
            print(f"{column}: {value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
