"""Incrementally load encounters_100k.csv into staging.encounters on the SQL Server VM.

Every CSV column is loaded as NVARCHAR text, exactly as it appears in the file.
Type casting and cleaning happen later in dbt (the bronze layer).

The table is append-only and keeps history. On each run, every CSV row is compared
with the latest stored version of the same encounter_id (by a hash of its values):
    new        encounter_id not in the table yet      -> inserted
    changed    values differ from the latest version  -> inserted as a new version
    unchanged  values identical to the latest version -> skipped
Running the same file twice therefore inserts nothing.

Each row also gets audit columns: _row_id, _loaded_at, _batch_id, _source_file and
_row_hash. All rows inserted by one run share one _loaded_at and are committed in a
single transaction, so a batch is either fully visible or not there at all.

The run stops before writing anything if the CSV has a duplicate or empty encounter_id,
a value longer than the column allows, or columns that differ from the existing table.
If the connection drops, the run is retried (it is safe to repeat).

Rows are sent as JSON and unpacked with OPENJSON, which needs SQL Server compatibility
level 130 or higher.

Connection details come from the same environment variables as profiles.yml:
    DBT_SQLSERVER_HOST, DBT_SQLSERVER_USER, DBT_SQLSERVER_PASSWORD

Usage (from the repository root, with .venv-dbt activated):
    python scripts/load_staging_encounters.py --limit 1000   # quick test run
    python scripts/load_staging_encounters.py                # load encounters_100k.csv

The CSV is not in git; place it in the repository root, or pass its path with --csv.
"""

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
import uuid
from pathlib import Path

import pyodbc

REPO_ROOT = Path(__file__).resolve().parent.parent

DATABASE = "Abdelrahman_E_Mohammed"
SCHEMA = "staging"
TABLE = "encounters"
KEY_COLUMN = "encounter_id"
COLUMN_TYPE_LENGTH = 255  # longest value in the file is 58 characters
HASH_LENGTH = 64  # SHA-256 as hex
AUDIT_COLUMNS = ["_row_id", "_loaded_at", "_batch_id", "_source_file", "_row_hash"]
INCOMING = "#incoming"  # temporary table holding the CSV rows during a run
CONNECTION_ATTEMPTS = 3
RETRY_WAIT_SECONDS = 15


def quote(name: str) -> str:
    return "[" + name.replace("]", "]]") + "]"


def connect() -> pyodbc.Connection:
    missing = [
        name
        for name in ("DBT_SQLSERVER_HOST", "DBT_SQLSERVER_USER", "DBT_SQLSERVER_PASSWORD")
        if not os.environ.get(name)
    ]
    if missing:
        sys.exit(f"Missing environment variables: {', '.join(missing)}")

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={os.environ['server_address']},1433;"
        f"DATABASE={DATABASE};"
        f"UID={os.environ['server_user_name']};"
        f"PWD={{{os.environ['server_password'].replace('}', '}}')}}};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, timeout=30)


def table_columns(cursor: pyodbc.Cursor, table: str) -> list[str]:
    rows = cursor.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
        SCHEMA,
        table,
    ).fetchall()
    return [row[0] for row in rows]


def ensure_table(cursor: pyodbc.Cursor, table: str, columns: list[str]) -> bool:
    """Create the table on the first run; on later runs check the columns still match.

    Returns True if the table was created.
    """
    existing = table_columns(cursor, table)

    if existing:
        expected = [c for c in existing if c not in AUDIT_COLUMNS]
        if set(expected) != set(columns):
            sys.exit(
                f"CSV columns do not match [{SCHEMA}].[{table}]. "
                f"Missing from CSV: {sorted(set(expected) - set(columns))}. "
                f"Not in table: {sorted(set(columns) - set(expected))}."
            )
        return False

    column_defs = ",\n    ".join(
        f"{quote(col)} NVARCHAR({COLUMN_TYPE_LENGTH}) NULL" for col in columns
    )
    cursor.execute(f"IF SCHEMA_ID('{SCHEMA}') IS NULL EXEC('CREATE SCHEMA {quote(SCHEMA)}');")
    cursor.execute(
        f"""CREATE TABLE {quote(SCHEMA)}.{quote(table)} (
    {column_defs},
    [_row_id] BIGINT IDENTITY(1,1) NOT NULL,
    [_loaded_at] DATETIME2(3) NOT NULL,
    [_batch_id] UNIQUEIDENTIFIER NOT NULL,
    [_source_file] NVARCHAR(255) NOT NULL,
    [_row_hash] CHAR({HASH_LENGTH}) NOT NULL,
    CONSTRAINT {quote('PK_' + table)} PRIMARY KEY CLUSTERED ([_row_id])
);"""
    )
    # Speeds up "find the latest version of this encounter"
    cursor.execute(
        f"CREATE INDEX {quote('IX_' + table + '_key')} "
        f"ON {quote(SCHEMA)}.{quote(table)} ({quote(KEY_COLUMN)}, [_row_id]);"
    )
    return True


def row_hash(row: list[str | None], hash_order: list[int]) -> str:
    """Fingerprint of a row's data values, independent of CSV column order."""
    payload = json.dumps([row[i] for i in hash_order], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def stage_csv_rows(
    cursor: pyodbc.Cursor,
    reader,
    columns: list[str],
    batch_size: int,
    limit: int | None,
) -> int:
    """Copy CSV rows (plus their hash) into the temporary table. Returns the row count."""
    column_defs = ",\n    ".join(
        f"{quote(col)} NVARCHAR({COLUMN_TYPE_LENGTH}) NULL" for col in columns
    )
    cursor.execute(
        f"CREATE TABLE {INCOMING} (\n    {column_defs},\n    [_row_hash] CHAR({HASH_LENGTH}) NOT NULL\n);"
    )

    # Each batch is sent as ONE JSON string (an array of rows) and unpacked on the server.
    # Binding 93 NVARCHAR(255) parameters per row with executemany pads every value to
    # full width, ~47 KB per row on the wire for ~0.6 KB of data, and was ~16x slower.
    # Note: OPENJSON silently cuts values that do not fit the column, so lengths are
    # checked below instead.
    json_columns = ", ".join(
        f"{quote(col)} NVARCHAR({COLUMN_TYPE_LENGTH}) '$[{i}]'" for i, col in enumerate(columns)
    ) + f", [_row_hash] CHAR({HASH_LENGTH}) '$[{len(columns)}]'"
    insert_sql = (
        f"INSERT INTO {INCOMING} ({', '.join(quote(c) for c in columns)}, [_row_hash]) "
        f"SELECT {', '.join(quote(c) for c in columns)}, [_row_hash] "
        f"FROM OPENJSON(?) WITH ({json_columns})"
    )

    def send(rows: list[list[str | None]]) -> None:
        cursor.execute(insert_sql, json.dumps(rows, ensure_ascii=False, separators=(",", ":")))

    key_index = columns.index(KEY_COLUMN)
    hash_order = sorted(range(len(columns)), key=lambda i: columns[i])
    seen_keys: set[str] = set()
    rows_read = 0
    batch = []

    for row in reader:
        if not row:
            continue  # blank line
        if len(row) != len(columns):
            sys.exit(f"Line {reader.line_num}: expected {len(columns)} fields, found {len(row)}")

        # Empty CSV fields become NULL
        values = [value if value != "" else None for value in row]

        for column, value in zip(columns, values):
            if value is not None and len(value) > COLUMN_TYPE_LENGTH:
                sys.exit(
                    f"Line {reader.line_num}: {column} is {len(value)} characters, "
                    f"longer than the {COLUMN_TYPE_LENGTH} the table allows"
                )

        key = values[key_index]
        if key is None:
            sys.exit(f"Line {reader.line_num}: empty {KEY_COLUMN}")
        if key in seen_keys:
            sys.exit(f"Line {reader.line_num}: duplicate {KEY_COLUMN} {key!r}; nothing was loaded")
        seen_keys.add(key)

        batch.append(values + [row_hash(values, hash_order)])
        rows_read += 1

        if len(batch) >= batch_size:
            send(batch)
            batch = []
            print(f"  {rows_read:>9,} rows sent")
        if limit and rows_read >= limit:
            break

    if batch:
        send(batch)

    return rows_read


def load(csv_path: Path, table: str, batch_size: int, limit: int | None) -> None:
    start = time.time()
    target = f"{quote(SCHEMA)}.{quote(table)}"
    key = quote(KEY_COLUMN)

    conn = connect()
    conn.autocommit = False
    cursor = conn.cursor()

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        columns = next(reader)

        if len({c.lower() for c in columns}) != len(columns) or "" in columns:
            sys.exit("CSV header has duplicate or empty column names")
        if KEY_COLUMN not in columns:
            sys.exit(f"CSV has no {KEY_COLUMN} column")

        if ensure_table(cursor, table, columns):
            print(f"Created {target} with {len(columns)} data columns")
        conn.commit()

        stage_start = time.time()
        rows_read = stage_csv_rows(cursor, reader, columns, batch_size, limit)
    stage_seconds = time.time() - stage_start
    print(f"Read {rows_read:,} rows from {csv_path.name} in {stage_seconds:.1f}s ({rows_read / max(stage_seconds, 0.001):,.0f} rows/s)")

    # Compare each incoming row with the LATEST stored version of the same encounter.
    # Comparing with the latest (not any) version means a change A -> B -> A is still seen.
    latest = f"""WITH latest AS (
    SELECT {key}, [_row_hash],
           ROW_NUMBER() OVER (PARTITION BY {key} ORDER BY [_row_id] DESC) AS rn
    FROM {target}
)"""
    join_latest = f"LEFT JOIN latest l ON l.{key} = i.{key} AND l.rn = 1"

    count_before = cursor.execute(f"SELECT COUNT_BIG(*) FROM {target}").fetchval()
    new, changed, unchanged = cursor.execute(
        f"""{latest}
SELECT COALESCE(SUM(CASE WHEN l.[_row_hash] IS NULL THEN 1 ELSE 0 END), 0),
       COALESCE(SUM(CASE WHEN l.[_row_hash] <> i.[_row_hash] THEN 1 ELSE 0 END), 0),
       COALESCE(SUM(CASE WHEN l.[_row_hash] = i.[_row_hash] THEN 1 ELSE 0 END), 0)
FROM {INCOMING} i {join_latest}"""
    ).fetchone()

    batch_id = str(uuid.uuid4())
    loaded_at = cursor.execute("SELECT CAST(SYSUTCDATETIME() AS DATETIME2(3))").fetchval()
    data_columns = ", ".join(quote(c) for c in columns)
    incoming_columns = ", ".join(f"i.{quote(c)}" for c in columns)
    cursor.execute(
        f"""{latest}
INSERT INTO {target} ({data_columns}, [_loaded_at], [_batch_id], [_source_file], [_row_hash])
SELECT {incoming_columns}, ?, ?, ?, i.[_row_hash]
FROM {INCOMING} i {join_latest}
WHERE l.[_row_hash] IS NULL OR l.[_row_hash] <> i.[_row_hash]""",
        loaded_at,
        batch_id,
        csv_path.name,
    )
    inserted = cursor.rowcount

    count_after = cursor.execute(f"SELECT COUNT_BIG(*) FROM {target}").fetchval()
    if inserted != new + changed or count_after - count_before != inserted:
        conn.rollback()
        sys.exit(
            f"Row count mismatch: expected {new + changed:,} inserts, "
            f"inserted {inserted:,}, table grew by {count_after - count_before:,}. Rolled back."
        )
    conn.commit()
    conn.close()

    print(f"  new        {new:>9,}")
    print(f"  changed    {changed:>9,}")
    print(f"  unchanged  {unchanged:>9,}")
    print(f"Inserted {inserted:,} rows into {target} in {time.time() - start:.1f}s (batch {batch_id})")
    print(f"Row count check passed: {target} has {count_after:,} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", type=Path, default=REPO_ROOT / "encounters_100k.csv", help="path to the CSV file")
    parser.add_argument("--table", default=TABLE, help=f"table in the {SCHEMA} schema (default: {TABLE}); use another name to test")
    parser.add_argument("--batch-size", type=int, default=5_000, help="rows sent per round trip")
    parser.add_argument("--limit", type=int, help="read only the first N rows (for testing)")
    args = parser.parse_args()

    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.table):
        sys.exit(f"Invalid table name: {args.table}")
    if not args.csv.exists():
        sys.exit(f"CSV not found: {args.csv}")

    # The connection to the VM sometimes drops. A run writes nothing until its final
    # transaction, and rerunning is safe (rows already stored are skipped), so retry.
    for attempt in range(1, CONNECTION_ATTEMPTS + 1):
        try:
            load(args.csv, args.table, args.batch_size, args.limit)
            return
        except (pyodbc.OperationalError, pyodbc.InterfaceError) as error:
            if attempt == CONNECTION_ATTEMPTS:
                sys.exit(f"Connection failed {CONNECTION_ATTEMPTS} times, giving up: {error}")
            print(f"Connection problem (attempt {attempt}/{CONNECTION_ATTEMPTS}): {error}\nRetrying in {RETRY_WAIT_SECONDS}s...")
            time.sleep(RETRY_WAIT_SECONDS)


if __name__ == "__main__":
    main()
