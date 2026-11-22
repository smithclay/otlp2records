import re
import sys
from pathlib import Path

import duckdb
import pyarrow.ipc

SIGNALS = ["logs", "traces", "gauge", "sum", "histogram", "exp_histogram"]
REMAP = {
    "BLOB": "VARCHAR",
    "INTERVAL": "BIGINT",
    "UTINYINT": "INTEGER",
    "USMALLINT": "INTEGER",
    "UINTEGER": "INTEGER",
    "UBIGINT": "BIGINT",
}
UNSIGNED = {"UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"}
PINNED = {
    "traces": {
        "trace_id": "VARCHAR",
        "span_id": "VARCHAR",
        "duration_time_unix_nano": "BIGINT",
        "start_time_unix_nano": "TIMESTAMP_NS",
        "flags": "INTEGER",
    },
    "logs": {"trace_id": "VARCHAR", "time_unix_nano": "TIMESTAMP_NS"},
    "histogram": {"bucket_counts": "BIGINT[]", "count": "BIGINT"},
}
EARLIEST, LATEST = "2000-01-01", "2100-01-01"

failures = []


def fail(message):
    failures.append(message)
    print("FAIL", message)


def element(sql_type):
    match = re.fullmatch(r"(.*)\[\]", sql_type)
    return element(match.group(1)) if match else sql_type


def duckdb_type(sql_type):
    match = re.fullmatch(r"(.*)\[\]", sql_type)
    if match:
        return duckdb_type(match.group(1)) + "[]"
    return REMAP.get(sql_type, sql_type)


def load(con, name, path):
    table = pyarrow.ipc.open_stream(path.read_bytes()).read_all()
    con.register(name, table)
    return {row[0]: row[1] for row in con.sql(f"DESCRIBE {name}").fetchall()}


def check_signal(con, out, signal):
    canonical = load(con, "c", out / f"{signal}.canonical.arrows")
    profiled = load(con, "d", out / f"{signal}.duckdb.arrows")

    if list(profiled) != list(canonical):
        fail(f"{signal}: DuckDb profile columns {list(profiled)} != canonical {list(canonical)}")
    for column, sql_type in canonical.items():
        if profiled.get(column) != duckdb_type(sql_type):
            fail(f"{signal}.{column}: DuckDB imports {profiled.get(column)}, expected {duckdb_type(sql_type)}")
    for column, sql_type in PINNED.get(signal, {}).items():
        if profiled.get(column) != sql_type:
            fail(f"{signal}.{column}: duckdb-otlp expects {sql_type}, DuckDB imports {profiled.get(column)}")

    rows = con.sql("SELECT count(*) FROM c").fetchone()[0]
    if rows == 0 or rows != con.sql("SELECT count(*) FROM d").fetchone()[0]:
        fail(f"{signal}: row counts differ or are zero")
    con.sql("CREATE OR REPLACE TABLE cr AS SELECT row_number() OVER () AS r, * FROM c")
    con.sql("CREATE OR REPLACE TABLE dr AS SELECT row_number() OVER () AS r, * FROM d")
    for column, sql_type in canonical.items():
        base = element(sql_type)
        quoted = f'"{column}"'
        if base == "INTERVAL":
            continue
        if base == "BLOB":
            left, right = f"lower(hex(cr.{quoted}))", f"dr.{quoted}"
            if sql_type != base:
                left = f"list_transform(cr.{quoted}, x -> lower(hex(x)))"
        elif base in UNSIGNED:
            cast = "HUGEINT[]" if sql_type != base else "HUGEINT"
            left, right = f"cr.{quoted}::{cast}", f"dr.{quoted}::{cast}"
        else:
            left, right = f"cr.{quoted}", f"dr.{quoted}"
        mismatched = con.sql(
            f"SELECT count(*) FROM cr JOIN dr USING (r) WHERE {left} IS DISTINCT FROM {right}"
        ).fetchone()[0]
        if mismatched:
            fail(f"{signal}.{column}: {mismatched} value(s) changed under the DuckDb profile")
        if base == "BLOB" and sql_type == base:
            bad = con.sql(
                f"SELECT count(*) FROM dr WHERE {quoted} IS NOT NULL "
                f"AND ({quoted} <> lower({quoted}) OR length({quoted}) NOT IN (16, 32))"
            ).fetchone()[0]
            if bad:
                fail(f"{signal}.{column}: {bad} id(s) are not lowercase 16/32-char hex")

    check_timestamps(con, f"{signal} (Arrow)", "d", profiled)
    parquet = {
        row[0]: row[1]
        for row in con.sql(f"DESCRIBE SELECT * FROM read_parquet('{out / (signal + '.parquet')}')").fetchall()
    }
    con.sql(f"CREATE OR REPLACE VIEW p AS SELECT * FROM read_parquet('{out / (signal + '.parquet')}')")
    if con.sql("SELECT count(*) FROM p").fetchone()[0] != rows:
        fail(f"{signal}: Parquet row count differs from Arrow")
    for column, sql_type in canonical.items():
        if sql_type.startswith("TIMESTAMP") and parquet.get(column) != sql_type:
            fail(f"{signal}.{column}: Parquet stores {parquet.get(column)}, Arrow {sql_type}")
    check_timestamps(con, f"{signal} (Parquet)", "p", parquet)


def check_timestamps(con, label, relation, columns):
    stamps = [c for c, t in columns.items() if t.startswith("TIMESTAMP")]
    if not stamps:
        fail(f"{label}: no timestamp columns")
    populated = 0
    for column in stamps:
        if columns[column] != "TIMESTAMP_NS":
            fail(f"{label}.{column}: {columns[column]}, expected TIMESTAMP_NS")
        low, high, present = con.sql(
            f'SELECT min("{column}"), max("{column}"), count("{column}") FROM {relation} '
            f'WHERE "{column}" <> TIMESTAMP \'1970-01-01\''
        ).fetchone()
        populated += present
        if present and not (str(low) >= EARLIEST and str(high) < LATEST):
            fail(f"{label}.{column}: values span {low} .. {high}, outside {EARLIEST} .. {LATEST}")
    if not populated:
        fail(f"{label}: every timestamp is the Unix epoch")


def main():
    out = Path(sys.argv[1])
    con = duckdb.connect()
    for signal in SIGNALS:
        check_signal(con, out, signal)
        print(f"{signal}: checked")
    if failures:
        print(f"{len(failures)} DuckDB check(s) failed")
        sys.exit(1)
    print(f"DuckDB {duckdb.__version__}: all checks passed")


main()
