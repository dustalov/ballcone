#!/usr/bin/env python3

import argparse
import duckdb  # pip install duckdb
import itertools
import monetdblite  # pip install -e 'git+https://github.com/MonetDB/MonetDBLite-Python.git@v0.6.3#egg=monetdblite'
from datetime import datetime, date
from pathlib import Path
from typing import Union, List, Optional, Any

try:
    from tqdm import trange  # pip install tqdm
except LoadError:
    def trange(*args, **kwargs):
        return range(*args)


def execute(db: Union[monetdblite.Connection, duckdb.DuckDBPyConnection], sql: str,
            many: Optional[List[Any]] = None) -> List:
    cursor = db.cursor()

    if isinstance(db, duckdb.DuckDBPyConnection):
        cursor.begin()

    if many is None:
        cursor.execute(sql)
        result = cursor.fetchall()
    else:
        cursor.executemany(sql, many)
        result = []

    cursor.commit()
    cursor.close()

    return result


SQL_MONETDB_TABLES = '''
SELECT t.name
FROM sys.tables AS t
JOIN sys.schemas AS s ON t.schema_id = s.id
WHERE s.name = 'ballcone'
-- AND t.name <> 'nlpub'
ORDER BY t.name
'''

SQL_MONETDB_COUNT = '''
SELECT COUNT(*) FROM ballcone.{table}
'''

SQL_MONETDB_DATA = '''
SELECT *
FROM ballcone.{table}
ORDER BY datetime
LIMIT {limit} OFFSET {offset}
'''

SQL_DUCKDB_TABLE = '''
CREATE OR REPLACE TABLE {table}(
    datetime TIMESTAMP NOT NULL,
    host VARCHAR NOT NULL,
    path VARCHAR NOT NULL,
    status SMALLINT NOT NULL,
    length INTEGER NOT NULL,
    generation_time DOUBLE NOT NULL,
    referer VARCHAR,
    ip VARCHAR NOT NULL,
    country_iso_code VARCHAR,
    platform_name VARCHAR,
    platform_version VARCHAR,
    browser_name VARCHAR,
    browser_version VARCHAR,
    is_robot BOOLEAN
)
'''

SQL_DUCKDB_INSERT = '''
INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
'''

SQL_DUCKDB_COUNT = '''
SELECT COUNT(*) FROM {table}
'''


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--batch', type=int, default=3072)
    parser.add_argument('monetdb', type=Path)
    parser.add_argument('duckdb', type=Path)
    args = parser.parse_args()

    db_monetdb = monetdblite.make_connection(str(args.monetdb.resolve()))
    db_duckdb = duckdb.connect(str(args.duckdb.resolve()))

    for table in itertools.chain.from_iterable(execute(db_monetdb, SQL_MONETDB_TABLES)):
        count = execute(db_monetdb, SQL_MONETDB_COUNT.format(table=table))[0][0]

        execute(db_duckdb, SQL_DUCKDB_TABLE.format(table=table))

        for offset in trange(0, 1 + count, args.batch, desc=table):
            data = execute(db_monetdb, SQL_MONETDB_DATA.format(table=table, limit=args.batch, offset=offset))

            for row in data:
                assert len(row) == 15, row

                # datetime
                row[0] = datetime.utcfromtimestamp(row[0])

                # status
                row[4] = int(row[4])

                # length
                row[5] = int(row[5])

                # generation_time
                row[6] = float(row[6])

                # is_robot
                row[14] = bool(row[14])

                # date
                del row[1]

            execute(db_duckdb, SQL_DUCKDB_INSERT.format(table=table), data)

        assert count == execute(db_duckdb, SQL_DUCKDB_COUNT.format(table=table))[0][0], table

    db_duckdb.close()
    db_monetdb.close()


if __name__ == '__main__':
    main()
