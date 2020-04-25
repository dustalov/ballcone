__author__ = 'Dmitry Ustalov'

import logging
from calendar import timegm
from contextlib import contextmanager
from datetime import datetime, date
from ipaddress import ip_address, IPv4Address, IPv6Address
from typing import Generator, NamedTuple, Optional, List, Sequence, Union, Any

import monetdblite
from monetdblite.monetize import monet_escape, monet_identifier_escape


class Entry(NamedTuple):
    datetime: datetime
    date: date
    host: str
    method: str
    path: str
    status: int
    length: int
    generation_time: float
    referer: Optional[str]
    # IP address and derivatives
    ip: Union[IPv4Address, IPv6Address]
    country_iso_code: str
    # derivatives from User-Agent
    platform_name: Optional[str]
    platform_version: Optional[str]
    browser_name: Optional[str]
    browser_version: Optional[str]
    is_robot: Optional[bool]


class Count(NamedTuple):
    date: date
    group: Optional[str]
    count: Any  # mypy prints an error if this is an int


class CountResult(NamedTuple):
    table: str
    field: Optional[str]
    distinct: bool
    ascending: Optional[bool]
    group: Optional[str]
    elements: List[Count]


class Average(NamedTuple):
    date: date
    avg: float
    sum: float
    count: Any  # mypy prints an error if this is an int


class AverageResult(NamedTuple):
    table: str
    field: str
    elements: List[Average]


class MonetDAO:
    def __init__(self, db: monetdblite.Connection, schema: str):
        self.db = db
        self.schema = schema

    def close(self):
        self.db.close()

    def schema_exists(self):
        stmt = f'SELECT name FROM sys.schemas WHERE name = {monet_escape(self.schema)};'

        logging.info(stmt)

        result = self.db.execute(stmt)

        return result['name'].size > 0

    def create_schema(self):
        stmt = f'CREATE SCHEMA {monet_identifier_escape(self.schema)};'

        logging.info(stmt)

        with self.cursor() as cursor:
            result = cursor.execute(stmt)
            cursor.commit()
            return result

    def tables(self) -> Sequence[str]:
        stmt = f'SELECT tables.name AS name ' \
               f'FROM sys.tables AS tables JOIN sys.schemas AS schemas ON schemas.id = tables.schema_id ' \
               f'WHERE schemas.name = {monet_escape(self.schema)};'

        logging.info(stmt)

        result = self.db.execute(stmt)

        return result['name'].tolist()

    def table_exists(self, table: str):
        stmt = f'SELECT tables.name AS name ' \
               f'FROM sys.tables AS tables JOIN sys.schemas AS schemas ON schemas.id = tables.schema_id ' \
               f'WHERE schemas.name = {monet_escape(self.schema)} AND tables.name = {monet_escape(table)};'

        logging.info(stmt)

        result = self.db.execute(stmt)

        return result['name'].size > 0

    def create_table(self, table: str):
        stmt = f'CREATE TABLE {monet_identifier_escape(self.schema)}.{monet_identifier_escape(table)} (' \
               f'datetime BIGINT NOT NULL, ' \
               f'date INT NOT NULL, ' \
               f'host TEXT NOT NULL, ' \
               f'method TEXT NOT NULL, ' \
               f'path TEXT NOT NULL, ' \
               f'status SMALLINT NOT NULL, ' \
               f'length INT NOT NULL, ' \
               f'generation_time DOUBLE, ' \
               f'referer TEXT, ' \
               f'ip TEXT NOT NULL, ' \
               f'country_iso_code TEXT NOT NULL, ' \
               f'platform_name TEXT, ' \
               f'platform_version TEXT, ' \
               f'browser_name TEXT, ' \
               f'browser_version TEXT, ' \
               f'is_robot BOOL' \
               f');'

        logging.info(stmt)

        with self.cursor() as cursor:
            result = cursor.execute(stmt)
            cursor.commit()
            return result

    def insert_into(self, table: str, entry: Entry, cursor: Optional[monetdblite.cursors.Cursor] = None) -> int:
        value_stmt = self.value_entry(entry)

        stmt = f'INSERT INTO {monet_identifier_escape(self.schema)}.{monet_identifier_escape(table)} ' \
               f'VALUES {value_stmt};'

        logging.info(stmt)

        if cursor:
            return cursor.execute(stmt, discard_previous=False)
        else:
            with self.cursor() as cursor:
                result = cursor.execute(stmt)

                cursor.commit()

                return result

    def batch_insert_into(self, table: str, entries: Sequence[Entry]) -> int:
        count = 0

        if entries:
            with self.cursor() as cursor:
                for entry in entries:
                    self.insert_into(table, entry, cursor=cursor)
                    count += 1

                cursor.commit()

        return count

    @staticmethod
    def value_entry(entry: Entry) -> str:
        return f'(' \
               f'{monet_escape(timegm(entry.datetime.utctimetuple()))}, ' \
               f'{monet_escape(entry.date.toordinal())}, ' \
               f'{monet_escape(entry.host)}, ' \
               f'{monet_escape(entry.method)}, ' \
               f'{monet_escape(entry.path)}, ' \
               f'{monet_escape(entry.status)}, ' \
               f'{monet_escape(entry.length)}, ' \
               f'{monet_escape(entry.generation_time)}, ' \
               f'{monet_escape(entry.referer) if entry.referer else "NULL"}, ' \
               f'{monet_escape(entry.ip)}, ' \
               f'{monet_escape(entry.country_iso_code)}, ' \
               f'{monet_escape(entry.platform_name) if entry.platform_name else "NULL"}, ' \
               f'{monet_escape(entry.platform_version) if entry.platform_version else "NULL"}, ' \
               f'{monet_escape(entry.browser_name) if entry.browser_name else "NULL"}, ' \
               f'{monet_escape(entry.browser_version) if entry.browser_version else "NULL"}, ' \
               f'{monet_escape(entry.is_robot) if entry.is_robot is not None else "NULL"}' \
               f')'

    def select(self, table: str, start: date = None, stop: date = None, limit: int = None) -> List[Entry]:
        where_stmt = self.where_dates(start, stop)

        if where_stmt:
            where_stmt = ' WHERE ' + where_stmt

        limit_stmt = f' LIMIT {int(limit)}' if limit is not None else ''

        stmt = f'SELECT * ' \
               f'FROM {monet_identifier_escape(self.schema)}.{monet_identifier_escape(table)}' \
               f'{where_stmt} ORDER BY date{limit_stmt};'

        logging.info(stmt)

        with self.cursor() as cursor:
            cursor.execute(stmt)

            results = []

            while True:
                current = cursor.fetchone()

                if not current:
                    break

                current[0] = datetime.utcfromtimestamp(current[0])  # datetime
                current[1] = date.fromordinal(current[1])  # date
                current[5] = int(current[5]) if current[5] is not None else None  # status
                current[6] = int(current[6]) if current[6] is not None else None  # length
                current[7] = float(current[7]) if current[7] is not None else None  # length
                current[8] = current[8] if current[8] else None  # referer
                current[9] = ip_address(current[9]) if current[9] else None  # ip
                current[15] = bool(current[15]) if current[15] is not None else None  # is_robot

                results.append(Entry(*current))

            return results

    def select_average(self, table: str, field: str, start: date = None, stop: date = None) -> AverageResult:
        where_stmt = self.where_dates(start, stop)

        if where_stmt:
            where_stmt = ' WHERE ' + where_stmt

        stmt = f'SELECT date, ' \
               f'AVG({monet_identifier_escape(field)}) AS average, ' \
               f'SUM({monet_identifier_escape(field)}) AS sum, ' \
               f'COUNT({monet_identifier_escape(field)}) AS count ' \
               f'FROM {monet_identifier_escape(self.schema)}.{monet_identifier_escape(table)}' \
               f'{where_stmt} GROUP BY date ORDER BY date;'

        logging.info(stmt)

        with self.cursor() as cursor:
            cursor.execute(stmt)

            result = AverageResult(table=table, field=field, elements=[])

            for current in cursor:
                current_date = date.fromordinal(current[0])

                result.elements.append(Average(
                    date=current_date,
                    avg=float(current[1]),
                    sum=float(current[2]) if current[3] else 0.,
                    count=int(current[2])
                ))

            return result

    def select_count(self, table: str, field: Optional[str], distinct: bool = False,
                     start: Optional[date] = None, stop: Optional[date] = None,
                     ascending: Optional[bool] = None, group: Optional[str] = None, limit: int = None) -> CountResult:
        where_stmt = self.where_dates(start, stop)

        if where_stmt:
            where_stmt = ' WHERE ' + where_stmt

        if distinct:
            count_stmt = f'DISTINCT {monet_identifier_escape(field if field else "1")}'
        else:
            count_stmt = f'{monet_identifier_escape(field) if field else "*"}'

        if group:
            select_group_stmt = f', {monet_identifier_escape(group)} AS _group_'
            group_by_stmt = f', {monet_identifier_escape(group)}'
        else:
            select_group_stmt = ', 0 AS _group_'
            group_by_stmt = ''

        limit_stmt = f' LIMIT {int(limit)}' if limit is not None else ''

        if ascending is None:
            order_stmt = ''
        elif ascending:
            order_stmt = ', count'
        else:
            order_stmt = ', count DESC'

        stmt = f'SELECT date{select_group_stmt}, COUNT({count_stmt}) AS count ' \
               f'FROM {monet_identifier_escape(self.schema)}.{monet_identifier_escape(table)}' \
               f'{where_stmt} GROUP BY date{group_by_stmt} ORDER BY date{order_stmt}{limit_stmt};'

        logging.info(stmt)

        with self.cursor() as cursor:
            cursor.execute(stmt)

            result = CountResult(table=table, field=field,
                                 distinct=distinct, group=group, ascending=ascending,
                                 elements=[])

            for current in cursor:
                current_date = date.fromordinal(current[0])

                result.elements.append(Count(
                    date=current_date,
                    group=str(current[1]) if current[1] else None,
                    count=int(current[2])
                ))

            return result

    @staticmethod
    def where_dates(start: Optional[date] = None, stop: Optional[date] = None) -> str:
        if start and stop:
            return f'{monet_identifier_escape("date")} BETWEEN {monet_escape(start.toordinal())} ' \
                   f'AND {monet_escape(stop.toordinal())}'
        elif start:
            return f'{monet_identifier_escape("date")} >= {monet_escape(start.toordinal())}'
        elif stop:
            return f'{monet_identifier_escape("date")} <= {monet_escape(stop.toordinal())}'

        return ''

    @contextmanager
    def cursor(self) -> Generator[monetdblite.cursors.Cursor, None, None]:
        cursor = self.db.cursor()
        yield cursor
        cursor.close()