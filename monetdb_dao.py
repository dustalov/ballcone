__author__ = 'Dmitry Ustalov'

import logging
from contextlib import contextmanager
from datetime import date
from ipaddress import ip_address, IPv4Address, IPv6Address
from typing import Generator, NamedTuple, Optional, List, Sequence, Union, Any

import monetdblite
from monetdblite.monetize import monet_escape, monet_identifier_escape


class Entry(NamedTuple):
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
    country_iso_name: str
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
    field: Optional[str]
    distinct: bool
    ascending: Optional[bool]
    group: Optional[str]
    elements: Sequence[Count]


class Average(NamedTuple):
    date: date
    avg: float
    sum: float
    count: Any  # mypy prints an error if this is an int


class AverageResult(NamedTuple):
    field: str
    elements: Sequence[Average]


class MonetDAO:
    def __init__(self, db: monetdblite.Connection, schema_name: str):
        self.db = db
        self.schema_name = schema_name

    def close(self):
        self.db.close()

    def schema_exists(self):
        stmt = f'SELECT name FROM sys.schemas WHERE name = {monet_escape(self.schema_name)};'

        logging.info(stmt)

        result = self.db.execute(stmt)

        return result['name'].size > 0

    def create_schema(self):
        stmt = f'CREATE SCHEMA {monet_identifier_escape(self.schema_name)};'

        logging.info(stmt)

        with self.cursor() as cursor:
            result = cursor.execute(stmt)
            cursor.commit()
            return result

    def table_exists(self, table_name: str):
        stmt = f'SELECT tables.name AS name ' \
               f'FROM sys.tables AS tables JOIN sys.schemas AS schemas ON schemas.id = tables.schema_id ' \
               f'WHERE schemas.name = {monet_escape(self.schema_name)} AND tables.name = {monet_escape(table_name)};'

        logging.info(stmt)

        result = self.db.execute(stmt)

        return result['name'].size > 0

    def create_table(self, table_name: str):
        stmt = f'CREATE TABLE {monet_identifier_escape(self.schema_name)}.{monet_identifier_escape(table_name)} (' \
               f'date INT NOT NULL, ' \
               f'host TEXT NOT NULL, ' \
               f'method TEXT NOT NULL, ' \
               f'path TEXT NOT NULL, ' \
               f'status SMALLINT NOT NULL, ' \
               f'length INT NOT NULL, ' \
               f'generation_time DOUBLE, ' \
               f'referer TEXT, ' \
               f'ip TEXT NOT NULL, ' \
               f'country_iso_name TEXT NOT NULL, ' \
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

    def insert_into(self, table_name: str, entry: Entry,
                    cursor: Optional[monetdblite.cursors.Cursor] = None) -> int:
        value_stmt = self.value_entry(entry)

        stmt = f'INSERT INTO {monet_identifier_escape(self.schema_name)}.{monet_identifier_escape(table_name)} ' \
               f'VALUES {value_stmt};'

        logging.info(stmt)

        if cursor:
            return cursor.execute(stmt, discard_previous=False)
        else:
            with self.cursor() as cursor:
                result = cursor.execute(stmt)

                cursor.commit()

                return result

    def batch_insert_into(self, table_name, entries: Sequence[Entry]) -> int:
        count = 0

        with self.cursor() as cursor:
            for entry in entries:
                count += self.insert_into(table_name, entry, cursor=cursor)

            cursor.commit()

        return count

    @staticmethod
    def value_entry(entry: Entry) -> str:
        return f'(' \
               f'{monet_escape(entry.date.toordinal())}, ' \
               f'{monet_escape(entry.host)}, ' \
               f'{monet_escape(entry.method)}, ' \
               f'{monet_escape(entry.path)}, ' \
               f'{monet_escape(entry.status)}, ' \
               f'{monet_escape(entry.length)}, ' \
               f'{monet_escape(entry.generation_time)}, ' \
               f'{monet_escape(entry.referer) if entry.referer else "NULL"}, ' \
               f'{monet_escape(entry.ip)}, ' \
               f'{monet_escape(entry.country_iso_name)}, ' \
               f'{monet_escape(entry.platform_name) if entry.platform_name else "NULL"}, ' \
               f'{monet_escape(entry.platform_version) if entry.platform_version else "NULL"}, ' \
               f'{monet_escape(entry.browser_name) if entry.browser_name else "NULL"}, ' \
               f'{monet_escape(entry.browser_version) if entry.browser_version else "NULL"}, ' \
               f'{monet_escape(entry.is_robot) if entry.is_robot else "NULL"}' \
               f')'

    def select(self, table_name: str, date_begin: date = None, date_end: date = None, limit: int = None) -> List[Entry]:
        where_stmt = self.where_dates(date_begin, date_end)

        if where_stmt:
            where_stmt = ' WHERE ' + where_stmt

        limit_stmt = f' LIMIT {int(limit)}' if limit is not None else ''

        stmt = f'SELECT * ' \
               f'FROM {monet_identifier_escape(self.schema_name)}.{monet_identifier_escape(table_name)}' \
               f'{where_stmt}{limit_stmt};'

        logging.info(stmt)

        with self.cursor() as cursor:
            cursor.execute(stmt)

            results = []

            while True:
                current = cursor.fetchone()

                if not current:
                    break

                current[0] = date.fromordinal(current[0])  # date
                current[4] = int(current[4]) if current[4] is not None else None  # status
                current[5] = int(current[5]) if current[5] is not None else None  # length
                current[6] = float(current[6]) if current[6] is not None else None  # length
                current[7] = current[7] if current[7] else None  # referer
                current[8] = ip_address(current[8]) if current[8] else None  # ip
                current[14] = bool(current[14]) if current[14] is not None else None  # is_robot

                results.append(Entry(*current))

            return results

    def select_count(self, table_name: str, field: Optional[str], distinct: bool = False,
                     date_begin: Optional[date] = None, date_end: Optional[date] = None,
                     ascending: Optional[bool] = None, group: Optional[str] = None, limit: int = None) -> CountResult:
        where_stmt = self.where_dates(date_begin, date_end)

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
            order_stmt = ' ORDER BY count'
        else:
            order_stmt = ' ORDER BY count DESC'

        stmt = f'SELECT date{select_group_stmt}, COUNT({count_stmt}) AS count ' \
               f'FROM {monet_identifier_escape(self.schema_name)}.{monet_identifier_escape(table_name)}' \
               f'{where_stmt} GROUP BY date{group_by_stmt}{order_stmt}{limit_stmt};'

        logging.info(stmt)

        with self.cursor() as cursor:
            cursor.execute(stmt)

            result = CountResult(field=field, distinct=distinct, group=group, ascending=ascending, elements=[])

            for current in cursor:
                current_date = date.fromordinal(current[0])

                result.elements.append(Count(
                    date=current_date,
                    group=str(current[1]) if current[1] else None,
                    count=int(current[2])
                ))

            return result

    def select_average(self, table_name: str, field: str,
                       date_begin: date = None, date_end: date = None) -> AverageResult:
        where_stmt = self.where_dates(date_begin, date_end)

        if where_stmt:
            where_stmt = ' WHERE ' + where_stmt

        stmt = f'SELECT date, ' \
               f'AVG({monet_identifier_escape(field)}) AS average, ' \
               f'SUM({monet_identifier_escape(field)}) AS sum, ' \
               f'COUNT({monet_identifier_escape(field)}) AS count ' \
               f'FROM {monet_identifier_escape(self.schema_name)}.{monet_identifier_escape(table_name)}' \
               f'{where_stmt} GROUP BY date;'

        logging.info(stmt)

        with self.cursor() as cursor:
            cursor.execute(stmt)

            result = AverageResult(field=field, elements=[])

            for current in cursor:
                current_date = date.fromordinal(current[0])

                result.elements.append(Average(
                    date=current_date,
                    avg=float(current[1]),
                    sum=float(current[2]) if current[3] else 0.,
                    count=int(current[2])
                ))

            return result

    @staticmethod
    def where_dates(date_begin: Optional[date] = None, date_end: Optional[date] = None) -> str:
        if date_begin and date_end:
            return f'{monet_identifier_escape("date")} BETWEEN {monet_escape(date_begin.toordinal())} ' \
                   f'AND {monet_escape(date_end.toordinal())}'
        elif date_begin:
            return f'{monet_identifier_escape("date")} >= {monet_escape(date_begin.toordinal())}'
        elif date_end:
            return f'{monet_identifier_escape("date")} <= {monet_escape(date_end.toordinal())}'

        return ''

    @contextmanager
    def cursor(self) -> Generator[monetdblite.cursors.Cursor, None, None]:
        cursor = self.db.cursor()
        yield cursor
        cursor.close()
