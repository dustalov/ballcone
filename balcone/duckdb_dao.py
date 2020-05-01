__author__ = 'Dmitry Ustalov'

import logging
from contextlib import contextmanager
from datetime import datetime, date
from ipaddress import ip_address, IPv4Address, IPv6Address
from typing import Generator, NamedTuple, Optional, List, Sequence, Union, Any, NewType, Tuple, Deque, Set, cast

import duckdb
from pypika import Query, Column, Field, Table, Order, functions as fn, analytics as an
from pypika.queries import QueryBuilder

smallint = NewType('smallint', int)

TYPES = {
    datetime: 'timestamp',
    date: 'date',
    str: 'varchar',
    smallint: 'smallint',
    int: 'integer',
    float: 'double',
    IPv4Address: 'varchar',
    IPv6Address: 'varchar',
    bool: 'boolean'
}


def is_empty(obj: Any) -> bool:
    if hasattr(obj, '__len__'):
        return not len(obj)

    return obj is None


def optional_types(annotation: Any) -> Tuple[Set[Any], bool]:
    if hasattr(annotation, '__args__'):
        types = set(annotation.__args__)
        null = type(None) in types

        if null:
            types.remove(type(None))

        return types, null
    else:
        return {annotation}, False


def python_type_to_sql(annotation: Any) -> str:
    types, null = optional_types(annotation)
    first_type = next(iter(types))

    if null:
        return TYPES[first_type]
    else:
        return TYPES[first_type] + ' NOT NULL'


def sql_value_to_python(name: str, annotation: Any, value: Any) -> Any:
    args, null = optional_types(annotation)
    first_type = next(iter(args))

    if isinstance(value, (datetime, date)):
        return value

    if first_type == smallint:
        return int(value)

    if first_type in (IPv4Address, IPv6Address):
        return ip_address(value)

    return None if is_empty(value) and null else first_type(value)


class Entry(NamedTuple):
    datetime: datetime
    date: date
    host: str
    method: str
    path: str
    status: smallint
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

    @staticmethod
    def from_values(entry: Tuple) -> 'Entry':
        return Entry(*(sql_value_to_python(name, annotation, value)
                       for (name, annotation), value in zip(Entry.__annotations__.items(), entry)))

    @staticmethod
    def as_value(value: Any, annotation: Any = None) -> Any:
        if isinstance(value, datetime):
            return cast(datetime, value).isoformat(' ')

        if isinstance(value, date):
            return cast(date, value).isoformat()

        if isinstance(value, (IPv4Address, IPv6Address)):
            return str(value)

        if annotation:
            _, null = optional_types(annotation)

            return None if is_empty(value) and null else value
        else:
            return value

    def as_values(self) -> Tuple:
        return tuple(self.as_value(getattr(self, name), annotation)
                     for name, annotation in self.__annotations__.items())


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


class DuckDAO:
    def __init__(self, db: duckdb.DuckDBPyConnection):
        self.db = db
        self.master = Table('sqlite_master')

    def tables(self) -> Sequence[str]:
        query = Query.from_(self.master).select(self.master.name). \
            where(self.master.type == 'table').distinct()

        return [table for table, *_ in self.run(query)]

    def table_exists(self, table: str) -> bool:
        query = Query.from_(self.master).select(self.master.name). \
            where((self.master.type == 'table') & (self.master.name == table))

        return len(self.run(query)) > 0

    def create_table(self, table: str):
        target = Table(table)

        columns = [Column(name, python_type_to_sql(annotation)) for name, annotation in Entry.__annotations__.items()]

        query = Query.create_table(target).columns(*columns)
        sql = str(query)

        logging.debug(sql)

        with self.transaction() as cursor:
            return cursor.execute(sql)

    def drop_table(self, table: str):
        # FIXME: escaping
        sql = f'DROP TABLE "{table}";'

        logging.debug(sql)

        with self.transaction() as cursor:
            return cursor.execute(sql)

    def insert_into(self, table: str, entry: Entry, cursor: Optional[duckdb.DuckDBPyConnection] = None):
        target = Table(table)

        query = Query.into(target).insert(*entry.as_values())
        sql = str(query)

        logging.debug(sql)

        if cursor:
            cursor.execute(sql)
        else:
            with self.transaction() as cursor:
                cursor.execute(sql)

    def batch_insert_into(self, table: str, entries: Sequence[Entry]) -> int:
        count = 0

        if entries:
            with self.transaction() as cursor:
                for entry in entries:
                    self.insert_into(table, entry, cursor=cursor)
                    count += 1

        return count

    def batch_insert_into_from_deque(self, table: str, entries: Deque[Entry]) -> int:
        count = 0

        if entries:
            with self.transaction() as cursor:
                while entries:
                    entry = entries.popleft()
                    self.insert_into(table, entry, cursor=cursor)
                    count += 1

        return count

    def select(self, table: str, start: Optional[date] = None, stop: Optional[date] = None,
               limit: Optional[int] = None) -> List[Entry]:
        target = Table(table)

        query = Query.from_(target).select('*').orderby(target.datetime).limit(limit)

        query = self.apply_dates(query, target, start, stop)

        rows = self.run(query)

        for i, current in enumerate(rows):
            rows[i] = Entry.from_values(current)

        return rows

    def select_average(self, table: str, field: str, start: date = None, stop: date = None) -> AverageResult:
        target = Table(table)
        target_field = Field(field, table=target)

        query = Query.from_(target).select(target.date,
                                           fn.Avg(target_field, alias='average'),
                                           fn.Sum(target_field, alias='sum'),
                                           fn.Count(target_field, alias='count')). \
            groupby(target.date).orderby(target.date)

        query = self.apply_dates(query, target, start, stop)

        result = AverageResult(table=table, field=field, elements=[])

        for current in self.run(query):
            current_date = current[0]

            result.elements.append(Average(
                date=current_date,
                avg=float(current[1]),
                sum=float(current[2]) if current[3] else 0.,
                count=int(current[3])
            ))

        return result

    def select_count(self, table: str, field: Optional[str] = None, start: Optional[date] = None,
                     stop: Optional[date] = None) -> CountResult:
        target = Table(table)
        count_field = fn.Count(Field(field, table=target) if field else target.date, alias='count')

        if field:
            count_field = count_field.distinct()

        query = Query.from_(target).select(target.date, count_field).groupby(target.date).orderby(target.date)

        query = self.apply_dates(query, target, start, stop)

        result = CountResult(table=table, field=field, distinct=field is not None, group=None, ascending=None,
                             elements=[])

        for current in self.run(query):
            result.elements.append(Count(
                date=current[0],
                group=None,
                count=int(current[1])
            ))

        return result

    def select_count_group(self, table: str, field: Optional[str], group: str, distinct: bool = False,
                           start: Optional[date] = None, stop: Optional[date] = None,
                           ascending: bool = True, limit: Optional[int] = None) -> CountResult:
        target = Table(table)
        count_field = fn.Count(Field(field, table=target) if field else target.date, alias='count')

        if distinct:
            count_field = count_field.distinct()

        group_field = Field(group, table=target)

        query = Query.from_(target).select(target.date, group_field.as_('group'), count_field). \
            groupby(target.date, group_field).orderby(target.date). \
            orderby(count_field, order=Order.asc if ascending else Order.desc). \
            orderby(group_field)

        query = self.apply_dates(query, target, start, stop)

        if limit is not None:
            window = Query.from_(query).select(query.date, query.group, query.count,
                                               an.RowNumber(alias='row_number').over(query.date))

            query = Query.from_(window).select(window.date, window.group, window.count). \
                where(window.row_number <= limit).orderby(window.date). \
                orderby(window.count, order=Order.asc if ascending else Order.desc). \
                orderby(window.group)

        result = CountResult(table=table, field=field, distinct=distinct, group=group, ascending=ascending,
                             elements=[])

        for current in self.run(query):
            result.elements.append(Count(
                date=current[0],
                group=current[1],
                count=int(current[2])
            ))

        return result

    def run(self, query: Union[QueryBuilder, str]) -> List:
        sql = str(query) if isinstance(query, QueryBuilder) else query

        logging.debug(sql)

        with self.cursor() as cursor:
            cursor.execute(sql)

            return cursor.fetchall()

    @staticmethod
    def apply_dates(query: QueryBuilder, target: Table,
                    start: Optional[date] = None, stop: Optional[date] = None) -> QueryBuilder:
        if start and stop:
            if start == stop:
                return query.where(target.date == Entry.as_value(start))
            else:
                return query.where(target.date[Entry.as_value(start):Entry.as_value(stop)])
        elif start:
            return query.where(target.date >= Entry.as_value(start))
        elif stop:
            return query.where(target.date <= Entry.as_value(stop))

        return query

    @contextmanager
    def cursor(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        cursor = self.db.cursor()

        try:
            yield cursor
        except RuntimeError as e:
            raise e
        finally:
            cursor.close()

    @contextmanager
    def transaction(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        with self.cursor() as cursor:
            cursor.begin()
            yield cursor
            cursor.commit()
