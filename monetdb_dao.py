__author__ = 'Dmitry Ustalov'

import logging
from calendar import timegm
from contextlib import contextmanager
from datetime import datetime, date
from ipaddress import ip_address, IPv4Address, IPv6Address
from typing import Generator, NamedTuple, Optional, List, Sequence, Union, Any, NewType, Tuple, Deque, Set, cast

import monetdblite
from monetdblite.monetize import monet_identifier_escape
from pypika import Query, Column, Field, Table, Order, functions as fn, analytics as an

smallint = NewType('smallint', int)

TYPES = {
    datetime: 'BIGINT',
    date: 'INT',
    str: 'TEXT',
    smallint: 'SMALLINT',
    int: 'INT',
    float: 'DOUBLE',
    IPv4Address: 'TEXT',
    IPv6Address: 'TEXT',
    bool: 'BOOL'
}


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

    if first_type == datetime:
        return datetime.utcfromtimestamp(value)

    if first_type == date:
        return date.fromordinal(value)

    if first_type == smallint:
        return int(value)

    if first_type in (IPv4Address, IPv6Address):
        return ip_address(value)

    return None if not value and null else first_type(value)


class Entry(NamedTuple):
    datetime: datetime
    date: date
    host: str
    method: str
    path: str
    status: smallint
    length: int
    generation_time: Optional[float]
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
    def as_value(value: Any) -> Any:
        if isinstance(value, datetime):
            return timegm(cast(datetime, value).utctimetuple())

        if isinstance(value, date):
            return cast(date, value).toordinal()

        if isinstance(value, (IPv4Address, IPv6Address)):
            return str(value)

        return value

    def as_values(self) -> Tuple:
        return tuple(self.as_value(getattr(self, name)) for name in self.__annotations__)


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

    def schema_exists(self) -> bool:
        schemas = Table('schemas', schema='sys')

        query = Query.from_(schemas).select(schemas.name). \
            where(schemas.name == self.schema)

        logging.debug(query)

        result = self.db.execute(str(query))

        return result['name'].size > 0

    def create_schema(self):
        query = f'CREATE SCHEMA {monet_identifier_escape(self.schema)};'

        logging.debug(query)

        with self.cursor() as cursor:
            result = cursor.execute(query)
            cursor.commit()
            return result

    def tables(self) -> Sequence[str]:
        schemas = Table('schemas', schema='sys')
        tables = Table('tables', schema='sys')

        query = Query.from_(tables).select(tables.name). \
            join(schemas).on(schemas.id == tables.schema_id). \
            where(schemas.name == self.schema)

        logging.debug(query)

        result = self.db.execute(str(query))

        return result['name'].tolist()

    def table_exists(self, table: str) -> bool:
        schemas = Table('schemas', schema='sys')
        tables = Table('tables', schema='sys')

        query = Query.from_(tables).select(tables.name). \
            join(schemas).on(schemas.id == tables.schema_id). \
            where((schemas.name == self.schema) & (tables.name == table))

        logging.debug(query)

        result = self.db.execute(str(query))

        return result['name'].size > 0

    def create_table(self, table: str):
        target = Table(table, schema=self.schema)

        columns = [Column(name, python_type_to_sql(annotation)) for name, annotation in Entry.__annotations__.items()]

        query = Query.create_table(target).columns(*columns)

        logging.debug(query)

        with self.cursor() as cursor:
            result = cursor.execute(str(query))
            cursor.commit()
            return result

    def insert_into(self, table: str, entry: Entry, cursor: Optional[monetdblite.cursors.Cursor] = None) -> int:
        target = Table(table, schema=self.schema)

        query = Query.into(target).insert(*entry.as_values())

        logging.debug(query)

        if cursor:
            return cursor.execute(str(query), discard_previous=False)
        else:
            with self.cursor() as cursor:
                result = cursor.execute(str(query))

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

    def batch_insert_into_from_deque(self, table: str, entries: Deque[Entry]) -> int:
        count = 0

        if entries:
            with self.cursor() as cursor:
                while entries:
                    entry = entries.popleft()
                    self.insert_into(table, entry, cursor=cursor)
                    count += 1

                cursor.commit()

        return count

    def select(self, table: str, start: Optional[date] = None, stop: Optional[date] = None,
               limit: Optional[int] = None) -> List[Entry]:
        target = Table(table, schema=self.schema)

        query = Query.from_(target).select('*').orderby(target.date).limit(limit)

        query = self.apply_dates(query, target, start, stop)

        logging.debug(query)

        with self.cursor() as cursor:
            cursor.execute(str(query))

            results = []

            while True:
                current = cursor.fetchone()

                if not current:
                    break

                results.append(Entry.from_values(current))

            return results

    def select_average(self, table: str, field: str, start: date = None, stop: date = None) -> AverageResult:
        target = Table(table, schema=self.schema)
        target_field = Field(field, table=target)

        query = Query.from_(target).select(target.date,
                                           fn.Avg(target_field, alias='average'),
                                           fn.Sum(target_field, alias='sum'),
                                           fn.Count(target_field, alias='count')). \
            groupby(target.date).orderby(target.date)

        query = self.apply_dates(query, target, start, stop)

        logging.debug(query)

        with self.cursor() as cursor:
            cursor.execute(str(query))

            result = AverageResult(table=table, field=field, elements=[])

            for current in cursor:
                current_date = date.fromordinal(current[0])

                result.elements.append(Average(
                    date=current_date,
                    avg=float(current[1]),
                    sum=float(current[2]) if current[3] else 0.,
                    count=int(current[3])
                ))

            return result

    def select_count(self, table: str, field: Optional[str] = None, start: Optional[date] = None,
                     stop: Optional[date] = None) -> CountResult:
        target = Table(table, schema=self.schema)
        count_field = fn.Count(Field(field, table=target) if field else target.date, alias='count')

        if field:
            count_field = count_field.distinct()

        query = Query.from_(target).select(target.date, count_field).groupby(target.date).orderby(target.date)

        query = self.apply_dates(query, target, start, stop)

        logging.debug(query)

        with self.cursor() as cursor:
            cursor.execute(str(query))

            result = CountResult(table=table, field=field, distinct=field is not None, group=None, ascending=None,
                                 elements=[])

            for current in cursor:
                result.elements.append(Count(
                    date=date.fromordinal(current[0]),
                    group=None,
                    count=int(current[1])
                ))

            return result

    def select_count_group(self, table: str, field: Optional[str], group: str, distinct: bool = False,
                           start: Optional[date] = None, stop: Optional[date] = None,
                           ascending: bool = True, limit: Optional[int] = None):
        target = Table(table, schema=self.schema)
        count_field = fn.Count(Field(field, table=target) if field else target.date, alias='count')

        if distinct:
            count_field = count_field.distinct()

        group_field = Field(group, table=target)

        query = Query.from_(target).select(target.date, group_field.as_('group'), count_field). \
            groupby(target.date, group_field).orderby(target.date). \
            orderby(count_field, order=Order.asc if ascending else Order.desc)

        query = self.apply_dates(query, target, start, stop)

        if limit is not None:
            window = Query.from_(query).select(query.date, query.group, query.count,
                                               an.RowNumber(alias='row_number').over(query.date))

            query = Query.from_(window).select(window.date, window.group, window.count). \
                where(window.row_number <= limit)

        logging.debug(query)

        with self.cursor() as cursor:
            cursor.execute(str(query))

            result = CountResult(table=table, field=field, distinct=distinct, group=group, ascending=ascending,
                                 elements=[])

            for current in cursor:
                result.elements.append(Count(
                    date=date.fromordinal(current[0]),
                    group=current[1],
                    count=int(current[2])
                ))

            return result

    @staticmethod
    def apply_dates(query: Query, target: Table, start: Optional[date] = None, stop: Optional[date] = None) -> Query:
        if start and stop:
            return query.where(target.date[Entry.as_value(start):Entry.as_value(stop)])
        elif start:
            return query.where(target.date >= Entry.as_value(start))
        elif stop:
            return query.where(target.date <= Entry.as_value(stop))

        return query

    @contextmanager
    def cursor(self) -> Generator[monetdblite.cursors.Cursor, None, None]:
        cursor = self.db.cursor()

        try:
            yield cursor
        except monetdblite.exceptions.DatabaseError as e:
            cursor.rollback()
            raise e
        finally:
            cursor.close()
