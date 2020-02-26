#!/usr/bin/env python3

__author__ = 'Dmitry Ustalov'

import asyncio
import json
import logging
import re
import statistics
import sys
import uuid
from array import array
from calendar import timegm
from collections import defaultdict, Counter
from datetime import date, datetime, timedelta
from functools import partial
from itertools import groupby
from json import JSONDecodeError
from operator import itemgetter
from time import gmtime
from typing import cast, NamedTuple, Type, Dict, List, Tuple, Union, Optional, Callable, Generator, \
    Counter as CounterType

# noinspection PyUnresolvedReferences
import capnp
import httpagentparser
import plyvel
from aiohttp import web
from geolite2 import geolite2, maxminddb
from record_capnp import Record

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


class Entry(NamedTuple):
    field: str
    ftype: Type


class DateRecord(NamedTuple):
    date: date
    record: 'Record'


class Average(NamedTuple):
    length: int
    mean: float
    median: float


def cast_ftype(ftype: str) -> Type:
    if 'int' in ftype:
        return int
    elif 'float' in ftype:
        return float
    else:
        return str


# XXX: Is there a better way for retrieving field types and $nginx annotation?
ENTRIES = {field.name: Entry(field.annotations[0].value.text if len(field.annotations) > 0 else None,
                             cast_ftype(next(iter(field.slot.type.to_dict()))))
           for field in Record.schema.node.struct.fields}


class DBdict(defaultdict):
    def __init__(self, db: plyvel.DB):
        super().__init__()
        self.db = db

    def __missing__(self, key: str):
        self[key] = self.db.prefixed_db(b'%b\t' % key.encode('utf-8'))
        return self[key]

    def count(self, service: str, field: Optional[Union[str, Callable]],
              start: date, stop: date) -> Dict[date, CounterType[str]]:
        db = self[service]

        result: Dict[date, CounterType[str]] = {}

        if callable(field):
            def _getattr(r, _):
                return field(r)
        elif isinstance(field, str):
            _getattr = getattr
        else:
            # field=None means COUNT(*), where None denotes the *
            _getattr = lambda *x: None

        for current, record in self.traverse(db, start, stop, include_value=field is not None):
            if current not in result:
                result[current] = Counter()

            result[current][_getattr(record, field)] += 1

        return result

    def average(self, service: str, field: str, start: date, stop: date) -> Dict[date, Average]:
        db = self[service]

        result: Dict[date, Average] = {}

        for current, group in groupby(self.traverse(db, start, stop), key=itemgetter(0)):
            values = array('f', (getattr(record, field) for _, record in group))

            result[current] = Average(len(values), statistics.mean(values), statistics.median(values))

        return result

    # noinspection PyProtectedMember
    @staticmethod
    def traverse(db: plyvel._plyvel.PrefixedDB, start: date, stop: date,
                 include_value=True) -> Generator[DateRecord, None, None]:
        # We need to iterate right before the next day after stop
        stop = stop + timedelta(days=1)

        start_ts, stop_ts = timegm(start.timetuple()) * 1000, timegm(stop.timetuple()) * 1000

        start_key, stop_key = b'%d\t' % start_ts, b'%d\t' % stop_ts

        for key_or_key_value in db.iterator(start=start_key, stop=stop_key, include_value=include_value):
            if include_value:
                key, value = key_or_key_value
                record = Record.from_bytes_packed(value)
            else:
                key = key_or_key_value
                record = None

            current_ts, _, _ = key.partition(b'\t')

            current = date(*gmtime(int(current_ts) // 1000)[:3])

            yield DateRecord(current, record)


class Balcone:
    N = 10

    def __init__(self, db: DBdict, reader: maxminddb.reader.Reader):
        self.db = db
        self.reader = reader

    def put(self, service: str, record: Record) -> Tuple[int, uuid.UUID]:
        timestamp = round(datetime.utcnow().timestamp() * 1000)
        request_id = uuid.uuid4()

        key = b'%d\t%b' % (timestamp, request_id.bytes)

        self.db[service].put(key, record.to_bytes_packed())

        return timestamp, request_id

    def time(self, service: str, start: date, stop: date) -> Dict[date, Average]:
        return self.db.average(service, 'time', start, stop)

    def bytes(self, service: str, start: date, stop: date) -> Dict[date, Average]:
        return self.db.average(service, 'body', start, stop)

    def os(self, service: str, start: date, stop: date, n: Optional[int]) -> Dict[date, List[Tuple[str, int]]]:
        result = self.db.count(service, lambda r: httpagentparser.simple_detect(r.userAgent)[0], start, stop)

        return {d: counter.most_common(self.unwrap_n(n)) for d, counter in result.items()}

    def browser(self, service: str, start: date, stop: date, n: Optional[int]) -> Dict[date, List[Tuple[str, int]]]:
        result = self.db.count(service, lambda r: httpagentparser.simple_detect(r.userAgent)[1], start, stop)

        return {d: counter.most_common(self.unwrap_n(n)) for d, counter in result.items()}

    def uri(self, service: str, start: date, stop: date, n: Optional[int]) -> Dict[date, List[Tuple[str, int]]]:
        result = self.db.count(service, 'uri', start, stop)

        return {d: counter.most_common(self.unwrap_n(n)) for d, counter in result.items()}

    def ip(self, service: str, start: date, stop: date, n: Optional[int]) -> Dict[date, List[Tuple[str, int]]]:
        result = self.db.count(service, 'remote', start, stop)

        return {d: counter.most_common(self.unwrap_n(n)) for d, counter in result.items()}

    def country(self, service: str, start: date, stop: date, n: Optional[int]) -> Dict[date, List[Tuple[str, int]]]:
        iso_code_partial = partial(self.iso_code, reader=self.reader)
        result = self.db.count(service, iso_code_partial, start, stop)

        return {d: counter.most_common(self.unwrap_n(n)) for d, counter in result.items()}

    def visits(self, service: str, start: date, stop: date, n: Optional[int]) -> Dict[date, List[Tuple[str, int]]]:
        result = self.db.count(service, None, start, stop)

        return {d: counter.most_common(self.unwrap_n(n)) for d, counter in result.items()}

    def unique(self, service: str, start: date, stop: date, n: Optional[int]) -> Dict[date, List[Tuple[str, int]]]:
        result = self.db.count(service, lambda r: r.remote, start, stop)

        return {d: counter.most_common(self.unwrap_n(n)) for d, counter in result.items()}

    @staticmethod
    def unwrap_n(n: Optional[int]) -> int:
        return n if n else Balcone.N

    @staticmethod
    def iso_code(reader: maxminddb.reader.Reader, record: Record) -> str:
        geo = reader.get(record.remote)

        if geo and 'country' in geo:
            return geo['country'].get('iso_code', 'UNKNOWN')
        else:
            return 'UNKNOWN'


def isint(str: str) -> bool:
    if not str:
        return False

    try:
        value = int(str)
        return value != 0
    except ValueError:
        return False


def isfloat(str: str) -> bool:
    if not str:
        return False

    try:
        value = float(str)
        return value > 0
    except ValueError:
        return False


# nginx's output cannot be properly parsed by any parser I tried
NGINX_SYSLOG = re.compile(r'\A\<[0-9]{1,3}\>.*?: (?P<message>.+)\Z')

VALID_SERVICE = re.compile(r'\A[\S]+\Z')


class SyslogProtocol(asyncio.DatagramProtocol):
    def __init__(self, balcone: Balcone):
        super().__init__()
        self.balcone = balcone

    def connection_made(self, transport: asyncio.BaseTransport):
        self.transport = transport

    def datagram_received(self, data: Union[bytes, str], addr: Tuple[str, int]):
        try:
            message = data.decode('utf-8') if isinstance(data, bytes) else data
        except UnicodeDecodeError:
            logging.info('Malformed UTF-8 received from {}'.format(addr))
            return

        match = NGINX_SYSLOG.match(message)

        if not match or not match.group('message'):
            logging.info('Missing payload from {}: {}'.format(addr, message))
            return

        try:
            content = json.loads(match.group('message'))
        except JSONDecodeError:
            logging.info('Malformed JSON received from {}: {}'.format(addr, message))
            return

        if 'service' not in content or not content['service']:
            logging.info('Missing service field from {}: {}'.format(addr, message))
            return
        else:
            service = content['service'].strip().lower()

        if not VALID_SERVICE.match(service):
            logging.info('Malformed service field from {}: {}'.format(addr, message))
            return

        record = Record.new_message()

        for attr, (field, ftype) in ENTRIES.items():
            if field not in content:
                continue

            value = content[field]

            if not value:
                continue

            if ftype == int and not isint(value):
                continue

            if ftype == float and not isfloat(value):
                continue

            setattr(record, attr, ftype(value))

        _, request_id = self.balcone.put(service, record)

        logging.debug('Record {}: {}'.format(request_id.hex, record.to_dict()))


HELLO_FORMAT = re.compile(r'\A(?P<command>[^\s]+?) (?P<service>[^\s]+?)(| (?P<parameter>[^\s]+))\n\Z')


class HelloProtocol(asyncio.Protocol):
    def __init__(self, balcone: Balcone):
        super().__init__()
        self.balcone = balcone

    def connection_made(self, transport: asyncio.BaseTransport):
        self.transport = cast(asyncio.Transport, transport)

    def data_received(self, data: bytes):
        try:
            message = data.decode('ascii')
        except UnicodeDecodeError:
            logging.info('Malformed ASCII received')
            self.transport.close()
            return

        if not message:
            self.transport.close()
            return

        match = HELLO_FORMAT.match(message)

        if not match:
            return

        command, service, parameter = match.group('command'), match.group('service'), match.group('parameter')

        logging.debug('Received command={} service={} parameter={}'.format(command, service, parameter))

        if not service or not VALID_SERVICE.match(service) or not command:
            self.transport.close()
            return

        stop = datetime.utcnow().date()
        start = stop - timedelta(days=7)

        response = None

        if command == 'time':
            response = str(self.balcone.time(service, start, stop))

        if command == 'bytes':
            response = str(self.balcone.bytes(service, start, stop))

        if command == 'os':
            n = int(parameter) if isint(parameter) else None
            response = str(self.balcone.os(service, start, stop, n))

        if command == 'browser':
            n = int(parameter) if isint(parameter) else None
            response = str(self.balcone.browser(service, start, stop, n))

        if command == 'uri':
            n = int(parameter) if isint(parameter) else None
            response = str(self.balcone.uri(service, start, stop, n))

        if command == 'ip':
            n = int(parameter) if isint(parameter) else None
            response = str(self.balcone.ip(service, start, stop, n))

        if command == 'country':
            n = int(parameter) if isint(parameter) else None
            response = str(self.balcone.country(service, start, stop, n))

        if command == 'visits':
            n = int(parameter) if isint(parameter) else None
            response = str(self.balcone.visits(service, start, stop, n))

        if command == 'unique':
            n = int(parameter) if isint(parameter) else None
            response = str(self.balcone.unique(service, start, stop, n))

        if response:
            self.transport.write(response.encode('utf-8'))
            self.transport.write(b'\n')

        self.transport.close()


class HTTPHandler:
    def __init__(self, balcone: Balcone):
        self.balcone = balcone

    async def home(self, request: web.Request):
        return web.Response(text='Balcone')

    async def query(self, request: web.Request):
        service, command = request.match_info['service'], request.match_info['query']

        stop = datetime.utcnow().date()
        start = stop - timedelta(days=7)

        parameter = request.query.get('parameter', None)

        response: Optional[Union[Dict[date, Average], Dict[date, List[Tuple[str, int]]]]] = None

        if command == 'time':
            response = self.balcone.time(service, start, stop)

        if command == 'bytes':
            response = self.balcone.bytes(service, start, stop)

        if command == 'os':
            n = int(parameter) if isint(parameter) else None
            response = self.balcone.os(service, start, stop, n)

        if command == 'browser':
            n = int(parameter) if isint(parameter) else None
            response = self.balcone.browser(service, start, stop, n)

        if command == 'uri':
            n = int(parameter) if isint(parameter) else None
            response = self.balcone.uri(service, start, stop, n)

        if command == 'ip':
            n = int(parameter) if isint(parameter) else None
            response = self.balcone.ip(service, start, stop, n)

        if command == 'country':
            n = int(parameter) if isint(parameter) else None
            response = self.balcone.country(service, start, stop, n)

        if command == 'visits':
            n = int(parameter) if isint(parameter) else None
            response = self.balcone.visits(service, start, stop, n)

        if command == 'unique':
            n = int(parameter) if isint(parameter) else None
            response = self.balcone.unique(service, start, stop, n)

        return web.Response(text=str(response) + '\n')


def main():
    db_root = plyvel.DB('db', create_if_missing=True)
    db = DBdict(db_root)

    reader = geolite2.reader()

    balcone = Balcone(db, reader)

    loop = asyncio.get_event_loop()

    syslog = loop.create_datagram_endpoint(lambda: SyslogProtocol(balcone), local_addr=('127.0.0.1', 65140))
    loop.run_until_complete(syslog)

    hello = loop.create_server(lambda: HelloProtocol(balcone), host='127.0.0.1', port=8888)
    loop.run_until_complete(hello)

    app = web.Application()
    handler = HTTPHandler(balcone)
    app.router.add_get('/', handler.home)
    app.router.add_get('/{service}/{query}', handler.query)
    web.run_app(app, host='127.0.0.1', port=8080)

    loop.run_forever()

    reader.close()
    db_root.close()


if __name__ == '__main__':
    sys.exit(main())
