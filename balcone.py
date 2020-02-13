#!/usr/bin/env python3

__author__ = 'Dmitry Ustalov'

import asyncio
import json
import re
import statistics
import sys
import uuid
from array import array
from calendar import timegm
from collections import namedtuple, Counter, defaultdict
from datetime import date, datetime, timedelta
from itertools import groupby
from json import JSONDecodeError
from operator import itemgetter
from time import gmtime
from typing import Type, Union, Callable

# noinspection PyUnresolvedReferences
import capnp
import httpagentparser
import plyvel
from geolite2 import geolite2
from record_capnp import Record

Entry = namedtuple('Entry', 'field ftype')


def cast_ftype(ftype):
    if 'int' in ftype:
        return int
    if 'float' in ftype:
        return float
    return str


# XXX: Is there a better way for retrieving field types and $nginx annotation?
ENTRIES = {field.name: Entry(field.annotations[0].value.text if len(field.annotations) > 0 else None,
                             cast_ftype(next(iter(field.slot.type.to_dict()))))
           for field in Record.schema.node.struct.fields}

DateRecord = namedtuple('DateRecord', 'date record')
Average = namedtuple('Average', 'count mean median')


class DBdict(defaultdict):
    def __init__(self, db: plyvel.DB):
        super().__init__()
        self.db = db

    def __missing__(self, key: str):
        self[key] = self.db.prefixed_db(b'%b\t' % key.encode('utf-8'))
        return self[key]

    def count(self, service: str, field: Type[Union[str, Callable, type(None)]], start: date, stop: date):
        db = self[service]

        result = {}

        if callable(field):
            def _getattr(r, _):
                return field(r)
        elif isinstance(field, str):
            _getattr = getattr
        else:
            # field=None means COUNT(*), where None denotes the *
            _getattr = lambda *x: None

        for current, group in groupby(self.traverse(db, start, stop), key=itemgetter(0)):
            result[current] = Counter()

            for _, record in group:
                result[current][_getattr(record, field)] += 1

        return result

    def average(self, service: str, field: str, start: date, stop: date):
        db = self[service]

        result = {}

        for current, group in groupby(self.traverse(db, start, stop), key=itemgetter(0)):
            values = array('f', (getattr(record, field) for _, record in group))

            result[current] = Average(len(values), statistics.mean(values), statistics.median(values))

        return result

    @staticmethod
    def traverse(db: plyvel.DB, start: date, stop: date, include_value=True):
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


def isint(str: str):
    if not str:
        return False

    try:
        value = int(str)
        return value != 0
    except ValueError:
        return False


def isfloat(str: str):
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
    def __init__(self, db: DBdict):
        super().__init__()
        self.db = db

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data: bytes, addr):
        try:
            data = data.decode('utf-8')
        except UnicodeDecodeError as e:
            print(e, file=sys.stderr)
            return

        match = NGINX_SYSLOG.match(data)

        if not match or not match.group('message'):
            print('Malformed data: %s' % data, file=sys.stderr)
            return

        try:
            content = json.loads(match.group('message'))
        except JSONDecodeError as e:
            print(e, file=sys.stderr)
            return

        if 'service' not in content or not content['service']:
            print('Missing the service field', file=sys.stderr)
            return
        else:
            service = content['service'].strip().lower()

        if not VALID_SERVICE.match(service):
            print('Bad service: %s' % service, file=sys.stderr)
            return

        db = self.db[service]

        timestamp = round(datetime.utcnow().timestamp() * 1000)
        request_id = uuid.uuid4()

        key = b'%d\t%b' % (timestamp, request_id.bytes)

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

        db.put(key, record.to_bytes_packed())

        print((request_id.hex, record))


HELLO_FORMAT = re.compile(r'\A(?P<command>[^\s]+?) (?P<service>[^\s]+?)(| (?P<parameter>[^\s]+))\n\Z')


class HelloProtocol(asyncio.Protocol):
    def __init__(self, db: DBdict, reader):
        super().__init__()
        self.db = db
        self.reader = reader

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data: bytes):
        try:
            message = data.decode('ascii')
        except UnicodeDecodeError as e:
            print(e, file=sys.stderr)
            self.transport.close()
            return

        if not message:
            self.transport.close()
            return

        match = HELLO_FORMAT.match(message)

        if not match:
            return

        command, service, parameter = match.group('command'), match.group('service'), match.group('parameter')
        print((command, service, parameter))

        if not service or not VALID_SERVICE.match(service) or not command:
            self.transport.close()
            return

        stop = datetime.utcnow().date()
        start = stop - timedelta(days=7)

        response = None

        if command == 'time':
            response = str(self.db.average(service, 'time', start, stop))

        if command == 'bytes':
            response = str(self.db.average(service, 'body', start, stop))

        if command == 'os':
            result = self.db.count(service, lambda r: httpagentparser.simple_detect(r.userAgent)[0], start, stop)

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = str({d: counter.most_common(count) for d, counter in result.items()})

        if command == 'browser':
            result = self.db.count(service, lambda r: httpagentparser.simple_detect(r.userAgent)[1], start, stop)

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = str({d: counter.most_common(count) for d, counter in result.items()})

        if command == 'uri':
            result = self.db.count(service, 'uri', start, stop)

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = str({d: counter.most_common(count) for d, counter in result.items()})

        if command == 'ip':
            result = self.db.count(service, 'ip', start, stop)

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = str({d: counter.most_common(count) for d, counter in result.items()})

        if command == 'country':
            def iso_code(record):
                geo = self.reader.get(record.remote)

                if geo and 'country' in geo:
                    return geo['country'].get('iso_code', 'UNKNOWN')
                else:
                    return 'UNKNOWN'

            result = self.db.count(service, iso_code, start, stop)

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = str({d: counter.most_common(count) for d, counter in result.items()})

        if command == 'visits':
            result = self.db.count(service, None, start, stop)

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = str({d: counter.most_common(count) for d, counter in result.items()})

        if command == 'unique':
            result = self.db.count(service, lambda r: r.remote, start, stop)

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = str({d: counter.most_common(count) for d, counter in result.items()})

        if response:
            self.transport.write(response.encode('utf-8'))
            self.transport.write(b'\n')

        self.transport.close()


def main():
    db_root = plyvel.DB('db', create_if_missing=True)
    db = DBdict(db_root)

    reader = geolite2.reader()

    loop = asyncio.get_event_loop()

    syslog = loop.create_datagram_endpoint(lambda: SyslogProtocol(db), local_addr=('127.0.0.1', 65140))
    loop.run_until_complete(syslog)

    hello = loop.create_server(lambda: HelloProtocol(db, reader), host='127.0.0.1', port=8888)
    loop.run_until_complete(hello)

    loop.run_forever()

    db_root.close()


if __name__ == '__main__':
    sys.exit(main())
