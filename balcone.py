#!/usr/bin/env python3

__author__ = 'Dmitry Ustalov'

import asyncio
import json
import re
import statistics
import sys
import uuid
from array import array
from collections import namedtuple, Counter, defaultdict
from datetime import datetime, timedelta
from json import JSONDecodeError

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


class DBdict(defaultdict):
    def __init__(self, db: plyvel.DB):
        super().__init__()
        self.db = db

    def __missing__(self, key: str):
        self[key] = self.db.prefixed_db(b'%b\t' % key.encode('utf-8'))
        return self[key]


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

        db = self.db[service]

        response = None

        if command == 'time':
            values = array('f', (Record.from_bytes_packed(value).time
                                 for value in db.iterator(include_key=False)))

            count, mean, median = len(values), statistics.mean(values), statistics.median(values)

            response = 'count={:d}\tmean={:.2f}\tmedian={:.2f}'.format(count, mean, median)

        if command == 'bytes':
            values = array('f', (Record.from_bytes_packed(value).body
                                 for value in db.iterator(include_key=False)))

            count, mean, median = len(values), statistics.mean(values), statistics.median(values)

            response = 'count={:d}\tmean={:.2f}\tmedian={:.2f}'.format(count, mean, median)

        if command == 'os':
            values = Counter()

            for value in db.iterator(include_key=False):
                record = Record.from_bytes_packed(value)

                os, _ = httpagentparser.simple_detect(record.userAgent)

                values[os] += 1

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = '\n'.join(['{}: {:d}'.format(os, found) for os, found in values.most_common(count)])

        if command == 'browser':
            values = Counter()

            for value in db.iterator(include_key=False):
                record = Record.from_bytes_packed(value)

                _, browser = httpagentparser.simple_detect(record.userAgent)

                values[browser] += 1

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = '\n'.join(['{}: {:d}'.format(browser, found) for browser, found in values.most_common(count)])

        if command == 'uri':
            values = Counter()

            for value in db.iterator(include_key=False):
                record = Record.from_bytes_packed(value)

                values[record.uri] += 1

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = '\n'.join(['{}: {:d}'.format(uri, found) for uri, found in values.most_common(count)])

        if command == 'ip':
            values = Counter()

            for value in db.iterator(include_key=False):
                record = Record.from_bytes_packed(value)

                values[record.remote] += 1

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = '\n'.join(['{}: {:d}'.format(uri, found) for uri, found in values.most_common(count)])

        if command == 'country':
            values = Counter()

            for value in db.iterator(include_key=False):
                record = Record.from_bytes_packed(value)

                geo = self.reader.get(record.remote)

                if geo and 'country' in geo and 'iso_code' in geo['country']:
                    values[geo['country']['iso_code']] += 1
                else:
                    values['UNKNOWN'] += 1

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = '\n'.join(['{}: {:d}'.format(uri, found) for uri, found in values.most_common(count)])

        if command == 'visits':
            if isint(parameter):
                delta = int(parameter)
            else:
                delta = 7

            present = datetime.utcnow()
            past = present - timedelta(days=delta)

            past_ts, present_ts = round(past.timestamp() * 1000), round(present.timestamp() * 1000)
            past_key, present_key = b'%d' % past_ts, b'%d' % (present_ts + 1)

            visits = Counter()

            for key in db.iterator(start=past_key, stop=present_key, include_value=False):
                timestamp, _, _ = key.partition(b'\t')

                if not isint(timestamp):
                    continue

                date = datetime.utcfromtimestamp(int(timestamp) / 1000)

                visits[date.strftime('%Y-%m-%d')] += 1

            response = '\n'.join(['{}: {:d}'.format(*pair) for pair in visits.items()])

        if command == 'unique':
            if isint(parameter):
                delta = int(parameter)
            else:
                delta = 7

            present = datetime.utcnow()
            past = present - timedelta(days=delta)

            past_ts, present_ts = round(past.timestamp() * 1000), round(present.timestamp() * 1000)
            past_key, present_key = b'%d' % past_ts, b'%d' % (present_ts + 1)

            unique = defaultdict(set)

            for key, value in db.iterator(start=past_key, stop=present_key):
                timestamp, _, _ = key.partition(b'\t')

                if not isint(timestamp):
                    continue

                date = datetime.utcfromtimestamp(int(timestamp) / 1000)

                record = Record.from_bytes_packed(value)

                unique[date.strftime('%Y-%m-%d')].add(record.remote)

            response = '\n'.join(['{}: {:d}'.format(date, len(ips)) for date, ips in unique.items()])

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
