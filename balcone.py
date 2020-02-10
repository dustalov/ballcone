#!/usr/bin/env python3

__author__ = 'Dmitry Ustalov'

import asyncio
import json
import re
import statistics
import sys
import uuid
from collections import namedtuple, Counter, defaultdict
from datetime import datetime, timedelta
from json import JSONDecodeError

import httpagentparser
import plyvel
from geolite2 import geolite2

DB_PREFIXES = (
    'args', 'body_bytes_sent', 'content_type', 'content_length', 'host', 'http_referrer',
    'http_user_agent_os', 'http_user_agent_browser', 'http_x_forwarded_for', 'remote_addr', 'request_method',
    'request_time', 'status', 'upstream_addr', 'uri'
)

DB = namedtuple('DB', DB_PREFIXES)


def db_wrapper(db):
    return DB._make([
        db.prefixed_db((prefix + '\t').encode('ascii')) for prefix in DB_PREFIXES
    ])


class DBdefaultdict(defaultdict):
    def __init__(self, db):
        super().__init__()
        self.db = db

    def __missing__(self, key):
        self[key] = db_wrapper(self.db.prefixed_db((key + '\t').encode('ascii')))
        return self[key]


def isint(str):
    if not str:
        return False

    try:
        value = int(str)
        return value != 0
    except ValueError:
        return False


def isfloat(str):
    if not str:
        return False

    try:
        value = float(str)
        return value != 0.
    except ValueError:
        return False


# nginx's output cannot be properly parsed by any parser I tried
NGINX_SYSLOG = re.compile(r'\A\<[0-9]{1,3}\>.*?: (?P<message>.+)\Z')


class SyslogProtocol(asyncio.DatagramProtocol):
    def __init__(self, db):
        super().__init__()
        self.db = db

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        match = NGINX_SYSLOG.match(data.decode('utf-8'))

        if not match or not match.group('message'):
            print(('bad', data), file=sys.stderr)
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

        db = self.db[service]

        timestamp = round(datetime.utcnow().timestamp() * 1000)
        request_id = uuid.uuid4().hex

        key = ('%d\t%s' % (timestamp, request_id)).encode('ascii')

        if 'args' in content and content['args']:
            db.args.put(key, content['args'].encode('utf-8'))

        if 'body_bytes_sent' in content and isint(content['body_bytes_sent']):
            db.body_bytes_sent.put(key, content['body_bytes_sent'].encode('utf-8'))

        if 'content_type' in content and content['content_type']:
            db.content_type.put(key, content['content_type'].encode('utf-8'))

        if 'content_length' in content and isint(content['content_length']):
            db.content_length.put(key, content['content_length'].encode('utf-8'))

        if 'host' in content and content['host']:
            db.host.put(key, content['host'].encode('utf-8'))

        if 'http_referrer' in content and content['http_referrer']:
            db.http_referrer.put(key, content['http_referrer'].encode('utf-8'))

        if 'http_user_agent' in content and content['http_user_agent']:
            os, browser = httpagentparser.simple_detect(content['http_user_agent'])

            if os:
                db.http_user_agent_os.put(key, os.encode('utf-8'))

            if browser:
                db.http_user_agent_browser.put(key, browser.encode('utf-8'))

        if 'http_x_forwarded_for' in content and content['http_x_forwarded_for']:
            db.http_x_forwarded_for.put(key, content['http_x_forwarded_for'].encode('utf-8'))

        if 'remote_addr' in content and content['remote_addr']:
            db.remote_addr.put(key, content['remote_addr'].encode('utf-8'))

        if 'request_method' in content and content['request_method']:
            db.request_method.put(key, content['request_method'].encode('utf-8'))

        if 'request_time' in content and isfloat(content['request_time']):
            db.request_time.put(key, content['request_time'].encode('utf-8'))

        if 'status' in content and isint(content['status']):
            db.status.put(key, content['status'].encode('utf-8'))

        if 'upstream_addr' in content and content['upstream_addr']:
            db.upstream_addr.put(key, content['upstream_addr'].encode('utf-8'))

        if 'uri' in content and content['uri']:
            db.uri.put(key, content['uri'].encode('utf-8'))

        print((request_id, content))


HELLO_FORMAT = re.compile(r'\A(?P<command>[^\s]+?) (?P<service>[^\s]+?)(| (?P<parameter>[^\s]+))\n\Z')


class HelloProtocol(asyncio.Protocol):
    def __init__(self, db, reader):
        super().__init__()
        self.db = db
        self.reader = reader

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        try:
            message = data.decode('ascii')
        except UnicodeDecodeError as e:
            print(e)
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

        if not service or not command:
            self.transport.close()
            return

        db = self.db[service]

        response = None

        if command == 'time':
            values = []

            for value in db.request_time.iterator(include_key=False):
                values.append(float(value))

            count, mean, median = len(values), statistics.mean(values), statistics.median(values)

            response = 'count={:d}\tmean={:.2f}\tmedian={:.2f}'.format(count, mean, median)

        if command == 'bytes':
            values = []

            for value in db.body_bytes_sent.iterator(include_key=False):
                values.append(float(value))

            count, mean, median = len(values), statistics.mean(values), statistics.median(values)

            response = 'count={:d}\tmean={:.2f}\tmedian={:.2f}'.format(count, mean, median)

        if command == 'os':
            values = Counter()

            for value in db.http_user_agent_os.iterator(include_key=False):
                values[value.decode('utf-8')] += 1

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = '\n'.join(['{}: {:d}'.format(os, found) for os, found in values.most_common(count)])

        if command == 'browser':
            values = Counter()

            for value in db.http_user_agent_browser.iterator(include_key=False):
                values[value.decode('utf-8')] += 1

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = '\n'.join(['{}: {:d}'.format(browser, found) for browser, found in values.most_common(count)])

        if command == 'uri':
            values = Counter()

            for value in db.uri.iterator(include_key=False):
                values[value.decode('utf-8')] += 1

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = '\n'.join(['{}: {:d}'.format(uri, found) for uri, found in values.most_common(count)])

        if command == 'ip':
            values = Counter()

            for value in db.remote_addr.iterator(include_key=False):
                values[value.decode('utf-8')] += 1

            if isint(parameter):
                count = int(parameter)
            else:
                count = 10

            response = '\n'.join(['{}: {:d}'.format(uri, found) for uri, found in values.most_common(count)])

        if command == 'country':
            values = Counter()

            for value in db.remote_addr.iterator(include_key=False):
                record = self.reader.get(value.decode('utf-8'))

                if record and 'country' in record and 'iso_code' in record['country']:
                    values[record['country']['iso_code']] += 1
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
            past_key = ('%d' % past_ts).encode('ascii')
            present_key = ('%d\tz' % present_ts).encode('ascii')

            visits = Counter()

            for key in db.uri.iterator(start=past_key, stop=present_key, include_value=False):
                timestamp, _, _ = key.decode('ascii').partition('\t')

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
            past_key = ('%d' % past_ts).encode('ascii')
            present_key = ('%d\tz' % present_ts).encode('ascii')

            unique = defaultdict(set)

            for key, value in db.remote_addr.iterator(start=past_key, stop=present_key):
                timestamp, _, _ = key.decode('ascii').partition('\t')

                if not isint(timestamp):
                    continue

                date = datetime.utcfromtimestamp(int(timestamp) / 1000)

                unique[date.strftime('%Y-%m-%d')].add(value.decode('utf-8'))

            response = '\n'.join(['{}: {:d}'.format(date, len(ips)) for date, ips in unique.items()])

        if response:
            self.transport.write(response.encode('utf-8'))
            self.transport.write(b'\n')

        self.transport.close()


def main():
    db_root = plyvel.DB('access', create_if_missing=True)
    db = DBdefaultdict(db_root)

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
