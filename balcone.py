#!/usr/bin/env python3

__author__ = 'Dmitry Ustalov'

import asyncio
import logging
import re
import sys
from collections import deque
from datetime import date, datetime, timedelta
from ipaddress import ip_address, IPv4Address, IPv6Address
from typing import cast, Tuple, Union, Optional, Dict, Deque, Any

import aiohttp_jinja2
import httpagentparser
import jinja2
import monetdblite
import simplejson
from aiohttp import web
from geolite2 import geolite2, maxminddb

from monetdb_dao import MonetDAO, Entry, AverageResult, CountResult

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


class BalconeJSONEncoder(simplejson.JSONEncoder):
    def default(self, obj: Any) -> str:
        if isinstance(obj, date):
            return obj.isoformat()

        if isinstance(obj, IPv4Address) or isinstance(obj, IPv6Address):
            return str(obj)

        return super().default(obj)


class Balcone:
    N = 10
    DELAY = 5

    def __init__(self, dao: MonetDAO, geoip: maxminddb.reader.Reader):
        self.dao = dao
        self.geoip = geoip
        self.queue: Dict[str, Deque[Entry]] = {}

    async def persist_timer(self):
        while await asyncio.sleep(self.DELAY, result=True):
            self.persist()

    def persist(self):
        for service, queue in self.queue.items():
            count = self.dao.batch_insert_into(service, queue)

            if count:
                logging.debug('Inserted {:d} entries for service {}'.format(count, service))

            queue.clear()

    def time(self, service: str, start: date, stop: date) -> AverageResult:
        return self.dao.select_average(service, 'generation_time', start, stop)

    def bytes(self, service: str, start: date, stop: date) -> AverageResult:
        return self.dao.select_average(service, 'length', start, stop)

    def os(self, service: str, start: date, stop: date, n: Optional[int]) -> CountResult:
        return self.dao.select_count(service, 'ip', ascending=False, group='platform_name',
                                     limit=self.unwrap_n(n), start=start, stop=stop)

    def browser(self, service: str, start: date, stop: date, n: Optional[int]) -> CountResult:
        return self.dao.select_count(service, 'ip', ascending=False, group='browser_name',
                                     limit=self.unwrap_n(n), start=start, stop=stop)

    def uri(self, service: str, start: date, stop: date, n: Optional[int]) -> CountResult:
        return self.dao.select_count(service, 'ip', ascending=False, group='path',
                                     limit=self.unwrap_n(n), start=start, stop=stop)

    def ip(self, service: str, start: date, stop: date, n: Optional[int]) -> CountResult:
        return self.dao.select_count(service, 'status', ascending=False, group='ip',
                                     limit=self.unwrap_n(n), start=start, stop=stop)

    def country(self, service: str, start: date, stop: date, n: Optional[int]) -> CountResult:
        return self.dao.select_count(service, 'ip', ascending=False, group='country_iso_code',
                                     limit=self.unwrap_n(n), start=start, stop=stop)

    def visits(self, service: str, start: date, stop: date) -> CountResult:
        return self.dao.select_count(service, 'ip', ascending=False, start=start, stop=stop)

    def unique(self, service: str, start: date, stop: date) -> CountResult:
        return self.dao.select_count(service, 'ip', distinct=True, ascending=False, start=start, stop=stop)

    @staticmethod
    def unwrap_n(n: Optional[int]) -> int:
        return n if n else Balcone.N

    @staticmethod
    def iso_code(geoip: maxminddb.reader.Reader, ip: str) -> str:
        geo = geoip.get(ip)

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


# nginx's output cannot be properly parsed by any parser I tried
NGINX_SYSLOG = re.compile(r'\A\<[0-9]{1,3}\>.*?: (?P<message>.+)\Z')

VALID_SERVICE = re.compile(r'\A[\S]+\Z')


class SyslogProtocol(asyncio.DatagramProtocol):
    def __init__(self, balcone: Balcone):
        super().__init__()
        self.balcone = balcone
        self.transport: Optional[asyncio.BaseTransport] = None

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
            content = simplejson.loads(match.group('message'))
        except simplejson.JSONDecodeError:
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

        if service not in self.balcone.queue:
            if not self.balcone.dao.table_exists(service):
                self.balcone.dao.create_table(service)

            self.balcone.queue[service] = deque()

        current_datetime = datetime.utcnow()
        current_date = current_datetime.date()

        user_agent = httpagentparser.detect(content['http_user_agent'])

        entry = Entry(
            datetime=current_datetime,
            date=current_date,
            host=content['host'],
            method=content['request_method'],
            path=content['uri'],
            status=int(content['status']),
            length=int(content['body_bytes_sent']),
            generation_time=float(content['request_time']),
            referer=content['http_referrer'],
            ip=ip_address(content['remote_addr']),
            country_iso_code=Balcone.iso_code(self.balcone.geoip, content['remote_addr']),
            platform_name=user_agent.get('platform', {}).get('name', None),
            platform_version=user_agent.get('platform', {}).get('version', None),
            browser_name=user_agent.get('browser', {}).get('name', None),
            browser_version=user_agent.get('browser', {}).get('version', None),
            is_robot=user_agent.get('bot', None)
        )

        self.balcone.queue[service].append(entry)


HELLO_FORMAT = re.compile(r'\A(?P<command>[^\s]+?) (?P<service>[^\s]+?)(| (?P<parameter>[^\s]+))\n\Z')


class DebugProtocol(asyncio.Protocol):
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
            response = str(self.balcone.visits(service, start, stop))

        if command == 'unique':
            response = str(self.balcone.unique(service, start, stop))

        if response:
            self.transport.write(response.encode('utf-8'))
            self.transport.write(b'\n')

        self.transport.close()


class HTTPHandler:
    def __init__(self, balcone: Balcone, encoder: simplejson.JSONEncoder):
        self.balcone = balcone
        self.encoder = encoder

    @aiohttp_jinja2.template('home.html')
    async def home(self, request: web.Request):
        pass

    async def query(self, request: web.Request):
        service, command = request.match_info['service'], request.match_info['query']

        stop = datetime.utcnow().date()
        start = stop - timedelta(days=7)

        parameter = request.query.get('parameter', None)

        response: Optional[Union[AverageResult, CountResult]] = None

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
            response = self.balcone.visits(service, start, stop)

        if command == 'unique':
            response = self.balcone.unique(service, start, stop)

        return web.json_response(response, dumps=self.encoder.encode)


def main():
    dao = MonetDAO(monetdblite.make_connection('monetdb'), 'balcone')

    if not dao.schema_exists():
        dao.create_schema()

    geoip = geolite2.reader()

    balcone = Balcone(dao, geoip)

    asyncio.ensure_future(balcone.persist_timer())

    loop = asyncio.get_event_loop()

    syslog = loop.create_datagram_endpoint(lambda: SyslogProtocol(balcone), local_addr=('127.0.0.1', 65140))
    loop.run_until_complete(syslog)

    debug = loop.create_server(lambda: DebugProtocol(balcone), host='127.0.0.1', port=8888)
    loop.run_until_complete(debug)

    app = web.Application()
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader('templates'))
    handler = HTTPHandler(balcone, BalconeJSONEncoder())
    app.router.add_get('/', handler.home)
    app.router.add_get('/{service}/{query}', handler.query)
    web.run_app(app, host='127.0.0.1', port=8080)

    try:
        loop.run_forever()
    finally:
        for task in asyncio.Task.all_tasks():
            task.cancel()

            with asyncio.suppress(asyncio.CancelledError):
                loop.run_until_complete(task)

        geoip.close()

        try:
            balcone.persist()
        finally:
            dao.close()


if __name__ == '__main__':
    sys.exit(main())
