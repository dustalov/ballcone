#!/usr/bin/env python3

__author__ = 'Dmitry Ustalov'
__version__ = 0

import asyncio
import logging
import re
import sys
from collections import OrderedDict, deque
from contextlib import suppress
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

from monetdb_dao import MonetDAO, Entry, AverageResult, CountResult, smallint

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


class BalconeJSONEncoder(simplejson.JSONEncoder):
    def default(self, obj: Any) -> str:
        if isinstance(obj, date):
            return obj.isoformat()

        if isinstance(obj, (IPv4Address, IPv6Address)):
            return str(obj)

        return super().default(obj)


class Balcone:
    N = 5
    DELAY = 5

    def __init__(self, dao: MonetDAO, geoip: maxminddb.reader.Reader):
        self.dao = dao
        self.geoip = geoip
        self.queue: Dict[str, Deque[Entry]] = {}
        self.json_dumps = BalconeJSONEncoder().encode

    async def persist_timer(self):
        while await asyncio.sleep(self.DELAY, result=True):
            self.persist()

    def persist(self):
        for service, queue in self.queue.items():
            count = self.dao.batch_insert_into_from_deque(service, queue)

            if count:
                logging.debug(f'Inserted {count} entries for service {service}')

    def check_service(self, service: str) -> bool:
        return VALID_SERVICE.match(service) is not None and self.dao.table_exists(service)

    def time(self, service: str, start: date, stop: date) -> AverageResult:
        return self.dao.select_average(service, 'generation_time', start, stop)

    def bytes(self, service: str, start: date, stop: date) -> AverageResult:
        return self.dao.select_average(service, 'length', start, stop)

    def os(self, service: str, start: date, stop: date,
           distinct: bool = False, limit: Optional[int] = None) -> CountResult:
        return self.dao.select_count_group(service, 'ip', 'platform_name', distinct=distinct,
                                           start=start, stop=stop, ascending=False, limit=limit)

    def browser(self, service: str, start: date, stop: date,
                distinct: bool = False, limit: Optional[int] = None) -> CountResult:
        return self.dao.select_count_group(service, 'ip', 'browser_name', distinct=distinct,
                                           start=start, stop=stop, ascending=False, limit=limit)

    def uri(self, service: str, start: date, stop: date,
            distinct: bool = False, limit: Optional[int] = None) -> CountResult:
        return self.dao.select_count_group(service, 'ip', 'path', distinct=distinct,
                                           start=start, stop=stop, ascending=False, limit=limit)

    def ip(self, service: str, start: date, stop: date,
           distinct: bool = False, limit: Optional[int] = None) -> CountResult:
        return self.dao.select_count_group(service, 'status', 'ip', distinct=distinct,
                                           start=start, stop=stop, ascending=False, limit=limit)

    def country(self, service: str, start: date, stop: date,
                distinct: bool = False, limit: Optional[int] = None) -> CountResult:
        return self.dao.select_count_group(service, 'ip', 'country_iso_code', distinct=distinct,
                                           start=start, stop=stop, ascending=False, limit=limit)

    def visits(self, service: str, start: date, stop: date) -> CountResult:
        return self.dao.select_count(service, start=start, stop=stop)

    def unique(self, service: str, start: date, stop: date) -> CountResult:
        return self.dao.select_count(service, 'ip', start=start, stop=stop)

    def handle_command(self, service: str, command: str, parameter: Optional[str],
                       start: date, stop: date) -> Optional[Union[AverageResult, CountResult]]:
        if command == 'time':
            return self.time(service, start, stop)

        if command == 'bytes':
            return self.bytes(service, start, stop)

        if command == 'os':
            n = self.unwrap_n(int(cast(int, parameter)) if isint(parameter) else None)
            return self.os(service, start, stop, limit=n)

        if command == 'browser':
            n = self.unwrap_n(int(cast(int, parameter)) if isint(parameter) else None)
            return self.browser(service, start, stop, limit=n)

        if command == 'uri':
            n = self.unwrap_n(int(cast(int, parameter)) if isint(parameter) else None)
            return self.uri(service, start, stop, limit=n)

        if command == 'ip':
            n = self.unwrap_n(int(cast(int, parameter)) if isint(parameter) else None)
            return self.ip(service, start, stop, limit=n)

        if command == 'country':
            n = self.unwrap_n(int(cast(int, parameter)) if isint(parameter) else None)
            return self.country(service, start, stop, limit=n)

        if command == 'visits':
            return self.visits(service, start, stop)

        if command == 'unique':
            return self.unique(service, start, stop)

        return None

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


def isint(s: Optional[str]) -> bool:
    if not s:
        return False

    try:
        value = int(s)
        return value != 0
    except ValueError:
        return False


# nginx's output cannot be properly parsed by any parser I tried
NGINX_SYSLOG = re.compile(r'\A<[0-9]{1,3}>.*?: (?P<message>.+)\Z')

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
            logging.info(f'Malformed UTF-8 received from {addr}')
            return

        match = NGINX_SYSLOG.match(message)

        if not match or not match.group('message'):
            logging.info(f'Missing payload from {addr}: {message}')
            return

        try:
            content = simplejson.loads(match.group('message'))
        except simplejson.JSONDecodeError:
            logging.info(f'Malformed JSON received from {addr}: {message}')
            return

        if 'service' not in content or not content['service']:
            logging.info(f'Missing service field from {addr}: {message}')
            return
        else:
            service = content['service'].strip().lower()

        if not VALID_SERVICE.match(service):
            logging.info(f'Malformed service field from {addr}: {message}')
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
            status=cast(smallint, int(content['status'])),
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


DEBUG_FORMAT = re.compile(r'\A(?P<command>[^\s]+?) (?P<service>[^\s]+?)(| (?P<parameter>[^\s]+))\n\Z')


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

        match = DEBUG_FORMAT.match(message)

        if not match:
            return

        command, service, parameter = match.group('command'), match.group('service'), match.group('parameter')

        logging.debug(f'Received command={command} service={service} parameter={parameter}')

        if not service or not self.balcone.check_service(service) or not command:
            self.transport.close()
            return

        stop = datetime.utcnow().date()
        start = stop - timedelta(days=7 - 1)

        response = self.balcone.handle_command(service, command, parameter, start, stop)

        if response:
            self.transport.write(self.balcone.json_dumps(response).encode('utf-8'))
            self.transport.write(b'\n')

        self.transport.close()


class HTTPHandler:
    def __init__(self, balcone: Balcone):
        self.balcone = balcone

    @aiohttp_jinja2.template('root.html')
    async def root(self, _: web.Request):
        today = datetime.utcnow().date()

        services = {}

        for service in self.balcone.dao.tables():
            count = self.balcone.visits(service, today, today)

            services[service] = count.elements[0].count if count.elements else 0

        return {'version': __version__, 'current_page': 'root', 'services': services}

    async def services(self, request: web.Request):
        raise web.HTTPFound(request.app.router['root'].url_for())

    @aiohttp_jinja2.template('service.html')
    async def service(self, request: web.Request):
        services = self.balcone.dao.tables()
        service = request.match_info.get('service')

        if not self.balcone.check_service(service):
            raise web.HTTPNotFound(text=f'No such service: {service}')

        stop = datetime.utcnow().date()
        start = stop - timedelta(days=7 - 1)

        queries = {
            'visits': self.balcone.visits(service, start, stop),
            'unique': self.balcone.unique(service, start, stop)
        }

        overview: Dict[date, Dict[str, int]] = OrderedDict()

        for query, result in queries.items():
            for element in result.elements:
                if element.date not in overview:
                    overview[element.date] = {}

                overview[element.date][query] = element.count

        time = self.balcone.time(service, start, stop)

        paths = self.balcone.uri(service, start, stop, limit=Balcone.N)

        browsers = self.balcone.browser(service, start, stop, limit=Balcone.N)

        return {
            'version': __version__,
            'services': services,
            'current_page': 'service',
            'current_service': service,
            'overview': overview,
            'time': time,
            'paths': paths,
            'browsers': browsers
        }

    async def query(self, request: web.Request):
        service, command = request.match_info['service'], request.match_info['query']

        if not self.balcone.check_service(service):
            raise web.HTTPNotFound(text=f'No such service: {service}')

        stop = datetime.utcnow().date()
        start = stop - timedelta(days=30 - 1)

        parameter = request.query.get('parameter', None)

        response = self.balcone.handle_command(service, command, parameter, start, stop)

        return web.json_response(response, dumps=self.balcone.json_dumps)

    @aiohttp_jinja2.template('sql.html')
    async def sql(self, request: web.Request):
        services = self.balcone.dao.tables()

        data = await request.post()
        sql, result, error = data.get('sql', ''), [], ''

        if sql:
            try:
                result = self.balcone.dao.run(sql)
            except monetdblite.exceptions.DatabaseError as e:
                error = str(e)

        return {
            'version': __version__,
            'current_page': 'sql',
            'title': 'SQL Console',
            'services': services,
            'sql': sql,
            'result': result,
            'error': error
        }


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
    handler = HTTPHandler(balcone)
    app.router.add_get('/', handler.root, name='root')
    app.router.add_get('/services', handler.services, name='services')
    app.router.add_get('/services/{service}', handler.service, name='service')
    app.router.add_get('/services/{service}/{query}', handler.query, name='query')
    app.router.add_get('/sql', handler.sql, name='sql')
    app.router.add_post('/sql', handler.sql, name='sql')
    web.run_app(app, host='127.0.0.1', port=8080)

    try:
        loop.run_forever()
    finally:
        for task in asyncio.Task.all_tasks():
            task.cancel()

            with suppress(asyncio.CancelledError):
                loop.run_until_complete(task)

        geoip.close()

        try:
            balcone.persist()
        finally:
            dao.close()


if __name__ == '__main__':
    sys.exit(main())
