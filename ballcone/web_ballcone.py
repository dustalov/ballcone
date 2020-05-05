__author__ = 'Dmitry Ustalov'

from collections import OrderedDict
from datetime import date, datetime, timedelta
from ipaddress import ip_address
from typing import Dict

import aiohttp_jinja2
import monetdblite
from aiohttp import web

from ballcone import __version__
from ballcone.core import VALID_SERVICE, Ballcone


class WebBallcone:
    def __init__(self, ballcone: Ballcone):
        self.ballcone = ballcone

    @aiohttp_jinja2.template('root.html')
    async def root(self, _: web.Request):
        today = datetime.utcnow().date()

        services = self.ballcone.dao.tables()

        dashboard = []

        for service in services:
            unique = self.ballcone.unique(service, today, today)

            dashboard.append((service, unique.elements[0].count if unique.elements else 0))

        dashboard.sort(key=lambda service_count: (-service_count[1], service_count[0]))

        return {
            'version': __version__,
            'current_page': 'root',
            'services': services,
            'dashboard': dashboard
        }

    async def services(self, request: web.Request):
        raise web.HTTPFound(request.app.router['root'].url_for())

    @aiohttp_jinja2.template('service.html')
    async def service(self, request: web.Request):
        services = self.ballcone.dao.tables()
        service = request.match_info.get('service', None)

        if not self.ballcone.check_service(service):
            raise web.HTTPNotFound(text=f'No such service: {service}')

        stop = datetime.utcnow().date()
        start = stop - timedelta(days=7 - 1)

        queries = {
            'visits': self.ballcone.visits(service, start, stop),
            'unique': self.ballcone.unique(service, start, stop)
        }

        overview: Dict[date, Dict[str, int]] = OrderedDict()

        for query, result in queries.items():
            for element in result.elements:
                if element.date not in overview:
                    overview[element.date] = {}

                overview[element.date][query] = element.count

        time = self.ballcone.time(service, start, stop)

        paths = self.ballcone.uri(service, start, stop, limit=self.ballcone.top_limit)

        browsers = self.ballcone.browser(service, start, stop, limit=self.ballcone.top_limit)

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

        if not self.ballcone.check_service(service):
            raise web.HTTPNotFound(text=f'No such service: {service}')

        stop = datetime.utcnow().date()
        start = stop - timedelta(days=30 - 1)

        parameter = request.query.get('parameter', None)

        response = self.ballcone.handle_command(service, command, parameter, start, stop)

        return web.json_response(response, dumps=self.ballcone.json_dumps)

    @aiohttp_jinja2.template('sql.html')
    async def sql(self, request: web.Request):
        data = await request.post()

        sql = str(data.get('sql', 'SELECT 1, 2, 3;'))

        if sql:
            try:
                result = self.ballcone.dao.run(sql)
                error = None
            except monetdblite.exceptions.DatabaseError as e:
                result = []
                error = str(e)

        services = self.ballcone.dao.tables()

        return {
            'version': __version__,
            'current_page': 'sql',
            'title': 'SQL Console',
            'services': services,
            'sql': sql,
            'result': result,
            'error': error
        }

    @aiohttp_jinja2.template('nginx.html')
    async def nginx(self, request: web.Request):
        services = self.ballcone.dao.tables()

        service = request.query.get('service')

        if not service:
            service = 'example'

        ip = request.query.get('ip')

        if not ip:
            ip = '127.0.0.1'

        error = []

        if not self.ballcone.check_service(service, should_exist=False):
            error.append(f'Invalid service name: {self.ballcone.json_dumps(service)}, '
                         f'must match /{VALID_SERVICE.pattern}/')

        try:
            ip_address(ip)
        except ValueError:
            error.append(f'Invalid Ballcone IP address: {self.ballcone.json_dumps(ip)}')

        return {
            'version': __version__,
            'current_page': 'nginx',
            'title': 'nginx Configuration',
            'services': services,
            'service': service,
            'ip': ip,
            'error': error
        }
