__author__ = 'Dmitry Ustalov'

from collections import OrderedDict
from datetime import date, datetime
from functools import lru_cache
from ipaddress import ip_address
from time import time
from typing import Dict, Optional, List, Any, cast

import aiohttp_jinja2
import monetdblite
from aiohttp import web

from ballcone import __version__
from ballcone.core import VALID_SERVICE, Ballcone


class WebBallcone:
    def __init__(self, ballcone: Ballcone) -> None:
        self.ballcone = ballcone

    @aiohttp_jinja2.template('root.html')
    async def root(self, _: web.Request) -> Dict[str, Any]:
        today = datetime.utcnow().date()

        services = self.ballcone.dao.tables()

        dashboard = []

        for service in services:
            unique = self.ballcone.dao.select_count(service, 'ip', start=today, stop=today)

            dashboard.append((service, unique.elements[0].count if unique.elements else 0))

        dashboard.sort(key=lambda service_count: (-service_count[1], service_count[0]))

        return {
            'version': __version__,
            'size': self.database_size(get_ttl_hash()),
            'current_page': 'root',
            'services': services,
            'dashboard': dashboard
        }

    async def services(self, request: web.Request) -> web.Response:
        raise web.HTTPFound(request.app.router['root'].url_for())

    @aiohttp_jinja2.template('service.html')
    async def service(self, request: web.Request) -> Dict[str, Any]:
        services = self.ballcone.dao.tables()
        service = request.match_info.get('service', None)

        if not self.ballcone.check_service(service):
            raise web.HTTPNotFound(text=f'No such service: {service}')

        service = cast(str, service)

        start, stop = self.ballcone.days_before(days=7)

        queries = {
            'visits': self.ballcone.dao.select_count(service, start=start, stop=stop),
            'unique': self.ballcone.dao.select_count(service, 'ip', start=start, stop=stop)
        }

        overview: Dict[date, Dict[str, int]] = OrderedDict()

        for query, result in queries.items():
            for element in result.elements:
                if element.date not in overview:
                    overview[element.date] = {}

                overview[element.date][query] = element.count

        limit = self.ballcone.unwrap_top_limit()

        time = self.ballcone.dao.select_average(service, 'generation_time', start, stop)

        paths = self.ballcone.dao.select_count_group(service, 'ip', 'path', ascending=False, limit=limit,
                                                     start=start, stop=stop)

        browsers = self.ballcone.dao.select_count_group(service, 'ip', 'browser_name', ascending=False, limit=limit,
                                                        start=start, stop=stop)

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

    async def average_or_count(self, request: web.Request) -> web.Response:
        service, field = request.match_info['service'], request.match_info['field']

        if not self.ballcone.check_service(service):
            raise web.HTTPNotFound(text=f'No such service: {service}')

        start, stop = self.ballcone.days_before()

        if request.match_info.route.name == 'average':
            average_response = self.ballcone.dao.select_average(service, field=field, start=start, stop=stop)
            return web.json_response(average_response, dumps=self.ballcone.json_dumps)
        else:
            count_response = self.ballcone.dao.select_count(service, field=field, start=start, stop=stop)
            return web.json_response(count_response, dumps=self.ballcone.json_dumps)

    async def count_group(self, request: web.Request) -> web.Response:
        service, group = request.match_info['service'], request.match_info['group']

        if not self.ballcone.check_service(service):
            raise web.HTTPNotFound(text=f'No such service: {service}')

        field = request.query.get('distinct', None)
        distinct = bool(request.query.get('distinct', None))
        ascending = bool(request.query.get('ascending', None))
        limit = int(request.query['limit']) if 'limit' in request.query else None

        start, stop = self.ballcone.days_before()

        response = self.ballcone.dao.select_count_group(service, field=field, group=group,
                                                        distinct=distinct, ascending=ascending, limit=limit,
                                                        start=start, stop=stop)

        return web.json_response(response, dumps=self.ballcone.json_dumps)

    @aiohttp_jinja2.template('sql.html')
    async def sql(self, request: web.Request) -> Dict[str, Any]:
        data = await request.post()

        sql = str(data.get('sql', f"SELECT '{self.ballcone.dao.schema}';"))

        result: List[List[Any]] = []
        error: Optional[str] = None

        if sql:
            try:
                result = self.ballcone.dao.run(sql)
            except monetdblite.exceptions.DatabaseError as e:
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
    async def nginx(self, request: web.Request) -> Dict[str, Any]:
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
            ip_version = ip_address(ip).version
        except ValueError:
            error.append(f'Invalid Ballcone IP address: {self.ballcone.json_dumps(ip)}')
            ip_version = None

        return {
            'version': __version__,
            'current_page': 'nginx',
            'title': 'nginx Configuration',
            'services': services,
            'service': service,
            'ip': ip,
            'ip_version': ip_version,
            'error': error
        }

    @lru_cache()
    def database_size(self, ttl_hash: Optional[int] = None) -> Optional[int]:
        return self.ballcone.dao.size()


def get_ttl_hash(seconds: int = 300) -> int:
    return round(time() / seconds)
