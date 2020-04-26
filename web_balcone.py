__author__ = 'Dmitry Ustalov'

from collections import OrderedDict
from datetime import date, datetime, timedelta
from typing import Dict

import aiohttp_jinja2
import monetdblite
from aiohttp import web

from balcone import __version__, Balcone


class WebBalcone:
    def __init__(self, balcone: Balcone):
        self.balcone = balcone

    @aiohttp_jinja2.template('root.html')
    async def root(self, _: web.Request):
        today = datetime.utcnow().date()

        services = {}

        for service in self.balcone.dao.tables():
            count = self.balcone.visits(service, today, today)

            services[service] = count.elements[0].count if count.elements else 0

        return {
            'version': __version__,
            'current_page': 'root',
            'services': services
        }

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
        data = await request.post()
        sql, result, error = str(data.get('sql', '')), [], ''

        if sql:
            try:
                result = self.balcone.dao.run(sql)
            except monetdblite.exceptions.DatabaseError as e:
                error = str(e)

        services = self.balcone.dao.tables()

        return {
            'version': __version__,
            'current_page': 'sql',
            'title': 'SQL Console',
            'services': services,
            'sql': sql,
            'result': result,
            'error': error
        }
