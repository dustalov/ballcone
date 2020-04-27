#!/usr/bin/env python3

__author__ = 'Dmitry Ustalov'

import asyncio
import logging
import sys
from contextlib import suppress

import aiohttp_jinja2
import jinja2
import monetdblite
from aiohttp import web
from geolite2 import geolite2

from balcone import Balcone
from debug_protocol import DebugProtocol
from monetdb_dao import MonetDAO
from syslog_protocol import SyslogProtocol
from web_balcone import WebBalcone


def main():
    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

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
    handler = WebBalcone(balcone)
    app.router.add_get('/', handler.root, name='root')
    app.router.add_get('/services', handler.services, name='services')
    app.router.add_get('/services/{service}', handler.service, name='service')
    app.router.add_get('/services/{service}/{query}', handler.query, name='query')
    app.router.add_get('/sql', handler.sql, name='sql')
    app.router.add_post('/sql', handler.sql, name='sql')
    app.router.add_get('/nginx', handler.nginx, name='nginx')
    web.run_app(app, host='127.0.0.1', port=8080)

    try:
        loop.run_forever()
    finally:
        all_tasks_func = asyncio.all_tasks if hasattr(asyncio, 'all_tasks') else asyncio.Task.all_tasks  # Python 3.6

        for task in all_tasks_func():
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
