#!/usr/bin/env python3

__author__ = 'Dmitry Ustalov'

import argparse
import asyncio
import logging
import sys
from contextlib import suppress

import aiohttp_jinja2
import jinja2
import monetdblite
from aiohttp import web
from geolite2 import geolite2

from balcone import Balcone, __version__
from debug_protocol import DebugProtocol
from monetdb_dao import MonetDAO
from syslog_protocol import SyslogProtocol
from web_balcone import WebBalcone


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--version', action='version', version=f'Balcone v{__version__}')
    parser.add_argument('-sh', '--syslog-host', default='127.0.0.1', help='syslog host to bind')
    parser.add_argument('-sp', '--syslog-port', default=65140, type=int, help='syslog UDP port to bind')
    parser.add_argument('-dh', '--debug-host', default='127.0.0.1', help='SQL debug host to bind')
    parser.add_argument('-dp', '--debug-port', default=65141, type=int, help='SQL debug TCP port to bind')
    parser.add_argument('-wh', '--web-host', default='127.0.0.1', help='Web interface host to bind')
    parser.add_argument('-wp', '--web-port', default=8080, type=int, help='Web interface TCP port to bind')
    parser.add_argument('-p', '--period', default=5, type=int, help='Persistence period, in seconds')
    parser.add_argument('-t', '--top-limit', default=5, type=int, help='Limit for top-n queries')
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

    dao = MonetDAO(monetdblite.make_connection('monetdb'), 'balcone')

    if not dao.schema_exists():
        dao.create_schema()

    geoip = geolite2.reader()

    balcone = Balcone(dao, geoip, args.top_limit, args.period)

    asyncio.ensure_future(balcone.persist_timer())

    loop = asyncio.get_event_loop()

    syslog = loop.create_datagram_endpoint(lambda: SyslogProtocol(balcone),
                                           local_addr=(args.syslog_host, args.syslog_port))
    loop.run_until_complete(syslog)

    debug = loop.create_server(lambda: DebugProtocol(balcone),
                               host=args.debug_host, port=args.debug_port)
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
    web.run_app(app, host=args.web_host, port=args.web_port)

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
