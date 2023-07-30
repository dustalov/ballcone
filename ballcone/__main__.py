#!/usr/bin/env python3

__author__ = 'Dmitry Ustalov'

import argparse
import asyncio
import logging
import os
import sys
from contextlib import suppress
from pathlib import Path
from typing import cast

import aiohttp_jinja2
import duckdb
import jinja2
from aiohttp import web
from geolite2 import geolite2

from ballcone import __version__
from ballcone.core import Ballcone
from ballcone.dao import DAO
from ballcone.syslog_protocol import SyslogProtocol
from ballcone.web_ballcone import WebBallcone


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--version', action='version',
                        version=f'Ballcone v{__version__} (DuckDB v{duckdb.__version__})')  # type: ignore
    parser.add_argument('-sh', '--syslog-host', default='127.0.0.1', help='syslog host to bind')
    parser.add_argument('-sp', '--syslog-port', default=65140, type=int, help='syslog UDP port to bind')
    parser.add_argument('-wh', '--web-host', default='127.0.0.1', help='Web interface host to bind')
    parser.add_argument('-wp', '--web-port', default=8080, type=int, help='Web interface TCP port to bind')
    parser.add_argument('-d', '--database', default='ballcone.duckdb', help='Path to DuckDB database')
    parser.add_argument('-p', '--period', default=5, type=int, help='Persistence period, in seconds')
    parser.add_argument('-t', '--top-limit', default=5, type=int, help='Limit for top-n queries')
    parser.add_argument('--days', default=30, type=int, help='Default number of days in plots')
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

    if args.database == ':memory:':
        connection = duckdb.connect(args.database)
    else:
        connection = duckdb.connect(str(Path(args.database).resolve()))

    dao = DAO(connection)

    geoip = geolite2.reader()

    ballcone = Ballcone(dao, geoip, args.top_limit, args.period)

    asyncio.ensure_future(ballcone.persist_timer())

    loop = asyncio.get_event_loop()

    syslog = loop.create_datagram_endpoint(lambda: SyslogProtocol(ballcone),
                                           local_addr=(args.syslog_host, args.syslog_port))

    # PyInstaller
    if getattr(sys, 'frozen', False):
        jinja2_loader = cast(jinja2.BaseLoader, jinja2.FileSystemLoader(
            os.path.join(getattr(sys, '_MEIPASS'), 'templates')
        ))
    else:
        jinja2_loader = cast(jinja2.BaseLoader, jinja2.PackageLoader('ballcone'))

    app = web.Application()
    aiohttp_jinja2.setup(app, loader=jinja2_loader)
    handler = WebBallcone(ballcone, args.days)
    app.router.add_get('/', handler.root, name='root')
    app.router.add_get('/services', handler.services, name='services')
    app.router.add_get('/services/{service}', handler.service, name='service')
    app.router.add_get('/services/{service}/average/{field}', handler.average_or_count, name='average')
    app.router.add_get('/services/{service}/count/{field}', handler.average_or_count, name='count')
    app.router.add_get('/services/{service}/count_group/{group}', handler.count_group, name='count_group')
    app.router.add_get('/sql', handler.sql, name='sql')
    app.router.add_post('/sql', handler.sql, name='sql')
    app.router.add_get('/nginx', handler.nginx, name='nginx')

    try:
        loop.run_until_complete(syslog)
        web.run_app(app, host=args.web_host, port=args.web_port, loop=loop)
    finally:
        with suppress(RuntimeError):
            for task in asyncio.all_tasks():
                task.cancel()

                with suppress(asyncio.CancelledError):
                    loop.run_until_complete(task)

        geoip.close()

        try:
            ballcone.persist()
        finally:
            connection.close()


if __name__ == '__main__':
    main()
