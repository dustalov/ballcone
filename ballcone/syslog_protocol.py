__author__ = 'Dmitry Ustalov'

import asyncio
import logging
import re
import urllib.parse
from collections import deque
from datetime import timezone
from ipaddress import ip_address
from typing import cast, Tuple, Union, Optional

import dateutil.parser
import httpagentparser
import simplejson

from ballcone.core import Ballcone
from ballcone.monetdb_dao import Entry, smallint

# nginx's output cannot be properly parsed by any parser I tried
NGINX_SYSLOG = re.compile(r'\A<[0-9]{1,3}>.*?: (?P<message>.+)\Z')


class SyslogProtocol(asyncio.DatagramProtocol):
    def __init__(self, ballcone: Ballcone) -> None:
        super().__init__()
        self.ballcone = ballcone
        self.transport: Optional[asyncio.BaseTransport] = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: Union[bytes, str], addr: Tuple[str, int]) -> None:
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

        if not self.ballcone.check_service(service, should_exist=False):
            logging.info(f'Malformed service field from {addr}: {message}')
            return

        if service not in self.ballcone.queue:
            if not self.ballcone.dao.table_exists(service):
                self.ballcone.dao.create_table(service)

            self.ballcone.queue[service] = deque()

        current_datetime = dateutil.parser.isoparse(content['date']).astimezone(timezone.utc)

        path = urllib.parse.unquote(content['path'])

        user_agent = httpagentparser.detect(content['user_agent'])

        entry = Entry(
            datetime=current_datetime,
            date=current_datetime.date(),
            host=content['host'],
            path=path,
            status=cast(smallint, int(content['status'])),
            length=int(content['length']),
            generation_time=float(content['generation_time_milli']),
            referer=content['referrer'],
            ip=ip_address(content['ip']),
            country_iso_code=Ballcone.iso_code(self.ballcone.geoip, content['ip']),
            platform_name=user_agent.get('platform', {}).get('name', None),
            platform_version=user_agent.get('platform', {}).get('version', None),
            browser_name=user_agent.get('browser', {}).get('name', None),
            browser_version=user_agent.get('browser', {}).get('version', None),
            is_robot=user_agent.get('bot', None)
        )

        self.ballcone.queue[service].append(entry)
