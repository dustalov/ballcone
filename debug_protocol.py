__author__ = 'Dmitry Ustalov'

import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import cast

from balcone import Balcone

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
