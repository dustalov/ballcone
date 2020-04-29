__author__ = 'Dmitry Ustalov'

import asyncio
import logging
from typing import cast

from balcone.core import Balcone


class DebugProtocol(asyncio.Protocol):
    def __init__(self, balcone: Balcone):
        super().__init__()
        self.balcone = balcone

    def connection_made(self, transport: asyncio.BaseTransport):
        self.transport = cast(asyncio.Transport, transport)

    def data_received(self, data: bytes):
        try:
            sql = data.decode('utf-8')
        except UnicodeDecodeError:
            logging.info('Malformed ASCII received')
            self.transport.close()
            return

        if not sql:
            self.transport.close()
            return

        result, error = [], ''

        if sql:
            try:
                result = self.balcone.dao.run(sql)
            except RuntimeError as e:
                error = str(e)

        if error:
            self.transport.write(error.encode('utf-8'))
        else:
            for row in result:
                for i, column in enumerate(row):
                    self.transport.write(str(column).encode('utf-8'))

                    if i < len(row) - 1:
                        self.transport.write(b'|')

                self.transport.write(b'\n')

        self.transport.close()
