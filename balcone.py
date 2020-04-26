__author__ = 'Dmitry Ustalov'
__version__ = 0

import asyncio
import logging
import re
from datetime import date
from ipaddress import IPv4Address, IPv6Address
from typing import cast, Union, Optional, Dict, Deque, Any

import simplejson
from geolite2 import maxminddb

from monetdb_dao import MonetDAO, Entry, AverageResult, CountResult

VALID_SERVICE = re.compile(r'\A[\S]+\Z')


def isint(s: Optional[str]) -> bool:
    if not s:
        return False

    try:
        value = int(s)
        return value != 0
    except ValueError:
        return False


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
