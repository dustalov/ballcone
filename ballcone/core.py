__author__ = 'Dmitry Ustalov'

import asyncio
import logging
import re
from datetime import datetime, date, timedelta
from ipaddress import IPv4Address, IPv6Address
from typing import Optional, Dict, Deque, Tuple, Any, cast

import simplejson
from geolite2 import maxminddb
from monetdblite.exceptions import DatabaseError

from .monetdb_dao import MonetDAO, Entry

VALID_SERVICE = re.compile(r'\A[\w]+\Z')


class BallconeJSONEncoder(simplejson.JSONEncoder):
    def default(self, obj: Any) -> str:
        if isinstance(obj, date):
            return obj.isoformat()

        if isinstance(obj, (IPv4Address, IPv6Address)):
            return str(obj)

        return cast(str, super().default(obj))


class Ballcone:
    def __init__(self, dao: MonetDAO, geoip: maxminddb.reader.Reader,
                 top_limit: int = 5, persist_period: int = 5) -> None:
        self.dao = dao
        self.geoip = geoip
        self.top_limit = top_limit
        self.persist_period = persist_period
        self.queue: Dict[str, Deque[Entry]] = {}
        self.json_dumps = BallconeJSONEncoder().encode

    async def persist_timer(self) -> None:
        while await asyncio.sleep(self.persist_period, result=True):
            self.persist()

    def persist(self) -> None:
        for service, queue in self.queue.items():
            try:
                count = self.dao.batch_insert_into_from_deque(service, queue)

                if count:
                    logging.debug(f'Inserted {count} entries for service {service}')
            except DatabaseError:
                logging.exception('Please check if the query is correct')

    def unwrap_top_limit(self, top_limit: Optional[int] = None) -> int:
        return top_limit if top_limit else self.top_limit

    def check_service(self, service: Optional[str], should_exist: bool = False) -> bool:
        return (
            service is not None
            and VALID_SERVICE.match(service) is not None
            and (not should_exist or self.dao.table_exists(service))
        )

    @staticmethod
    def iso_code(geoip: maxminddb.reader.Reader, ip: str) -> Optional[str]:
        geo = geoip.get(ip)

        return geo['country'].get('iso_code', None) if geo and 'country' in geo else None

    @staticmethod
    def days_before(stop_date: Optional[date] = None, days: int = 30) -> Tuple[date, date]:
        stop = stop_date if stop_date else datetime.utcnow().date()

        start = stop - timedelta(days=days - 1)

        return start, stop
