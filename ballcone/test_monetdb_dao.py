import unittest
from collections import deque
from datetime import datetime, date
from ipaddress import ip_address
from typing import cast

import monetdblite

from ballcone.monetdb_dao import MonetDAO, Entry, smallint


class TestMonetDAO(unittest.TestCase):
    SCHEMA = 'test_dao'

    ENTRIES_20200101 = [
        Entry(datetime=datetime(2020, 1, 1, 12), date=date(2020, 1, 1), host='example.com', path='/',
              status=cast(smallint, 200), length=1024, generation_time=0.1, referer=None,
              ip=ip_address('192.168.1.1'), country_iso_code='UNKNOWN',
              platform_name='Mac OS', platform_version='X 10.15',
              browser_name='Firefox', browser_version='75.0', is_robot=False),

        Entry(datetime=datetime(2020, 1, 1, 12, 15), date=date(2020, 1, 1), host='example.com', path='/robots.txt',
              status=cast(smallint, 404), length=0, generation_time=0.01, referer=None,
              ip=ip_address('192.168.1.1'), country_iso_code='UNKNOWN',
              platform_name='Linux', platform_version=None,
              browser_name=None, browser_version=None, is_robot=True)
    ]

    ENTRIES_20200102 = [
        Entry(datetime=datetime(2020, 1, 2, 23, 59), date=date(2020, 1, 2), host='example.com', path='/',
              status=cast(smallint, 200), length=256, generation_time=0.01, referer='https://github.com/dustalov',
              ip=ip_address('192.168.1.2'), country_iso_code='UNKNOWN',
              platform_name='iOS', platform_version='13.3.1',
              browser_name='Safari', browser_version='13.0.5', is_robot=False),

        Entry(datetime=datetime(2020, 1, 2, 23, 59, 59), date=date(2020, 1, 2), host='example.com', path='/post',
              status=cast(smallint, 200), length=512, generation_time=1, referer=None,
              ip=ip_address('192.168.1.2'), country_iso_code='UNKNOWN',
              platform_name='iOS', platform_version='13.3.1',
              browser_name='Safari', browser_version='13.0.5', is_robot=False),
    ]

    ENTRIES = [*ENTRIES_20200101, *ENTRIES_20200102]

    def setUp(self) -> None:
        self.db = monetdblite.make_connection(':memory:')
        self.dao = MonetDAO(self.db, self.SCHEMA)
        self.dao.create_schema()

    def tearDown(self) -> None:
        self.db.close()

    def test_database_size(self) -> None:
        self.assertLess(0, self.dao.size())

    def test_schema_exists(self) -> None:
        self.assertTrue(self.dao.schema_exists())

    def test_create_and_drop_table(self) -> None:
        table1 = __name__ + '_1'
        table2 = __name__ + '_2'

        self.assertEqual([], self.dao.tables())
        self.assertFalse(self.dao.table_exists(table1))

        self.dao.create_table(table1)
        self.dao.create_table(table2)

        self.assertTrue(self.dao.table_exists(table1))
        self.assertEqual([table1, table2], self.dao.tables())

        self.dao.drop_table(table1)

        self.assertEqual([table2], self.dao.tables())
        self.assertFalse(self.dao.table_exists(table1))

    def test_insert_into(self) -> None:
        table = __name__

        self.seed(table, insert_entries=False)
        self.assertEqual(0, len(self.dao.select(table)))

        self.dao.insert_into(table, self.ENTRIES[0])
        self.assertEqual(1, len(self.dao.select(table)))

    def test_batch_insert_into_and_select(self) -> None:
        table = __name__

        self.seed(table, insert_entries=False)
        self.assertEqual(0, len(self.dao.select(table)))

        self.seed(table, create_table=False)
        entries = self.dao.select(table)

        self.assertEqual(self.ENTRIES, entries)

    def test_batch_insert_into_from_deque_and_select(self) -> None:
        table = __name__

        self.seed(table, insert_entries=False)
        self.assertEqual(0, len(self.dao.select(table)))

        entries_deque = deque(self.ENTRIES)
        self.assertEqual(len(self.ENTRIES), len(entries_deque))

        count = self.dao.batch_insert_into_from_deque(table, entries_deque)
        self.assertEqual(len(self.ENTRIES), count)
        self.assertEqual(0, len(entries_deque))

        entries = self.dao.select(table)
        self.assertEqual(self.ENTRIES, entries)

    def test_select(self) -> None:
        table = __name__

        self.seed(table)

        before = self.dao.select(table, stop=date(2019, 12, 31))
        self.assertEqual([], before)

        before_exact = self.dao.select(table, stop=date(2020, 1, 1))
        self.assertEqual(self.ENTRIES_20200101, before_exact)

        exact = self.dao.select(table, start=date(2020, 1, 1), stop=date(2020, 1, 1))
        self.assertEqual(self.ENTRIES_20200101, exact)

        after_exact = self.dao.select(table, start=date(2020, 1, 1))
        self.assertEqual(self.ENTRIES, after_exact)

        after = self.dao.select(table, start=date(2020, 1, 2))
        self.assertEqual(self.ENTRIES_20200102, after)

    def test_select_average(self) -> None:
        table = __name__

        self.seed(table)

        before = self.dao.select_average(table, 'generation_time', stop=date(2019, 12, 31))
        self.assertEqual(table, before.table)
        self.assertEqual('generation_time', before.field)
        self.assertEqual(0, len(before.elements))

        before_exact = self.dao.select_average(table, 'generation_time', stop=date(2020, 1, 1))
        self.assertEqual(table, before_exact.table)
        self.assertEqual('generation_time', before_exact.field)
        self.assertEqual(1, len(before_exact.elements))
        self.assertEqual(date(2020, 1, 1), before_exact.elements[0].date)
        self.assertEqual(0.055, before_exact.elements[0].avg)
        self.assertEqual(len(self.ENTRIES_20200101), before_exact.elements[0].count)

        exact = self.dao.select_average(table, 'generation_time', start=date(2020, 1, 1), stop=date(2020, 1, 1))
        self.assertEqual(table, exact.table)
        self.assertEqual('generation_time', exact.field)
        self.assertEqual(1, len(exact.elements))
        self.assertEqual(date(2020, 1, 1), exact.elements[0].date)
        self.assertEqual(0.055, exact.elements[0].avg)
        self.assertEqual(len(self.ENTRIES_20200101), exact.elements[0].count)

        after_exact = self.dao.select_average(table, 'generation_time', start=date(2020, 1, 1))
        self.assertEqual(table, after_exact.table)
        self.assertEqual('generation_time', after_exact.field)
        self.assertEqual(2, len(after_exact.elements))
        self.assertEqual(date(2020, 1, 1), after_exact.elements[0].date)
        self.assertEqual(0.055, after_exact.elements[0].avg)
        self.assertEqual(len(self.ENTRIES_20200101), after_exact.elements[0].count)
        self.assertEqual(date(2020, 1, 2), after_exact.elements[1].date)
        self.assertEqual(0.505, after_exact.elements[1].avg)
        self.assertEqual(len(self.ENTRIES_20200102), after_exact.elements[1].count)

        after = self.dao.select_average(table, 'generation_time', start=date(2020, 1, 2))
        self.assertEqual(table, after.table)
        self.assertEqual('generation_time', after.field)
        self.assertEqual(1, len(after.elements))
        self.assertEqual(date(2020, 1, 2), after.elements[0].date)
        self.assertEqual(0.505, after.elements[0].avg)
        self.assertEqual(len(self.ENTRIES_20200102), after.elements[0].count)

    def test_select_count(self) -> None:
        table = __name__

        self.seed(table)

        before = self.dao.select_count(table, stop=date(2019, 12, 31))
        self.assertEqual(table, before.table)
        self.assertIsNone(before.field)
        self.assertFalse(before.distinct)
        self.assertIsNone(before.ascending)
        self.assertIsNone(before.group)
        self.assertEqual(0, len(before.elements))

        before_exact = self.dao.select_count(table, stop=date(2020, 1, 1))
        self.assertEqual(table, before_exact.table)
        self.assertIsNone(before_exact.field)
        self.assertFalse(before_exact.distinct)
        self.assertIsNone(before_exact.ascending)
        self.assertIsNone(before_exact.group)
        self.assertEqual(1, len(before_exact.elements))
        self.assertEqual(date(2020, 1, 1), before_exact.elements[0].date)
        self.assertEqual(2, before_exact.elements[0].count)

        exact = self.dao.select_count(table, start=date(2020, 1, 1), stop=date(2020, 1, 1))
        self.assertEqual(table, exact.table)
        self.assertIsNone(exact.field)
        self.assertFalse(exact.distinct)
        self.assertIsNone(exact.ascending)
        self.assertIsNone(exact.group)
        self.assertEqual(1, len(exact.elements))
        self.assertEqual(date(2020, 1, 1), exact.elements[0].date)
        self.assertEqual(2, before_exact.elements[0].count)

        after_exact = self.dao.select_count(table, start=date(2020, 1, 1))
        self.assertEqual(table, after_exact.table)
        self.assertIsNone(after_exact.field)
        self.assertFalse(after_exact.distinct)
        self.assertIsNone(after_exact.ascending)
        self.assertIsNone(after_exact.group)
        self.assertEqual(2, len(after_exact.elements))
        self.assertEqual(date(2020, 1, 1), after_exact.elements[0].date)
        self.assertEqual(2, after_exact.elements[0].count)
        self.assertEqual(date(2020, 1, 2), after_exact.elements[1].date)
        self.assertEqual(2, after_exact.elements[1].count)

        after = self.dao.select_count(table, start=date(2020, 1, 2))
        self.assertEqual(table, after.table)
        self.assertIsNone(after.field)
        self.assertFalse(after.distinct)
        self.assertIsNone(after.ascending)
        self.assertIsNone(after.group)
        self.assertEqual(1, len(after.elements))
        self.assertEqual(date(2020, 1, 2), after.elements[0].date)
        self.assertEqual(2, after.elements[0].count)

    def test_select_count_group(self) -> None:
        table = __name__

        self.seed(table)

        before = self.dao.select_count_group(table, 'ip', 'platform_name', stop=date(2019, 12, 31))
        self.assertEqual(table, before.table)
        self.assertEqual('ip', before.field)
        self.assertFalse(before.distinct)
        self.assertTrue(before.ascending)
        self.assertEqual('platform_name', before.group)
        self.assertEqual(0, len(before.elements))

        before_exact = self.dao.select_count_group(table, 'ip', 'platform_name', stop=date(2020, 1, 1))
        self.assertEqual(table, before_exact.table)
        self.assertEqual('ip', before_exact.field)
        self.assertFalse(before_exact.distinct)
        self.assertTrue(before_exact.ascending)
        self.assertEqual('platform_name', before_exact.group)
        self.assertEqual(2, len(before_exact.elements))
        self.assertEqual(date(2020, 1, 1), before_exact.elements[0].date)
        self.assertEqual(1, before_exact.elements[0].count)
        self.assertEqual(date(2020, 1, 1), before_exact.elements[1].date)
        self.assertEqual(1, before_exact.elements[1].count)

        exact = self.dao.select_count_group(table, 'ip', 'platform_name', start=date(2020, 1, 1), stop=date(2020, 1, 1))
        self.assertEqual(table, exact.table)
        self.assertEqual('ip', exact.field)
        self.assertFalse(exact.distinct)
        self.assertTrue(exact.ascending)
        self.assertEqual('platform_name', exact.group)
        self.assertEqual(2, len(exact.elements))
        self.assertEqual(date(2020, 1, 1), exact.elements[0].date)
        self.assertEqual(1, exact.elements[0].count)
        self.assertEqual(date(2020, 1, 1), exact.elements[1].date)
        self.assertEqual(1, exact.elements[1].count)

        after_exact = self.dao.select_count_group(table, 'ip', 'platform_name', start=date(2020, 1, 1))
        self.assertEqual(table, after_exact.table)
        self.assertEqual('ip', after_exact.field)
        self.assertFalse(after_exact.distinct)
        self.assertTrue(after_exact.ascending)
        self.assertEqual('platform_name', after_exact.group)
        self.assertEqual(3, len(after_exact.elements))
        self.assertEqual(date(2020, 1, 1), after_exact.elements[0].date)
        self.assertEqual(1, after_exact.elements[0].count)
        self.assertEqual(date(2020, 1, 1), after_exact.elements[1].date)
        self.assertEqual(1, after_exact.elements[1].count)
        self.assertEqual(date(2020, 1, 2), after_exact.elements[2].date)
        self.assertEqual(2, after_exact.elements[2].count)

        after = self.dao.select_count_group(table, 'ip', 'platform_name', start=date(2020, 1, 2))
        self.assertEqual(table, after.table)
        self.assertEqual('ip', after.field)
        self.assertFalse(after.distinct)
        self.assertTrue(after.ascending)
        self.assertEqual('platform_name', after.group)
        self.assertEqual(1, len(after.elements))
        self.assertEqual(date(2020, 1, 2), after.elements[0].date)
        self.assertEqual(2, after.elements[0].count)

    def test_error(self) -> None:
        with self.assertRaises(monetdblite.exceptions.DatabaseError):
            self.dao.run('SELECT UNSELECT;')

    def seed(self, table: str, create_table: bool = True, insert_entries: bool = True) -> None:
        if create_table:
            self.assertFalse(self.dao.table_exists(table))
            self.dao.create_table(table)
            self.assertTrue(self.dao.table_exists(table))

        if insert_entries:
            count = self.dao.batch_insert_into(table, self.ENTRIES)

            self.assertEqual(len(self.ENTRIES), count)


if __name__ == '__main__':
    unittest.main()
