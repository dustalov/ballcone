# Balcone

Balcone is a fast and lightweight server-side Web analytics solution.

[![Unit Tests on GitHub Actions][github_tests_badge]][github_tests_link]

[github_tests_badge]: https://github.com/dustalov/balcone/workflows/Unit%20Tests/badge.svg?branch=master
[github_tests_link]: https://github.com/dustalov/balcone/actions?query=workflow%3A%22Unit+Tests%22

## Requirements

* Python 3.6 or 3.7
* [MonetDBLite](https://github.com/monetDB/MonetDBLite-Python)

## Design Goals

* Almost zero-configuration needed (thanks to [syslog logger](https://nginx.org/en/docs/syslog.html) bundled in nginx &geq; 1.7.1)
* Columnar data storage for lighting-fast analytic queries ([MonetDBLite](https://github.com/monetDB/MonetDBLite-Python) is currently used)

## Alternatives

* Web analytics solutions: [Matomo](https://matomo.org/), [Google Analytics](http://google.com/analytics/), [Yandex.Metrica](https://metrica.yandex.com/), etc.
* Columnar data storages: [ClickHouse](https://clickhouse.tech/), [PostgreSQL cstore_fdw](https://github.com/citusdata/cstore_fdw), [MariaDB ColumnStore](https://mariadb.com/kb/en/mariadb-columnstore/), etc.
* Log management: [Graylog](https://www.graylog.org/), [Fluentd](https://www.fluentd.org/), [Elasticsearch](https://github.com/elastic/elasticsearch), etc.

## Roadmap

* Switch to [DuckDB](https://github.com/cwida/duckdb) (as soon as sparse tables are supported)

## Installation

```Nginx
log_format balcone_json_petrovich escape=json
    '{'
    '"service": "petrovich", '
    '"args": "$args", '
    '"body_bytes_sent": "$body_bytes_sent", '
    '"content_length": "$content_length", '
    '"content_type": "$content_type", '
    '"host": "$host", '
    '"http_referrer": "$http_referrer", '
    '"http_user_agent": "$http_user_agent", '
    '"http_x_forwarded_for": "$http_x_forwarded_for", '
    '"remote_addr": "$remote_addr", '
    '"request_method": "$request_method", '
    '"request_time": "$request_time", '
    '"status": "$status", '
    '"upstream_addr": "$upstream_addr", '
    '"uri": "$uri"'
    '}';

    access_log syslog:server=127.0.0.1:65140 balcone_json_petrovich;
```

## Copyright

Copyright &copy; 2020 Dmitry Ustalov. See [LICENSE](LICENSE) for details.
