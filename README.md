# Balcone

Balcone is a fast and lightweight server-side Web analytics solution.

[![Unit Tests on GitHub Actions][github_tests_badge]][github_tests_link] [![Docker Hub][docker_hub_badge]][docker_hub_link]

[github_tests_badge]: https://github.com/dustalov/balcone/workflows/Unit%20Tests/badge.svg?branch=master
[github_tests_link]: https://github.com/dustalov/balcone/actions?query=workflow%3A%22Unit+Tests%22
[docker_hub_badge]: https://img.shields.io/docker/pulls/dustalov/balcone
[docker_hub_link]: https://hub.docker.com/r/dustalov/balcone

## Requirements

* Python 3.6 or 3.7
* [MonetDBLite](https://github.com/monetDB/MonetDBLite-Python), an embedded database

## Design Goals

* Almost zero-configuration needed (thanks to [syslog logger](https://nginx.org/en/docs/syslog.html) bundled in nginx &geq; 1.7.1)
* Columnar data storage for lighting-fast analytic queries ([MonetDBLite](https://github.com/monetDB/MonetDBLite-Python) is currently used)

## Demo

This repository contains an example configuration of nginx and Balcone. First, build the `balcone` Docker image locally and run the container using Docker Compose. nginx will be available at <http://localhost:8888/> and Balcone will be available at <http://localhost:8080/>.

```shell
make docker # docker build --rm -t balcone .
docker-compose up
```

## Installation

### Getting Balcone

Running the Docker image is the simplest way to get started. Docker Hub performs automated builds of the Balcone source code from GitHub: <https://hub.docker.com/r/dustalov/balcone>. The following command runs Balcone on `127.0.0.1`: the syslog protocol will be available via `65140/udp`, the Web interface will be available via `8080/tcp`, and the data will be stored in `/var/lib/balcone` of the host machine.

```shell
docker run --init -p '127.0.0.1:8080:8080' -p '127.0.0.1:65140:65140/udp' -v '/var/lib/balcone/monetdb:/usr/src/app/monetdb' dustalov/balcone balcone -sh '0.0.0.0' -wh '0.0.0.0'
```

However, Docker is not the only option. Alternatively, Balcone can be installed directly on the host machine:

```shell
pip3 install -e git+https://github.com/dustalov/balcone@master#egg=balcone
```

Then it can be either runned manually (`balcone` without arguments will create the `monetdb` directory inside the current directory) or be configured as a [systemd](https://systemd.io/) service, see [balcone.service](balcone.service) as an example.

### Configuring nginx

You need to define in the nginx configuration file the JSON-compatible log format for your service. Let us call it `balcone_json_petrovich`. This block is similar to the one used in Matomo (see [matomo-log-analytics](https://github.com/matomo-org/matomo-log-analytics)). It should be put *before* the `server` block.

```Nginx
log_format balcone_json_petrovich escape=json
    '{'
    '"service": "example", '
    '"ip": "$remote_addr", '
    '"host": "$host", '
    '"method": "$request_method", '
    '"path": "$request_uri", '
    '"status": "$status", '
    '"referrer": "$http_referer", '
    '"user_agent": "$http_user_agent", '
    '"length": $bytes_sent, '
    '"generation_time_milli": $request_time, '
    '"date": "$time_iso8601"'
    '}';
```

Then, you should put this `access_log` directive inside the `server` block to transfer logs via the [syslog protocol](https://nginx.org/en/docs/syslog.html).

```Nginx
access_log syslog:server=127.0.0.1:65140 balcone_json_petrovich;
```

Please look at the complete example of nginx configuration in [demo/nginx.conf](demo/nginx.conf).

## Roadmap

* Support more versions of Python (requires no effort as soon as MonetDB/MonetDBLite-Python#46 is fixed)
* Switch to [DuckDB](https://github.com/cwida/duckdb) (as soon as sparse tables are supported)

## Alternatives

* Web analytics solutions: [Matomo](https://matomo.org/), [Google Analytics](http://google.com/analytics/), [Yandex.Metrica](https://metrica.yandex.com/), etc.
* Columnar data storages: [ClickHouse](https://clickhouse.tech/), [PostgreSQL cstore_fdw](https://github.com/citusdata/cstore_fdw), [MariaDB ColumnStore](https://mariadb.com/kb/en/mariadb-columnstore/), etc.
* Log management: [Graylog](https://www.graylog.org/), [Fluentd](https://www.fluentd.org/), [Elasticsearch](https://github.com/elastic/elasticsearch), etc.

## Copyright

Copyright &copy; 2020 Dmitry Ustalov. See [LICENSE](LICENSE) for details.
