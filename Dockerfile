FROM python:3.8

MAINTAINER Dmitry Ustalov <dmitry.ustalov@gmail.com>

EXPOSE 65140/udp 8080/tcp

WORKDIR /usr/src/app

COPY ballcone /usr/src/app/ballcone/

COPY pyproject.toml Pipfile Pipfile.lock setup.cfg setup.py README.md LICENSE /usr/src/app/

RUN \
apt-get update && \
apt-get install --no-install-recommends -y -o Dpkg::Options::="--force-confold" tini && \
apt-get clean && \
rm -rf /var/lib/apt/lists/* && \
python3 -m pip install --upgrade pip && \
pip install pipenv && \
pipenv install --system

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD /usr/local/bin/ballcone
