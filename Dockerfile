FROM python:3-alpine

MAINTAINER Dmitry Ustalov <dmitry.ustalov@gmail.com>

EXPOSE 65140/udp 8888/tcp

COPY . /usr/src/app

WORKDIR /usr/src/app

RUN \
apk add --no-cache leveldb && \
apk add --no-cache --virtual .installdeps build-base leveldb-dev && \
pip3 install -r requirements.txt && \
apk del .installdeps

CMD python3 balcone.py
