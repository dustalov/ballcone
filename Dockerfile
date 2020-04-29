FROM alpine

MAINTAINER Dmitry Ustalov <dmitry.ustalov@gmail.com>

EXPOSE 65140/udp 8888/tcp 8080/tcp

COPY . /usr/src/app

WORKDIR /usr/src/app

RUN \
apk add --no-cache python3 py3-numpy libstdc++ && \
apk add --no-cache --virtual .installdeps build-base python3-dev py3-numpy-dev git && \
sed -re 's|^(monetdblite)==(.+)$|-e git+https://github.com/MonetDB/MonetDBLite-Python@v\2#egg=\1|' -i requirements.txt && \
pip3 install pandas && \
pip3 install -r requirements.txt && \
apk del .installdeps && \
python3 setup.py install

CMD balcone
