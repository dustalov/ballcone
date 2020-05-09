FROM python:3.7

MAINTAINER Dmitry Ustalov <dmitry.ustalov@gmail.com>

EXPOSE 65140/udp 8080/tcp

COPY . /usr/src/app

WORKDIR /usr/src/app

RUN \
apt-get update && apt-get install tini && \
pip3 install 'numpy>=1.14' 'pandas>=0.23' && \
pip3 install -r requirements.txt && \
python3 setup.py install

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD /usr/local/bin/ballcone
