FROM python:3.7

MAINTAINER Dmitry Ustalov <dmitry.ustalov@gmail.com>

EXPOSE 65140/udp 8080/tcp

WORKDIR /usr/src/app

COPY ballcone /usr/src/app/ballcone/

COPY requirements.txt setup.py README.md LICENSE /usr/src/app/

RUN \
apt-get update && \
apt-get install --no-install-recommends -y -o Dpkg::Options::="--force-confold" tini && \
apt-get clean && \
rm -rf /var/lib/apt/lists/* && \
pip3 install 'numpy>=1.14' 'pandas>=0.23' && \
pip3 install -r requirements.txt && \
python3 setup.py install

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD /usr/local/bin/ballcone
