FROM python

MAINTAINER Dmitry Ustalov <dmitry.ustalov@gmail.com>

EXPOSE 65140/udp 8888/tcp 8080/tcp

COPY . /usr/src/app

WORKDIR /usr/src/app

RUN \
pip3 install 'numpy>=1.14' 'pandas>=0.23' && \
pip3 install -r requirements.txt && \
python3 setup.py install

CMD balcone
