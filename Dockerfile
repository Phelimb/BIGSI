FROM python:3.6.3
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN pip install --upgrade pip
ARG TRAVIS
RUN echo $TRAVIS
## Install berkeleydb
ENV BERKELEY_VERSION 4.8.30
# Download, configure and install BerkeleyDB
RUN wget -P /tmp http://download.oracle.com/berkeley-db/db-"${BERKELEY_VERSION}".tar.gz && \
    tar -xf /tmp/db-"${BERKELEY_VERSION}".tar.gz -C /tmp && \
    rm -f /tmp/db-"${BERKELEY_VERSION}".tar.gz
RUN cd /tmp/db-"${BERKELEY_VERSION}"/build_unix && \
    ../dist/configure && make && make install

# Upgrade your gcc to version at least 4.7 to get C++11 support. gflags snappy zlib bzip2
#RUN apt-get install -y build-essential checkinstall zlib1g zlib1g-dev libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev 


# Clone rocksdb
# RUN cd /tmp && git clone https://github.com/facebook/rocksdb.git && cd rocksdb && make clean && make

# Install mccortex
RUN git clone --recursive https://github.com/mcveanlab/mccortex
WORKDIR /usr/src/app/mccortex
RUN make all
WORKDIR /usr/src/app

## Install bigsi
COPY . /usr/src/app
RUN pip install cython
RUN  pip install --no-cache-dir -r requirements.txt

# install bigsi
WORKDIR /usr/src/app
RUN python setup.py install
RUN sh clean.sh
RUN python setup.py install

CMD bigsi --help