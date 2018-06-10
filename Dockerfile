FROM debian:sid

ARG BUILD_DATE
ARG VCS_REF

ARG ROCKSDB_REPO='https://github.com/facebook/rocksdb.git'
ARG ROCKSDB_VERSION='5.2.1'
ARG ROCKSDB_TAG="rocksdb-${ROCKSDB_VERSION}"
RUN set -x\
  && echo 'deb http://deb.debian.org/debian experimental main' > /etc/apt/sources.list.d/experimental.list\
  && apt-get update && apt-get install -y\
    build-essential\
    git\
    libbz2-dev\
    libgflags-dev\
    libjemalloc-dev\
    libsnappy-dev\
    libtbb-dev\
    libzstd-dev\
    zlib1g-dev\
  && git clone $ROCKSDB_REPO /tmp/rocksdb\
  && cd /tmp/rocksdb\
  && git checkout tags/${ROCKSDB_TAG}\
  && make -j$(nproc) shared_lib\
  && make install-shared\
  && strip /usr/local/lib/librocksdb.so.${ROCKSDB_VERSION}\
  && rm -rf /tmp/rocksdb\
  && apt-get purge -y\
    build-essential\
    libgflags-dev\
    libjemalloc-dev\
    libsnappy-dev\
    libtbb-dev\
    libzstd-dev\
    zlib1g-dev\
  && apt-get install -y\
    libbz2-1.0\
    libjemalloc1\
    libsnappy1v5\
    libtbb2\
    libzstd1\
    zlib1g

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN apt-get update -y && apt-get upgrade -y
RUN apt-get install -y python3-pip && pip3 install --upgrade pip
RUN apt-get install -y zlib1g zlib1g-dev
ARG TRAVIS
RUN echo $TRAVIS
## Install berkeleydb
ENV BERKELEY_VERSION 4.8.30
# Download, configure and install BerkeleyDB
RUN apt-get install -y wget build-essential git
RUN wget -P /tmp http://download.oracle.com/berkeley-db/db-"${BERKELEY_VERSION}".tar.gz && \
    tar -xf /tmp/db-"${BERKELEY_VERSION}".tar.gz -C /tmp && \
    rm -f /tmp/db-"${BERKELEY_VERSION}".tar.gz
RUN cd /tmp/db-"${BERKELEY_VERSION}"/build_unix && \
    ../dist/configure && make && make install

# Upgrade your gcc to version at least 4.7 to get C++11 support. gflags snappy zlib bzip2
#RUN apt-get -y install -y build-essential checkinstall zlib1g zlib1g-dev libgflags-dev libsnappy-dev libbz2-dev cmake liblz4-dev
#RUN git clone https://github.com/facebook/rocksdb.git && mkdir rocksdb/build
#WORKDIR /usr/src/app/rocksdb
#RUN make shared_lib -j `nproc`
#ENV CPLUS_INCLUDE_PATH=${CPLUS_INCLUDE_PATH}:`pwd`/../include
#ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:`pwd`
#ENV LIBRARY_PATH=${LIBRARY_PATH}:`pwd`
WORKDIR /usr/src/app/

# Install mccortex
RUN git clone --recursive https://github.com/mcveanlab/mccortex
WORKDIR /usr/src/app/mccortex
RUN make all
WORKDIR /usr/src/app

## Install bigsi
COPY . /usr/src/app
RUN pip3 install cython
RUN  pip3 install --no-cache-dir -r requirements.txt

# install bigsi
WORKDIR /usr/src/app
RUN python setup.py install
RUN sh clean.sh
RUN python setup.py install

CMD bigsi --help