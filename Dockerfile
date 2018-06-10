FROM debian:sid

ARG BUILD_DATE
ARG VCS_REF

ARG ROCKSDB_REPO='https://github.com/facebook/rocksdb.git'
ARG ROCKSDB_VERSION='5.2.1'
ARG ROCKSDB_TAG="rocksdb-${ROCKSDB_VERSION}"

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

# Install mccortex
RUN set -x && echo 'deb http://deb.debian.org/debian experimental main' > /etc/apt/sources.list.d/experimental.list
RUN apt-get update
RUN apt-get install -y git liblzma-dev libbz2-dev build-essential zlib1g-dev
# RUN git clone --recursive https://github.com/mcveanlab/mccortex
# WORKDIR /usr/src/app/mccortex
# RUN make all
# WORKDIR /usr/src/app

RUN  apt-get install -y  libgflags-dev libjemalloc-dev libsnappy-dev libtbb-dev libzstd-dev python3.6 python3-pip zlib1g zlib1g-dev wget build-essential liblz4-dev
RUN pip3 install --upgrade pip

RUN git clone $ROCKSDB_REPO
WORKDIR /usr/src/app/rocksdb
RUN git checkout tags/${ROCKSDB_TAG}
RUN make -j$(nproc) shared_lib
#RUN make install-shared
#RUN strip /usr/local/lib/librocksdb.so.${ROCKSDB_VERSION}

ENV CPLUS_INCLUDE_PATH=${CPLUS_INCLUDE_PATH}:`pwd`/include
ENV CPLUS_INCLUDE_PATH=${CPLUS_INCLUDE_PATH}:/usr/src/app/rocksdb/include
ENV LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:`pwd`:/usr/src/app/rocksdb/
ENV LIBRARY_PATH=${LIBRARY_PATH}:`pwd`:/usr/src/app/rocksdb/
WORKDIR /usr/src/app/



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




## Install bigsi
COPY . /usr/src/app

RUN pip3 install cython

RUN  pip3 install --no-cache-dir -r requirements.txt

# install bigsi
WORKDIR /usr/src/app
RUN python3 setup.py install
RUN sh clean.sh
RUN python3 setup.py install

CMD bigsi --help