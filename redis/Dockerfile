FROM redis
COPY redis.conf /usr/local/etc/redis/redis.conf
COPY run_redis.sh /usr/local/etc/redis/run_redis.sh
WORKDIR /data
CMD [ "sh", "/usr/local/etc/redis/run_redis.sh" ]