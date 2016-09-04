FROM redis:3.2.3-32bit
COPY redis.conf /usr/local/etc/redis/redis.conf
COPY run_redis.sh /usr/local/etc/redis/run_redis.sh
CMD [ "sh", "/usr/local/etc/redis/run_redis.sh" ]