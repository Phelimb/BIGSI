sed  -i "s/{PORT}/$PORT/g" /usr/local/etc/redis/redis.conf
mkdir -p /data
cd /data
redis-server /usr/local/etc/redis/redis.conf