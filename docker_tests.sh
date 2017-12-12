source .env_test
docker-compose up -d
docker exec -it bigsi_redismanager_1 /bin/bash -c 'yes yes | ./redis-trib.rb create --replicas 0 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002'
docker exec bigsi_redis1_1 redis-cli -c -p 7000 flushall
docker exec bigsi_main_1 py.test -v --cov=bigsi --cov-config .coveragerc bigsi/tests/
