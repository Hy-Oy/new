#! /usr/bin/env python
# -*- coding: utf-8 -*-
# author: ouyangshaokun
# date:
import time

import redis

# pool = redis.ConnectionPool(host="127.0.0.1", port=6379, db=0)
config = {
    'host': 'redis-y9qbhk61b0nn-proxy-nlb.jvessel-open-gz.jdcloud.com',
    'port': 6379,
    'password': 'bleEleven3243',
    'db': 0
}
pool = redis.ConnectionPool(host=config['host'], port=config['port'],
                            password=config.get('password', ''), db=config.get('db', 0),
                            socket_timeout=1, decode_responses=True, encoding='UTF-8')
redis_conn = redis.Redis(connection_pool=pool)


def del_large_hash(key):
    large_hash_key ="%s*" % key
    cursor = 0
    len_data = 0
    while 1:
        res_cursor, data = redis_conn.scan(cursor, match=large_hash_key, count=1000)
        res = redis_conn.delete(*data)
        print(res)
        len_data += len(data)
        if res_cursor == 0:
            break
        cursor = res_cursor
        time.sleep(0.1)
    print(len_data)


if __name__ == "__main__":
    # pattern = 'minipro_hot_skus_merge:*'
    # pool = redis.ConnectionPool(host="127.0.0.1", port=6379, db=0)
    # redis_conn = redis.Redis(connection_pool=pool)
    # pattern = 'aa*'
    # res = redis_conn.delete(*redis_conn.keys(pattern=pattern))
    # print(res)
    key = "minipro_hot_skus_merge:"
    del_large_hash(key)
