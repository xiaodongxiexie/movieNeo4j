# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2021/8/28

from redis import Redis

from spider.config import REDIS_DB, REDIS_PORT, REDIS_PASSWORD, REDIS_HOST


class Filter:

    def put(self, v) -> bool:
        return True

    def exist(self, v) -> bool:
        return False


class RedisFilter(Filter):

    def __init__(self, name, host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=REDIS_DB):
        self.name = name
        self.rds = Redis(host=host, port=port, db=db, password=password, decode_responses=True)

    def put(self, v):
        return self.rds.sadd(self.name, v)

    def exist(self, v):
        return self.rds.sismember(self.name, v)


filterme = RedisFilter(name="baidu:spider")
