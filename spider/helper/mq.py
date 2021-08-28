# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2021/8/28

from redis import Redis

from spider.config import REDIS_DB, REDIS_PORT, REDIS_PASSWORD, REDIS_HOST


class MQ:

    def push(self, message):
        pass

    def pull(self):
        pass


class RedisMQ(MQ):

    def __init__(self, topic, host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=REDIS_DB):
        self.topic = topic
        self.rds = Redis(host=host, port=port, db=db, password=password, decode_responses=True)
        self.preprocess()

    def preprocess(self):
        self.ps = self.rds.pubsub()
        self.ps.subscribe(self.topic)

    def push(self, message):
        self.rds.publish(self.topic, message)

    def pull(self, timeout=0):
        try:
            return self.ps.get_message(timeout=timeout)["data"]
        except TypeError:
            return None

class RedisMQ2(MQ):

    def __init__(self, topic, host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=REDIS_DB):
        self.topic = topic
        self.rds = Redis(host=host, port=port, db=db, password=password, decode_responses=True)

    def push(self, message):
        self.rds.rpush(self.topic, message)

    def pull(self, timeout=0):
        return self.rds.lpop(self.topic)


mq = RedisMQ2

moviemq = mq("movie")
actormq = mq("actor")

movie_with_actor = mq("movie-with-actor")
