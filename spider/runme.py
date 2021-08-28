# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2021/8/28

import json
import logging
import pathlib

import requests
from py2neo import cypher_escape as escape

from spider.config import START_URL, START_URL_PREFIX
from spider.helper.ua import get_ua
from spider.helper.prettprint import pp
from spider.parse import ParseActor, ParseDesc, ParseActIn
from spider.helper.filter import filterme
from spider.helper.mq import moviemq, movie_with_actor


logger = logging.getLogger(__name__)
DATA_DIR = pathlib.Path(__file__).parent.parent.joinpath("data")


class Runme:

    def get_headers(self):
        return {"User-Agent": get_ua()}

    def full_url(self, url):
        if not url.startswith(START_URL_PREFIX):
            url = START_URL_PREFIX + url
        return url

    def cut_url(self, url):
        if url.startswith(START_URL_PREFIX):
            url = url[len(START_URL_PREFIX):]
        return url

    def runme(self, url):

        logger.debug("[url] %s", url)

        if filterme.exist(self.cut_url(url)):
            return

        logger.debug("[put url] %s", self.cut_url(url))
        filterme.put(self.cut_url(url))

        logger.debug("[get url] %s", self.full_url(url))
        response = requests.get(self.full_url(url), headers=self.get_headers())
        actors_short = ParseActor(response.text).pipeline()
        movie_long = ParseDesc(response.text).pipeline()

        for actor, actor_info in actors_short.items():
            logger.info("[actor name] %s", actor)
            url = START_URL_PREFIX + actor_info["url"]
            response = requests.get(self.full_url(url), headers=self.get_headers())
            actor_long = ParseDesc(response.text).pipeline()
            movie_short = ParseActIn(response.text).pipeline()

            for moviename, movieurl in movie_short.items():
                logger.debug("[mq push] moviename: %s, movieurl: %s", moviename, movieurl)
                moviemq.push(movieurl)

            movie_with_actor.push(
                json.dumps({
                    "movie": movie_long,
                    "actor": actor_long
                }, ensure_ascii=False)
            )


if __name__ == '__main__':

    logging.basicConfig(format="[%(asctime)s] %(levelname)s %(message)s", level=logging.INFO)

    runme = Runme()
    moviemq.push(START_URL)
    url = moviemq.pull()

    while url:
        try:
            runme.runme(url)
        except:
            logger.warning("[error] %s", url)
        url = moviemq.pull()
