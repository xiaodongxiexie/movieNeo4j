# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2021/8/28

import os
import random
import pathlib
import logging

import requests
from lxml.etree import HTML
from diskcache import Cache

from spider.config import UA_URL


logger = logging.getLogger(__name__)


def make_uas_cache_dir():
    cache: pathlib.Path = pathlib.Path(__file__).parent.parent.parent.joinpath("data/ua_cache")
    if not cache.exists():
        logger.warning("[makedirs] %s", cache)
        os.makedirs(cache)
    return cache


def get_ua(cachedir: str = None):
    if cachedir is None:
        cachedir = make_uas_cache_dir()
    with Cache(cachedir, timeout=60) as cache:
        uas = cache.get("uas")
        if uas is None:
            uas = download_ua()
            cache.set("uas", uas)
        return random.choice(uas)


def download_ua():
    logger.warning("[ua][download][from] %s", UA_URL)
    r = requests.get(UA_URL)
    html = HTML(r.text)
    uas = html.xpath("//ul/li/a/text()")
    return uas
