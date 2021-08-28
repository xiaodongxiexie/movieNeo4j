# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2021/8/28

import re
from collections import namedtuple
from collections import defaultdict

from lxml.etree import HTML


class P:

    def __init__(self, text):
        self.text = text

    def preprocess(self, text):
        text = text.replace("&nbsp;", "")
        return text

    def to_HTML(self, text):
        html = HTML(text)
        return html

    def pipeline(self):
        text = self.preprocess(self.text)
        html = self.to_HTML(text)
        return html


class ParseActor(P):

    def pipeline(self):
        """
        {
        actor_name:
            {
                "url": actor_url,
                "role_url": role_url,
                "role_name": role_name,
            }
        }
        """

        out = defaultdict(dict)
        html = super().pipeline()
        # 演员名称，角色名称，演员名称，角色名称...
        actors = html.xpath("//ul[@class='actorList']/li/dl/dt/a/text()")
        urls = html.xpath("//ul[@class='actorList']/li/dl/dt/a/@href")

        actor_and_url = namedtuple("actor_and_url", "actor url")
        for i, (actor, url) in enumerate(zip(actors, urls)):
            if i % 2 == 0:
                last = actor_and_url(actor, url)
            else:
                out[last.actor]["url"] = last.url
                out[last.actor]["role_url"] = url
                out[last.actor]["role_name"] = actor
        return out


class ParseDesc(P):

    def pipeline(self):
        out = {}
        html = super().pipeline()

        ks = html.xpath("//dt[@class='basicInfo-item name']/text()")
        vs = []
        for k in ks:
            vs.append(
                re.sub("\[\d+\]", "", html.xpath(f"string(//dt[contains(text(), '{k}')]/following-sibling::dd[1])"))
                .strip()
            )
        for k, v in zip(ks, vs):
            out[k] = v
        return out


class ParseActIn(P):

    def pipeline(self):
        out = {}
        html = super().pipeline()

        movies = html.xpath("//ul[@class='starWorksList']/li/div/p/b/a/text()")
        urls = html.xpath("//ul[@class='starWorksList']/li/div/p/b/a/@href")
        for movie, url in zip(movies, urls):
            out[movie] = url
        return out
