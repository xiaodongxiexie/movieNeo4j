# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2021/8/28

import re

from py2neo import Graph, cypher_escape as escape
from jinja2 import Template, environment

from spider.config import NEO4J_PASSWORD, NEO4J_URL


def split(obj) -> list:
    return [obj for obj in re.split("[、，\n]", obj) if obj.strip()]


def compose(obj, style="、") -> str:
    return style.join(split(obj))


def fix(d: dict):
    out = {}
    for k, v in d.items():
        if not v.strip():
            continue
        if v == "null":
            continue
        out[k] = escape(v)
    return out


environment.DEFAULT_FILTERS["compose"] = compose
environment.DEFAULT_FILTERS["split"] = split


class Neo4j:
    graph = Graph(NEO4J_URL, password=NEO4J_PASSWORD)

    @classmethod
    def commit2neo4j(cls, detail: list):
        tx = cls.graph.begin()
        # tx.run("CREATE CONSTRAINT ON (movie: 电影) ASSERT movie.中文名 IS UNIQUE;")
        # tx.run("CREATE CONSTRAINT ON (actor: 演员) ASSERT actor.中文名 IS UNIQUE;")

        tx.run("""
                UNWIND $detail as detail
                MERGE (movie: 电影 {中文名: detail["movie"]["中文名"]}) 
                   ON CREATE SET 
                       movie.外文名 = detail["movie"]["外文名"],
                       movie.全球票房 = detail["movie"]["全球票房"],
                       movie.其他译名 = detail["movie"]["其他译名"],
                       movie.类型 = detail["movie"]["类型"],
                       movie.出品公司 = detail["movie"]["出品公司"],
                       movie.制片地区 = detail["movie"]["制片地区"],
                       movie.拍摄日期 = detail["movie"]["拍摄日期"],
                       movie.拍摄地点 = detail["movie"]["拍摄地点"],
                       movie.发行公司 = detail["movie"]["发行公司"],
                       movie.导演 = detail["movie"]["导演"],
                       movie.编剧 = detail["movie"]["编剧"],
                       movie.制片人 = detail["movie"]["制片人"],
                       movie.主演 = detail["movie"]["主演"],
                       movie.片长 = detail["movie"]["片长"],
                       movie.上映时间 = detail["movie"]["上映时间"],
                       movie.票房 = detail["movie"]["票房"],
                       movie.对白语言 = detail["movie"]["对白语言"],
                       movie.色彩 = detail["movie"]["色彩"],
                       movie.imdb编码 = detail["movie"]["imdb编码"],
                       movie.主要奖项 = detail["movie"]["主要奖项"],
                       movie.在线播放平台 = detail["movie"]["在线播放平台"],
                       movie.出品时间 = detail["movie"]["出品时间"],
                       movie.制片成本 = detail["movie"]["制片成本"],
                       movie.前作 = detail["movie"]["前作"],
                       movie.create_date = LOCALDATETIME() + DURATION({hours: 8}),    // 转成本地时区
                       movie.update_date = LOCALDATETIME() + DURATION({hours: 8})
                   ON MATCH SET
                       movie.update_date =  
                           CASE 
                             WHEN movie.全球票房 <> detail["movie"]["全球票房"] THEN LOCALDATETIME() + DURATION({hours: 8})
                             ELSE movie.update_date 
                           END,   // 如果价格无变化则不更新时间
                       movie.全球票房 = detail["movie"]["全球票房"],
                       movie.外文名 = detail["movie"]["外文名"],
                       movie.全球票房 = detail["movie"]["全球票房"],
                       movie.其他译名 = detail["movie"]["其他译名"],
                       movie.类型 = detail["movie"]["类型"],
                       movie.出品公司 = detail["movie"]["出品公司"],
                       movie.制片地区 = detail["movie"]["制片地区"],
                       movie.拍摄日期 = detail["movie"]["拍摄日期"],
                       movie.拍摄地点 = detail["movie"]["拍摄地点"],
                       movie.发行公司 = detail["movie"]["发行公司"],
                       movie.导演 = detail["movie"]["导演"],
                       movie.编剧 = detail["movie"]["编剧"],
                       movie.制片人 = detail["movie"]["制片人"],
                       movie.主演 = detail["movie"]["主演"],
                       movie.片长 = detail["movie"]["片长"],
                       movie.上映时间 = detail["movie"]["上映时间"],
                       movie.票房 = detail["movie"]["票房"],
                       movie.对白语言 = detail["movie"]["对白语言"],
                       movie.色彩 = detail["movie"]["色彩"],
                       movie.imdb编码 = detail["movie"]["imdb编码"],
                       movie.主要奖项 = detail["movie"]["主要奖项"],
                       movie.在线播放平台 = detail["movie"]["在线播放平台"],
                       movie.出品时间 = detail["movie"]["出品时间"],
                       movie.制片成本 = detail["movie"]["制片成本"],
                       movie.前作 = detail["movie"]["前作"],
                       movie.create_date = movie.create_date
        
                MERGE (actor: 演员 {中文名: detail["actor"]["中文名"]})
                    ON CREATE SET 
                        actor.外文名 = detail["actor"]["外文名"],
                        actor.国籍 = detail["actor"]["国籍"],
                        actor.民族 = detail["actor"]["民族"],
                        actor.出生地 = detail["actor"]["出生地"],
                        actor.出生日期 = detail["actor"]["出生日期"],
                        actor.星座 = detail["actor"]["星座"],
                        actor.血型 = detail["actor"]["血型"],
                        actor.身高 = detail["actor"]["身高"],
                        actor.毕业院校 = detail["actor"]["毕业院校"],
                        actor.职业 = detail["actor"]["职业"],
                        actor.经纪公司 = detail["actor"]["经纪公司"],
                        actor.代表作品 = detail["actor"]["代表作品"],
                        actor.主要成就 = detail["actor"]["主要成就"],
                        actor.配偶 = detail["actor"]["配偶"]
                
                MERGE (actor)-[:出演] -> (movie)
               """,
               detail=detail)
        tx.commit()

    @classmethod
    def commit2neo4j2(cls, detail: dict):
        movie = fix(detail["movie"])
        actor = fix(detail["actor"])
        tx = cls.graph.begin()
        template = """
                MERGE (movie: 电影 {中文名: "{{movie.中文名}}"})
                    ON CREATE SET
                        {% for k, v in movie.items() -%}
                            {%- if loop.last -%}
                                movie.{{k}} = '{{v | compose('、')}}'
                            {%- else -%}
                                movie.{{k}} = '{{v | compose('、')}}',
                            {% endif -%}
                        {% endfor %}
                    ON MATCH SET
                        {% if movie | length > 3 %}
                            {% for k, v in movie.items() -%}
                                {%- if loop.last -%}
                                    movie.{{k}} = '{{v | compose('、')}}'
                                {%- else -%}
                                    movie.{{k}} = '{{v | compose('、')}}',
                                {% endif -%}
                            {% endfor %}
                        {% endif %}
                {% if '色彩' in movie %}
                 MERGE (color: 电影色彩 {色彩: "{{movie.色彩}}"})
                    MERGE (movie) -[:色彩] -> (color)
                {% endif %} 

                 {%- if '对白语言' in movie %}
                 MERGE (language: 对白语言 {对白语言: "{{movie.对白语言}}"})  
                    MERGE (movie) -[:对白语言] -> (language)
                {% endif %}

                {% if style %}
                MERGE (style: 类型 {电影类型: "{{style}}"})
                    merge (movie) -[:类型] -> (style)
                {% endif %} 

                {% if person %}
                MERGE (person: 人物 {中文名: "{{person}}"})
                    merge (movie) <-[:{{role}}] - (person)
                {% endif %}

                {% if company %}
                MERGE (company: 公司 {公司: "{{company}}"})
                    merge (movie) <-[:{{control}}] - (company)
                {% endif %} 

                {% if location %}
                MERGE (location: 位置 {位置: "{{location}}"})
                    merge (movie) -[:{{do}}] -> (location)
                {% endif %}   

                {% if award %}
                MERGE (award: 主要奖项 {主要奖项: "{{award}}"})
                    merge (movie) -[:{{get}}] -> (award)
                {% endif %}  

                {% if player %}
                MERGE (player: 播放平台 {播放平台: "{{player}}"})
                    merge (movie) -[:{{play}}] -> (player)
                {% endif %}  

                {% if previous %}
                MERGE (movie2: 电影 {中文名: "{{previous}}"})
                    merge (movie) -[:前作] -> (movie2)
                {% endif %}
            """

        template2 = """
                MERGE (movie: 电影 {中文名: "{{movie.中文名}}"})
                MERGE (person: 人物 {中文名: "{{person.中文名}}"})
                    ON CREATE SET
                        {% for k, v in person.items() -%}
                            {%- if loop.last -%}
                                person.{{k}} = '{{v | compose('、')}}'
                            {%- else -%}
                                person.{{k}} = '{{v | compose('、')}}',
                            {% endif -%}
                        {% endfor %}
                {% if person | length > 3 %}
                    ON MATCH SET
                        {% for k, v in person.items() -%}
                            {%- if loop.last -%}
                                person.{{k}} = '{{v | compose('、')}}'
                            {%- else -%}
                                person.{{k}} = '{{v | compose('、')}}',
                            {% endif -%}
                        {% endfor %}
                {% endif %}

                MERGE (movie) <-[:出演] - (person)

                {% if '国籍' in person %}
                 MERGE (country: 国籍 {国籍: "{{person.国籍}}"})
                    MERGE (person) -[:国籍] -> (country)
                {% endif %} 

                {% if '民族' in person %}
                 MERGE (nation: 民族 {民族: "{{person.民族}}"})
                    MERGE (person) -[:民族] -> (nation)
                {% endif %} 

                {% if '出生地' in person %}
                 MERGE (location: 位置 {出生地: "{{person.出生地}}"})
                    MERGE (person) -[:出生地] -> (location)
                {% endif %} 

                {% if '星座' in person %}
                 MERGE (constellation: 星座 {星座: "{{person.星座}}"})
                    MERGE (person) -[:星座] -> (constellation)
                {% endif %} 

                {% if '血型' in person %}
                 MERGE (blood: 血型 {血型: "{{person.血型}}"})
                    MERGE (person) -[:血型] -> (blood)
                {% endif %} 

                {% if '毕业院校' in person %}
                 MERGE (school: 毕业院校 {毕业院校: "{{person.毕业院校}}"})
                    MERGE (person) -[:毕业于] -> (school)
                {% endif %} 

                {% if '职业' in person %}
                 MERGE (job: 职业 {职业: "{{job}}"})
                    MERGE (person) -[:职业] -> (job)
                {% endif %}

                 {% if '经纪公司' in person %}
                 MERGE (company: 公司 {经纪公司: "{{person.经纪公司}}"})
                    MERGE (person) -[:经纪公司] -> (company)
                {% endif %}

                {% if '代表作品' in person %}
                 MERGE (movie2: 电影 {中文名: "{{master_work}}"})
                    MERGE (person) -[:代表作] -> (movie2)
                {% endif %}

                {% if '主要成就' in person %}
                 MERGE (main_achievements: 主要奖项 {主要奖项: "{{main_achievements}}"})
                    MERGE (person) -[:主要奖项] -> (main_achievements)
                {% endif %} 

                {% if '配偶' in person %}
                 MERGE (person2: 人物 {中文名: "{{person.配偶}}"})
                    MERGE (person) -[:配偶] -> (person2)
                {% endif %} 

            """

        for style in split(movie.get("类型", "")):
            t = Template(template).render(movie=movie, style=style)
            tx.run(t)

        for person in split(movie.get("主演", "")):
            t = Template(template).render(movie=movie, person=person, role="主演")
            tx.run(t)

        for person in split(movie.get("导演", "")):
            t = Template(template).render(movie=movie, person=person, role="导演")
            tx.run(t)

        for person in split(movie.get("编剧", "")):
            t = Template(template).render(movie=movie, person=person, role="编剧")
            tx.run(t)

        for person in split(movie.get("制片人", "")):
            t = Template(template).render(movie=movie, person=person, role="制片人")
            tx.run(t)

        for company in split(movie.get("出品公司", "")):
            t = Template(template).render(movie=movie, company=company, control="出品")
            tx.run(t)

        for company in split(movie.get("发行公司", "")):
            t = Template(template).render(movie=movie, company=company, control="发行")
            tx.run(t)

        for location in split(movie.get("制片地区", "")):
            t = Template(template).render(movie=movie, location=location, do="制片地区")
            tx.run(t)

        for location in split(movie.get("拍摄地点", "")):
            t = Template(template).render(movie=movie, location=location, do="拍摄地点")
            tx.run(t)

        for award in split(movie.get("主要奖项", "")):
            t = Template(template).render(movie=movie, award=award, get="主要奖项")
            tx.run(t)

        for player in split(movie.get("在线播放平台", "")):
            t = Template(template).render(movie=movie, player=player, play="播放平台")
            tx.run(t)

        for previous in split(movie.get("前作", "")):
            t = Template(template).render(movie=movie, previous=previous)
            tx.run(t)

        for job in split(actor.get("职业", "")):
            t = Template(template2).render(movie=movie, person=actor, job=job)
            tx.run(t)

        for master_work in split(actor.get("代表作品", "")):
            t = Template(template2).render(movie=movie, person=actor, master_work=master_work)
            tx.run(t)

        for main_achievements in split(actor.get("主要成就", "")):
            t = Template(template2).render(movie=movie, person=actor, main_achievements=main_achievements)
            tx.run(t)
        tx.commit()


neo4j = Neo4j()
commit2neo4j = neo4j.commit2neo4j
commit2neo4j2 = neo4j.commit2neo4j2


if __name__ == '__main__':

    import json
    import logging

    from spider.helper.mq import movie_with_actor

    logging.basicConfig(format="[%(asctime)s] %(levelname)s %(message)s", level=logging.INFO)

    logger = logging.getLogger("neo4j")

    s, error = 0, 0
    trace = None
    while s < 10000:
        detail = movie_with_actor.pull()
        detail = json.loads(detail)
        # movie_with_actor.push(json.dumps(detail, ensure_ascii=False))

        try:
            logger.info("[movie name] %s, [actor name] %s", detail["movie"]["中文名"], detail["actor"]["中文名"])
            commit2neo4j2(detail)
            s += 1
        except:
            if trace is None:
                trace = detail
            error += 1
            pass

        logger.info("[progress] %s, [error] %s", s, error)
