# -*- coding: utf-8 -*-
# @Author: xiaodong
# @Date  : 2021/8/28

import json


def pp(out):
    print(json.dumps(out, indent=2, ensure_ascii=False))
