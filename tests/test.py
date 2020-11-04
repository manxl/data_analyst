import pandas as pd
import numpy as np
from dao.db_pool import get_engine
from sqlalchemy.types import VARCHAR, Integer, DATE, DECIMAL, INT, BIGINT, FLOAT, Float, NUMERIC
import re, inspect
from decimal import *

import tushare as ts
import conf.config as config
# import stock.calc as calc
# import dao.tushare_dao as tsdao
import abc


class C:
    def __init__(self, code):
        print('init')
        self._key = code

    @property
    def ts_code(self):
        return self._key

    @ts_code.setter
    def ts_code(self, value):
        self._key = value

    def process(self):
        print(self.ts_code)


# a = C('adsaa')
# print(a.ts_code)

growth = 1.04
revenge = 1.08
t = 0
for i in range(1,200):
    n = 1 * (growth ** i) / (revenge ** i)
    t = t + n
    print('{} - {} - {}'.format(i,n,t))


print(1/(revenge - growth))
