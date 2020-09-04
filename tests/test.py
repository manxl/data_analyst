import pandas as pd
import numpy as np
from dao.db_pool import get_engine
from sqlalchemy.types import VARCHAR, Integer, DATE, DECIMAL, INT, BIGINT, FLOAT, Float, NUMERIC
import re, inspect
from decimal import *


# import conf.config as config
# import stock.calc as calc
# import dao.tushare_dao as tsdao


def drop_more_nan_row(df, column_name):
    pass


def test():
    import pandas as pd
    a = pd.DataFrame({'a': [1, 2, 3], 'b': [2, 3, 4]})
    b = pd.DataFrame({'c': [4, 3, 2], 'd': [3, 2, 1]})
    c = pd.concat([a, b], axis=1)

    m1 = a['a'] % 2 != 0
    m2 = a['b'] > 2
    m3 = a['a'] == 3
    lst = [m1, m2, m3]
    r = None
    for i in range(0, len(lst)):
        if r is None:
            r = lst[i]
        else:
            r = r & lst[i]
        print(lst[i])

    a = a[r]
    print(a)


def test_mp():
    m = {'a': 1, 'b': 2, 'c': 3}
    df = pd.DataFrame(m, index=pd.Series(2019))
    print(df)


def lbd(x):
    return x ** 2, x ** 0.5


class Student:
    def __init__(self, name, age):
        self.__name = name
        self.__age = age

    @property
    def name(self):
        print('get name')
        return self.__name

    @name.setter
    def name(self, name):
        print('set name')
        self.__name = name


def match_names(file_path, dict_sheet, like):
    df_d = pd.read_excel(file_path, dict_sheet)
    for i, row in df_d.iterrows():
        desc = row['desc']
        name = row['name']
        r = '.*' + like + '.*'
        o = re.match(r, desc)
        if o:
            print('{}-{}'.format(desc, name))


if __name__ == '__main__':
    test()
    # test_func('fa', 'fb')
    # test_mp()
