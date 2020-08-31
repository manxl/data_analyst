import pandas as pd
import numpy as np
from dao.db_pool import get_engine
from sqlalchemy.types import VARCHAR, Integer, DATE, DECIMAL, INT, BIGINT, FLOAT, Float, NUMERIC
import re


# import conf.config as config
# import stock.calc as calc
# import dao.tushare_dao as tsdao


def drop_more_nan_row(df, column_name):
    pass


def test():
    import pandas as pd
    a = pd.DataFrame({'a': [1, 2, 3], 'b': [2, 3, 4]})
    b = pd.DataFrame({'c': [11, 22, 33], 'd': [22, 33, 44]})
    c = pd.concat([a, b], axis=1)
    print('-' * 32, 'a')
    print(a)
    print('-' * 32, 'b')
    print(b)
    print('-' * 32, 'c')
    print(c)
    d = c.reset_index().rename(columns={'index': 'i'})
    print(d)
    d['c'] = 'kknd'
    print(d)
    # e = d.sort_index(by=['i'], ascending=False)
    e = d.sort_values(by=['i'], ascending=False)
    print(e)


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
