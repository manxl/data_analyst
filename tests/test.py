import pandas as pd
import numpy as np
from dao.db_pool import get_engine
from sqlalchemy.types import VARCHAR, Integer, DATE, DECIMAL, INT, BIGINT, FLOAT, Float, NUMERIC
import re, inspect


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

    d_1 = c['a'] + c['b'] > c['d']
    d_2 = c['a'] + c['b'] > c['c'] * 1.6
    c['c13'] = c['c'] * 1.3
    print(c)
    print(d_1)
    print(d_2)


def test_mp():
    a = {'k1': {'a': 2, 'b': 1}, 'k2': {'a': 4, 'b': 3}}
    for k, v in a.items():
        print(k)
        print('\ta:', v['a'])
        print('\tb:', v['b'])
        print('\tper:%-3.2f' % (v['a'] / v['b']))

    df = pd.DataFrame({'name': ['Addy', 'Billy', 'Merian']})

    sql = "SELECT * FROM tb_attribute WHERE name in ({}) and is_delete = 0".format(
        ','.join(["'%s'" % item for item in df.name]))

    print(sql)


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
    # test()
    # test_func('fa', 'fb')
    test_mp()
