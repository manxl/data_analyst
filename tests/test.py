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


def i():
    sql = """select
    a.*,
    f.tangible_asset,a.total_assets - a.total_liab  - a.intan_assets
        -r_and_d-goodwill -lt_amor_exp -defer_tax_assets - oth_nca
        as tg,
    a.total_hldr_eqy_inc_min_int  - a.intan_assets as tb,
    a.total_hldr_eqy_inc_min_int as tc
from stock_balancesheet a,
     stock_month_matrix_basic b,
     stock_fina_indicator f
where a.ts_code = b.ts_code and a.ts_code = f.ts_code
  and a.y = b.y and a.y = f.y
  and a.m = b.m and a.m = f.m
  and a.ts_code = '601318.SH'
  and a.y = 2019
  and a.m = 12;"""
    sql = """select *
from stock_income i
where i.ts_code = '601318.SH' and i.y = 2019 and i.m= 12;"""
    df = pd.read_sql_query(sql, get_engine())
    for name in df.columns.values:
        print("{}\t{}".format(name,df.loc[0][name]))

if __name__ == '__main__':
    # test()
    # test_func('fa', 'fb')
    # test_mp()
    # i()
    import tushare as ts
    __pro = ts.pro_api()
    # data = __pro.daily_basic(ts_code='601318.SH', trade_date='20191231')
    data = __pro.income(ts_code='601318.SH', trade_date='income')

    print(data)