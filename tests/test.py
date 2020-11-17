import pandas as pd
from dao.db_pool import get_engine, get_pro
import datetime
from dao.db_pool import get_pro
from tools.utils import df_add_y_m
from sqlalchemy.types import VARCHAR, DATE, INT, Float, DECIMAL, Integer, FLOAT


class C:
    def __init__(self, code, kk):
        print('init')
        self._key = code
        self.kk = None

    @property
    def ts_code(self):
        return self._key

    @ts_code.setter
    def ts_code(self, value):
        self._key = value

    def process(self):
        print(self.ts_code)


def init_month_matrix_basic():
    table_name = 'stock_month_matrix_basic'
    sql = 'select * from trade_date where m != 0 ;'
    yms = pd.read_sql_query(sql, get_engine())

    df = None
    for i, row in yms.iterrows():
        first_trade_date_str = row['first'].strftime('%Y%m%d')
        last_last_date_str = row['last'].strftime('%Y%m%d')
        data = get_pro().daily_basic(ts_code='', trade_date=last_last_date_str)
        print(last_last_date_str)
        if df is None:
            df = data
        else:
            df = df.append(data)
    df_add_y_m(df, 'trade_date')
    df.reset_index(drop=True)
    df = df.iloc[::-1]
    dtype = {'ts_code': VARCHAR(length=10), 'trade_date': DATE(), 'close': FLOAT(),
             'y': INT(), 'm': INT(),
             'turnover_rate': FLOAT(), 'turnover_rate_f': FLOAT(), 'volume_ratio': FLOAT(),
             'pe': FLOAT(), 'pe_ttm': FLOAT(), 'pb': FLOAT(),
             'ps': FLOAT(), 'ps_ttm': FLOAT(), 'dv_ratio': FLOAT(),
             'dv_ttm': FLOAT(), 'total_share': FLOAT(), 'float_share': FLOAT(),
             'free_share': FLOAT(), 'total_mv': FLOAT(), 'circ_mv': FLOAT()}
    df.to_sql(table_name, get_engine(), dtype=dtype, index=False, if_exists='append')


if __name__ == '__main__——':
    init_month_matrix_basic()


def create_obj():
    from controller.controllers import MyIndexController, BaseController
    k = type('MyIndexController',(BaseController,),{'biz_code':'tangchao'})
    import sys
    module_name = 'controller.controllers'
    module = __import__(module_name, fromlist=True)
    clz = getattr(module, 'MyIndexController')
    ctl = clz('tangchao')
    t = ctl.get_table_name()
    print(t)

class ABBBB():
    def __index__(self):
        pass

if __name__ == '__main__':
    # create_obj()
    a = ABBBB()
    k = []
    print(type(a) == list)
    print(type(k) == list)
