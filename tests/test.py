import pandas as pd
from dao.db_pool import get_engine
import datetime
from dao.db_pool import get_pro

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


# sql = """select * from index_weight where index_code = '399396.SZ' and con_code = '000799.SZ';"""
# d1 = pd.read_sql_query(sql, get_engine())
# d2 = d1[2:]

# finas = 'income,cashflow,fina_indicator,balancesheet'.split(',')
# for f in finas:
#     print("select count(*) from {} where ts_code = '300437.SZ' \nUNION all ".format(f))


biz_code = '000016.SH'
df = get_pro().index_weight( index_code=biz_code, start_date='20201001', end_date='20201101')
print(df)
