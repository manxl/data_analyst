import stock.tushare_dao as tsdao
import pandas as pd
import numpy as np
from sqlalchemy.types import VARCHAR, DATE, INT, Integer


class Test:
    @staticmethod
    def test_date():
        con = tsdao.get_engine()

        sql = 'select * from trade_date_o where is_open = 1'

        df = pd.read_sql(sql, con)

        print(df.dtypes)
        df['y'] = df['cal_date'].apply(lambda s: int(s[:4]))
        df['m'] = df['cal_date'].apply(lambda s: int(s[4:6]))
        df['cal_date'] = df['cal_date'].astype('M8[D]')
        df.reset_index(drop=True)
        df.drop('index', 1)
        df.set_index(['y', 'm', 'cal_date'])
        df = df[df['is_open'] == 1]
        df = df.reindex(columns=['y', 'm', 'cal_date', 'is_open', 'exchange'])
        df.to_sql('trade_date_detail', tsdao.get_engine(),
                  index=False,
                  dtype={'cal_date': DATE(), 'y': Integer(), 'm': INT(), 'is_open': INT(), 'exchange': VARCHAR(8)}
                  , if_exists='replace', schema=tsdao.db_name)
        '''
        分组插入扩展表
        '''
        grouped_m = df.groupby(['y', 'm'])
        # for a, g in grouped_m:
        #     print(a)
        #     print(g)
        r1 = grouped_m['cal_date'].agg([np.min, np.max])
        r1 = r1.rename(columns={'amin': 'frist', 'amax': 'last'})
        r1['y'] = pd.Series(r1.index.get_level_values('y'), index=r1.index)
        r1['m'] = pd.Series(r1.index.get_level_values('m'), index=r1.index)

        grouped_m = df.groupby(['y'])
        r2 = grouped_m['cal_date'].agg([np.min, np.max])
        r2 = r2.rename(columns={'amin': 'frist', 'amax': 'last'})
        r2['y'] = pd.Series(r2.index.get_level_values('y'), index=r2.index)
        r2['m'] = pd.Series(np.zeros(len(r2)), index=r2.index)

        r = r1.append(r2, ignore_index=True)
        r = r.reindex(columns=['y', 'm', 'first', 'last'])
        r.to_sql('trade_date', tsdao.get_engine(),
                 index=False,
                 dtype={'start': DATE(), 'end': DATE(), 'y': Integer(), 'm': INT()}
                 , if_exists='replace', schema=tsdao.db_name)

    @staticmethod
    def test_info():
        data = {'a': list('abc'), 'b': np.arange(1, 4, 1)}
        df1 = pd.DataFrame(data)
        df2 = pd.DataFrame(data)
        df3 = df1.append(df2)
        df4 = df1.append(df2, ignore_index=True)
        print(df1)
        print(df2)
        print(df3)
        print(df4)
        d_map = {'a': 'kk', 'b': 44}
        df5 = df1.append(d_map, ignore_index=True)
        print(df5)
        print(df5.index)
        c = pd.Series(df5.index).apply(lambda x: x + 1)


if __name__ == '__main__':
    # Test.test_date()
    # Test.test_info()
    print(2**31)


