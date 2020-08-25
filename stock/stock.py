import tushare as ts
import pandas as pd
import numpy as np
import csv
import time

import os

p = os.path.dirname


class MyShare:

    @staticmethod
    def test_call_2_csv():
        pro = ts.pro_api()
        df = pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20180830')
        # df.to_hdf('hdf.h5', '000875')
        f_name = p(p(__file__)).__add__('/data/test_data_k.csv')
        df.to_csv(f_name)

    @staticmethod
    def test_call_2_csv_revert():
        pro = ts.pro_api()
        df = pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20180830')

        df = df[::-1]

        f_name = p(p(__file__)).__add__('/data/test_data_k.csv')

        df.to_csv(f_name)

    @staticmethod
    def test_read_from_csv():
        # ,ts_code,trade_date
        # ,open,high,low,
        # close,pre_close,
        # change,pct_chg,vol,amount
        f_name = p(p(__file__)).__add__('/data/test_data_k.csv')
        a = np.loadtxt(f_name, delimiter=',',
                       usecols=(2, 3, 4, 5, 6, 7),
                       skiprows=1,
                       unpack=False,
                       dtype='M8[D], f8, f8, f8, f8,f8',
                       # converters={0: lambda ymd: '-'.join([ymd[0:4], ymd[4:6], ymd[6:]])}
                       converters={2: lambda ymd: '-'.join([ymd.decode()[0:4], ymd.decode()[4:6], ymd.decode()[6:]])}
                       )

        print(a)


if __name__ == '__main__':
    # MyShare.test_read_from_csv()
    # MyShare.test_call_2_csv()
    MyShare.test_call_2_csv_revert()
