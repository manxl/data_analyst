import numpy as np
import os
import datetime as dt

p = os.path.dirname


class CsvDAO:
    @staticmethod
    def read_test_data_from_csv():
        # ,ts_code,trade_date
        # ,open,high,low,
        # close,pre_close,
        # change,pct_chg,vol,amount

        file_name = p(p(__file__)).__add__('/data/test_data_k.csv')
        return np.loadtxt(file_name, delimiter=',',
                          usecols=(2, 3, 4, 5, 6),
                          skiprows=3,
                          unpack=True,
                          dtype='M8[D], f8, f8, f8, f8',
                          converters={2: lambda ymd: '-'.join([ymd.decode()[0:4], ymd.decode()[4:6], ymd.decode()[6:]])}
                          )

    @staticmethod
    def read_test_data_from_csv_week_day():
        # ,ts_code,trade_date
        # ,open,high,low,
        # close,pre_close,
        # change,pct_chg,vol,amount

        file_name = p(p(__file__)).__add__('/data/test_data_k.csv')
        return np.loadtxt(file_name, delimiter=',',
                          usecols=(2, 3, 4, 5, 6),
                          skiprows=3,
                          unpack=True,
                          dtype='f8, f8, f8, f8, f8',
                          converters={2: lambda ymd: dt.datetime.strptime(ymd.decode(), '%Y%m%d').date().weekday()}
                          )
