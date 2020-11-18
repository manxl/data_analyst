import tushare as ts
import pandas as pd
import numpy as np
import csv
import time, sys, os
from conf.config import *
from dao.db_pool import get_engine

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

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

    def test_plot(self):
        df = pd.DataFrame(np.arange(12).reshape(4, 3), columns=['aaa', 'bbb', 'c'])
        df['c'] = 10 * np.random.rand(len(df))
        p_r = (16, 9)
        fig, ax = plt.subplots(1, 1, figsize=p_r)
        ax1 = ax.twinx()

        @FuncFormatter
        def format(x, pos):
            return '%d' % x

        ax.yaxis.set_major_formatter(format)
        ax1.yaxis.set_major_formatter(format)
        df['c'].plot(ax=ax1, figsize=p_r, style=['g-'])
        ax1.legend(('helloworld',), loc='upper center')
        df[['aaa', 'bbb']].plot(kind='bar', grid=True, ax=ax,
                                figsize=(9, 7))  # 柱状图应该在折线图之后画，防止因为折线图所需要的坐标轴面积更小而让柱状图结果显示不全

        # ax.plot(df.index, [1, 2, 3, 4])
        # ax.legend(('new', 'aaa', 'bbb'))
        ax.legend(('aaa', 'bbb'))
        plt.show()

    def test_plot_with_my_df(self):
        d = get_df()
        dk = d.iloc[:4]
        dk.reset_index(drop=True, inplace=True)

        df = pd.DataFrame(np.arange(12).reshape(4, 3), columns=['aaa', 'bbb', 'c'])
        do = pd.DataFrame(np.arange(12).reshape(4, 3), columns=['aaa', 'bbb', 'c'])

        df['y'] = dk['yl']
        # dk['n3a'] = dk['n3'].apply(lambda x: int(x))
        # df['aaa'] = dk['n3']

        # df = df.set_index(['y'])
        # RangeIndex(start=2011, stop=2014, step=1)
        # df.index = list(df.index)
        df['y'] = df['y']
        df.index = list(df['y'].tolist())

        df['c'] = 10 * np.random.rand(len(df))
        p_r = (16, 9)
        fig, ax = plt.subplots(1, 1, figsize=p_r)
        ax1 = ax.twinx()

        @FuncFormatter
        def format(x, pos):
            return '%d' % x

        ax.yaxis.set_major_formatter(format)
        ax1.yaxis.set_major_formatter(format)

        k = df['c']
        k.index = df['c'].index - df['c'].index.min()
        k.plot(ax=ax1, figsize=p_r, style=['g-'])

        ax1.legend(('helloworld',), loc='upper center')
        df[['aaa', 'bbb']].plot(kind='bar', ax=ax)  # 柱状图应该在折线图之后画，防止因为折线图所需要的坐标轴面积更小而让柱状图结果显示不全
        ax.legend(('aaa', 'bbb'))

        # ax.plot(df.index, [1, 2, 3, 4])
        # ax.legend(('new', 'aaa', 'bbb'))

        plt.show()

    def test_2_plot(self):
        df = get_df()

        p_r = (16, 9)
        fig, ax = plt.subplots(1, 1, figsize=p_r)
        ax1 = ax.twinx()

        @FuncFormatter
        def format(x, pos):
            return '%d' % x

        ax.yaxis.set_major_formatter(format)
        ax1.yaxis.set_major_formatter(format)

        k = df['roe']
        k.index = k.index - k.index.min()

        k.plot(ax=ax, figsize=p_r, style=['g-'])

        ax.legend(('helloworld',), loc='upper center')

        df[['n3', 'n6', 'n9', 'n12']].plot(kind='bar', grid=True, ax=ax1,
                                           figsize=(9, 7))  # 柱状图应该在折线图之后画，防止因为折线图所需要的坐标轴面积更小而让柱状图结果显示不全

        ax1.legend(('n3', 'n6', 'n9', 'n12'))

        plt.show()


def make_plot(ts_code, render=None):
    df = get_df(ts_code)
    t = df.index

    fig, (main, down) = plt.subplots(2)
    sub = main.twinx()  # instantiate a second axes that shares the same x-axis

    color = 'tab:red'
    sub.set_xlabel('Year')
    sub.set_ylabel('ROE')
    sub.plot(df['r3'], label='R3')
    sub.plot(df['r6'], label='R6')
    sub.plot(df['r9'], label='R9')
    sub.plot(df['r12'], label='R12')
    sub.legend()

    sub.tick_params(axis='y')

    main.set_ylabel('NR ({})'.format(len(str(int(df['n12'].max())))))
    width = 0.45

    main.bar(t - 0.5 * width, df['n3'], width, label='n3')
    main.bar(t - 0.2 * width, df['n6'], width, label='n6')
    main.bar(t + 0.1 * width, df['n9'], width, label='n9')
    main.bar(t + 0.4 * width, df['n12'], width, label='n12')
    main.legend()
    main.tick_params(axis='y')

    # fig.tight_layout()  # otherwise the right y-label is slightly clipped
    df['pe'] = df['pe'].apply(lambda x: 100 if x > 100 else x)
    df['pe_ttm'] = df['pe_ttm'].apply(lambda x: 100 if x > 100 else x)
    df[['pe', 'pe_ttm']].plot(ax=down)
    # down.semilogy(df.index, df['pe'],label='PE')
    down.scatter(2019, df.loc[2020]['p2'], marker='o', s=35,
                 label='Persons W&H')
    down.scatter(2019.5, df.loc[2020]['pt2'], marker='o', s=35,
                 label='Persons W&H')
    # down.legend(('pe', 'pe_ttm'))

    if render:
        plt.show()
    else:
        p = os.path.dirname
        f = p(p(__file__)).__add__('/web/static/temp/{}')
        f_name = ts_code + str(time.time()) + '.png'
        w = f.format(f_name)
        plt.savefig(w)
        return w[w.find('/web/') + 4:]


def get_df(ts_code):
    sql_abc = """select 
	s.ts_code,f.y,f.m,f.end_date,
	i.n_income_attr_p ,
	f.roe,
	m.close ,m.pe,m.pe_ttm,
	d.close as c2 ,d.pe p2 ,d.pe_ttm pt2
from 
	stock_basic s,fina_indicator f, daily_basic_month m ,daily_basic d,income i
where 
	s.ts_code = f.ts_code and f.ts_code = m.ts_code and m.ts_code = d.ts_code and d.ts_code = i.ts_code
	and f.y = m.y  and m.y = i.y 
	and f.m = m.m  and m.m = i.m
	and s.ts_code = '{}'
	and f.y > 2010 
order by f.y ,f.m ;""".format(ts_code)
    all_abc = pd.read_sql_query(sql_abc, get_engine())
    df = all_abc[all_abc.m == 3]
    df['yl'] = df['y']
    df = df.set_index(['y'])
    for i in range(1, 5):
        t = all_abc[all_abc.m == 3 * i]
        t = t.set_index(['y'])
        df['n{}'.format(i * 3)] = t['n_income_attr_p']
        df['r{}'.format(i * 3)] = t['roe']

    return df


if __name__ == '__main__':
    m = MyShare()
    make_plot('600519.SH', render='a')
