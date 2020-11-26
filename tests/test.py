import numpy as np
import pandas as pd
from dao.db_pool import get_engine, get_pro
import datetime
from dao.db_pool import get_pro
from tools.utils import df_add_y_m
from sqlalchemy.types import VARCHAR, DATE, INT, Float, DECIMAL, Integer, FLOAT
import matplotlib.pyplot as plt
from conf.config import *


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


def test_plot_2():
    import numpy as np

    # Fixing random state for reproducibility
    np.random.seed(19680801)

    dt = 0.01
    t = np.arange(0, 30, dt)
    nse1 = np.random.randn(len(t))  # white noise 1
    nse2 = np.random.randn(len(t))  # white noise 2

    # Two signals with a coherent part at 10Hz and a random part
    s1 = np.sin(2 * np.pi * 10 * t) + nse1
    s2 = np.sin(2 * np.pi * 10 * t) + nse2

    fig, axs = plt.subplots(2, 1)
    axs[0].plot(t, s1, t, s2)
    axs[0].set_xlim(0, 2)
    axs[0].set_xlabel('time')
    axs[0].set_ylabel('s1 and s2')
    axs[0].grid(True)

    cxy, f = axs[1].cohere(s1, s2, 256, 1. / dt)
    axs[1].set_ylabel('coherence')

    fig.tight_layout()
    plt.show()


def test_d():
    import numpy as np
    import matplotlib.pyplot as plt

    x = np.arange(0.1, 4, 0.1)
    y1 = np.exp(-1.0 * x)
    y2 = np.exp(-0.5 * x)

    y1err = 0.1 + 0.1 * np.sqrt(x)
    y2err = 0.1 + 0.1 * np.sqrt(x / 2)

    fig, (ax0, ax1, ax2) = plt.subplots(nrows=1, ncols=3, sharex=True,
                                        figsize=(12, 6))

    ax0.set_title('all errorbars')
    ax0.errorbar(x, y1, yerr=y1err)
    ax0.errorbar(x, y2, yerr=y2err)

    ax1.set_title('only every 6th errorbar')
    ax1.errorbar(x, y1, yerr=y1err, errorevery=6)
    ax1.errorbar(x, y2, yerr=y2err, errorevery=6)

    ax2.set_title('second series shifted by 3')
    ax2.errorbar(x, y1, yerr=y1err, errorevery=(0, 6))
    ax2.errorbar(x, y2, yerr=y2err, errorevery=(3, 6))

    fig.suptitle('Errorbar subsampling')
    plt.show()


def test_set_color():
    import matplotlib.pyplot as plt
    import numpy as np

    # 生成x y
    np.random.seed(0)
    x = np.arange(5)
    y = np.random.randint(-5, 5, 5)
    # 添加颜色
    v_bar = plt.bar(x, y, color='blue')

    # 对y值大于0设置为蓝色  小于0的柱设置为绿色
    for bar, x in zip(v_bar, x):
        if x > 2:
            bar.set(color='green')

    plt.show()


def survey(results, category_names):
    labels = list(results.keys())
    data = np.array(list(results.values()))
    data_cum = data.cumsum(axis=1)
    category_colors = plt.get_cmap('RdYlGn')(np.linspace(0.15, 0.85, data.shape[1]))

    h = 5 / 10 * len(results)

    fig, ax = plt.subplots(figsize=(9.2, h))
    ax.invert_yaxis()
    ax.xaxis.set_visible(False)
    ax.set_xlim(0, np.sum(data, axis=1).max())

    for i, (colname, color) in enumerate(zip(category_names, category_colors)):
        widths = data[:, i]
        starts = data_cum[:, i] - widths
        ax.barh(labels, widths, left=starts, height=0.5, label=colname, color=color)
        xcenters = starts + widths / 2

        r, g, b, _ = color
        text_color = 'white' if r * g * b < 0.5 else 'darkgrey'
        for y, (x, c) in enumerate(zip(xcenters, widths)):
            ax.text(x, y, str(int(c)), ha='center', va='center', color=text_color)
    ax.legend(ncol=len(category_names), bbox_to_anchor=(0, 1), loc='lower left', fontsize='small')

    return fig, ax


def test_set_percent():
    category_names = ['Strongly disagree', 'Disagree',
                      'Neither agree nor disagree', 'Agree', 'Strongly agree']
    results = {
        'Question 1': [10, 15, 17, 32, 26],
        'Question 2': [26, 22, 29, 10, 13],
        'Question 3': [35, 37, 7, 2, 19],
        'Question 4': [32, 11, 9, 15, 33],
        'Question 5': [21, 29, 5, 5, 40],
        'Question 6': [8, 19, 5, 30, 38]
    }

    # for k,v in results.

    survey(results, category_names)
    plt.show()


def get_data(ts_code, y=None):
    if not y:
        sql = f"""select
	b.y,
	notes_receiv,accounts_receiv,prepayment,inventories,total_cur_assets
# ,total_assets
# 	,notes_payable,acct_payable,adv_receipts,total_cur_liab,total_liab,b.ts_code
from 
	balancesheet b
where 
	b.ts_code  = '{ts_code}' and b.m = 12 
order by 
	b.y desc;"""
    else:
        sql = f"""select
	notes_receiv,accounts_receiv,prepayment,inventories,total_cur_assets,b.ts_code
from 
	stock_basic s,balancesheet b 
where 
	s.ts_code = b.ts_code
  and s.industry in (select industry from stock_basic where ts_code = '{ts_code}')
	and b.y = {y} and b.m = 12 ;"""
    df = pd.read_sql_query(sql, get_engine())

    if 'y' in df.columns:
        df = df.set_index(['y'])
    elif 'ts_code' in df.columns:
        print('mark')
        df.loc[df[df['ts_code'] == ts_code].index[0], ['ts_code']] = ts_code + '(*)'
        df = df.set_index(['ts_code'])
    else:
        raise AttributeError('Nether y nor ts_code must in dataframe!')

    v_list = df.columns.values.tolist()
    last_col = v_list[len(v_list) - 1]

    for col in df.columns.values.tolist():
        # delete nan
        df[col] = df[col].apply(lambda x: 0 if x != x else x)
        # calc last_col
        if last_col != col:
            df[last_col] = df[last_col] - df[col]

    sm = df.sum(axis=1)

    for col in df.columns.values.tolist():
        df[col] = round(df[col] * 100 / sm, 2)

    print(df.sum(axis=1))

    results = {}
    for key in df.index:
        arr = []
        results[key] = arr
        for col in v_list:
            arr.append(df.loc[key][col])

    print(results)

    # return v_list, results
    return df


def survey_df(dfs):
    total_row = 0
    for df in dfs:
        total_row += len(df)

    h = 5 / 10 * total_row

    fig, axs = plt.subplots(len(dfs), figsize=(9.2, h))

    for ax_idx, df in enumerate(dfs):
        if len(dfs) == 1:
            ax = axs
        else:
            ax = axs[ax_idx]
        ax.invert_yaxis()
        ax.xaxis.set_visible(False)
        labels = list(df.index)
        # data = np.array(list(dfs[1].values()))
        data_cum = df.cumsum(axis=1)
        category_colors = plt.get_cmap('RdYlGn')(np.linspace(0.15, 0.85, df.shape[1]))

        ax.set_xlim(0, round(data_cum.max().max(), 0))

        category_names = df.columns.values.tolist()

        for i, (colname, color) in enumerate(zip(category_names, category_colors)):
            widths = df.iloc[:, i]
            starts = data_cum.iloc[:, i] - widths
            ax.barh(labels, widths, left=starts, height=0.5, label=colname, color=color)
            xcenters = starts + widths / 2

            r, g, b, _ = color
            text_color = 'white' if r * g * b < 0.5 else 'darkgrey'
            for y, (x, c) in enumerate(zip(xcenters, widths)):
                ax.text(x, y, str(int(c)), ha='center', va='center', color=text_color)
        ax.legend(ncol=len(category_names), bbox_to_anchor=(0, 1), loc='lower left', fontsize='small')

    return fig, ax


if __name__ == '__main__':
    # test_set_color()
    # test_set_percent()
    # names, results = get_data(TEST_TS_CODE_GZMT)
    # survey(results, names)
    # plt.show()

    plt.show()
