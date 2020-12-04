import tushare as ts
import pandas as pd
import numpy as np
import base64
from io import BytesIO
import time, sys, os
from dao.db_pool import get_engine

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from matplotlib.gridspec import GridSpec
from analyse.my_data import *

p = os.path.dirname


def plot_nincome_roe_pe_meta(ts_code, render=None):
    df = get_nincome_roe_pe_meta(ts_code)
    t = df.index

    fig = plt.figure(constrained_layout=True, figsize=(12, 6))
    gs = GridSpec(3, 3, figure=fig)
    main = fig.add_subplot(gs[:2, :])

    sub = main.twinx()  # instantiate a second axes that shares the same x-axis
    down = fig.add_subplot(gs[2, :])

    sub.set_xlabel('Year')
    sub.set_ylabel('ROE')
    sub.plot(df['r3'], label='R3')
    sub.plot(df['r6'], label='R6')
    sub.plot(df['r9'], label='R9')
    sub.plot(df['r12'], label='R12')
    sub.legend()

    sub.tick_params(axis='y')

    # main.set_ylabel('NR ({})'.format(len(str(int(df['n12'].max())))))
    main.set_ylabel('NR')
    width = 0.45

    main.bar(t - 0.5 * width, df['n3'], width, label='n3')
    main.bar(t - 0.2 * width, df['n6'], width, label='n6')
    main.bar(t + 0.1 * width, df['n9'], width, label='n9')
    main.bar(t + 0.4 * width, df['n12'], width, label='n12')
    main.legend()
    main.tick_params(axis='y')

    # fig.tight_layout()  # otherwise the right y-label is slightly clipped
    df['pe12'] = df['pe12'].apply(lambda x: 100 if x > 100 else x)
    df['pe_ttm12'] = df['pe_ttm12'].apply(lambda x: 100 if x > 100 else x)
    df[['pe12', 'pe_ttm12']].plot(ax=down)
    # down.semilogy(df.index, df['pe'],label='PE')
    down.scatter(2019, df.loc[2020]['p2'], marker='o', s=35, label='Persons W&H')
    down.scatter(2019.5, df.loc[2020]['pt2'], marker='o', s=35, label='Persons W&H')
    # down.legend(('pe', 'pe_ttm'))

    return show(fig, render)


def plot_hbar_by_dfs(dfs, render=None):
    total_row = 0
    for df in dfs[0]:
        total_row += len(df)

    h = 5 / 10 * total_row

    fig, axs = plt.subplots(2, 2, figsize=(9.2, h))

    # for ax_idx, df in enumerate(dfs):
    #     if len(dfs) == 1:
    #         ax = axs
    #     else:
    #         ax = axs[ax_idx]

    for row in range(2):
        for col in range(2):
            df = dfs[row][col]
            ax = axs[row][col]

            # ax.invert_yaxis()
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
                    if type(df.index.max()) is not str:
                        y = df.index.max() - y
                    ax.text(x, y, str(int(c)), ha='center', va='center', color=text_color)

            if col == 0:
                ax.legend(ncol=len(category_names), bbox_to_anchor=(0, 1), loc='lower left', fontsize='small')

    return show(fig, render)


def show(fig, render=None):
    if render == 'web':
        p = os.path.dirname
        f = p(p(__file__)).__add__('/web/static/temp/{}')
        f_name = str(time.time()) + '.png'
        w = f.format(f_name)
        plt.savefig(w)
        return w[w.find('/web/') + 4:]
    elif render:
        plt.show()
    else:
        return get_plot_base64(fig)


def get_plot_base64(fig):
    buf = BytesIO()
    fig.savefig(buf, format="png")
    data = base64.b64encode(buf.getbuffer()).decode("ascii")
    return data


def plot_balancesheet(ts_code, render=None):
    dfs_liab = []
    t = 'liab'
    d1 = get_balancesheet_df(ts_code, t, 2019)
    if d1 is not None:
        dfs_liab.append(d1)
    d2 = get_balancesheet_df(ts_code, t)
    if d2 is not None:
        dfs_liab.append(d2)

    dfs_assert = []
    t = 'assert'
    d1 = get_balancesheet_df(ts_code, t, 2019)
    if d1 is not None:
        dfs_assert.append(d1)
    d2 = get_balancesheet_df(ts_code, t)
    if d2 is not None:
        dfs_assert.append(d2)

    return plot_hbar_by_dfs([dfs_assert, dfs_liab], render=render)


if __name__ == '__main__':
    plot_balancesheet('002027.SZ', render='a')
