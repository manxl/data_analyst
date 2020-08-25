import pandas as pd
import numpy as np
import matplotlib.pyplot as mp


class TestPandas:
    @staticmethod
    def test_series():
        data = np.array(['Tom', 'Lily', 'Jerry', 'Lilei'])
        # simple
        s = pd.Series(data)
        print(s)
        print(s[0])
        s = pd.Series(data, index=[10, 20, 30, 40])
        print(s)
        print(s[10])
        d = {'a': 0., 'b': 1., 'c': 2}
        s = pd.Series(d)
        print(s)
        print(s['b'])
        s = pd.Series(5, index=[1, 2, 3, 4, 5])
        print(s)

        # date
        date_series_str = pd.Series(['2011', '2011-02', '2011-03-01', '2014/04/01',
                                     '2011/05/01 01:01:01', '01 Jun 2011'])
        datas = pd.to_datetime(date_series_str)
        print(datas)
        delta = datas - pd.to_datetime('1970-01-01')
        print('-' * 32)
        print(delta)
        print(delta.dt.days)

        dr = pd.date_range('2019-08-20', periods=5, freq='D')
        print(dr)

        start = pd.datetime(2019, 8, 1)
        end = pd.to_datetime('2019/08/30')
        print('=' * 32)
        rd = pd.date_range(start, end)
        print(rd)

        datas = pd.bdate_range('2019/08/01', '2019/09/01')
        print(datas)

        d_map = [{'a': 1, 'b': 2}, {'a': 1, 'b': 2, 'c': 3}]
        datas = pd.DataFrame(d_map)
        print(datas)
        d_map = {'a': [1, 2, 3, 4], 'b': ['c1', 'c2', 'c3', 'c4']}
        datas = pd.DataFrame(d_map)
        print(datas)
        print(datas[1:3])

        print('==loc:\n', datas.loc[1])
        d_copy = datas[:]
        print('==d_copy:\n', d_copy)
        d_copy = d_copy.drop(2)
        print('==d_copy drop 2:\n', d_copy)
        d_copy.pop('a')
        print('==d_copy pop:\n', d_copy)
        d_copy['b'][1] = 'rename'
        print('==d_copy rename:\n', d_copy)
        print('==d_copy', d_copy.loc[1]['b'])
        print('== append')
        a = pd.DataFrame([['aa', 12], ['bb', 3]], columns=['a', 'b'])
        datas_append = datas.append(a)
        print(datas_append)
        r = datas_append.loc[0]
        print(r)
        i = np.arange(0, datas_append['a'].size)
        datas_append.reindex(index=i)
        print(datas_append)

    @staticmethod
    def test_sort_and_group():
        idx = [1, 4, 6, 2, 3, 5, 9, 8, 0, 7]
        df = pd.DataFrame(np.random.randn(10, 2),
                          index=idx,

                          columns=['a', 'b'])
        print(df)
        print('sort index:\n', df.sort_index())
        print('sort index ~ascending:\n', df.sort_index(ascending=False))
        print('sort val:\n', df.sort_values(by='a', ascending=True))
        print('sort val:\n', df.sort_values(by=['a', 'b'], ascending=[True, False]))

        print('=====min:\n', np.min(df[['a']]))
        print('=====max:\n', np.max(df[['a']]))
        print('=====mean:\n', np.mean(df[['a']]))
        print('=====median:\n', np.median(df[['a']]))
        # print('ptp:\n', np.ptp(df[['a']]))
        print('=====std:\n', np.std(df[['a']]))
        print('=====var:\n', np.var(df[['a']]))

        c = pd.Series(list('abcdabcdkb'), index=idx)
        df['adc'] = c
        print('=====df:\n', df)

        print('=====df sum:\n', df.sum())
        print('=====df sum(0):\n', df.sum(0))
        print('=====df sum(1):\n', df.sum(1))

        # ******  prod
        print('=====describe():\n', df.describe())
        print('=====describe( object):\n', df.describe(include=['object']))
        print('=====describe( number):\n', df.describe(include=['number']))

        # group
        grouped = df.groupby('adc')
        print('=====group\n', grouped)
        print('=====group groups\n', grouped.groups)
        print('=' * 32)
        for adc, group in grouped:
            print('adc:', adc)
            print(group)
        print(grouped['b'].agg(np.mean))
        print(grouped['b'].agg([np.mean, np.sum, np.std]))

        ot = pd.DataFrame({'name': ['Allan', 'Billy', 'Condom', 'Destory', 'En'], 'adc': list('abcde')})
        print('=====group inner\n', pd.merge(df, ot, how='inner'))
        print('=====group left\n', pd.merge(df, ot, how='left'))
        print('=====group right\n', pd.merge(df, ot, how='right'))
        print('=====group outer\n', pd.merge(df, ot, how='outer'))

        print(' group by')
        gen = list('aaaaabbbbb')
        df['gen'] = gen
        group_by = df.pivot_table(index=['adc', 'gen'])

        print(group_by)


if __name__ == '__main__':
    # TestPandas.test_series()
    TestPandas.test_sort_and_group()
