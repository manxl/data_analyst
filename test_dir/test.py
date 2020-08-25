import pandas as pd
import matplotlib.pyplot as mp
import numpy as np


class TestNumpy:

    @staticmethod
    def test_abc():
        pass

    @staticmethod
    def test_init_np():
        data = [('zs', [98, 70, 88], 16),
                ('ls', [91, 71, 81], 15),
                ('ww', [92, 72, 82], 17)]

        # data.sort(key=lambda x: x[1][2])
        print('=' * 32, 'method 1', '=' * 32)
        ary = np.array(data, dtype='U2,3int32,int32')
        print(ary, 'ww age:', ary[2]['f1'])

        #
        print('=' * 32, 'method 2', '=' * 32)
        ary = np.array(data, dtype=[('name', 'str', 2),
                                    ('score', 'int32', 3),
                                    ('age', 'int32', 1)])
        print(ary[1]['age'])

        #
        print('=' * 32, 'way 3', '=' * 32)
        ary = np.array(data, dtype={'names': ['name', 'scores', 'age'], 'formats': ['U2', '3int32', 'int32']})
        print('way 3 ', ary)

        print('=' * 32, 'way 4', '=' * 32)
        d = np.array(data, dtype={'name': ('U3', 0), 'scores': ('3int32', 16), 'age': ('int32', 32)})
        print(d[0]['name'])

        print('=' * 32, 'way 4', '=' * 32)
        d = np.array

    @staticmethod
    def init_datetime64():
        data = ['2011', '2011-01-02', '2012-01-01', '2012-02-01', '2012-02-01 10:11:12']

        print('=' * 32, 'origin', '=' * 32)
        dates = np.array(data)
        print(dates, dates.dtype)

        print('=' * 32, 'M8[D]', '=' * 32)
        dates = dates.astype('M8[M]')
        print(dates, dates.dtype)
        print(dates[2] - dates[0])

    @staticmethod
    def test_d():
        a = np.arange(1, 9)
        print("a", a)
        b = a.reshape(2, 4)
        print('b', b)
        c = b.reshape(2, 2, 2)
        print('c', c)
        d = c.ravel()
        print('d', d)

        print('=' * 32, 'view reshape')
        a = np.arange(1, 10)
        print(a, a.shape)
        b = a.reshape(3, 3)
        print(b, b.reshape)
        b[0, 0] = 999
        print('a:', a)
        print('b:', b)

        print('=' * 32, 'copy reshape')
        d = b.flatten()
        # d = b.ravel()
        print('d(1):', d)
        b[0, 0] = 333
        print('d(1):', d)

        d.shape = (3, 3)
        print(d)
        d.resize((9,))
        print(d)

    @staticmethod
    def test_slice():
        print('=' * 32, '1d')
        a = np.arange(1, 10)
        print(a[1:4:1])
        print(a[4:-1:1])
        print(a[4:1:1])
        print(a[-1:-4:-1])
        print(a[-1:-4:1])

        print('=' * 32, '3d')
        a = np.arange(1, 28)
        a.resize(3, 3, 3)
        print(a)
        print('=', 1)
        print(a[1, :, :])
        print('=', 2)
        print(a[:, 1, :])
        print('=', 3)
        print(a[:, :, 1])

        print(a[:, :, 1])

    @staticmethod
    def test_d():
        a = np.arange(0, 8).reshape(2, 4)
        b = np.arange(8, 16).reshape(2, 4)
        c = np.arange(16, 24).reshape(2, 4)
        d = np.arange(24, 32).reshape(2, 4)

        ap = np.dstack((a, b))
        bp = np.dstack((a, b))

        print('=' * 32, 'ap:\n')
        print(ap)
        print('=' * 32, 'bp:\n')
        print(bp)

        print('=' * 48, 'aa:\n')
        o = np.dstack((a, b, c, d))
        print(o)
        print('')
        print(np.dsplit(o, 2))

        print('o.shape:', o.shape)
        print('o.dtype:', o.dtype)
        print('o.ndim:', o.ndim)
        print('o.size:', o.size)
        print('o.itemsize:', o.itemsize)
        print('o.nbytes:', o.nbytes)
        print('o.real:\n', o.real)
        print('for:\n', [e for e in a.flat])
        print('o.tolist:\n', o.tolist())

    @staticmethod
    def test_np_a():
        a = np.ones(5)
        b = a * 2
        r = np.vstack((a, b))
        print(r)
        b = r[:, 1] / 5
        print(b)


class TestMetaplotlib:
    @staticmethod
    def test_mat_01():
        xs = np.arange(6)
        ys = np.array([12, 3, 4, 32, 44, 2])
        # xs = range(6)
        # ys = [12, 3, 4, 32, 44, 2]
        # curve line
        mp.plot(xs, ys)
        # mp.plot(xs, 3* ys / max(ys))

        m = max(ys)

        # straight line
        mp.vlines(2.5, 20, 30)
        mp.hlines(30, 2, 4)

        # curve line
        x = np.linspace(-np.pi / 2, 2 * np.pi, 1000)
        print(x.shape)
        sinx = np.sin(x)
        cosx = np.cos(x)
        mp.plot(x, sinx * m, linestyle='--', linewidth=4, label='sin')
        mp.plot(x, cosx * m, linestyle=':', color='green', label='cos')

        # set shown area
        mp.xlim(0, np.pi * 2)
        mp.ylim(0, m + 2)

        # set axis position
        ax = mp.gca()
        axis = ax.spines['top'].set_color('none')
        axis = ax.spines['right'].set_color('none')
        axis = ax.spines['left'].set_position(('data', 6 / 2))
        axis = ax.spines['bottom'].set_position(('data', m / 2))

        # axis.set_position(('data','aa'))

        # set scale
        xt = np.linspace(0, 6, 6 * 2 + 1)
        mp.xticks(xt)

        # show legend
        mp.legend()

        # render
        mp.show()


if __name__ == '__main__':
    # TestNumpy.test_np_a()
    # TestMetaplotlib.test_mat_01()
    import datetime as dt
