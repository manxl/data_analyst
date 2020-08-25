import numpy as np
import matplotlib.pyplot as mp
import matplotlib.dates as md


# from dao.file_dao import read_test_data_from_csv, read_test_data_from_csv_week_day


class TestScience:
    @staticmethod
    def func_test():
        d_date, d_open, d_high, d_low, d_close = read_test_data_from_csv()
        std = np.std(d_close)
        print('std 0:{}'.format(std))
        mean = d_close.mean()
        print('mean:{}'.format(mean))
        # 离差方
        d = (d_close - mean) ** 2
        # 离差方均值
        v = np.mean(d)
        # 标准差
        std = v ** (1 / 2)
        print('std 1:', std)
        std = np.sqrt(v)
        print('std 2:', std)

    @staticmethod
    def test_ave():
        d_date, d_open, d_high, d_low, d_close = read_test_data_from_csv_week_day()
        avg = np.zeros(5)
        for w in range(avg.size):
            avg[w] = np.mean(d_close[d_date == w])
        print(avg)

    @staticmethod
    def test_super_slice_array():
        a = np.arange(1, 20)
        n = 5
        b = [a[i:i + n] for i in range(0, a.size, n)]
        print(b)
        c = [a[i - n:i] for i in range(n, a.size)]
        print(c)

    @staticmethod
    def test_apply_along_axis():
        def func(a):
            a.mean()

        arr = np.arange(1, 21).reshape(4, 5)
        r1 = np.apply_along_axis(lambda x: x.mean(), 0, arr)
        r2 = np.apply_along_axis(lambda x: x.mean(), 1, arr)

        print(arr)
        print('-' * 32)
        print(r1)
        print('-' * 32)
        print(r2)

    @staticmethod
    def test_super_k():
        d_date, d_open, d_high, d_low, d_close = read_test_data_from_csv()

        # mp.figure('New 000001.SZ K', facecolor='lightgray')
        mp.figure('New 000001.SZ K')
        mp.title('T 000002.SZ K')
        mp.xlabel('date')
        mp.ylabel('price')

        # mp.tick_params(labelsize=10)
        mp.grid(linestyle=':')

        ax = mp.gca()
        ax.xaxis.set_major_locator(md.WeekdayLocator(byweekday=md.MO))
        ax.xaxis.set_major_formatter(md.DateFormatter('%d %b %Y'))
        ax.xaxis.set_minor_locator(md.DayLocator())

        d_date = d_date.astype(md.datetime.datetime)
        mp.plot(d_date, d_close, alpha=0.3, color='dodgerblue', linewidth=2, linestyle='--', label='0001 close')

        rise = d_close >= d_open
        cm = np.array(['white' if x else 'green' for x in rise])
        ce = np.array(['red' if x else 'green' for x in rise])

        mp.bar(d_date, d_close - d_open, 0.8, d_open, color=cm, edgecolor=ce, zorder=3)
        n = 5
        # c_5 = [d_close[i - 5:i] for i in range(5, d_close.size)]
        # c_5 = np.array(c_5)
        # c_5 = np.mean(c_5, 1)
        # mp.vlines(d_date, d_low, d_high, color=ce, alpha=0.4)
        # mp.plot(d_date[5:], c_5, color='orangered', label='sma-5')

        # sma5 = np.zeros(d_close.size - 4)
        # for i in range(sma5.size):
        #     sma5[i] = d_close[i:i + 5].mean()
        # mp.plot(d_date[4:], sma5)

        co = np.convolve(d_close, np.ones(n) / 5, mode='valid')
        kernal = np.exp(np.linspace(0, 1, n))[::-1]
        kernal /= kernal.sum()
        ema53 = np.convolve(d_close, kernal, mode='valid')
        mp.plot(d_date[4:], co, color='red')
        mp.plot(d_date[4:], ema53, color='green', linewidth=7, alpha=0.3)

        stds = np.zeros(ema53.size)
        for i in range(stds.size):
            stds[i] = d_close[i:i + 5].std()

        upper = ema53 + 2 * stds
        lower = ema53 - 2 * stds
        mp.plot(d_date[4:], upper, color='orangered', label='upper')
        mp.plot(d_date[4:], lower, color='orangered', label='lower')

        mp.fill_between(d_date[4:], upper, lower, upper > lower, color='orangered', alpha=0.1)

        mp.gcf().autofmt_xdate()
        mp.legend()
        mp.show()

    @staticmethod
    def test_will():
        d_date, d_open, d_high, d_low, d_close = read_test_data_from_csv()
        n = 3
        a = np.zeros((n, n))
        for i in range(n):
            a[i,] = d_close[i: i + n]
        b = d_close[n:n * 2]
        x = np.linalg.lstsq(a, b)[0]
        pred = b.dot(x)
        print(d_close[:4 * n])
        print(a)
        print(b)
        print(x)
        print(pred)

    @staticmethod
    def test_base_science():
        d_date, d_open, d_high, d_low, d_close = read_test_data_from_csv()
        #  方差计算
        d_open = d_open[:5]

        std = d_open.std()
        d_delta = d_open - d_open.mean()
        std_my = (d_delta ** 2).mean() ** (1 / 2)

        print('-' * 32, "\npython's d_std:", std)
        print("\nhand d_delta:", d_delta)
        print("\nmy std:", std_my)

        # 协方差
        d_close = d_close[:5]
        o_d = d_open - d_open.mean()
        c_d = d_close - d_close.mean()
        o2c = np.mean(o_d * c_d)
        print('=' * 32, '\n协方差:', o2c)
        print('=' * 32, '\n协方差 - n:', o2c * d_close.size / (d_close.size - 1))

        # 相关系数
        relative = o2c / (d_open.std() * d_close.std())

        print('relative:', relative)
        # 相关矩阵
        print('cov:,', np.cov(d_open, d_close))
        print('corrcoef:', np.corrcoef(d_close, d_open))

    @staticmethod
    def test_smooth():
        d_date, d_open, d_high, d_low, d_close = read_test_data_from_csv()
        d = d_date[:]
        n = 7
        profit_a = np.diff(d_open)
        profit_b = np.diff(d_close)
        mp.figure('figure test_smooth')
        mp.title('title test_smooth')
        # orgine diff
        mp.plot(d[1:], profit_a, color='orangered', label='diff open', alpha=0.3, linestyle=':')
        mp.plot(d[1:], profit_b, color='blue', label='diff close', alpha=0.3, linestyle=':')

        # convolve dinoise
        convolve_sequnce = np.hanning(n)
        convolve_core = convolve_sequnce / convolve_sequnce.sum()

        convolve_a = np.convolve(profit_a, convolve_core, 'valid')
        convolve_b = np.convolve(profit_b, convolve_core, 'valid')

        mp.plot(d[n:], convolve_a, color='orangered', label='diff open', alpha=0.5, linestyle='--')
        mp.plot(d[n:], convolve_b, color='blue', label='diff close', alpha=0.5, linestyle='--')

        # get smooth
        days = d_date.astype(int)
        try:
            # 二逼问题 搞不懂
            np.polyfit(days[n:], convolve_a, 3)
        except Exception as a:
            print(a)
        func_a = np.polyfit(days[n:], convolve_a, 3)
        func_b = np.polyfit(days[n:], convolve_b, 3)
        v_a = np.polyval(func_a, days[n:])
        v_b = np.polyval(func_b, days[n:])

        mp.plot(d[n:], v_a, color='orangered', label='diff open')
        mp.plot(d[n:], v_b, color='blue', label='diff close')

        #  join point
        delta_func = np.polysub(func_a, func_b)
        print('functions:\n{}\n{}\n{}'.format(func_a, func_b, delta_func))
        xs = np.roots(delta_func)
        print('join points:', xs)
        ds = np.polyval(func_a, xs)
        xs = xs.astype('i4').astype('M8[D]')
        print('join points:', xs)
        mp.scatter(xs, ds)

        s = d_date.astype('M8[D]').astype('i4')
        vs = np.polyval(func_a, s)
        mp.scatter(d_date, vs)

        mp.gcf().autofmt_xdate()
        mp.figure()
        mp.show()

    @staticmethod
    def test_methods():
        d_date, d_open, d_high, d_low, d_close = read_test_data_from_csv()
        diff_open = np.diff(d_open)
        mp.figure('figure test methods ')
        mp.title('title test methods ')
        mp.bar(d_date[1:], diff_open, width=0.4, label='diff open')
        d_sign = np.sign(diff_open)
        mp.bar(d_date[1:], d_sign * 0.1, width=0.8, label='diff open', alpha=0.1)

        piece_wise = np.piecewise(diff_open,
                                  # [diff_open > 0.2, diff_open = 0.2 and diff_open >= -0.2, diff_open < -0.2],
                                  [diff_open > 0.2,
                                   (diff_open <= 0.2) & (diff_open >= -0.2),
                                   diff_open < -0.2],
                                  [0.4, 0.2, -0.3])

        mp.scatter(d_date[1:], piece_wise, label='piece wides')

        def foo(x, y):
            return np.sqrt(x ** 2 + y ** 2)

        a = np.arange(3, 9).reshape(2, 3)
        b = np.arange(4, 10).reshape(2, 3)
        print(a)
        print(b)
        ifoo = np.vectorize(foo)(a, b)
        print(ifoo)

        def test_trade(d_open, d_low, d_close):
            if d_low < (d_open * 0.99):
                return d_close - d_open * 0.99
            else:
                return np.nan

        test_result = np.vectorize(test_trade)(d_open, d_low, d_close)

        print('rest result:', test_result)
        nan_mask = np.isnan(test_result)
        mp.scatter(d_date[~nan_mask], test_result[~nan_mask], label='test_profit')
        print('test_result', test_result[~nan_mask].sum())

        mp.legend()
        mp.gcf().autofmt_xdate()
        mp.figure()
        mp.show()

    @staticmethod
    def test_matrix():
        # 逆
        a = np.mat('1 3 2;4 5 6 ; 7 8 9')
        i = np.mat('1 0 0 ;0 1 0;0 0 1')
        print(a * i)
        print(a.I)
        print(a * a.I)
        # 应用题
        a = np.mat('3 3.2;3.5 3.6')
        b = np.mat('118.4;135.2')
        s1 = np.linalg.lstsq(a, b)
        s2 = np.linalg.solve(a, b)
        s3 = a.I * b
        print(s1)
        print(s2)
        print(s3)

        # clip compress
        a = np.arange(1, 10)
        print(a)
        print(a.clip(3, 8))
        print(a.reshape(3, 3).clip(3, 8))
        print(a.compress(a > 4))
        print(a.reshape(3, 3).compress(a > 4))
        print(a.reshape(3, 3).compress(np.all([a > 4, a % 2 == 0], 0)))

    @staticmethod
    def test_array():
        a = np.arange(1, 7)
        print(a)
        print(np.add(a, a))
        print(np.add.reduce(a))
        print(a.sum())
        print(np.add.accumulate(a))

        print('-' * 32)
        print(np.add.outer([10, 20, 30], a))

        print(a.prod())
        print(a.cumprod())

        print('-' * 32, 'divide')
        a = np.array([20, 30, -20, -24])
        b = np.array([3, -3, -6, -6])
        print(np.divide(a, b))
        print(np.floor_divide(a, b))
        print(np.ceil(a / b))
        print(np.trunc(a / b))
        print(np.round(a / b))

        n = 100
        x = np.linspace(-2 * np.pi, 2 * np.pi, n)
        y = np.zeros(n)
        for i in range(1, n):
            y_ = 4 / ((2 * i - 1) * np.pi) * np.sin((2 * i - 1) * x)
            mp.plot(x, y_)
            y += y_
        mp.plot(x, y, label='fb')

        mp.legend()
        mp.show()

    @staticmethod
    def test_random():
        t = 100
        a = np.random.binomial(10, 0.7, t)
        x = np.arange(1, 11)
        g = np.zeros(10)
        for i in x:
            tp = a[a == i]
            s = np.sign(tp).sum()
            g[i - 1] = s / t

        mp.plot(x + 1, g, label='binomial 7')

        a = np.random.hypergeometric(6, 4, 3, 3)
        print(a)

        x = np.arange(0, 4)
        g = []
        for i in x:
            c = a[a == i].size
            g.append(c)
        print(x)
        print(g)
        mp.scatter(x, g, label='apple 6 4')

        names = np.array(['Huawei', 'Apple', 'Mi', 'Oppo', 'Vivo'])
        prices = np.array([4999, 8888, 1999, 3000, 3499])
        volumes = np.array([80, 40, 100, 25, 30])
        lidx = np.lexsort((prices, volumes))
        print(names[lidx])

        a = np.array([1, 2, 4, 5, 6, 8, 9])
        b = np.array([7, 3])
        c = np.searchsorted(a, b)
        d = np.insert(a, c, b)
        print('-' * 32)
        print(a)
        print(b)
        print(c)
        print(d)

        mp.show()

    @staticmethod
    def test_interp1d():
        x_s = -50
        x_e = 50
        import scipy.interpolate as si
        x_a = np.linspace(x_s, x_e, 15)
        y_a = np.sinc(x_a)
        mp.scatter(x_a, y_a, marker='d', label='scatter')

        # make scatter 2 succession
        x = np.linspace(x_s, x_e, 1000)
        linear = si.interp1d(x_a, y_a, kind='linear')(x)
        cubic = si.interp1d(x_a, y_a, kind='cubic')(x)

        mp.plot(x, linear, label='linear')
        mp.plot(x, cubic, label='cubic')

        mp.legend()
        mp.show()

    @staticmethod
    def test_sci():
        import matplotlib.patches as mc
        import scipy.interpolate as si
        import scipy.integrate as si

        def f(x):
            return 2 * x ** 2 + 3 * x + 5

        a, b, n = -5, 5, 500
        x = np.linspace(a, b, n + 1)
        y = f(x)
        mp.plot(x, y, label='t line')

        area = 0.0
        for i in range(x.size - 1):
            a_s = (y[i] + y[i + 1]) * (x[i + 1] - x[i]) / 2
            p1 = [x[i], 0]
            p2 = [x[i], y[i]]
            p3 = [x[i + 1], y[i + 1]]
            p4 = [x[i + 1], 0]
            area += a_s
            mp.gca().add_patch(mc.Polygon([p1, p2, p3, p4],
                                          fc='deepskyblue', ec='dodgerblue',
                                          alpha=0.5))
        print(area)
        area_quad = si.quad(f, a, b)[0]
        print(area_quad)
        print((area / area_quad - 1) * 100)

        mp.show()

    @staticmethod
    def test_fin():
        # 终值
        fv = np.fv(0.01, 5, -100, -1000)
        print('fv：', round(fv, 2))
        # 现值
        pv = np.pv(0.01, 5, -100, fv)
        print('pv', round(pv, 2))
        pmt = np.pmt(0.01, 5, 1000)
        print('pmt', round(pmt, 2))
        nper = np.nper(0.01, pmt, 1000)
        print('nper', nper)
        # print('nper', round(nper[0], 2))


if __name__ == '__main__':
    # TestScience.func_test()
    # TestScience.test_apply_along_axis()
    # TestScience.test_super_slice_array()
    # TestScience.test_super_k()
    # TestScience.test_will()
    # TestScience.test_base_science()
    # TestScience.test_smooth()
    # TestScience.test_methods()
    # TestScience.test_matrix()
    # TestScience.test_array()
    # TestScience.test_random()
    # TestScience.test_interp()
    # TestScience.test_sci()
    TestScience.test_fin()
