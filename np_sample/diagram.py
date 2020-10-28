import numpy as np
import matplotlib.pyplot as mp
import matplotlib.gridspec as mg
from mpl_toolkits.mplot3d import axes3d


class TestMatplotlib:
    @staticmethod
    def test_1():
        # set axis
        ax = mp.gca()
        ax.spines['top'].set_color('none')
        ax.spines['right'].set_color('none')
        ax.spines['left'].set_position(('data', 0))
        ax.spines['bottom'].set_position(('data', 0))

        # set line
        xs = np.linspace(-np.pi, np.pi, 600)
        mp.plot(xs, np.sin(xs), linestyle='--', label='sin')
        mp.plot(xs, np.cos(xs), linestyle=':', label='cos')

        # set s point
        p1 = [0, np.cos(0)]
        p2 = [np.pi / 2, np.sin(np.pi / 2)]
        p3 = [0, np.sin(0)]
        p4 = [np.pi / 2, np.cos(np.pi / 2)]
        o = np.vstack((p1, p2, p3, p4))
        mp.scatter(o[:, 0], o[:, 1],
                   marker='x', color='red', s=70, zorder=3,
                   edgecolors='red', facecolor='green')
        # legend
        mp.legend(loc='best')

        # desc target
        mp.annotate('1-cos', xycoords='data', xy=(np.pi / 2, np.cos(np.pi / 2)),
                    textcoords='offset points',
                    xytext=(-40, -40), fontsize=14,
                    arrowprops=dict(
                        arrowstyle='-|>',
                        connectionstyle='angle3'
                    ))
        mp.show()

    """
        2D figures
    """

    @staticmethod
    def test_2D_figures():
        # data
        x_data = np.linspace(-np.pi, np.pi, 600)

        # TestMatplotlib.figure_matrix(x_data)

        TestMatplotlib.figure_grid(x_data)

        # flow layout
        TestMatplotlib.figure_flow_layout(x_data)

        # locators
        TestMatplotlib.figure_locators()

        TestMatplotlib.figure_girds()

        TestMatplotlib.figure_points()

        TestMatplotlib.figure_fill(x_data)

        TestMatplotlib.figure_bar()

        TestMatplotlib.figure_pie()

        TestMatplotlib.figure_contour_and_hot()

        mp.legend()
        mp.show()

        mp.show()

    @staticmethod
    def figure_contour_and_hot():
        mp.figure('figure contour')
        mp.title('figure contour')
        n = 500
        x, y = np.meshgrid(np.linspace(-3, 3, n), np.linspace(-3, 3, n))
        z = (1 - x / 2 + x ** 5 + y ** 3) * np.exp(-x ** 2 - y ** 2)
        mp.grid(linestyle=':')
        cntr = mp.contour(x, y, z, 8, colors='black', linewidths=0.5)
        mp.clabel(cntr, inline_spacing=1, fmt='%.1f', fontsize=10)
        mp.contourf(x, y, z, 8, cmap='jet')
        mp.figure('figure hot grama')
        mp.title('title hot grama')
        mp.imshow(z, cmap='jet')
        mp.colorbar()

    @staticmethod
    def figure_pie():
        mp.figure('figure Pie')
        mp.title('title pie')
        labels = ['Python', 'JavaScript', 'C++', 'Java', 'PHP']
        values = [26, 17, 21, 29, 11]
        spaces = [0.05, 0.01, 0.01, 0.01, 0.01]
        colors = ['dodgerblue', 'orangered', 'limegreen', 'violet', 'gold']
        mp.pie(values, spaces, labels, colors, '%.2f%%'
               , shadow=True, startangle=0, radius=1)

    @staticmethod
    def figure_bar():
        mp.figure('figure bar')
        n1 = np.random.random_integers(0, 100, 10)
        n2 = np.random.random_integers(0, 100, 10)
        x = np.arange(10)
        mp.bar(x - 0.2, n1, 0.4, color='dodgerblue', label='Apple')
        mp.bar(x + 0.2, n2, 0.4, color='orangered', label='chare')

    @staticmethod
    def figure_fill(x_data):
        mp.figure('Fill between', facecolor='lightgreen')

        d_sin = np.sin(x_data)
        d_cos = np.cos(1 + x_data * 2)
        mp.plot(x_data, d_sin, label='sin')
        mp.plot(x_data, d_cos, label='cos')
        mp.fill_between(x_data, d_sin, d_cos, d_sin < d_cos,
                        color='dodgerblue', alpha=0.3)
        mp.fill_between(x_data, d_sin, d_cos, d_sin > d_cos,
                        color='orangered', alpha=0.3)

    @staticmethod
    def figure_points():
        n = 250
        x = np.random.normal(173, 6, n)
        y = np.random.normal(65, 10, n)
        mp.figure('figure point', facecolor='lightgreen')
        mp.title('title figure point', fontsize=16)
        mp.xlabel('hight', fontsize=14)
        mp.ylabel('weight', fontsize=14)
        mp.grid(linestyle=':')
        d = (x - 173) ** 2 + (y - 65) ** 2
        mp.scatter(x, y, marker='o', s=35,
                   label='Persons W&H',
                   # color='dodgerblue',
                   c=d, cmap='jet')

    @staticmethod
    def figure_girds():
        mp.figure('Locators grid a', facecolor='red')
        ax = mp.gca()
        y_mi_loc = mp.MultipleLocator(50)
        ax.yaxis.set_minor_locator(y_mi_loc)
        ax.grid(which='major', axis='both',
                linewidth=2, color='orangered',
                linestyle='-')
        ax.grid(which='minor', axis='both',
                linewidth=0.2, color='orangered',
                linestyle=':')
        dx = np.array([0.2, 0.4, 0.6, 0.8, 1, 1.2, 1.4])
        dy = np.array([1, 10, 100, 1000, 100, 10, 1])
        # mp.plot(dx, dy)
        # mp.plot(dx, dy, 'o-')
        mp.semilogy(dx, dy)

    @staticmethod
    def figure_locators():
        locators = ['mp.MultipleLocator(0.2)',
                    'mp.NullLocator()',
                    'mp.MaxNLocator(nbins=5)',
                    'mp.FixedLocator(locs=[0.25,0.6,1.3])']
        mp.figure('Locators', facecolor='red')
        for i, locator in enumerate(locators):
            if i < len(locators) - 1:
                mp.subplot(len(locators) + 1, 1, i + 1)
            else:
                gs = mg.GridSpec(len(locators) + 1, 1)
                mp.subplot(gs[3:5, :])

            mp.title(locator)
            ax = mp.gca()
            ma_loc = eval(locator)
            ax.xaxis.set_major_locator(ma_loc)

    @staticmethod
    def figure_flow_layout(x_data):
        mp.figure('Flow layout', facecolor='lightgreen')
        # big
        mp.axes([0.3, 0.3, 0.94, 0.94])
        mp.text(0.5, 0.5, '1', ha='center', va='center', size=36)
        ax = mp.gca()
        ax.spines['bottom'].set_position(('data', 0.3))
        mp.xlim(-np.pi, np.pi)
        mp.yticks([])
        ma_loc = mp.MultipleLocator(1)
        ax.xaxis.set_major_locator(ma_loc)
        mi_loc = mp.MultipleLocator(0.1)
        ax.xaxis.set_minor_locator(mi_loc)
        mp.plot(x_data, np.sin(x_data))
        # small
        mp.axes([0.01, 0.01, 0.28, 0.28])
        mp.text(0.1, 0.1, '2', ha='center', va='center', size=36)

    @staticmethod
    def figure_grid(x_data):
        mp.figure('Figure Grid', facecolor='lightgray')
        mp.plot(x_data, np.cos(x_data))
        mp.title('Figure Grid\'s title')
        mp.xlabel("F B's X axis", fontsize=14)
        mp.ylabel("F B's Y axis", fontsize=14)
        mp.tick_params(labelsize=10)
        mp.grid(linestyle=':')

        # sub plot grid
        gs = mg.GridSpec(3, 3)
        mp.subplot(gs[0, :2])
        mp.text(0.5, 0.5, '1', ha='center', va='center', size=36)

        mp.subplot(gs[1, 1])

    @staticmethod
    def figure_matrix(x_data):
        mp.figure('figure matrix', facecolor='gray')
        mp.plot(x_data, np.sin(x_data))
        for i in range(1, 10):
            mp.subplot(3, 3, i)
            mp.text(
                0.5, 0.5, i,
                ha='center',
                va='center',
                size=36,
                alpha=0.5,
                color='#D2{}1E'.format(11 * i)
            )
            mp.xticks([])
            mp.yticks([])

    """
        3D firgures
    """

    @staticmethod
    def test_3D_figures():
        # TestMatplotlib.figure_3d_scatter()

        n = 500
        x, y = np.meshgrid(np.linspace(-3, 3, n), np.linspace(-3, 3, n))
        z = (1 - x / 2 + x ** 5 + y ** 3) * np.exp(-x ** 2 - y ** 2)

        TestMatplotlib.figure_3d_hight(x, y, z)

        # TestMatplotlib.figure_3d_line(x, y, z)

        mp.show()

    @staticmethod
    def figure_3d_line(x, y, z):
        mp.figure('figure 3d line')
        mp.title('title 3d line')
        a3d = mp.gca(projection='3d', label='hight - line')
        a3d.plot_wireframe(x, y, z, rstride=10, cstride=20, linewidth=1,
                           color='dodgerblue')
        mp.tight_layout()

    @staticmethod
    def figure_3d_hight(x, y, z):
        mp.figure('figure 3d contour')
        mp.title('title 3d contour')
        a3d = mp.gca(projection='3d', label='hight')
        a3d.set_xlabel('x', fontsize=14)
        a3d.set_ylabel('y', fontsize=14)
        a3d.set_zlabel('z', fontsize=14)
        a3d.plot_surface(x, y, z, cmap='jet',
                         rstride=10, cstride=10)
        mp.tight_layout()

    @staticmethod
    def figure_3d_scatter():
        n = 500
        x = np.random.normal(0, 1, n)
        y = np.random.normal(0, 1, n)
        z = np.random.normal(0, 1, n)
        mp.figure('3D Scatter')
        a3d = mp.gca(projection='3d', label='scatter')
        color = x ** 2 + y ** 2 + z ** 2
        a3d.scatter(x, y, z, marker='o', s=70,
                    # color = 'orangered'
                    c=color, cmap='jet',
                    alpha=0.3)
        a3d.set_xlabel('x', fontsize=16)
        a3d.set_ylabel('y', fontsize=16)
        a3d.set_zlabel('z', fontsize=16)
        mp.tight_layout()


def test_init_file():
    d_date, d_open, d_high, d_low, d_close = test_read_from_csv()
    mp.figure('AAPL figure')
    mp.title('AAPL title')
    mp.xlabel('Date', fontsize=14)
    mp.ylabel('Date', fontsize=14)
    mp.grid(linestyle=':')
    mp.plot(d_date, d_open, color='dodgerblue', linewidth=2,
            linestyle='--', label='AAPL CP')

    ax = mp.gca()
    import matplotlib.dates as md
    ax.xaxis.set_major_locator(md.WeekdayLocator(byweekday=md.MO))
    ax.xaxis.set_minor_locator(md.DayLocator())
    mp.gcf().autofmt_xdate()
    mp.legend()
    mp.show()


def test_init_k():
    d_date, d_open, d_high, d_low, d_close = test_read_from_csv()
    mp.figure('AAPL k figure')
    mp.title('AAPL k title')
    mp.xlabel('Date', fontsize=14)
    mp.ylabel('Date', fontsize=14)
    mp.grid(linestyle=':')
    mp.plot(d_date, d_open, color='dodgerblue', linewidth=2,
            linestyle='--', label='AAPL CP')

    flag = d_close >= d_open
    colors = ['white' if x else 'green' for x in flag]
    ecolors = ['red' if x else 'green' for x in flag]
    colors = np.array(colors)
    ecolors = np.array(ecolors)
    alpha = [0. if x else 1. for x in flag]

    mp.bar(d_date, d_close - d_open, 0.8, d_open, color=colors, edgecolor=ecolors, zorder=3)
    # mp.bar(d_date, d_close - d_open, 0.8, d_open, color='green',alpha=alpha, edgecolor='red')
    # mp.bar(d_date, d_high - d_low, 0.1, d_low, color='black')
    mp.vlines(d_date, d_low, d_high, colors=ecolors)
    ax = mp.gca()
    import matplotlib.dates as md
    ax.xaxis.set_major_locator(md.WeekdayLocator(byweekday=md.MO))
    ax.xaxis.set_minor_locator(md.DayLocator())
    mp.gcf().autofmt_xdate()
    mp.legend()
    mp.show()


def test_read_from_csv():
    # ,ts_code,trade_date
    # ,open,high,low,
    # close,pre_close,
    # change,pct_chg,vol,amount
    f_name = 'test_data_k.csv'
    return np.loadtxt(f_name, delimiter=',',
                      usecols=(2, 3, 4, 5, 6),
                      skiprows=3,
                      unpack=True,
                      dtype='M8[D], f8, f8, f8, f8',
                      converters={2: lambda ymd: '-'.join([ymd.decode()[0:4], ymd.decode()[4:6], ymd.decode()[6:]])}
                      )

# TestMatplotlib.test_2D_figures()
