import pandas as pd
from dao.db_pool import get_engine, get_pro
import datetime
from dao.db_pool import get_pro
from tools.utils import df_add_y_m
from sqlalchemy.types import VARCHAR, DATE, INT, Float, DECIMAL, Integer, FLOAT
import matplotlib.pyplot as plt


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


if __name__ == '__main__':
    test_set_color()
