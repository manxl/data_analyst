import random
from pyecharts.faker import Faker
from pyecharts import options as opts
from pyecharts.charts import Bar3D
import pandas as pd
import numpy as np


def read_do():  # 负责数据的读取和整理
    init_data = pd.read_excel("data_6_15.xlsx")
    init_data = np.array(init_data)
    data_tip = ["设备出口", "船舶出口", "高新技术产品", "一般机电产品", "对外承包工程", "境外投资", "农产品出口", "其他"]
    data_year = [2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014]
    data_pre = []

    num = 0
    N = 0
    for st in data_tip:
        ofr = 0
        for dy in data_year:
            fuck = [st, dy, float(init_data[ofr][num])]
            data_pre.append(fuck)
            N = N + 1
            ofr = ofr + 1
        num = num + 1
    return data_pre


def bar3d_base() -> Bar3D:
    data = read_do()
    data_tip = ["设备出口", "船舶出口", "高新技术产品", "一般机电产品", "对外承包工程", "境外投资", "农产品出口", "其他"]
    data_year = [2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014]
    c = (
        Bar3D()
            .add(
            "",
            data,
            xaxis3d_opts=opts.Axis3DOpts(data_tip, type_="category", max_=8),
            yaxis3d_opts=opts.Axis3DOpts(data_year, type_="time", max_=2015),
            zaxis3d_opts=opts.Axis3DOpts(type_="value", max_=1),
            grid3d_opts=opts.Grid3DOpts(width="280", height="100")
        )
            .set_global_opts(
            visualmap_opts=opts.VisualMapOpts(max_=1),
            title_opts=opts.TitleOpts(title="按行业分的支持力度"),

        )
    )

    return c


abc = bar3d_base()
abc.render("index.html")
