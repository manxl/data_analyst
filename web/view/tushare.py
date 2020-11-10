from flask import Blueprint, redirect, url_for


main = Blueprint('ts', __name__)
print('-' * 128)

from flask import request, render_template
import pandas as pd
from dao.db_pool import get_engine
import matplotlib.pyplot as mp
from controller.controllers import *


@main.route('/meta/reload_income')
def reload_income():
    force = request.args['force']
    # income_dao.process()
    return render_template('demo/main.html')


@main.route('/ml')
def test_graph122():
    df = pd.read_sql_query("select * from stock_balancesheet where ts_code = '000001.SZ' and m = 12;",
                           get_engine())

    mp.plot(df['end_date'], df['total_cur_assets'], label='123')
    mp.savefig('D:/storage/workspaces/idea-2020/data_analyst/web/static/images/new_plot1.png')
    return render_template('demo/abc.html', name=df, url='static/images/new_plot1.png')





####################################################
@main.route('/stock_basic/process', methods=['GET'])
def stock_basic_process():
    stock_basic_ctl = StockBasicController()
    stock_basic_ctl.process()
    return r()


@main.route('/stock_basic/delete', methods=['GET'])
def stock_basic_delete():
    stock_basic_ctl = StockBasicController()
    stock_basic_ctl.delete()
    return r()


@main.route('/trade_cal/process', methods=['GET'])
def trade_cal_process():
    stock_basic_ctl = TradeCalController()
    stock_basic_ctl.process()
    return r()


@main.route('/trade_cal/delete', methods=['GET'])
def trade_cal_delete():
    stock_basic_ctl = TradeCalController()
    stock_basic_ctl.delete()
    return r()


####################################################
@main.route('/')
def root():
    stock_basic_ctl = StockBasicController()
    stock_basic_flag = stock_basic_ctl.is_need_process()

    trade_cal_ctl = TradeCalController()
    trade_cal_flag = trade_cal_ctl.is_need_process()

    return render_template('main.html', **locals())
    # return render_template('main.html', stock_basic_ctl=stock_basic_ctl, stock_basic_his=stock_basic_his)


def r():
    return redirect(url_for('ts.root'))
