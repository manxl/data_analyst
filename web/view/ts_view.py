from flask import Blueprint, redirect, url_for

main = Blueprint('ts', __name__)

from flask import request, render_template
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


@main.route('/index_weight/<index_code>/process', methods=['GET'])
def index_weight_process(index_code):
    stock_basic_ctl = IndexWeightController(index_code)
    stock_basic_ctl.process()
    return r()


@main.route('/index_weight/<index_code>/delete', methods=['GET'])
def index_weight_delete(index_code):
    stock_basic_ctl = IndexWeightController(index_code)
    stock_basic_ctl.delete()
    return r()


@main.route('/income/<ts_code>/process', methods=['GET'])
def income_process(ts_code):
    ctl = IncomeController(ts_code)
    ctl.process()
    return r()


@main.route('/income/<ts_code>/delete', methods=['GET'])
def income_delete(ts_code):
    ctl = IncomeController(ts_code)
    ctl.delete()
    return r()


@main.route('/one/<ts_code>/process', methods=['GET'])
def one_process(ts_code):
    finas = 'income,cashflow,fina_indicator,balancesheet'.split(',')
    for fina in finas:
        FinaBaseController(fina, ts_code).process()
    # FinaBaseController('balancesheet', ts_code).process()
    return r()


@main.route('/one/<ts_code>/delete', methods=['GET'])
def one_delete(ts_code):
    finas = 'income,cashflow,fina_indicator,balancesheet'.split(',')
    for fina in finas:
        FinaBaseController(fina, ts_code).delete()
    return r()


####################################################
@main.route('/')
def root():
    stock_basic_ctl = StockBasicController()
    stock_basic_flag = stock_basic_ctl.is_need_process()

    trade_cal_ctl = TradeCalController()
    trade_cal_flag = trade_cal_ctl.is_need_process()

    index_code = '399300.SZ'
    index_weight_ctl_510310 = IndexWeightController(index_code)
    index_weight_flag_510310 = index_weight_ctl_510310.is_need_process()

    index_code = '000016.SH'
    index_weight_ctl_000016 = IndexWeightController(index_code)
    index_weight_flag_000016 = index_weight_ctl_000016.is_need_process()

    ts_code = TEST_TS_CODE_4_QSY
    income_ctl_sqy = IncomeController(ts_code)
    income_ctl_fag = income_ctl_sqy.is_need_process()

    return render_template('main.html', **locals())
    # return render_template('main.html', stock_basic_ctl=stock_basic_ctl, stock_basic_his=stock_basic_his)


@main.route('/test')
def i_test():
    return r()


def r():
    return redirect(url_for('ts.root'))
