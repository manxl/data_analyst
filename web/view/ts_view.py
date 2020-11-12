from flask import Blueprint, redirect, url_for

main = Blueprint('ts', __name__)

from flask import request, render_template, session
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
"""
    stock basic controllers 
"""


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


####################################################
"""
    stock finance controllers 
"""


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


@main.route('/all_stock/<operate>', methods=['GET'])
def all_stock(operate):
    if 'process' == operate:
        AllController().process()
    elif 'delete' == operate:
        AllController().delete()
    else:
        raise Exception('Unsupported type {}', operate)
    return r()


@main.route('/fina/<fina>/<ts_code>/<operate>', methods=['GET'])
def fina_process(fina, ts_code, operate):
    if 'process' == operate:
        FinaBaseController(fina, ts_code).process()
    elif 'delete' == operate:
        FinaBaseController(fina, ts_code).delete()
    else:
        raise Exception('Unsupported type {}', operate)
    return r()


@main.route('/one_fina/<ts_code>/<operate>', methods=['GET'])
def one_fina_process(ts_code, operate):
    ctl = OneFinaController(ts_code)
    if 'process' == operate:
        ctl.process()
    elif 'delete' == operate:
        ctl.delete()
    elif 'view' == operate:
        meta = ctl.get_biz_data()
        return render_template('stk.html', meta=meta)
    else:
        raise Exception('Unsupported type {}', operate)
    return r()


@main.route('/one_index/<index_code>/<operate>', methods=['GET'])
def one_index_process(index_code, operate):
    ctl = OneIndexController(index_code)
    if 'process' == operate:
        ctl.process()
    elif 'delete' == operate:
        ctl.delete()
    elif 'view' == operate:
        ctls = ctl.get_biz_data()
        return render_template('idx.html', ctls=ctls)
    else:
        raise Exception('Unsupported type {}', operate)
    return r()


@main.route('/set/session', methods=['GET', 'POST'])
def set_session_ts_code():
    ts_code = request.values.get('ts_code')

    if not ts_code:
        return 'need input ts code '
    sql = """select ts_code from stock_basic where ts_code like '{}%%'""".format(ts_code)
    df = pd.read_sql_query(sql, get_engine())
    if len(df) == 0:
        return 'error ts_code'

    ts_code = df.iloc[0, 0]
    session['ts_code'] = ts_code

    return r()


####################################################
"""
    Daily Controller  
"""


@main.route('/daily/<operate>', methods=['GET'])
def daily(operate):
    ctl = DailyBasicController()
    if 'process' == operate:
        ctl.process()
    elif 'delete' == operate:
        ctl.delete()
    elif 'view' == operate:
        ctls = ctl.get_biz_data()
        return render_template('idx.html', ctls=ctls)
    else:
        raise Exception('Unsupported type {}', operate)
    return r()


####################################################
@main.route('/')
def root():
    """
        basic meta
    """
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

    """
        stock finance
    """
    if 'ts_code' not in session:
        ts_code = TEST_TS_CODE_ZGPA
    else:
        ts_code = session['ts_code']

    income = IncomeController(ts_code)
    dividend = FinaBaseController('dividend', ts_code)

    one_ctls = []
    for n in 'balancesheet,income,cashflow,fina_indicator,dividend'.split(','):
        one_ctls.append(FinaBaseController(n, ts_code))

    one_fina = OneFinaController(ts_code)

    one_index = OneIndexController(TEST_INDEX_CODE_SZ50)

    all_stock = AllController()
    """
        dayly
    """

    daily_basic = DailyBasicController()

    return render_template('main.html', **locals())
    # return render_template('main.html', stock_basic_ctl=stock_basic_ctl, stock_basic_his=stock_basic_his)


@main.route('/test')
def i_test():
    return r()


def r():
    return redirect(url_for('ts.root'))
