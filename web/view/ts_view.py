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
    df = pd.read_sql_query("select * from balancesheet where ts_code = '000001.SZ' and m = 12;",
                           get_engine())

    mp.plot(df['end_date'], df['total_cur_assets'], label='123')
    mp.savefig('D:/storage/workspaces/idea-2020/data_analyst/web/static/images/new_plot1.png')
    return render_template('demo/abc.html', name=df, url='static/images/new_plot1.png')


####################################################
"""
    stock basic controllers 
"""


@main.route('/one_fina/<ts_code>/<operate>', methods=['GET'])
def one_fina_process(ts_code, operate):
    ctl = OneFinaController(ts_code)
    if 'process' == operate:
        ctl.process()
    elif 'delete' == operate:
        ctl.delete()
    elif 'view' == operate:
        meta = ctl.get_biz_data()
        from analyse.stock import plot_nincome_roe_pe_meta,plot_balancesheet
        picture = plot_nincome_roe_pe_meta(ctl.biz_code)

        picture2 = plot_balancesheet(ctl.biz_code)


        return render_template('stk.html', meta=meta, picture=picture,picture2=picture2)
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
    index_code = request.values.get('index_code')
    if ts_code:
        sql = """select ts_code from stock_basic where ts_code like '{}%%'""".format(ts_code)
        df = pd.read_sql_query(sql, get_engine())
        if len(df) == 0:
            return 'error ts_code'

        ts_code = df.iloc[0, 0]
        session['ts_code'] = ts_code
    elif index_code:
        sql = """select index_code from index_weight where index_code like '{}%%'""".format(index_code)
        df = pd.read_sql_query(sql, get_engine())
        if len(df) == 0:
            return 'error ts_code'
        ts_code = df.iloc[0, 0]
        session['index_code'] = ts_code

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
@main.route('/ctl/<interface>/<biz_code>/<operate>', methods=['GET'])
def ctl_operate(interface, biz_code, operate):
    if interface in CTL_INTERFACE_CLASS_MAPPING:
        clz = CTL_INTERFACE_CLASS_MAPPING[interface]
    else:
        # raise Exception('errer type flag')
        return redirect('/')
    module = __import__(clz.__module__, fromlist=True)
    constructor = getattr(module, clz.__name__)

    if biz_code and 'None' != biz_code:
        if constructor is FinaBaseController:
            ctl = constructor(interface, biz_code)
        else:
            ctl = constructor(biz_code)
    else:
        ctl = constructor()

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


@main.route('/123')
def root123():
    """
        basic meta
    """
    stock_basic_ctl = StockBasicController()
    trade_cal_ctl = TradeCalController()

    daily_basic_month_ctl = DailyBasicMonthController()

    daily_basic_ctl = DailyBasicController()

    index_code = '399300.SZ'
    index_weight_ctl_510310 = IndexWeightController(index_code)

    index_code = '000016.SH'
    index_weight_ctl_000016 = IndexWeightController(index_code)

    idx_tangchao_1 = MyIndexController('tangchao')
    idx_manxl = MyIndexController('manxl')

    kk = ['abc']

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

    if 'index_code' not in session:
        index_code = TEST_INDEX_CODE_SZ50
    else:
        index_code = session['index_code']

    one_index = OneIndexController(index_code)

    all_stock = AllController()

    """
        dayly
    """

    # t = type
    # l = list

    return render_template('main.html', **locals())
    # return render_template('main.html', stock_basic_ctl=stock_basic_ctl, stock_basic_his=stock_basic_his)


@main.route('/')
def root():
    ctls = {}

    ctls['Basic'] = StockBasicController()
    ctls['Trade Cal'] = TradeCalController()

    ctls['daily_basic_month'] = DailyBasicMonthController()
    ctls['daily_basic'] = DailyBasicController()

    # my index
    my_index_list = []
    ctls['My Indexes'] = my_index_list
    my_index_list.append(MyIndexController('tangchao'))
    my_index_list.append(MyIndexController('manxl'))

    # index
    index_list = []
    ctls['Indexes'] = my_index_list
    index_list.append(IndexWeightController('399300.SZ'))
    index_list.append(IndexWeightController('000016.SH'))

    """
        stock finance
    """
    if 'ts_code' not in session:
        ts_code = TEST_TS_CODE_ZGPA
    else:
        ts_code = session['ts_code']
    # ctls['Income'] = IncomeController(ts_code)
    # ctls['Dividend'] = FinaBaseController('dividend', ts_code)

    one_ctls = []
    for n in 'balancesheet,income,cashflow,fina_indicator,dividend'.split(','):
        one_ctls.append(FinaBaseController(n, ts_code))
    ctls['One Con Code'] = one_ctls

    ctls['One Fina Code'] = OneFinaController(ts_code)

    if 'index_code' not in session:
        index_code = TEST_INDEX_CODE_SZ50
    else:
        index_code = session['index_code']

    ctls['One Index Code'] = OneIndexController(index_code)
    ctls['All'] = AllController()

    ctls['ValueCalcController'] = ValueCalcController(ts_code)
    ctls['OneIndexValueController'] = OneIndexValueController(index_code)

    return render_template('index.html', ctls=ctls)


@main.route('/test')
def i_test():
    from analyse.stock import test_plt
    picture = test_plt()
    return f"<img src='data:image/png;base64,{picture}'/>"


def r():
    return redirect(url_for('ts.root'))
