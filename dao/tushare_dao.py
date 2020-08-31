import numpy as np
import pandas as pd
import tushare as ts
from sqlalchemy.types import VARCHAR, Integer, DATE, DECIMAL, INT, BIGINT, FLOAT
import conf.config as config
from dao.db_pool import get_engine
import dao.db_dao as dao
import time
from concurrent.futures import ThreadPoolExecutor

__pro = ts.pro_api()


# __engine = pool.get_engine()


def df_add_y_m(df, column_name):
    loc = np.where(df.columns.values.T == column_name)[0][0]
    loc = int(loc) + 1
    y = df[column_name].apply(lambda s: int(s[:4]))
    t = df[column_name].apply(lambda s: int(s[4:6]))

    df.insert(loc, 'm', t)
    df.insert(loc, 'y', y)


def df_add_y(df, column_name):
    loc = np.where(df.columns.values.T == column_name)[0][0]
    loc = int(loc) + 1
    y = df[column_name].apply(lambda s: int(s[:4]))
    df.insert(loc, 'y', y)


def drop_more_nan_row(df, column_name):
    grouped = df.groupby(column_name)
    mask = []
    for k in grouped.groups:
        row_nums = grouped.groups[k]
        min = 999999
        t = 0
        for row_num in row_nums:
            n = df.loc[row_num].isnull().sum()
            if n < min:
                min = n
                t = row_num
        mask.append(t)
    return df.loc[mask]


def need_pull_check(code, table_name, force=None, condition_column='ts_code'):
    if force is None:
        sql = "select count(*) from {} where {} = '{}';".format(table_name, condition_column, code)
        try:
            size = get_engine().execute(sql).fetchone()[0]
        except Exception as e:
            if 'f405' == e.code:
                return True
            else:
                print(e)
                exit(4)
        return False if size > 0 else True
    else:
        if 'delete' == force:
            sql = "delete from {} where {} = '{}';".format(table_name, condition_column, code)
        elif 'drop' == force:
            sql = "drop table {};".format(table_name)
        else:
            print('need_pull_check {} force flag error:{}'.format(table_name, force))
            exit(4)

        try:
            r = get_engine().execute(sql)
            print('force clean {} rows 4 {}'.format(r.rowcount, code))
        except Exception as e:
            if 'f405' == e.code or 'e3q8' == e.code:
                return True
            else:
                print(e)
                exit(4)

        return True


def init_stock_list_all():
    print('start init list...')
    fileds = 'ts_code,symbol,name,area,industry,fullname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs'
    d_l = __pro.stock_basic(exchange='', list_status='L', fields=fileds)
    print('L', len(d_l))
    d_d = __pro.stock_basic(exchange='', list_status='D', fields=fileds)
    print('D', len(d_d))
    d_p = __pro.stock_basic(exchange='', list_status='P', fields=fileds)
    print('P', len(d_p))
    df = d_l.append(d_d).append(d_p)
    print('all size:', len(df))

    dtype = {'ts_code': VARCHAR(length=10), 'symbol': VARCHAR(length=8), 'name': VARCHAR(length=20),
             'area': VARCHAR(length=10), 'industry': VARCHAR(length=32), 'fullname': VARCHAR(length=32),
             'market': VARCHAR(length=10), 'exchange': VARCHAR(length=10), 'curr_type': VARCHAR(length=5),
             'list_status': VARCHAR(length=1), 'list_date': DATE(), 'delist_date': DATE(),
             'is_hs': VARCHAR(length=1)}

    # df.reset_index(drop=True)
    # df = df.reindex(columns='ts_code,end_date,ex_date,div_proc,stk_div,cash_div'.split(','))
    df.to_sql('stock_list', get_engine(), dtype=dtype, index=False, if_exists='replace')

    print('finished init list.')


def init_stock_index(index_code, force=None):
    table_name = 'index_weight'

    if not need_pull_check(index_code, table_name, force, condition_column='index_code'):
        return

    y_start = 1990

    __pool = ThreadPoolExecutor(max_workers=config.MULTIPLE, thread_name_prefix="test_")
    fs = []
    i = 0
    for y_i in range(31)[::-1]:
        y = y_start + y_i
        first, last = dao.get_trade_date(y, 0)
        if not first:
            continue
        print("{}-{}".format(y, 0))
        first = first.strftime('%Y%m%d')
        last = last.strftime('%Y%m%d')
        f1 = __pool.submit(__pro.index_weight, index_code=index_code, start_date=first, end_date=first)
        f2 = __pool.submit(__pro.index_weight, index_code=index_code, start_date=last, end_date=last)
        fs.append(f1)
        fs.append(f2)
        i += 2
        if i > 197:
            print('198次后休息60秒')
            time.sleep(60)
            i = 0

    df = None
    for f2 in fs:
        temp_df = f2.result()
        if len(temp_df):
            if df is None:
                df = temp_df
            else:
                df = df.append(temp_df, ignore_index=True)

    df_add_y_m(df, 'trade_date')

    dtype = {'index_code': VARCHAR(length=10), 'con_code': VARCHAR(length=10), 'y': INT, 'm': INT,
             'trade_date': DATE(), 'weight': DECIMAL(precision=10, scale=6)}

    df = df.reindex(columns='index_code,con_code,y,m,trade_date,weight'.split(','))

    df.to_sql(table_name, get_engine(), dtype=dtype, index=False, if_exists='append')


def init_trade_date():
    template_start = '{}00101'
    template_end = '{}91231'
    data = None
    for i in range(4):
        print(i)
        t = 199 + i
        start, end = template_start.format(t), template_end.format(t)
        df = __pro.query('trade_cal', start_date=start, end_date=end)
        if data is not None:
            data = data.append(df, ignore_index=True)
        else:
            data = df
        print('start:{},date:{}'.format(start, len(data)))

    # data.to_sql('trade_date_o', get_engine(), if_exists='replace', schema=db_name)
    df = data
    df_add_y_m(df, 'cal_date')
    # df['y'] = df['cal_date'].apply(lambda s: int(s[:4]))
    # df['m'] = df['cal_date'].apply(lambda s: int(s[4:6]))

    df.set_index(['y', 'm', 'cal_date'])
    df = df[df['is_open'] == 1]
    df = df.reindex(columns=['y', 'm', 'cal_date', 'is_open', 'exchange'])
    df.to_sql('trade_date_detail',
              get_engine(),
              index=False,
              dtype={'cal_date': DATE(), 'y': Integer(), 'm': INT(), 'is_open': INT(), 'exchange': VARCHAR(8)},
              # dtype={'cal_date': 'M8[d]'},
              if_exists='replace')
    '''
    分组插入扩展表
    '''
    grouped_m = df.groupby(['y', 'm'])
    # for a, g in grouped_m:
    #     print(a)
    #     print(g)
    r1 = grouped_m['cal_date'].agg([np.min, np.max])
    r1 = r1.rename(columns={'amin': 'first', '': 'last'})
    r1['y'] = pd.Series(r1.index.get_level_values('y'), index=r1.index)
    r1['m'] = pd.Series(r1.index.get_level_values('m'), index=r1.index)

    grouped_m = df.groupby(['y'])
    r2 = grouped_m['cal_date'].agg([np.min, np.max])
    r2 = r2.rename(columns={'amin': 'first', 'amax': 'last'})
    r2['y'] = pd.Series(r2.index.get_level_values('y'), index=r2.index)
    r2['m'] = pd.Series(np.zeros(len(r2)), index=r2.index)

    r = r1.append(r2, ignore_index=True)
    r = r.reindex(columns=['y', 'm', 'first', 'last'])
    r.to_sql('trade_date', get_engine(),
             index=False,
             dtype={'first': DATE(), 'last': DATE(), 'y': Integer(), 'm': INT()}
             , if_exists='replace')


def init_stock_price_monthly(ts_code, force=None):
    table_name = 'stock_price_monthly'

    if not need_pull_check(ts_code, table_name, force):
        print('need not 2 pull {} -> {}'.format(table_name, ts_code))
        return
    else:
        print('start 2 pull {} -> {} .'.format(table_name, ts_code))

    df = __pro.monthly(ts_code=ts_code, fields='ts_code,trade_date,open,high,low,close,vol,amount')
    if not len(df):
        return

    df_add_y_m(df, 'trade_date')
    dtype = {'ts_code': VARCHAR(length=10), 'trade_date': DATE(), 'y': INT, 'm': INT,
             'open': DECIMAL(precision=8, scale=2), 'high': DECIMAL(precision=8, scale=2),
             'low': DECIMAL(precision=8, scale=2), 'close': DECIMAL(precision=8, scale=2),
             'vol': BIGINT(), 'amount': BIGINT()}
    df.to_sql(table_name, get_engine(), dtype=dtype, index=False, if_exists='append')


def init_dividend(ts_code, force=None):
    table_name = 'stock_dividend_detail'

    if not need_pull_check(ts_code, table_name, force):
        print('need not 2 pull {} -> {}'.format(table_name, ts_code))
        return
    else:
        print('start 2 pull {} -> {} .'.format(table_name, ts_code))

    df = __pro.dividend(ts_code=ts_code, fields='ts_code,end_date,div_proc,stk_div,cash_div,ex_date')
    df = df[df['div_proc'].str.contains('实施')]
    df_add_y(df, 'end_date')
    df.reset_index(drop=True)
    df = df.reindex(columns='ts_code,end_date,y,ex_date,div_proc,stk_div,cash_div'.split(','))
    dtype = {'ts_code': VARCHAR(length=10), 'end_date': DATE(), 'div_proc': VARCHAR(length=10),
             'stk_div': DECIMAL(precision=10, scale=8), 'cash_div': DECIMAL(precision=12, scale=8),
             'ex_date': DATE(), 'y': INT()}

    df.to_sql(table_name, get_engine(), dtype=dtype, index=False, if_exists='append')

    '''
        statistical
    '''
    table_name = 'stock_dividend'
    if not need_pull_check(ts_code, table_name, force):
        print('need not 2 pull {} -> {}'.format(table_name, ts_code))
        return
    else:
        print('start 2 pull {} -> {} .'.format(table_name, ts_code))

    grouped = df.groupby('y')
    r = grouped['stk_div', 'cash_div'].agg([np.sum])
    r = r.reset_index()
    r = r.rename(columns={('stk_div', 'sum'): 'stk_div', ('cash_div', 'sum'): 'cash_div', ('y'): 'y'})
    r = r.sort_values(by=['y'], ascending=False)

    data = {'ts_code': np.full((len(r)), ts_code), 'y': r['y'], 'stk_div': r[('stk_div', 'sum')],
            'cash_div': r[('cash_div', 'sum')]}
    df = pd.DataFrame(data)
    dtype = {'ts_code': VARCHAR(length=10), 'end_date': DATE(), 'y': INT(),
             'stk_div': DECIMAL(precision=10, scale=8), 'cash_div': DECIMAL(precision=12, scale=8)}

    df.to_sql(table_name, get_engine(), dtype=dtype, index=False, if_exists='append')


def init_balancesheet(ts_code, force=None):
    table_name = 'stock_balancesheet'

    if not need_pull_check(ts_code, table_name, force):
        print('need not 2 pull {} -> {}'.format(table_name, ts_code))
        return
    else:
        print('start 2 pull {} -> {} .'.format(table_name, ts_code))

    # df = pro.income(ts_code='600000.SH', start_date='20180101', end_date='20180730', fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps')

    dtype = {'ts_code': VARCHAR(length=10), 'ann_date': DATE(), 'f_ann_date': DATE(),
             'y': INT(), 'm': INT(),
             'end_date': DATE(), 'report_type': VARCHAR(length=1), 'comp_type': VARCHAR(length=1),
             'total_share': BIGINT(), 'cap_rese': BIGINT(), 'undistr_porfit': BIGINT(),
             'surplus_rese': BIGINT(), 'special_rese': BIGINT(), 'money_cap': BIGINT(),
             'trad_asset': BIGINT(), 'notes_receiv': BIGINT(), 'accounts_receiv': BIGINT(),
             'oth_receiv': BIGINT(), 'prepayment': BIGINT(), 'div_receiv': BIGINT(),
             'int_receiv': BIGINT(), 'inventories': BIGINT(), 'amor_exp': BIGINT(),
             'nca_within_1y': BIGINT(), 'sett_rsrv': BIGINT(), 'loanto_oth_bank_fi': BIGINT(),
             'premium_receiv': BIGINT(), 'reinsur_receiv': BIGINT(), 'reinsur_res_receiv': BIGINT(),
             'pur_resale_fa': BIGINT(), 'oth_cur_assets': BIGINT(), 'total_cur_assets': BIGINT(),
             'fa_avail_for_sale': BIGINT(), 'htm_invest': BIGINT(), 'lt_eqt_invest': BIGINT(),
             'invest_real_estate': BIGINT(), 'time_deposits': BIGINT(), 'oth_assets': BIGINT(),
             'lt_rec': BIGINT(), 'fix_assets': BIGINT(), 'cip': BIGINT(),
             'const_materials': BIGINT(), 'fixed_assets_disp': BIGINT(), 'produc_bio_assets': BIGINT(),
             'oil_and_gas_assets': BIGINT(), 'intan_assets': BIGINT(), 'r_and_d': BIGINT(),
             'goodwill': BIGINT(), 'lt_amor_exp': BIGINT(), 'defer_tax_assets': BIGINT(),
             'decr_in_disbur': BIGINT(), 'oth_nca': BIGINT(), 'total_nca': BIGINT(),
             'cash_reser_cb': BIGINT(), 'depos_in_oth_bfi': BIGINT(), 'prec_metals': BIGINT(),
             'deriv_assets': BIGINT(), 'rr_reins_une_prem': BIGINT(), 'rr_reins_outstd_cla': BIGINT(),
             'rr_reins_lins_liab': BIGINT(), 'rr_reins_lthins_liab': BIGINT(), 'refund_depos': BIGINT(),
             'ph_pledge_loans': BIGINT(), 'refund_cap_depos': BIGINT(), 'indep_acct_assets': BIGINT(),
             'client_depos': BIGINT(), 'client_prov': BIGINT(), 'transac_seat_fee': BIGINT(),
             'invest_as_receiv': BIGINT(), 'total_assets': BIGINT(), 'lt_borr': BIGINT(),
             'st_borr': BIGINT(), 'cb_borr': BIGINT(), 'depos_ib_deposits': BIGINT(),
             'loan_oth_bank': BIGINT(), 'trading_fl': BIGINT(), 'notes_payable': BIGINT(),
             'acct_payable': BIGINT(), 'adv_receipts': BIGINT(), 'sold_for_repur_fa': BIGINT(),
             'comm_payable': BIGINT(), 'payroll_payable': BIGINT(), 'taxes_payable': BIGINT(),
             'int_payable': BIGINT(), 'div_payable': BIGINT(), 'oth_payable': BIGINT(),
             'acc_exp': BIGINT(), 'deferred_inc': BIGINT(), 'st_bonds_payable': BIGINT(),
             'payable_to_reinsurer': BIGINT(), 'rsrv_insur_cont': BIGINT(), 'acting_trading_sec': BIGINT(),
             'acting_uw_sec': BIGINT(), 'non_cur_liab_due_1y': BIGINT(), 'oth_cur_liab': BIGINT(),
             'total_cur_liab': BIGINT(), 'bond_payable': BIGINT(), 'lt_payable': BIGINT(),
             'specific_payables': BIGINT(), 'estimated_liab': BIGINT(), 'defer_tax_liab': BIGINT(),
             'defer_inc_non_cur_liab': BIGINT(), 'oth_ncl': BIGINT(), 'total_ncl': BIGINT(),
             'depos_oth_bfi': BIGINT(), 'deriv_liab': BIGINT(), 'depos': BIGINT(),
             'agency_bus_liab': BIGINT(), 'oth_liab': BIGINT(), 'prem_receiv_adva': BIGINT(),
             'depos_received': BIGINT(), 'ph_invest': BIGINT(), 'reser_une_prem': BIGINT(),
             'reser_outstd_claims': BIGINT(), 'reser_lins_liab': BIGINT(), 'reser_lthins_liab': BIGINT(),
             'indept_acc_liab': BIGINT(), 'pledge_borr': BIGINT(), 'indem_payable': BIGINT(),
             'policy_div_payable': BIGINT(), 'total_liab': BIGINT(), 'treasury_share': BIGINT(),
             'ordin_risk_reser': BIGINT(), 'forex_differ': BIGINT(), 'invest_loss_unconf': BIGINT(),
             'minority_int': BIGINT(), 'total_hldr_eqy_exc_min_int': BIGINT(), 'total_hldr_eqy_inc_min_int': BIGINT(),
             'total_liab_hldr_eqy': BIGINT(), 'lt_payroll_payable': BIGINT(), 'oth_comp_income': BIGINT(),
             'oth_eqt_tools': BIGINT(), 'oth_eqt_tools_p_shr': BIGINT(), 'lending_funds': BIGINT(),
             'acc_receivable': BIGINT(), 'st_fin_payable': BIGINT(), 'payables': BIGINT(),
             'hfs_assets': BIGINT(), 'hfs_sales': BIGINT(), 'update_flag': VARCHAR(length=1)}
    # call
    df = __pro.balancesheet(ts_code=ts_code, start_date='19901201', end_date='20210101')
    # clean
    # df = df.drop_duplicates(["end_date"], keep="first")
    df = drop_more_nan_row(df, 'end_date')
    # format
    df_add_y_m(df, 'end_date')

    df.reset_index(drop=True)
    df.to_sql(table_name, get_engine(), dtype=dtype, index=False, if_exists='append')


def init_income(ts_code, force=None):
    table_name = 'stock_income'

    if not need_pull_check(ts_code, table_name, force):
        print('need not 2 pull {} -> {}'.format(table_name, ts_code))
        return
    else:
        print('start 2 pull {} -> {} '.format(table_name, ts_code))

    dtype = {'ts_code': VARCHAR(length=10), 'ann_date': DATE(), 'f_ann_date': DATE(),
             'y': INT(), 'm': INT(),
             'end_date': DATE(), 'report_type': VARCHAR(length=1), 'comp_type': VARCHAR(length=1),
             'basic_eps': BIGINT(), 'diluted_eps': BIGINT(), 'total_revenue': BIGINT(),
             'revenue': BIGINT(), 'int_income': BIGINT(), 'prem_earned': BIGINT(),
             'comm_income': BIGINT(), 'n_commis_income': BIGINT(), 'n_oth_income': BIGINT(),
             'n_oth_b_income': BIGINT(), 'prem_income': BIGINT(), 'out_prem': BIGINT(),
             'une_prem_reser': BIGINT(), 'reins_income': BIGINT(), 'n_sec_tb_income': BIGINT(),
             'n_sec_uw_income': BIGINT(), 'n_asset_mg_income': BIGINT(), 'oth_b_income': BIGINT(),
             'fv_value_chg_gain': BIGINT(), 'invest_income': BIGINT(), 'ass_invest_income': BIGINT(),
             'forex_gain': BIGINT(), 'total_cogs': BIGINT(), 'oper_cost': BIGINT(),
             'int_exp': BIGINT(), 'comm_exp': BIGINT(), 'biz_tax_surchg': BIGINT(),
             'sell_exp': BIGINT(), 'admin_exp': BIGINT(), 'fin_exp': BIGINT(),
             'assets_impair_loss': BIGINT(), 'prem_refund': BIGINT(), 'compens_payout': BIGINT(),
             'reser_insur_liab': BIGINT(), 'div_payt': BIGINT(), 'reins_exp': BIGINT(),
             'oper_exp': BIGINT(), 'compens_payout_refu': BIGINT(), 'insur_reser_refu': BIGINT(),
             'reins_cost_refund': BIGINT(), 'other_bus_cost': BIGINT(), 'operate_profit': BIGINT(),
             'non_oper_income': BIGINT(), 'non_oper_exp': BIGINT(), 'nca_disploss': BIGINT(),
             'total_profit': BIGINT(), 'income_tax': BIGINT(), 'n_income': BIGINT(),
             'n_income_attr_p': BIGINT(), 'minority_gain': BIGINT(), 'oth_compr_income': BIGINT(),
             't_compr_income': BIGINT(), 'compr_inc_attr_p': BIGINT(), 'compr_inc_attr_m_s': BIGINT(),
             'ebit': BIGINT(), 'ebitda': BIGINT(), 'insurance_exp': BIGINT(),
             'undist_profit': BIGINT(), 'distable_profit': BIGINT(), 'update_flag': VARCHAR(length=1)}

    df = __pro.income(ts_code=ts_code, start_date='19901201', end_date='20210101')
    # clean
    df = drop_more_nan_row(df, 'end_date')
    # format
    df_add_y_m(df, 'end_date')
    #
    df.reset_index(drop=True)

    df.to_sql(table_name, get_engine(), dtype=dtype, index=False, if_exists='append')


def init_cashflow(ts_code, force=None):
    table_name = 'stock_cashflow'

    if not need_pull_check(ts_code, table_name, force):
        print('need not 2 pull {} -> {}'.format(table_name, ts_code))
        return
    else:
        print('start 2 pull {} -> {} .'.format(table_name, ts_code))

    dtype = {'ts_code': VARCHAR(length=10), 'ann_date': DATE(), 'f_ann_date': DATE(),
             'y': INT(), 'm': INT(),
             'end_date': DATE(), 'comp_type': VARCHAR(length=1), 'report_type': VARCHAR(length=1),
             'net_profit': BIGINT(), 'finan_exp': BIGINT(), 'c_fr_sale_sg': BIGINT(),
             'recp_tax_rends': BIGINT(), 'n_depos_incr_fi': BIGINT(), 'n_incr_loans_cb': BIGINT(),
             'n_inc_borr_oth_fi': BIGINT(), 'prem_fr_orig_contr': BIGINT(), 'n_incr_insured_dep': BIGINT(),
             'n_reinsur_prem': BIGINT(), 'n_incr_disp_tfa': BIGINT(), 'ifc_cash_incr': BIGINT(),
             'n_incr_disp_faas': BIGINT(), 'n_incr_loans_oth_bank': BIGINT(), 'n_cap_incr_repur': BIGINT(),
             'c_fr_oth_operate_a': BIGINT(), 'c_inf_fr_operate_a': BIGINT(), 'c_paid_goods_s': BIGINT(),
             'c_paid_to_for_empl': BIGINT(), 'c_paid_for_taxes': BIGINT(), 'n_incr_clt_loan_adv': BIGINT(),
             'n_incr_dep_cbob': BIGINT(), 'c_pay_claims_orig_inco': BIGINT(), 'pay_handling_chrg': BIGINT(),
             'pay_comm_insur_plcy': BIGINT(), 'oth_cash_pay_oper_act': BIGINT(), 'st_cash_out_act': BIGINT(),
             'n_cashflow_act': BIGINT(), 'oth_recp_ral_inv_act': BIGINT(), 'c_disp_withdrwl_invest': BIGINT(),
             'c_recp_return_invest': BIGINT(), 'n_recp_disp_fiolta': BIGINT(), 'n_recp_disp_sobu': BIGINT(),
             'stot_inflows_inv_act': BIGINT(), 'c_pay_acq_const_fiolta': BIGINT(), 'c_paid_invest': BIGINT(),
             'n_disp_subs_oth_biz': BIGINT(), 'oth_pay_ral_inv_act': BIGINT(), 'n_incr_pledge_loan': BIGINT(),
             'stot_out_inv_act': BIGINT(), 'n_cashflow_inv_act': BIGINT(), 'c_recp_borrow': BIGINT(),
             'proc_issue_bonds': BIGINT(), 'oth_cash_recp_ral_fnc_act': BIGINT(), 'stot_cash_in_fnc_act': BIGINT(),
             'free_cashflow': BIGINT(), 'c_prepay_amt_borr': BIGINT(), 'c_pay_dist_dpcp_int_exp': BIGINT(),
             'incl_dvd_profit_paid_sc_ms': BIGINT(), 'oth_cashpay_ral_fnc_act': BIGINT(),
             'stot_cashout_fnc_act': BIGINT(),
             'n_cash_flows_fnc_act': BIGINT(), 'eff_fx_flu_cash': BIGINT(), 'n_incr_cash_cash_equ': BIGINT(),
             'c_cash_equ_beg_period': BIGINT(), 'c_cash_equ_end_period': BIGINT(), 'c_recp_cap_contrib': BIGINT(),
             'incl_cash_rec_saims': BIGINT(), 'uncon_invest_loss': BIGINT(), 'prov_depr_assets': BIGINT(),
             'depr_fa_coga_dpba': BIGINT(), 'amort_intang_assets': BIGINT(), 'lt_amort_deferred_exp': BIGINT(),
             'decr_deferred_exp': BIGINT(), 'incr_acc_exp': BIGINT(), 'loss_disp_fiolta': BIGINT(),
             'loss_scr_fa': BIGINT(), 'loss_fv_chg': BIGINT(), 'invest_loss': BIGINT(),
             'decr_def_inc_tax_assets': BIGINT(), 'incr_def_inc_tax_liab': BIGINT(), 'decr_inventories': BIGINT(),
             'decr_oper_payable': BIGINT(), 'incr_oper_payable': BIGINT(), 'others': BIGINT(),
             'im_net_cashflow_oper_act': BIGINT(), 'conv_debt_into_cap': BIGINT(),
             'conv_copbonds_due_within_1y': BIGINT(),
             'fa_fnc_leases': BIGINT(), 'end_bal_cash': BIGINT(), 'beg_bal_cash': BIGINT(),
             'end_bal_cash_equ': BIGINT(), 'beg_bal_cash_equ': BIGINT(), 'im_n_incr_cash_equ': BIGINT()
             }

    df = __pro.cashflow(ts_code=ts_code, start_date='19901201', end_date='20210101')

    # clean
    # df = df.drop_duplicates(["end_date"], keep="first")
    df = drop_more_nan_row(df, 'end_date')

    df_add_y_m(df, 'end_date')

    df.reset_index(drop=True)

    df.to_sql(table_name, get_engine(), dtype=dtype, index=False, if_exists='append')


def init_fina_indicator(ts_code, force=None):
    table_name = 'stock_fina_indicator'

    if not need_pull_check(ts_code, table_name, force):
        print('need not 2 pull {} -> {}'.format(table_name, ts_code))
        return
    else:
        print('start 2 pull {} -> {} .'.format(table_name, ts_code))

    dtype = {'ts_code': VARCHAR(length=10), 'ann_date': DATE(), 'end_date': DATE(),
             'y': INT(), 'm': INT(),
             'eps': FLOAT(), 'dt_eps': FLOAT(), 'total_revenue_ps': FLOAT(),
             'revenue_ps': FLOAT(), 'capital_rese_ps': FLOAT(), 'surplus_rese_ps': FLOAT(),
             'undist_profit_ps': FLOAT(), 'extra_item': FLOAT(), 'profit_dedt': FLOAT(),
             'gross_margin': FLOAT(), 'current_ratio': FLOAT(), 'quick_ratio': FLOAT(),
             'cash_ratio': FLOAT(), 'invturn_days': FLOAT(), 'arturn_days': FLOAT(),
             'inv_turn': FLOAT(), 'ar_turn': FLOAT(), 'ca_turn': FLOAT(),
             'fa_turn': FLOAT(), 'assets_turn': FLOAT(), 'op_income': FLOAT(),
             'valuechange_income': FLOAT(), 'interst_income': FLOAT(), 'daa': FLOAT(),
             'ebit': FLOAT(), 'ebitda': FLOAT(), 'fcff': FLOAT(),
             'fcfe': FLOAT(), 'current_exint': FLOAT(), 'noncurrent_exint': FLOAT(),
             'interestdebt': FLOAT(), 'netdebt': FLOAT(), 'tangible_asset': FLOAT(),
             'working_capital': FLOAT(), 'networking_capital': FLOAT(), 'invest_capital': FLOAT(),
             'retained_earnings': FLOAT(), 'diluted2_eps': FLOAT(), 'bps': FLOAT(),
             'ocfps': FLOAT(), 'retainedps': FLOAT(), 'cfps': FLOAT(),
             'ebit_ps': FLOAT(), 'fcff_ps': FLOAT(), 'fcfe_ps': FLOAT(),
             'netprofit_margin': FLOAT(), 'grossprofit_margin': FLOAT(), 'cogs_of_sales': FLOAT(),
             'expense_of_sales': FLOAT(), 'profit_to_gr': FLOAT(), 'saleexp_to_gr': FLOAT(),
             'adminexp_of_gr': FLOAT(), 'finaexp_of_gr': FLOAT(), 'impai_ttm': FLOAT(),
             'gc_of_gr': FLOAT(), 'op_of_gr': FLOAT(), 'ebit_of_gr': FLOAT(),
             'roe': FLOAT(), 'roe_waa': FLOAT(), 'roe_dt': FLOAT(),
             'roa': FLOAT(), 'npta': FLOAT(), 'roic': FLOAT(),
             'roe_yearly': FLOAT(), 'roa2_yearly': FLOAT(), 'roe_avg': FLOAT(),
             'opincome_of_ebt': FLOAT(), 'investincome_of_ebt': FLOAT(), 'n_op_profit_of_ebt': FLOAT(),
             'tax_to_ebt': FLOAT(), 'dtprofit_to_profit': FLOAT(), 'salescash_to_or': FLOAT(),
             'ocf_to_or': FLOAT(), 'ocf_to_opincome': FLOAT(), 'capitalized_to_da': FLOAT(),
             'debt_to_assets': FLOAT(), 'assets_to_eqt': FLOAT(), 'dp_assets_to_eqt': FLOAT(),
             'ca_to_assets': FLOAT(), 'nca_to_assets': FLOAT(), 'tbassets_to_totalassets': FLOAT(),
             'int_to_talcap': FLOAT(), 'eqt_to_talcapital': FLOAT(), 'currentdebt_to_debt': FLOAT(),
             'longdeb_to_debt': FLOAT(), 'ocf_to_shortdebt': FLOAT(), 'debt_to_eqt': FLOAT(),
             'eqt_to_debt': FLOAT(), 'eqt_to_interestdebt': FLOAT(), 'tangibleasset_to_debt': FLOAT(),
             'tangasset_to_intdebt': FLOAT(), 'tangibleasset_to_netdebt': FLOAT(), 'ocf_to_debt': FLOAT(),
             'ocf_to_interestdebt': FLOAT(), 'ocf_to_netdebt': FLOAT(), 'ebit_to_interest': FLOAT(),
             'longdebt_to_workingcapital': FLOAT(), 'ebitda_to_debt': FLOAT(), 'turn_days': FLOAT(),
             'roa_yearly': FLOAT(), 'roa_dp': FLOAT(), 'fixed_assets': FLOAT(),
             'profit_prefin_exp': FLOAT(), 'non_op_profit': FLOAT(), 'op_to_ebt': FLOAT(),
             'nop_to_ebt': FLOAT(), 'ocf_to_profit': FLOAT(), 'cash_to_liqdebt': FLOAT(),
             'cash_to_liqdebt_withinterest': FLOAT(), 'op_to_liqdebt': FLOAT(), 'op_to_debt': FLOAT(),
             'roic_yearly': FLOAT(), 'total_fa_trun': FLOAT(), 'profit_to_op': FLOAT(),
             'q_opincome': FLOAT(), 'q_investincome': FLOAT(), 'q_dtprofit': FLOAT(),
             'q_eps': FLOAT(), 'q_netprofit_margin': FLOAT(), 'q_gsprofit_margin': FLOAT(),
             'q_exp_to_sales': FLOAT(), 'q_profit_to_gr': FLOAT(), 'q_saleexp_to_gr': FLOAT(),
             'q_adminexp_to_gr': FLOAT(), 'q_finaexp_to_gr': FLOAT(), 'q_impair_to_gr_ttm': FLOAT(),
             'q_gc_to_gr': FLOAT(), 'q_op_to_gr': FLOAT(), 'q_roe': FLOAT(),
             'q_dt_roe': FLOAT(), 'q_npta': FLOAT(), 'q_opincome_to_ebt': FLOAT(),
             'q_investincome_to_ebt': FLOAT(), 'q_dtprofit_to_profit': FLOAT(), 'q_salescash_to_or': FLOAT(),
             'q_ocf_to_sales': FLOAT(), 'q_ocf_to_or': FLOAT(), 'basic_eps_yoy': FLOAT(),
             'dt_eps_yoy': FLOAT(), 'cfps_yoy': FLOAT(), 'op_yoy': FLOAT(),
             'ebt_yoy': FLOAT(), 'netprofit_yoy': FLOAT(), 'dt_netprofit_yoy': FLOAT(),
             'ocf_yoy': FLOAT(), 'roe_yoy': FLOAT(), 'bps_yoy': FLOAT(),
             'assets_yoy': FLOAT(), 'eqt_yoy': FLOAT(), 'tr_yoy': FLOAT(),
             'or_yoy': FLOAT(), 'q_gr_yoy': FLOAT(), 'q_gr_qoq': FLOAT(),
             'q_sales_yoy': FLOAT(), 'q_sales_qoq': FLOAT(), 'q_op_yoy': FLOAT(),
             'q_op_qoq': FLOAT(), 'q_profit_yoy': FLOAT(), 'q_profit_qoq': FLOAT(),
             'q_netprofit_yoy': FLOAT(), 'q_netprofit_qoq': FLOAT(), 'equity_yoy': FLOAT(),
             'rd_exp': FLOAT(), 'update_flag': VARCHAR(length=1)}
    columns = 'ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp,update_flag'
    df = __pro.fina_indicator(ts_code=ts_code, start_date='19901201', end_date='20210101', columns=columns)

    # clean
    # df = df.drop_duplicates(["end_date"], keep="first")
    # df = drop_more_nan_row(df, 'end_date')

    df_add_y_m(df, 'end_date')

    df.reset_index(drop=True)

    df.to_sql(table_name, get_engine(), dtype=dtype, index=False, if_exists='append')


if __name__ == '__main__':
    ts_code = config.TEST_TS_CODE_1
    index_code = config.TEST_INDEX_CODE_1
    # init_stock_index(index_code)
    # init_balancesheet(ts_code, force='drop')
    # init_income(ts_code, force='drop')
    # init_cashflow(ts_code, force='drop')
    # init_fina_indicator(ts_code, force='drop')
