import numpy as np
from numpy import dtype
import pandas as pd
from sqlalchemy.types import VARCHAR, DATE, INT, Float, DECIMAL
from dao.db_pool import *
import time, calendar
import abc, logging
from conf.config import *


class CodeDao:

    def __init__(self, ts_code, his):
        super().__init__()
        self._interface = None
        self._biz_code = ts_code

        self._his = his

        self._biz_col_name = 'ts_code'
        self._unique_column = 'end_date'
        self._df = None
        self._fields = None

    @property
    def ts_code(self):
        return self._biz_code

    @ts_code.setter
    def ts_code(self, value):
        self._biz_code = value

    def _add_y_m(self):
        loc = np.where(self._df.columns.values.T == 'end_date')[0][0]
        loc = int(loc) + 1
        y = self._df['end_date'].apply(lambda s: int(s[:4]))
        m = self._df['end_date'].apply(lambda s: int(s[4:6]))

        self._df.insert(loc, 'm', m)
        self._df.insert(loc, 'y', y)

    def _tail(self):
        self._dtype = _type_mapping[self._interface]
        self._fields = ','.join(self._dtype).replace('y,m,', '')

    def _clean(self):
        #
        # # check if need clean
        # if 'update_flag' not in self._df.columns:
        #     return

        grouped = self._df.groupby(self._unique_column)
        mask = []
        for k in grouped.groups:
            row_nums = grouped.groups[k]
            if len(row_nums) == 1:
                mask.append(row_nums[0])
                continue

            for row_num in row_nums:
                if self._df.loc[row_num]['update_flag'] == '1':
                    mask.append(row_num)
                    break

        self._df = self._df.loc[mask]

    def _pull(self):
        if not self._his:
            self._df = get_pro().query(self._interface, ts_code=self.ts_code, fields=self._fields)
        else:
            start_date = self._his.end_date.strftime('%Y%m%d')
            self._df = get_pro().query(self._interface, ts_code=self.ts_code, start_date=start_date,
                                       fields=self._fields)

    def _flush(self):
        # clean
        self._clean()

        # filter
        # self._filter_stored()

        # format
        self._add_y_m()
        # reindex
        self._df.reset_index(drop=True)

    def _filter_stored(self):
        if self._his:
            sql = "select * from {} where {} = '{}' ".format(self._interface, self._biz_col_name, self._biz_code)
            d_sub = pd.read_sql_query(sql, get_engine())
            from tools.utils import drop_row_by_cal
            self._df = drop_row_by_cal(self._df, d_sub, self._unique_column)

    def _store_db(self):
        self._df.to_sql(self._interface, get_engine(), dtype=self._dtype, index=False, if_exists='append')

    def process(self):

        self._pull()
        if len(self._df) == 0:
            return 'ok'

        self._flush()

        self._store_db()

        self._second_process()

        return 'ok'

    def _second_process(self):
        pass


class BalanceSheet(CodeDao):
    def __init__(self, ts_code, his=None):
        super().__init__(ts_code, his)
        self._interface = 'balancesheet'
        self._tail()


class Income(CodeDao):
    def __init__(self, ts_code, his=None):
        super().__init__(ts_code, his)
        self._interface = 'income'
        self._tail()


class CashFlow(CodeDao):
    def __init__(self, ts_code, his=None):
        super().__init__(ts_code, his)
        self._interface = 'cashflow'
        self._tail()


class FinaIndicator(CodeDao):
    def __init__(self, ts_code, his=None):
        super().__init__(ts_code, his)
        self._interface = 'fina_indicator'
        self._tail()


class Dividend(CodeDao):
    def __init__(self, ts_code, his=None):
        super().__init__(ts_code, his)
        self._interface = 'dividend'
        self._tail()

    def _clean(self):
        self._df = self._df[self._df['div_proc'].str.contains('实施')]

    def _add_y_m(self):
        self._add_y()

    def _add_y(self):
        loc = np.where(self._df.columns.values.T == 'end_date')[0][0]
        loc = int(loc) + 1
        y = self._df['end_date'].apply(lambda s: int(s[:4]))
        self._df.insert(loc, 'y', y)

    def _second_process(self):
        table_name = self._interface + '_stat'

        df = self._df
        if len(df) == 0:
            return

        grouped = df.groupby('y')
        r = grouped[['stk_div', 'cash_div']].agg([np.sum])
        r = r.reset_index()
        r = r.rename(columns={('stk_div', 'sum'): 'stk_div', ('cash_div', 'sum'): 'cash_div', ('y'): 'y'})
        r = r.sort_values(by=['y'], ascending=False)

        data = {'ts_code': np.full((len(r)), self._biz_code), 'y': r['y'], 'stk_div': r[('stk_div', 'sum')],
                'cash_div': r[('cash_div', 'sum')]}
        df = pd.DataFrame(data)
        dtype = {'ts_code': VARCHAR(length=10), 'end_date': DATE(), 'y': INT(),
                 'stk_div': DECIMAL(precision=10, scale=8), 'cash_div': DECIMAL(precision=12, scale=8)}

        df.to_sql(table_name, get_engine(), dtype=dtype, index=False, if_exists='append')


_type_mapping = {
    'balancesheet': {'ts_code': VARCHAR(length=10), 'ann_date': DATE(), 'f_ann_date': DATE(), 'end_date': DATE(),
                     'y': INT(),
                     'm': INT(),
                     'report_type': VARCHAR(length=1), 'comp_type': VARCHAR(length=1),
                     'total_share': Float(precision=53),
                     'cap_rese': Float(precision=53), 'undistr_porfit': Float(precision=53),
                     'surplus_rese': Float(precision=53),
                     'special_rese': Float(precision=53), 'money_cap': Float(precision=53),
                     'trad_asset': Float(precision=53),
                     'notes_receiv': Float(precision=53), 'accounts_receiv': Float(precision=53),
                     'oth_receiv': Float(precision=53),
                     'prepayment': Float(precision=53), 'div_receiv': Float(precision=53),
                     'int_receiv': Float(precision=53),
                     'inventories': Float(precision=53), 'amor_exp': Float(precision=53),
                     'nca_within_1y': Float(precision=53),
                     'sett_rsrv': Float(precision=53), 'loanto_oth_bank_fi': Float(precision=53),
                     'premium_receiv': Float(precision=53),
                     'reinsur_receiv': Float(precision=53), 'reinsur_res_receiv': Float(precision=53),
                     'pur_resale_fa': Float(precision=53),
                     'oth_cur_assets': Float(precision=53), 'total_cur_assets': Float(precision=53),
                     'fa_avail_for_sale': Float(precision=53),
                     'htm_invest': Float(precision=53), 'lt_eqt_invest': Float(precision=53),
                     'invest_real_estate': Float(precision=53),
                     'time_deposits': Float(precision=53), 'oth_assets': Float(precision=53),
                     'lt_rec': Float(precision=53),
                     'fix_assets': Float(precision=53), 'cip': Float(precision=53),
                     'const_materials': Float(precision=53),
                     'fixed_assets_disp': Float(precision=53), 'produc_bio_assets': Float(precision=53),
                     'oil_and_gas_assets': Float(precision=53),
                     'intan_assets': Float(precision=53), 'r_and_d': Float(precision=53),
                     'goodwill': Float(precision=53),
                     'lt_amor_exp': Float(precision=53), 'defer_tax_assets': Float(precision=53),
                     'decr_in_disbur': Float(precision=53),
                     'oth_nca': Float(precision=53), 'total_nca': Float(precision=53),
                     'cash_reser_cb': Float(precision=53),
                     'depos_in_oth_bfi': Float(precision=53), 'prec_metals': Float(precision=53),
                     'deriv_assets': Float(precision=53),
                     'rr_reins_une_prem': Float(precision=53), 'rr_reins_outstd_cla': Float(precision=53),
                     'rr_reins_lins_liab': Float(precision=53), 'rr_reins_lthins_liab': Float(precision=53),
                     'refund_depos': Float(precision=53),
                     'ph_pledge_loans': Float(precision=53), 'refund_cap_depos': Float(precision=53),
                     'indep_acct_assets': Float(precision=53),
                     'client_depos': Float(precision=53), 'client_prov': Float(precision=53),
                     'transac_seat_fee': Float(precision=53),
                     'invest_as_receiv': Float(precision=53), 'total_assets': Float(precision=53),
                     'lt_borr': Float(precision=53),
                     'st_borr': Float(precision=53), 'cb_borr': Float(precision=53),
                     'depos_ib_deposits': Float(precision=53),
                     'loan_oth_bank': Float(precision=53), 'trading_fl': Float(precision=53),
                     'notes_payable': Float(precision=53),
                     'acct_payable': Float(precision=53), 'adv_receipts': Float(precision=53),
                     'sold_for_repur_fa': Float(precision=53),
                     'comm_payable': Float(precision=53), 'payroll_payable': Float(precision=53),
                     'taxes_payable': Float(precision=53),
                     'int_payable': Float(precision=53), 'div_payable': Float(precision=53),
                     'oth_payable': Float(precision=53),
                     'acc_exp': Float(precision=53), 'deferred_inc': Float(precision=53),
                     'st_bonds_payable': Float(precision=53),
                     'payable_to_reinsurer': Float(precision=53), 'rsrv_insur_cont': Float(precision=53),
                     'acting_trading_sec': Float(precision=53),
                     'acting_uw_sec': Float(precision=53), 'non_cur_liab_due_1y': Float(precision=53),
                     'oth_cur_liab': Float(precision=53),
                     'total_cur_liab': Float(precision=53), 'bond_payable': Float(precision=53),
                     'lt_payable': Float(precision=53),
                     'specific_payables': Float(precision=53), 'estimated_liab': Float(precision=53),
                     'defer_tax_liab': Float(precision=53),
                     'defer_inc_non_cur_liab': Float(precision=53), 'oth_ncl': Float(precision=53),
                     'total_ncl': Float(precision=53),
                     'depos_oth_bfi': Float(precision=53), 'deriv_liab': Float(precision=53),
                     'depos': Float(precision=53),
                     'agency_bus_liab': Float(precision=53), 'oth_liab': Float(precision=53),
                     'prem_receiv_adva': Float(precision=53),
                     'depos_received': Float(precision=53), 'ph_invest': Float(precision=53),
                     'reser_une_prem': Float(precision=53),
                     'reser_outstd_claims': Float(precision=53), 'reser_lins_liab': Float(precision=53),
                     'reser_lthins_liab': Float(precision=53),
                     'indept_acc_liab': Float(precision=53), 'pledge_borr': Float(precision=53),
                     'indem_payable': Float(precision=53),
                     'policy_div_payable': Float(precision=53), 'total_liab': Float(precision=53),
                     'treasury_share': Float(precision=53),
                     'ordin_risk_reser': Float(precision=53), 'forex_differ': Float(precision=53),
                     'invest_loss_unconf': Float(precision=53),
                     'minority_int': Float(precision=53), 'total_hldr_eqy_exc_min_int': Float(precision=53),
                     'total_hldr_eqy_inc_min_int': Float(precision=53), 'total_liab_hldr_eqy': Float(precision=53),
                     'lt_payroll_payable': Float(precision=53), 'oth_comp_income': Float(precision=53),
                     'oth_eqt_tools': Float(precision=53),
                     'oth_eqt_tools_p_shr': Float(precision=53), 'lending_funds': Float(precision=53),
                     'acc_receivable': Float(precision=53),
                     'st_fin_payable': Float(precision=53), 'payables': Float(precision=53),
                     'hfs_assets': Float(precision=53),
                     'hfs_sales': Float(precision=53), 'update_flag': VARCHAR(length=1)},
    'income': {'ts_code': VARCHAR(length=10), 'ann_date': DATE(), 'f_ann_date': DATE(), 'y': INT(), 'm': INT(),
               'end_date': DATE(),
               'report_type': VARCHAR(length=1), 'comp_type': VARCHAR(length=1), 'basic_eps': Float(precision=53),
               'diluted_eps': Float(precision=53), 'total_revenue': Float(precision=53), 'revenue': Float(precision=53),
               'int_income': Float(precision=53), 'prem_earned': Float(precision=53),
               'comm_income': Float(precision=53),
               'n_commis_income': Float(precision=53), 'n_oth_income': Float(precision=53),
               'n_oth_b_income': Float(precision=53),
               'prem_income': Float(precision=53), 'out_prem': Float(precision=53),
               'une_prem_reser': Float(precision=53),
               'reins_income': Float(precision=53), 'n_sec_tb_income': Float(precision=53),
               'n_sec_uw_income': Float(precision=53),
               'n_asset_mg_income': Float(precision=53), 'oth_b_income': Float(precision=53),
               'fv_value_chg_gain': Float(precision=53),
               'invest_income': Float(precision=53), 'ass_invest_income': Float(precision=53),
               'forex_gain': Float(precision=53),
               'total_cogs': Float(precision=53), 'oper_cost': Float(precision=53), 'int_exp': Float(precision=53),
               'comm_exp': Float(precision=53), 'biz_tax_surchg': Float(precision=53), 'sell_exp': Float(precision=53),
               'admin_exp': Float(precision=53), 'fin_exp': Float(precision=53),
               'assets_impair_loss': Float(precision=53),
               'prem_refund': Float(precision=53), 'compens_payout': Float(precision=53),
               'reser_insur_liab': Float(precision=53),
               'div_payt': Float(precision=53), 'reins_exp': Float(precision=53), 'oper_exp': Float(precision=53),
               'compens_payout_refu': Float(precision=53), 'insur_reser_refu': Float(precision=53),
               'reins_cost_refund': Float(precision=53),
               'other_bus_cost': Float(precision=53), 'operate_profit': Float(precision=53),
               'non_oper_income': Float(precision=53),
               'non_oper_exp': Float(precision=53), 'nca_disploss': Float(precision=53),
               'total_profit': Float(precision=53),
               'income_tax': Float(precision=53), 'n_income': Float(precision=53),
               'n_income_attr_p': Float(precision=53),
               'minority_gain': Float(precision=53), 'oth_compr_income': Float(precision=53),
               't_compr_income': Float(precision=53),
               'compr_inc_attr_p': Float(precision=53), 'compr_inc_attr_m_s': Float(precision=53),
               'ebit': Float(precision=53),
               'ebitda': Float(precision=53), 'insurance_exp': Float(precision=53),
               'undist_profit': Float(precision=53),
               'distable_profit': Float(precision=53), 'update_flag': VARCHAR(length=1)},
    'cashflow': {'ts_code': VARCHAR(length=10), 'ann_date': DATE(), 'f_ann_date': DATE(), 'y': INT(), 'm': INT(),
                 'end_date': DATE(),
                 'comp_type': VARCHAR(length=1), 'report_type': VARCHAR(length=1), 'net_profit': Float(precision=53),
                 'finan_exp': Float(precision=53), 'c_fr_sale_sg': Float(precision=53),
                 'recp_tax_rends': Float(precision=53),
                 'n_depos_incr_fi': Float(precision=53), 'n_incr_loans_cb': Float(precision=53),
                 'n_inc_borr_oth_fi': Float(precision=53),
                 'prem_fr_orig_contr': Float(precision=53), 'n_incr_insured_dep': Float(precision=53),
                 'n_reinsur_prem': Float(precision=53),
                 'n_incr_disp_tfa': Float(precision=53), 'ifc_cash_incr': Float(precision=53),
                 'n_incr_disp_faas': Float(precision=53),
                 'n_incr_loans_oth_bank': Float(precision=53), 'n_cap_incr_repur': Float(precision=53),
                 'c_fr_oth_operate_a': Float(precision=53), 'c_inf_fr_operate_a': Float(precision=53),
                 'c_paid_goods_s': Float(precision=53),
                 'c_paid_to_for_empl': Float(precision=53), 'c_paid_for_taxes': Float(precision=53),
                 'n_incr_clt_loan_adv': Float(precision=53),
                 'n_incr_dep_cbob': Float(precision=53), 'c_pay_claims_orig_inco': Float(precision=53),
                 'pay_handling_chrg': Float(precision=53), 'pay_comm_insur_plcy': Float(precision=53),
                 'oth_cash_pay_oper_act': Float(precision=53), 'st_cash_out_act': Float(precision=53),
                 'n_cashflow_act': Float(precision=53),
                 'oth_recp_ral_inv_act': Float(precision=53), 'c_disp_withdrwl_invest': Float(precision=53),
                 'c_recp_return_invest': Float(precision=53), 'n_recp_disp_fiolta': Float(precision=53),
                 'n_recp_disp_sobu': Float(precision=53), 'stot_inflows_inv_act': Float(precision=53),
                 'c_pay_acq_const_fiolta': Float(precision=53), 'c_paid_invest': Float(precision=53),
                 'n_disp_subs_oth_biz': Float(precision=53), 'oth_pay_ral_inv_act': Float(precision=53),
                 'n_incr_pledge_loan': Float(precision=53), 'stot_out_inv_act': Float(precision=53),
                 'n_cashflow_inv_act': Float(precision=53),
                 'c_recp_borrow': Float(precision=53), 'proc_issue_bonds': Float(precision=53),
                 'oth_cash_recp_ral_fnc_act': Float(precision=53), 'stot_cash_in_fnc_act': Float(precision=53),
                 'free_cashflow': Float(precision=53), 'c_prepay_amt_borr': Float(precision=53),
                 'c_pay_dist_dpcp_int_exp': Float(precision=53),
                 'incl_dvd_profit_paid_sc_ms': Float(precision=53), 'oth_cashpay_ral_fnc_act': Float(precision=53),
                 'stot_cashout_fnc_act': Float(precision=53), 'n_cash_flows_fnc_act': Float(precision=53),
                 'eff_fx_flu_cash': Float(precision=53), 'n_incr_cash_cash_equ': Float(precision=53),
                 'c_cash_equ_beg_period': Float(precision=53), 'c_cash_equ_end_period': Float(precision=53),
                 'c_recp_cap_contrib': Float(precision=53), 'incl_cash_rec_saims': Float(precision=53),
                 'uncon_invest_loss': Float(precision=53), 'prov_depr_assets': Float(precision=53),
                 'depr_fa_coga_dpba': Float(precision=53),
                 'amort_intang_assets': Float(precision=53), 'lt_amort_deferred_exp': Float(precision=53),
                 'decr_deferred_exp': Float(precision=53), 'incr_acc_exp': Float(precision=53),
                 'loss_disp_fiolta': Float(precision=53),
                 'loss_scr_fa': Float(precision=53), 'loss_fv_chg': Float(precision=53),
                 'invest_loss': Float(precision=53),
                 'decr_def_inc_tax_assets': Float(precision=53), 'incr_def_inc_tax_liab': Float(precision=53),
                 'decr_inventories': Float(precision=53), 'decr_oper_payable': Float(precision=53),
                 'incr_oper_payable': Float(precision=53),
                 'others': Float(precision=53), 'im_net_cashflow_oper_act': Float(precision=53),
                 'conv_debt_into_cap': Float(precision=53),
                 'conv_copbonds_due_within_1y': Float(precision=53), 'fa_fnc_leases': Float(precision=53),
                 'end_bal_cash': Float(precision=53),
                 'beg_bal_cash': Float(precision=53), 'end_bal_cash_equ': Float(precision=53),
                 'beg_bal_cash_equ': Float(precision=53),
                 'im_n_incr_cash_equ': Float(precision=53), 'update_flag': VARCHAR(length=1)},
    'dividend': {'ts_code': VARCHAR(length=10), 'end_date': DATE(), 'y': INT(), 'm': INT(), 'ann_date': DATE(),
                 'div_proc': VARCHAR(length=10),
                 'stk_div': Float(precision=53), 'stk_bo_rate': Float(precision=53), 'stk_co_rate': Float(precision=53),
                 'cash_div': Float(precision=53),
                 'cash_div_tax': Float(precision=53), 'record_date': DATE(), 'ex_date': DATE(), 'pay_date': DATE(),
                 'div_listdate': VARCHAR(length=10),
                 'imp_ann_date': DATE(), 'base_date': DATE(), 'base_share': Float(precision=53)},
    'fina_indicator': {'ts_code': VARCHAR(length=10), 'ann_date': DATE(), 'end_date': DATE(), 'y': INT(), 'm': INT(),
                       'eps': Float(precision=53),
                       'dt_eps': Float(precision=53), 'total_revenue_ps': Float(precision=53),
                       'revenue_ps': Float(precision=53),
                       'capital_rese_ps': Float(precision=53), 'surplus_rese_ps': Float(precision=53),
                       'undist_profit_ps': Float(precision=53),
                       'extra_item': Float(precision=53), 'profit_dedt': Float(precision=53),
                       'gross_margin': Float(precision=53),
                       'current_ratio': Float(precision=53), 'quick_ratio': Float(precision=53),
                       'cash_ratio': Float(precision=53),
                       'invturn_days': Float(precision=53), 'arturn_days': Float(precision=53),
                       'inv_turn': Float(precision=53),
                       'ar_turn': Float(precision=53), 'ca_turn': Float(precision=53), 'fa_turn': Float(precision=53),
                       'assets_turn': Float(precision=53), 'op_income': Float(precision=53),
                       'valuechange_income': Float(precision=53),
                       'interst_income': Float(precision=53), 'daa': Float(precision=53), 'ebit': Float(precision=53),
                       'ebitda': Float(precision=53),
                       'fcff': Float(precision=53), 'fcfe': Float(precision=53), 'current_exint': Float(precision=53),
                       'noncurrent_exint': Float(precision=53), 'interestdebt': Float(precision=53),
                       'netdebt': Float(precision=53),
                       'tangible_asset': Float(precision=53), 'working_capital': Float(precision=53),
                       'networking_capital': Float(precision=53),
                       'invest_capital': Float(precision=53), 'retained_earnings': Float(precision=53),
                       'diluted2_eps': Float(precision=53),
                       'bps': Float(precision=53), 'ocfps': Float(precision=53), 'retainedps': Float(precision=53),
                       'cfps': Float(precision=53),
                       'ebit_ps': Float(precision=53), 'fcff_ps': Float(precision=53), 'fcfe_ps': Float(precision=53),
                       'netprofit_margin': Float(precision=53), 'grossprofit_margin': Float(precision=53),
                       'cogs_of_sales': Float(precision=53),
                       'expense_of_sales': Float(precision=53), 'profit_to_gr': Float(precision=53),
                       'saleexp_to_gr': Float(precision=53),
                       'adminexp_of_gr': Float(precision=53), 'finaexp_of_gr': Float(precision=53),
                       'impai_ttm': Float(precision=53),
                       'gc_of_gr': Float(precision=53), 'op_of_gr': Float(precision=53),
                       'ebit_of_gr': Float(precision=53),
                       'roe': Float(precision=53), 'roe_waa': Float(precision=53), 'roe_dt': Float(precision=53),
                       'roa': Float(precision=53),
                       'npta': Float(precision=53), 'roic': Float(precision=53), 'roe_yearly': Float(precision=53),
                       'roa2_yearly': Float(precision=53), 'roe_avg': Float(precision=53),
                       'opincome_of_ebt': Float(precision=53),
                       'investincome_of_ebt': Float(precision=53), 'n_op_profit_of_ebt': Float(precision=53),
                       'tax_to_ebt': Float(precision=53),
                       'dtprofit_to_profit': Float(precision=53), 'salescash_to_or': Float(precision=53),
                       'ocf_to_or': Float(precision=53),
                       'ocf_to_opincome': Float(precision=53), 'capitalized_to_da': Float(precision=53),
                       'debt_to_assets': Float(precision=53),
                       'assets_to_eqt': Float(precision=53), 'dp_assets_to_eqt': Float(precision=53),
                       'ca_to_assets': Float(precision=53),
                       'nca_to_assets': Float(precision=53), 'tbassets_to_totalassets': Float(precision=53),
                       'int_to_talcap': Float(precision=53),
                       'eqt_to_talcapital': Float(precision=53), 'currentdebt_to_debt': Float(precision=53),
                       'longdeb_to_debt': Float(precision=53),
                       'ocf_to_shortdebt': Float(precision=53), 'debt_to_eqt': Float(precision=53),
                       'eqt_to_debt': Float(precision=53),
                       'eqt_to_interestdebt': Float(precision=53), 'tangibleasset_to_debt': Float(precision=53),
                       'tangasset_to_intdebt': Float(precision=53), 'tangibleasset_to_netdebt': Float(precision=53),
                       'ocf_to_debt': Float(precision=53), 'ocf_to_interestdebt': Float(precision=53),
                       'ocf_to_netdebt': Float(precision=53),
                       'ebit_to_interest': Float(precision=53), 'longdebt_to_workingcapital': Float(precision=53),
                       'ebitda_to_debt': Float(precision=53), 'turn_days': Float(precision=53),
                       'roa_yearly': Float(precision=53),
                       'roa_dp': Float(precision=53), 'fixed_assets': Float(precision=53),
                       'profit_prefin_exp': Float(precision=53),
                       'non_op_profit': Float(precision=53), 'op_to_ebt': Float(precision=53),
                       'nop_to_ebt': Float(precision=53),
                       'ocf_to_profit': Float(precision=53), 'cash_to_liqdebt': Float(precision=53),
                       'cash_to_liqdebt_withinterest': Float(precision=53), 'op_to_liqdebt': Float(precision=53),
                       'op_to_debt': Float(precision=53),
                       'roic_yearly': Float(precision=53), 'total_fa_trun': Float(precision=53),
                       'profit_to_op': Float(precision=53),
                       'q_opincome': Float(precision=53), 'q_investincome': Float(precision=53),
                       'q_dtprofit': Float(precision=53),
                       'q_eps': Float(precision=53), 'q_netprofit_margin': Float(precision=53),
                       'q_gsprofit_margin': Float(precision=53),
                       'q_exp_to_sales': Float(precision=53), 'q_profit_to_gr': Float(precision=53),
                       'q_saleexp_to_gr': Float(precision=53),
                       'q_adminexp_to_gr': Float(precision=53), 'q_finaexp_to_gr': Float(precision=53),
                       'q_impair_to_gr_ttm': Float(precision=53),
                       'q_gc_to_gr': Float(precision=53), 'q_op_to_gr': Float(precision=53),
                       'q_roe': Float(precision=53),
                       'q_dt_roe': Float(precision=53), 'q_npta': Float(precision=53),
                       'q_opincome_to_ebt': Float(precision=53),
                       'q_investincome_to_ebt': Float(precision=53), 'q_dtprofit_to_profit': Float(precision=53),
                       'q_salescash_to_or': Float(precision=53), 'q_ocf_to_sales': Float(precision=53),
                       'q_ocf_to_or': Float(precision=53),
                       'basic_eps_yoy': Float(precision=53), 'dt_eps_yoy': Float(precision=53),
                       'cfps_yoy': Float(precision=53),
                       'op_yoy': Float(precision=53), 'ebt_yoy': Float(precision=53),
                       'netprofit_yoy': Float(precision=53),
                       'dt_netprofit_yoy': Float(precision=53), 'ocf_yoy': Float(precision=53),
                       'roe_yoy': Float(precision=53),
                       'bps_yoy': Float(precision=53), 'assets_yoy': Float(precision=53),
                       'eqt_yoy': Float(precision=53),
                       'tr_yoy': Float(precision=53), 'or_yoy': Float(precision=53), 'q_gr_yoy': Float(precision=53),
                       'q_gr_qoq': Float(precision=53),
                       'q_sales_yoy': Float(precision=53), 'q_sales_qoq': Float(precision=53),
                       'q_op_yoy': Float(precision=53),
                       'q_op_qoq': Float(precision=53), 'q_profit_yoy': Float(precision=53),
                       'q_profit_qoq': Float(precision=53),
                       'q_netprofit_yoy': Float(precision=53), 'q_netprofit_qoq': Float(precision=53),
                       'equity_yoy': Float(precision=53),
                       'rd_exp': Float(precision=53), 'update_flag': VARCHAR(length=1)}

}
