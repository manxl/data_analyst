


balancesheet_get_stock_info_his_by_ts_code = """select
	b.y,
	notes_receiv,accounts_receiv,prepayment,inventories,total_cur_assets
# ,total_assets
# 	,notes_payable,acct_payable,adv_receipts,total_cur_liab,total_liab,b.ts_code
from 
	balancesheet b
where 
	b.ts_code  = '{ts_code}' and b.m = 12 
order by 
	b.y desc;"""


balancesheet_get_stock_industry_info = """select
	notes_receiv,accounts_receiv,prepayment,inventories,total_cur_assets,b.ts_code
from 
	stock_basic s,balancesheet b 
where 
	s.ts_code = b.ts_code
    and s.industry in (select industry from stock_basic where ts_code = '{ts_code}')
    and b.y = {y} and b.m = 12 ;"""


balancesheet_get_stock_info_his_by_ts_code_2 = """select
	b.y,notes_payable,acct_payable,adv_receipts,total_cur_liab
from 
	balancesheet b
where 
	b.ts_code  = '{ts_code}' and b.m = 12 
order by 
	b.y desc;"""


balancesheet_get_stock_industry_info_2 = """select
	notes_payable,acct_payable,adv_receipts,total_cur_liab,b.ts_code
from 
	stock_basic s,balancesheet b 
where 
	s.ts_code = b.ts_code
    and s.industry in (select industry from stock_basic where ts_code = '{ts_code}')
    and b.y = {y} and b.m = 12 ;"""
