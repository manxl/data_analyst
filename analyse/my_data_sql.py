


i_data = """
drop table if exists i_data;
create table i_data as 
select 
	b.ts_code, b.y,b.m, b.total_assets ,b.total_liab,
	i.n_income_attr_p,
	f.roe,f.netprofit_yoy,f.debt_to_assets
from 
	balancesheet b,
	income i,
	cashflow c,
	fina_indicator f
where 
	1 =1 
	and b.ts_code = i.ts_code and i.ts_code = c.ts_code and c.ts_code = f.ts_code
	and b.y =i.y and i.y = c.y and c.y = f.y
	and b.m = i.m and i.m = c.m and c.m = f.m
-- 	and b.ts_code = @ts_code
  and b.y >= 2010 and b.m = 12
	
	and b.ts_code in (
								select ts_code from stock_basic where list_date < '2010-01-01'
								and is_hs != 'N'
							)
	and b.ts_code not in (
		select ts_code from (
			select ts_code,count(* ) as c from income  
			where n_income_attr_p < 0 
					and y >= 2010 and m =  12
			group by ts_code
			having c > 2 
		) t
	)
	and b.ts_code in (
		select ts_code from fina_indicator where y = 2020 and m = 9 and debt_to_assets < 50
	)
order BY
	b.y desc,b.m desc;
"""


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
