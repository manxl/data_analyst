from conf.config import TEST_TS_CODE_GSYH
import pandas as pd
from dao.db_pool import get_engine
import openpyxl

sql = """
select 
	b.y,b.total_hldr_eqy_inc_min_int as e,
	i.n_income as r,
	i.n_income /total_hldr_eqy_exc_min_int as roe,
	d.cash_div_tax*d.base_share as s
from 
	balancesheet b,income i , fina_indicator f,dividend d
where 1=1
	and b.ts_code = i.ts_code and i.ts_code = f.ts_code and f.ts_code = d.ts_code
	and b.y = i.y and i.y = f.y and f.y	= d.y
	and b.m = i.m and i.m = f.m
	and b.ts_code = '{}' and b.y between {} and {} and b.m = 12 
	order by b.y asc;

""".format(TEST_TS_CODE_GSYH, 2006, 2010)

df = pd.read_sql_query(sql, get_engine())

# for i in df.iterrows():
#     print(i)

row_num = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

wb = openpyxl.Workbook()
s = wb.active


# sheet['A1'] = 200
# sheet['A2'] = 300
# sheet['A3'] = '=SUM(A1:A2)'
# wb.save('writeFormula.xlsx')
def r(j):
    return str(3 + j)


i = 0
c = row_num[i:i + 1]
i += 1

s[c + r(2)] = 'RP'
s[c + r(3)] = 'NA'
s[c + r(4)] = 'ROE'
s[c + r(5)] = 'SOB'
s[c + r(6)] = 'SOBr'

# for i, row in yms.iterrows():
#     first_trade_date_str = row['first'].strftime('%Y%m%d')

for i, row in df.iterrows():
    c = row_num[i + 1:i + 2]

    s[c + r(1)] = row['y']
    s[c + r(2)] = row['e']
    s[c + r(3)] = row['r']
    s[c + r(4)] = '=' + c + r(3) + '/' + c + r(2)
    s[c + r(5)] = row['s']
    s[c + r(6)] = 'sobr'

wb.save('writeFormula.xlsx')
