set @index_code= '399300.SZ'
-- set @index_code= '000016.SH'
select DISTINCT index_code from index_weight;
set @y = 2010 ;
set @m = 12;

-- Graham
select
	s.name,s.industry,i.debt_to_assets,
	m.*
from
	stock_basic s,
	stock_month_matrix_basic m,
	fina_indicator i,
	index_weight w
where
	s.ts_code = m.ts_code
	and s.ts_code = i.ts_code
	and s.ts_code = w.con_code
	and w.y = m.y and m.y = i.y and i.y = @y
	and w.m = m.m	and m.m = i.m	and i.m = @m
	and w.index_code = @index_code
  and m.pe_ttm < 10
	and i.debt_to_assets < 50
order by
	m.pe_ttm asc;

set @y = 2017;
set @m = 12;

select
  s.name,s.industry,
	b.ts_code,
	m.pe,m.pe_ttm,m.pb,
	i.debt_to_assets,i.roe,
	m.pe/i.roe as peg,
-- 	b.total_hldr_eqy_exc_min_int,
-- 	b2.total_hldr_eqy_exc_min_int,

	b2.total_hldr_eqy_exc_min_int/b.total_hldr_eqy_exc_min_int as gr,
	m.close as c1,m2.close as c2,d.close, m2.close / m.close as pc1,d.close / m.close as pcn

from
	stock_basic s,
	daily_basic_month m,
	daily_basic_month m2,
	balancesheet b,
	balancesheet b2,
	fina_indicator i,
	liability lb,
	daily_basic d

-- 	,index_weight w

where
	m.ts_code = s.ts_code  and m.y = lb.y and s.ts_code =d.ts_code
	and m.ts_code = m2.ts_code and m.y = m2.y-1 and m.m = m2.m
	and m.ts_code = b.ts_code and m.y = b.y and m.m = b.m
	and m.ts_code = b2.ts_code and m.y = b2.y-1 and m.m = b2.m
	and m.ts_code = i.ts_code and m.y = i.y and m.m = i.m

--   and m.pe_ttm < 10
 	and m.pe < (50 / (lb.ratio))
	and i.debt_to_assets < 50
  and i.roe > 0
	and m.y = @y
	and m.m = @m
-- 	and s.ts_code = w.con_code
-- 	and w.y = m.y and w.m = m.m
-- 	and w.index_code = @index_code
order by
	peg asc;



-- metric
select
	s.ts_code,f.y,f.m,f.end_date,
	i.n_income_attr_p ,
	f.roe,
	m.close ,m.pe,m.pe_ttm,
	d.close,d.pe,d.pe_ttm
from
	stock_basic s,fina_indicator f, daily_basic_month m ,daily_basic d,income i
where
	s.ts_code = f.ts_code and f.ts_code = m.ts_code and m.ts_code = d.ts_code and d.ts_code = i.ts_code
	and f.y = m.y  and m.y = i.y
	and f.m = m.m  and m.m = i.m
	and s.ts_code = '600309.SH'
	and f.y > 2010
order by f.y ,f.m;


select
	b.y,b.total_hldr_eqy_inc_min_int as e,
	i.n_income as r,
	i.n_income /total_hldr_eqy_exc_min_int roe,
from
	balancesheet b,income i , fina_indicator f
where 1=1
	and b.ts_code = i.ts_code and i.ts_code = f.ts_code
	and b.y = i.y and i.y = f.y
	and b.m = i.m and i.m = f.m
	and b.ts_code = '601398.SH' and b.y between 2006 and 2010 and b.m = 12
	order by b.y asc;


select
	y,
	cash_div,cash_div_tax,
	cash_div*base_share,cash_div_tax*base_share
from
	dividend
	where ts_code = '601398.SH' and y between 2006 and 2010
order by y asc;




select
	m.ts_code,m.`close`,m.pe,m.pe_ttm,
	d.close,d.pe as dp,d.pe_ttm as dpt,
	d.pe/m.pe,d.pe_ttm/m.pe_ttm,d.`close`/m.`close`
from
	daily_basic d,daily_basic_month m,stock_basic s,index_weight idx
WHERE
	d.ts_code = s.ts_code and d.ts_code = m .ts_code and d.ts_code = idx.con_code
	and m.y= 2018 and m.m = 6
	and idx.index_code = 'tangchao';


select d.* from dividend d,index_weight i
where d.ts_code = i.con_code
and i.index_code = 'tangchao'
and d.y >= 2018;
