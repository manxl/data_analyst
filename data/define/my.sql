set @index_code= '399300.SZ'
-- set @index_code= '000016.SH'
select DISTINCT index_code from index_weight;
set @y = 2010 ;
set @m = 12;


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
