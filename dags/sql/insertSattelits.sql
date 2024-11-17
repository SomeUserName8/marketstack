insert into dds.sat_company_details(hk_company_id,"name",load_dt,load_src)
select distinct MD5("data"->> 'symbol')::uuid, "data"->> 'name' as "name",
		now(), 'Marketstack API'
from stg.tickers;

insert into dds.sat_exchange_details(hk_exchange_id,"name",acronym,country,country_code,city,website, load_dt,load_src)
select distinct MD5("data"-> 'stock_exchange' ->> 'mic')::uuid as hk_exchange_id,
		"data"-> 'stock_exchange' ->> 'name' as "name",
		"data"-> 'stock_exchange' ->> 'acronym' as "acronym",
		"data"-> 'stock_exchange' ->> 'country' as "country",
		"data"-> 'stock_exchange' ->> 'country_code' as "country_code",
		"data"-> 'stock_exchange' ->> 'city' as "city",
		"data"-> 'stock_exchange' ->> 'website' as "website",
		now() as "load_dt", 'Marketstack API' as "load_src"	
from stg.tickers;

insert into dds.sat_stock_details(hk_stock_id,"open",high,low,"close",volume,adj_open,adj_high,adj_low,adj_close,adj_volume,split_factor,dividend,"date",load_dt,load_src)
select distinct MD5("data"->> 'symbol')::uuid as "hk_stock_id",
		("data"->> 'open')::NUMERIC(10,2) as "open",
		("data"->> 'high')::NUMERIC(10,2) as "high",
		("data"->> 'low')::NUMERIC(10,2) as "low",
		("data"->> 'close')::NUMERIC(10,2) as "close",
		("data"->> 'volume')::NUMERIC(20,0) as "volume",
		("data"->> 'adj_open')::NUMERIC(10,2) as "adj_open",
		("data"->> 'adj_high')::NUMERIC(10,2) as "adj_high",
		("data"->> 'adj_low')::NUMERIC(10,2) as "adj_low",
		("data"->> 'adj_close')::NUMERIC(10,2) as "adj_close",
		("data"->> 'adj_volume')::NUMERIC(20,0) as "adj_volume",
		("data"->> 'split_factor')::NUMERIC(5,2) as "split_factor",
		("data"->> 'dividend')::NUMERIC(10,2) as "dividend",
		("data"->> 'date')::timestamp as "date",
		now() as "load_dt",
		'Marketstack API' as "load_src"	
from stg.end_of_day_data;