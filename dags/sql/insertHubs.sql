insert into dds.hub_company(hk_company_id,symbol,load_dt,load_src)
select distinct MD5("data"->> 'symbol')::uuid, "data"->> 'symbol' as "symbol",
		now(), 'Marketstack API'
from stg.tickers
where  MD5("data"->> 'symbol')::uuid not in (select hk_company_id from dds.hub_company);

insert into dds.hub_exchange(hk_exchange_id,mic,load_dt,load_src)
select distinct MD5("data"-> 'stock_exchange' ->> 'mic')::uuid, "data"-> 'stock_exchange' ->> 'mic' as "mic",
		now(), 'Marketstack API'
from stg.tickers
where  MD5("data"-> 'stock_exchange' ->> 'mic')::uuid not in (select hk_exchange_id from dds.hub_exchange);

insert into dds.hub_stock(hk_stock_id,symbol,load_dt,load_src)
select distinct MD5("data"->> 'symbol')::uuid, "data"->> 'symbol' as "symbol",
		now(), 'Marketstack API'
from stg.end_of_day_data
where  MD5("data"->> 'symbol')::uuid not in (select hk_stock_id from dds.hub_stock);