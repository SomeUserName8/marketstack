insert into dds.Link_Company_Exchange(hk_link_company_exchange,hk_company_id,hk_exchange_id,load_dt,load_src)
select distinct MD5(concat(hc.hk_company_id::text, he.hk_exchange_id::text))::uuid, hc.hk_company_id, he.hk_exchange_id, now(), 'Marketstack API'
from stg.tickers t
left join dds.hub_company hc on (MD5(t."data"->> 'symbol')::uuid = hc.hk_company_id)
left join dds.hub_exchange he on (MD5(t."data"-> 'stock_exchange' ->> 'mic')::uuid = he.hk_exchange_id)
where MD5(concat(hc.hk_company_id::text, he.hk_exchange_id::text))::uuid not in (select hk_link_company_exchange from dds.link_company_exchange);

insert into dds.link_stock_exchange(hk_link_stock_exchange,hk_stock_id,hk_exchange_id,load_dt,load_src)
select distinct MD5(concat(hs.hk_stock_id::text, he.hk_exchange_id::text))::uuid, hs.hk_stock_id, he.hk_exchange_id, now(), 'Marketstack API'
from stg.end_of_day_data
left join dds.hub_stock hs on (MD5("data"->> 'symbol')::uuid = hs.hk_stock_id)
left join dds.hub_exchange he on (MD5("data" ->> 'exchange')::uuid = he.hk_exchange_id)
where MD5(concat(hs.hk_stock_id::text, he.hk_exchange_id::text))::uuid not in (select hk_link_stock_exchange from dds.link_stock_exchange);