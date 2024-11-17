--Схемы
create schema stg;
create schema dds;
create schema ddm;

--Стейдж
create table stg.tickers(
id SERIAL PRIMARY key,
data JSON not null);

create table stg.end_of_day_data(
id SERIAL PRIMARY key,
data JSON not null);

--Хабы
CREATE TABLE dds.Hub_Company (
    hk_company_ID uuid PRIMARY KEY,
    Symbol VARCHAR(100) UNIQUE NOT null,
    load_dt timestamp,
    load_src varchar(100)
);

CREATE TABLE dds.Hub_Exchange (
    hk_Exchange_ID uuid PRIMARY KEY,
    MIC VARCHAR(100) UNIQUE NOT null,
    load_dt timestamp,
    load_src varchar(100)
);

CREATE TABLE dds.Hub_Stock (
    hk_Stock_ID uuid PRIMARY KEY,
    Symbol VARCHAR(100) UNIQUE NOT null,
    load_dt timestamp,
    load_src varchar(100)
);

-- Спутники
CREATE TABLE dds.Sat_Company_Details (
    hk_Company_ID uuid REFERENCES dds.Hub_Company(hk_Company_ID),
    Name VARCHAR(255),
    load_dt timestamp,
    load_src varchar(100)
);

CREATE TABLE dds.Sat_Exchange_Details (
    hk_Exchange_ID uuid REFERENCES dds.Hub_Exchange(hk_Exchange_ID),
    Name VARCHAR(255),
    Acronym VARCHAR(50),
    Country VARCHAR(100),
    Country_Code VARCHAR(10),
    City VARCHAR(100),
    Website VARCHAR(255),
    load_dt timestamp,
    load_src varchar(100)
);

CREATE TABLE dds.Sat_Stock_Details (
    hk_Stock_ID uuid REFERENCES dds.Hub_Stock(hk_Stock_ID),
    Open NUMERIC(10,2),
    High NUMERIC(10,2),
    Low NUMERIC(10,2),
    Close NUMERIC(10,2),
    Volume NUMERIC(20,0),
    Adj_Open NUMERIC(10,2),
    Adj_High NUMERIC(10,2),
    Adj_Low NUMERIC(10,2),
    Adj_Close NUMERIC(10,2),
    Adj_Volume NUMERIC(20,0),
    Split_Factor NUMERIC(5,2),
    Dividend NUMERIC(10,2),
    Date TIMESTAMP,
    load_dt timestamp,
    load_src varchar(100)
);

-- Ссылки
CREATE TABLE dds.Link_Company_Exchange (
	hk_Link_Company_Exchange uuid,
    hk_Company_ID uuid REFERENCES dds.Hub_Company(hk_Company_ID),
    hk_Exchange_ID uuid REFERENCES dds.Hub_Exchange(hk_Exchange_ID),
    load_dt timestamp,
    load_src varchar(100)    
);

CREATE TABLE dds.Link_Stock_Exchange (
	hk_Link_Stock_Exchange uuid,
    hk_Stock_ID uuid REFERENCES dds.Hub_Stock(hk_Stock_ID),
    hk_Exchange_ID uuid REFERENCES dds.Hub_Exchange(hk_Exchange_ID),
    load_dt timestamp,
    load_src varchar(100) 
);

--Вью
create view ddm.avg_price_fluctuation as
select
    hs.symbol,
    AVG(ssd.high - ssd.low) AS avg_price_fluctuation
FROM 
    dds.hub_stock hs
join dds.sat_stock_details ssd ON hs.hk_stock_id=ssd.hk_stock_id
JOIN dds.link_stock_exchange lse ON hs.hk_stock_id = lse.hk_stock_id
JOIN dds.hub_exchange he ON lse.hk_exchange_id = he.hk_exchange_id
JOIN dds.sat_exchange_details sed ON he.hk_exchange_id = sed.hk_exchange_id
WHERE 
    sed.city = 'New York' AND 
    EXTRACT(MONTH FROM ssd.date) = 9
GROUP by
    hs.symbol;