-- Použitie databázy GECKO_DB
USE DATABASE GECKO_DB;

-- Vytvorenie schém pre staging a dátový sklad (DWH)
create or replace schema SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING;
create or replace schema SHEMA_CHEMICAL_PRICE_ASSESSMENTS_DWH;

-- Prepnutie do staging schémy
use schema SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING;

-- Vytvorenie interného stage pre načítanie CSV súborov
create or replace stage STAGE_STATING;

-- Vytvorenie staging tabuľky,
-- ktorá dočasne uchováva surové dáta načítané zo súboru CSV
CREATE OR REPLACE TABLE chemical_price_assessments_staging (
    KEY VARCHAR,
    TERMINATED BOOLEAN,
    SERIES_NAME VARCHAR,
    LAUNCH_DATE DATE,

    COMMODITY_ID VARCHAR,
    COMMODITY VARCHAR,
    COMMODITY_P1_ID VARCHAR,
    COMMODITY_P1 VARCHAR,
    COMMODITY_P2_ID VARCHAR,
    COMMODITY_P2 VARCHAR,

    LOCATION_ID VARCHAR,
    LOCATION VARCHAR,
    LOCATION_TYPE VARCHAR,

    CURRENCY_CODE VARCHAR,
    CURRENCY VARCHAR,
    CURRENCY_SYMBOL VARCHAR,

    MEASURE_UNIT VARCHAR,
    MEASURE_UNIT_SYMBOL VARCHAR,

    FREQUENCY VARCHAR,
    TRADE_TERMS VARCHAR,
    TRADE_TERMS_DESCRIPTION VARCHAR,
    TRANSACTION_TYPE VARCHAR,

    QUOTE_APPROACH VARCHAR,
    QUOTE_MEASUREMENT_STYLE VARCHAR,

    FACTORY VARCHAR,
    TRANSPORT VARCHAR,
    TRANSPORT_TYPE VARCHAR,
    ORIGINATOR VARCHAR,
    DELIVERY_TIMEFRAME VARCHAR,

    ASSESSMENT_HIGH_PRECISION VARIANT,
    ASSESSMENT_HIGH_DELTA_PRECISION VARIANT,
    ASSESSMENT_LOW_PRECISION VARIANT,
    ASSESSMENT_LOW_DELTA_PRECISION VARIANT,
    MID_PRECISION VARIANT,
    MID_DELTA_PRECISION VARIANT,

    SERIES_KEY VARCHAR,
    PUBLISH_STATUS VARCHAR,
    CREATED_FOR DATE,
    RELEASED_ON TIMESTAMP_NTZ,
    PRICE_ITEM_TYPE VARCHAR,

    ASSESSMENT_LOW NUMBER(18,5),
    ASSESSMENT_LOW_DELTA NUMBER(18,5),
    ASSESSMENT_MID NUMBER(18,5),
    ASSESSMENT_MID_DELTA NUMBER(18,5),
    ASSESSMENT_HIGH NUMBER(18,5),
    ASSESSMENT_HIGH_DELTA NUMBER(18,5),

    DELTA_TYPE VARCHAR,
    CONTRACT_PERIOD VARCHAR,
    START_DATE DATE,
    END_DATE DATE,
    IS_ESTIMATED VARIANT
);

-- Kontrola obsahu staging tabuľky
select * from chemical_price_assessments_staging;

-- Definícia formátu CSV súboru
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  NULL_IF = ('NULL','null')
  EMPTY_FIELD_AS_NULL = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"';

-- Načítanie dát zo súboru CSV do staging tabuľky
COPY INTO chemical_price_assessments_staging
FROM '@STAGE_STATING/Chemical Price Assessments - Examples_2026-01-04-1315.csv'
FILE_FORMAT = my_csv_format
ON_ERROR = 'ABORT_STATEMENT';

-- Prepnutie do schémy dátového skladu (DWH)
use schema SHEMA_CHEMICAL_PRICE_ASSESSMENTS_DWH;

-- Vytvorenie časovej dimenzie DIM_TIME
-- Táto tabuľka slúži na časovú analýzu cien
create or replace table DIM_TIME as 
select distinct 
    to_number(to_char(created_for, 'YYYYMMDD')) as TIME_ID,
    extract(day from created_for)               as DAY,
    extract(month from created_for)             as MONTH,
    to_char(created_for, 'MMMM')                as MONTH_NAME,
    'Q' || extract(quarter from created_for)    as QUARTER,
    extract(year from created_for)              as YEAR
    
from GECKO_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging
where created_for is not null;

-- Vytvorenie dimenzie DIM_SERIES,
-- ktorá popisuje sériu cenových hodnotení
create or replace table DIM_SERIES as 
select distinct 
    series_key::varchar(50)             as SERIES_KEY,
    originator::varchar(25)             as ORIGINATOR,
    series_name::varchar                as SERIES_NAME,
    price_item_type::varchar(30)        as PRICE_ITEM_TYPE,
    publish_status::varchar(30)         as PUBLISH_STATUS,
    LAUNCH_DATE::date                   as LAUNCH_DATE,
    START_DATE::date                    as START_DATE,
    end_date::date                      as END_DATE,
    terminated::boolean                 as TERMINATED,
    frequency::varchar(20)              as FREQUENCY
    
from GECKO_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging;

-- Vytvorenie dimenzie DIM_COMMODITY,
-- ktorá uchováva informácie o produktoch
create or replace table DIM_COMMODITY as
select distinct
    commodity_id::varchar(60)                   as COMMODITY_ID,
    commodity::varchar(60)                      as COMMODITY_,
    commodity_p1_id::varchar(60)                as COMMODITY_P1_ID,
    commodity_p1::varchar(60)                   as COMMODITY_P1,
    commodity_p2_id::varchar(60)                as COMMODITY_P2_ID,
    commodity_p2::varchar(60)                   as COMMODITY_P2
    
from GECKO_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging;

-- Vytvorenie dimenzie DIM_LOCATION,
-- ktorá popisuje geografický kontext cien
create or replace table DIM_LOCATION as
select distinct
    location_id::varchar      as LOCATION_ID,
    location::varchar         as LOCATION,
    location_type::varchar    as LOCATION_TYPE
    
from GECKO_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging;

-- Vytvorenie dimenzie DIM_LOGISTIC s technickým ID
create or replace table DIM_LOGISTIC (
    LOGISTIC_ID    INT autoincrement start 1 primary key,
    FACTORY        varchar(200),
    TRANSPORT      varchar(100),
    TRANSPORT_TYPE varchar(100)
);

-- Naplnenie dimenzie DIM_LOGISTIC unikátnymi kombináciami
insert into DIM_LOGISTIC (FACTORY, TRANSPORT, TRANSPORT_TYPE)
select distinct
    factory::varchar(200),
    transport::varchar(100),
    transport_type::varchar(100)
from GECKO_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging;

-- Vytvorenie dimenzie DIM_TRADE,
-- ktorá obsahuje obchodné a kontraktačné podmienky
create or replace table DIM_TRADE (
    TRADE_ID INT autoincrement start 1 primary key,
    TRADE_TERMS varchar(50),
    TRADE_TERMS_DESCRIPTION varchar(200),
    TRANSACTION_TYPE varchar(100),
    QUOTE_APPROACH varchar(200),
    QUOTE_MEASUREMENT_STYLE varchar(200),
    DELIVERY_TIMEFRAME varchar(150),
    CONTRACT_PERIOD varchar(50),
    DELTA_TYPE varchar(200)                
);

-- Naplnenie dimenzie DIM_TRADE
INSERT INTO DIM_TRADE (TRADE_TERMS, TRADE_TERMS_DESCRIPTION, TRANSACTION_TYPE, QUOTE_APPROACH,QUOTE_MEASUREMENT_STYLE,DELIVERY_TIMEFRAME, CONTRACT_PERIOD, DELTA_TYPE)
select distinct
    trade_terms::varchar(50)               as TRADE_TERMS,
    trade_terms_description::varchar(200)  as TRADE_TERMS_DESCRIPTION,
    transaction_type::varchar(100)         as TRANSACTION_TYPE,
    quote_approach::varchar(200)           as QUOTE_APPROACH,
    quote_measurement_style::varchar(200)  as QUOTE_MEASUREMENT_STYLE,
    delivery_timeframe::varchar(150)       as DELIVERY_TIMEFRAME,
    contract_period::varchar(50)           as CONTRACT_PERIOD,
    delta_type::varchar(200)               as DELTA_TYPE
    
from GECKO_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging;

-- Vytvorenie dimenzie DIM_METRICS,
-- ktorá uchováva menu a merné jednotky
create or replace table DIM_METRICS (
    CURRENCY_ID  INT autoincrement start 1 primary key,
    CURRENCY varchar(100),              
    CURRENCY_CODE varchar(20),          
    CURRENCY_SYMBOL varchar(20),        
    MEASURE_UNIT varchar(50),           
    MEASURE_UNIT_SYMBOL varchar(20)
);

-- Naplnenie dimenzie DIM_METRICS
INSERT INTO DIM_METRICS (CURRENCY, CURRENCY_CODE, CURRENCY_SYMBOL, MEASURE_UNIT, MEASURE_UNIT_SYMBOL)
select distinct
    currency::varchar(100)                as CURRENCY,
    currency_code::varchar(20)            as CURRENCY_CODE,
    currency_symbol::varchar(20)          as CURRENCY_SYMBOL,
    measure_unit::varchar(50)             as MEASURE_UNIT,
    measure_unit_symbol::varchar(20)      as MEASURE_UNIT_SYMBOL
    
from GECKO_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging;

-- Vytvorenie faktovej tabuľky FACT_PRICE,
-- ktorá predstavuje centrálnu tabuľku modelu typu star schema
create or replace table FACT_PRICE as
select
    s.key::varchar(50)                              as KEY,
    s.series_key::varchar(50)                       as SERIES_KEY,

    to_number(to_char(s.created_for,'YYYYMMDD'))    as TIME_ID,
    s.created_for::date                             as CREATED_FOR,
    s.released_on::timestamp_ntz                    as RELEASED_ON,

    s.commodity_id::varchar(60)                     as COMMODITY_ID,
    s.location_id::varchar                          as LOCATION_ID,
 
    lg.LOGISTIC_ID                                  as LOGISTIC_ID,
    tr.TRADE_ID                                     as TRADE_ID,
    m.CURRENCY_ID                                   as CURRENCY_ID,

    s.is_estimated                                  as IS_ESTIMATED,

    s.assessment_high_precision                     as ASSESSMENT_HIGH_PRECISION,
    s.assessment_high_delta_precision               as ASSESSMENT_HIGH_DELTA_PRECISION,
    s.assessment_low_precision                      as ASSESSMENT_LOW_PRECISION,
    s.assessment_low_delta_precision                as ASSESSMENT_LOW_DELTA_PRECISION,
    s.mid_precision                                 as MID_PRECISION,
    s.mid_delta_precision                           as MID_DELTA_PRECISION,

    s.assessment_low::number(18,5)                  as ASSESSMENT_LOW,
    s.assessment_low_delta::number(18,5)            as ASSESSMENT_LOW_DELTA,
    s.assessment_mid::number(18,5)                  as ASSESSMENT_MID,
    s.assessment_mid_delta::number(18,5)            as ASSESSMENT_MID_DELTA,
    s.assessment_high::number(18,5)                 as ASSESSMENT_HIGH,
    s.assessment_high_delta::number(18,5)           as ASSESSMENT_HIGH_DELTA,

    row_number() over (
     partition by s.series_key
     order by s.created_for desc, s.released_on desc
     ) as RN_IN_SERIES,

    lag(s.assessment_mid::number(18,5)) over (
     partition by s.series_key
     order by s.created_for
     ) as PREV_MID,

    (s.assessment_mid::number(18,5)
    - lag(s.assessment_mid::number(18,5)) over (partition by s.series_key order by s.created_for)
    ) as MID_CHANGE


from GECKO_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging s

left join DIM_LOGISTIC lg
  on lg.FACTORY = s.factory
 and lg.TRANSPORT = s.transport
 and lg.TRANSPORT_TYPE = s.transport_type

left join DIM_TRADE tr
  on tr.TRADE_TERMS = s.trade_terms
 and tr.TRADE_TERMS_DESCRIPTION = s.trade_terms_description
 and tr.TRANSACTION_TYPE = s.transaction_type
 and tr.QUOTE_APPROACH = s.quote_approach
 and tr.QUOTE_MEASUREMENT_STYLE = s.quote_measurement_style
 and tr.DELIVERY_TIMEFRAME = s.delivery_timeframe
 and tr.CONTRACT_PERIOD = s.contract_period
 and tr.DELTA_TYPE = s.delta_type

left join DIM_METRICS m
  on m.CURRENCY_CODE = s.currency_code
 and m.MEASURE_UNIT = s.measure_unit
 and m.CURRENCY_SYMBOL = s.currency_symbol
 and m.MEASURE_UNIT_SYMBOL = s.measure_unit_symbol;

-- Odstránenie staging tabuľky po úspešnom spracovaní dát
DROP TABLE IF EXISTS chemical_price_assessments_staging;