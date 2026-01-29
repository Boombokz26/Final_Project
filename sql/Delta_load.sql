create schema if not exists  SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING;
create schema if not exists  SHEMA_CHEMICAL_PRICE_ASSESSMENTS_DWH;

use schema SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING;


create stage if not exists STAGE_STATING;


LIST @GECKO_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.STAGE_STATING;
SHOW STAGES IN SCHEMA SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING;

create table if not exists chemical_price_assessments_staging (
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

truncate table chemical_price_assessments_staging;

create file format if not exists  my_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL','null')
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

COPY INTO chemical_price_assessments_staging
FROM '@STAGE_STATING/Chemical Price Assessments - Examples_2026-01-02-2357.csv'
FILE_FORMAT = my_csv_format
ON_ERROR = 'CONTINUE';

use schema SHEMA_CHEMICAL_PRICE_ASSESSMENTS_DWH;


create table if not exists DIM_TIME (
    TIME_ID number primary key,
    DAY number,
    MONTH number,
    MONTH_NAME varchar,
    QUARTET varchar,
    YEAR number
);

insert into DIM_TIME (TIME_ID, DAY, MONTH, MONTH_NAME, QUARTET, YEAR)
select distinct
    to_number(to_char(created_for, 'YYYYMMDD')) as TIME_ID,
    extract(day from created_for)               as DAY,
    extract(month from created_for)             as MONTH,
    to_char(created_for, 'MMMM')                as MONTH_NAME,
    'Q' || extract(quarter from created_for)    as QUARTET,
    extract(year from created_for)              as YEAR
    
from FOX_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging s

where created_for is not null
    and not exists (
    select 1
    from DIM_TIME t
    where t.TIME_ID = to_number(to_char(s.created_for,'YYYYMMDD'))
  );



create table if not exists DIM_SERIES (
  SERIES_KEY   varchar(50) not null,
  RELEASED_ON  timestamp_ntz not null,
  ORIGINATOR varchar(25),
  SERIES_NAME varchar,
  PRICE_ITEM_TYPE varchar(30),
  PUBLISH_STATUS varchar(30),
  LAUNCH_DATE date,
  START_DATE date,
  END_DATE date,
  TERMINATED boolean,
  FREQUENCY varchar(20),

  primary key (SERIES_KEY, RELEASED_ON)
);


insert into DIM_SERIES (
  SERIES_KEY, RELEASED_ON,
  ORIGINATOR, SERIES_NAME, PRICE_ITEM_TYPE, PUBLISH_STATUS,
  LAUNCH_DATE, START_DATE, END_DATE, TERMINATED, FREQUENCY
)
select distinct
  s.series_key::varchar(50)          as SERIES_KEY,
  s.released_on::timestamp_ntz       as RELEASED_ON,
  s.originator::varchar(25)          as ORIGINATOR,
  s.series_name::varchar             as SERIES_NAME,
  s.price_item_type::varchar(30)     as PRICE_ITEM_TYPE,
  s.publish_status::varchar(30)      as PUBLISH_STATUS,
  s.launch_date::date                as LAUNCH_DATE,
  s.start_date::date                 as START_DATE,
  s.end_date::date                   as END_DATE,
  s.terminated::boolean              as TERMINATED,
  s.frequency::varchar(20)           as FREQUENCY
from FOX_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging s
where s.series_key is not null
  and s.released_on is not null
  and not exists (
    select 1
    from DIM_SERIES d
    where d.SERIES_KEY  = s.series_key::varchar(50)
      and d.RELEASED_ON = s.released_on::timestamp_ntz
  );



select * from DIM_SERIES where SERIES_KEY = 'petchem_8602737';

create table if not exists DIM_COMMODITY (
    COMMODITY_ID varchar(60) primary key,
    COMMODITY_ varchar(60),
    COMMODITY_P1_ID varchar(60),
    COMMODITY_P1 varchar(60),
    COMMODITY_P2_ID varchar(60),
    COMMODITY_P2 varchar(60)
);

insert into DIM_COMMODITY (COMMODITY_ID, COMMODITY_, COMMODITY_P1_ID, COMMODITY_P1, COMMODITY_P2_ID, COMMODITY_P2)
select distinct
    commodity_id::varchar(60),
    commodity::varchar(60),
    commodity_p1_id::varchar(60),
    commodity_p1::varchar(60),
    commodity_p2_id::varchar(60),
    commodity_p2::varchar(60)
from FOX_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging s
where commodity_id is not null
  and not exists (
    select 1 from DIM_COMMODITY d
    where d.COMMODITY_ID = s.commodity_id::varchar(60)
  );

create table if not exists DIM_LOCATION (
    LOCATION_ID varchar primary key,
    LOCATION varchar,
    LOCATION_TYPE varchar
);

insert into DIM_LOCATION (LOCATION_ID, LOCATION, LOCATION_TYPE)
select distinct
    location_id::varchar      as LOCATION_ID,
    location::varchar         as LOCATION,
    location_type::varchar    as LOCATION_TYPE
from FOX_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging s
where location_id is not null
  and not exists (
    select 1
    from DIM_LOCATION d
    where d.LOCATION_ID = s.location_id::varchar
  );

create table if not exists DIM_LOGISTIC (
    LOGISTIC_ID    INT autoincrement start 1 primary key,
    FACTORY        varchar(200),
    TRANSPORT      varchar(100),
    TRANSPORT_TYPE varchar(100)
);

insert into DIM_LOGISTIC (FACTORY, TRANSPORT, TRANSPORT_TYPE)
select distinct
    factory::varchar(200)        as FACTORY,
    transport::varchar(100)      as TRANSPORT,
    transport_type::varchar(100) as TRANSPORT_TYPE
from FOX_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging s
where factory is not null
  and transport is not null
  and transport_type is not null
  and not exists (
    select 1
    from DIM_LOGISTIC d
    where d.FACTORY = s.factory::varchar(200)
      and d.TRANSPORT = s.transport::varchar(100)
      and d.TRANSPORT_TYPE = s.transport_type::varchar(100)
  );

create table if not exists DIM_TRADE (
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


insert into DIM_TRADE (
  TRADE_TERMS, TRADE_TERMS_DESCRIPTION, TRANSACTION_TYPE,
  QUOTE_APPROACH, QUOTE_MEASUREMENT_STYLE, DELIVERY_TIMEFRAME,
  CONTRACT_PERIOD, DELTA_TYPE
)
select distinct
    trade_terms::varchar(50)              as TRADE_TERMS,
    trade_terms_description::varchar(200) as TRADE_TERMS_DESCRIPTION,
    transaction_type::varchar(100)        as TRANSACTION_TYPE,
    quote_approach::varchar(200)          as QUOTE_APPROACH,
    quote_measurement_style::varchar(200) as QUOTE_MEASUREMENT_STYLE,
    delivery_timeframe::varchar(150)      as DELIVERY_TIMEFRAME,
    contract_period::varchar(50)          as CONTRACT_PERIOD,
    delta_type::varchar(200)              as DELTA_TYPE
from FOX_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging s
where trade_terms is not null
  and not exists (
    select 1
    from DIM_TRADE d
    where d.TRADE_TERMS = s.trade_terms::varchar(50)
      and d.TRADE_TERMS_DESCRIPTION = s.trade_terms_description::varchar(200)
      and d.TRANSACTION_TYPE = s.transaction_type::varchar(100)
      and d.QUOTE_APPROACH = s.quote_approach::varchar(200)
      and d.QUOTE_MEASUREMENT_STYLE = s.quote_measurement_style::varchar(200)
      and d.DELIVERY_TIMEFRAME = s.delivery_timeframe::varchar(150)
      and d.CONTRACT_PERIOD = s.contract_period::varchar(50)
      and d.DELTA_TYPE = s.delta_type::varchar(200)
  );

select * from DIM_TRADE;

create table if not exists DIM_METRICS (
      CURRENCY_ID  INT autoincrement start 1 primary key,
      CURRENCY varchar(100),
      CURRENCY_CODE varchar(20),
      CURRENCY_SYMBOL varchar(20),
      MEASURE_UNIT varchar(50),
      MEASURE_UNIT_SYMBOL varchar(20)
);

insert into DIM_METRICS (CURRENCY, CURRENCY_CODE, CURRENCY_SYMBOL, MEASURE_UNIT, MEASURE_UNIT_SYMBOL)
select distinct
      currency::varchar(100)           as CURRENCY,
      currency_code::varchar(20)       as CURRENCY_CODE,
      currency_symbol::varchar(20)     as CURRENCY_SYMBOL,
      measure_unit::varchar(50)        as MEASURE_UNIT,
      measure_unit_symbol::varchar(20) as MEASURE_UNIT_SYMBOL
from FOX_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging s
where currency_code is not null
  and measure_unit is not null
  and not exists (
    select 1
    from DIM_METRICS d
    where d.CURRENCY_CODE = s.currency_code::varchar(20)
      and d.MEASURE_UNIT = s.measure_unit::varchar(50)
      and d.CURRENCY_SYMBOL = s.currency_symbol::varchar(20)
      and d.MEASURE_UNIT_SYMBOL = s.measure_unit_symbol::varchar(20)
      and d.CURRENCY = s.currency::varchar(100)
  );

select * from DIM_METRICS;

create table if not exists FACT_PRICE as
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
      partition by s.series_key,s.released_on
      order by s.created_for desc, s.released_on desc
    ) as RN_IN_SERIES,
    lag(s.assessment_mid::number(18,5)) over (
      partition by s.series_key,s.released_on
      order by s.created_for
    ) as PREV_MID,
    (s.assessment_mid::number(18,5)
     - lag(s.assessment_mid::number(18,5)) over (partition by s.series_key,s.released_on order by s.created_for)
    ) as MID_CHANGE
from FOX_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging s
left join DIM_SERIES ds
  on ds.SERIES_KEY  = s.series_key::varchar(50)
 and ds.RELEASED_ON = s.released_on::timestamp_ntz
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
 and m.MEASURE_UNIT_SYMBOL = s.measure_unit_symbol
where 1=0;   -- ключевой трюк: создаёт таблицу, но не грузит данные

insert into FACT_PRICE
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
    partition by s.series_key, s.released_on
    order by s.created_for desc, s.released_on desc
    ) as RN_IN_SERIES,

    lag(s.assessment_mid::number(18,5)) over (
    partition by s.series_key, s.released_on
    order by s.created_for
    ) as PREV_MID,


    (s.assessment_mid::number(18,5)
        - lag(s.assessment_mid::number(18,5)) over (
      partition by s.series_key, s.released_on
      order by s.created_for
     )
) as MID_CHANGE

from FOX_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging s

left join DIM_SERIES ds
  on ds.SERIES_KEY  = s.series_key::varchar(50)
 and ds.RELEASED_ON = s.released_on::timestamp_ntz

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
   and m.MEASURE_UNIT_SYMBOL = s.measure_unit_symbol

where s.key is not null
  and not exists (
      select 1
      from FACT_PRICE f
      where f.KEY = s.key::varchar(50) and f.RELEASED_ON = s.released_on::timestamp_ntz
  );



select * from FACT_PRICE;

select count(*) as CNT
from FACT_PRICE f
join DIM_METRICS m
  on m.CURRENCY_ID = f.CURRENCY_ID
where m.CURRENCY_CODE = 'CNY';


select
  series_key, released_on, created_for,
  originator, series_name, price_item_type, publish_status,
  launch_date, start_date, end_date, terminated, frequency
from FOX_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging
where series_key = 'petchem_1905483'
order by released_on desc nulls last, created_for desc nulls last;