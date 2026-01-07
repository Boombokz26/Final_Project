-- Graf 1: Najpopulárnejší čas doručenia

select
  tr.DELIVERY_TIMEFRAME,
  count(*) as CNT
from FACT_PRICE f
left join DIM_TRADE tr on tr.TRADE_ID = f.TRADE_ID
where tr.DELIVERY_TIMEFRAME is not null
group by 1
order by CNT desc;

-- Graf 2: Najbežnejšie podmienky zmlúv

select
  tr.TRADE_TERMS,
  count(*) as CNT
from FACT_PRICE f
left join DIM_TRADE tr
  on tr.TRADE_ID = f.TRADE_ID
where tr.TRADE_TERMS is not null
group by tr.TRADE_TERMS
order by CNT desc;

-- Graf 3: Najbežnejšie meny pri platbe za Tovar

select
  m.CURRENCY_CODE,
  count(*) as CNT
from FACT_PRICE f
join DIM_METRICS m
  on m.CURRENCY_ID = f.CURRENCY_ID
group by
  m.CURRENCY_CODE,
  m.CURRENCY
order by CNT desc;

-- Graf 4: Najobľúbenejšie regióny pre nákupy(Top 10 regionov)

select
  l.LOCATION,
  count(*) as CNT
from FACT_PRICE f
join DIM_LOCATION l
  on l.LOCATION_ID = f.LOCATION_ID
group by
  l.LOCATION,
  l.LOCATION_TYPE
order by CNT desc
limit 10;

-- Graf 5: Zmeny ceny ropy Brent

select
  f.CREATED_FOR,
  f.MID_CHANGE
from FACT_PRICE f
where f.SERIES_KEY = 'petchem_1901011'
  and f.MID_CHANGE is not null
order by f.CREATED_FOR, f.RELEASED_ON;

-- Graf 6: TOP 10 najrizikovejších komodít iba v juanoch (CNY)

select
    c.COMMODITY_ as COMMODITY,
    avg(f.ASSESSMENT_MID) as AVG_PRICE_CNY,
    stddev(f.ASSESSMENT_MID) as STD_PRICE_CNY,
    stddev(f.ASSESSMENT_MID) / nullif(avg(f.ASSESSMENT_MID), 0) as CV_RISK,
    count(*) as N_POINTS
from FACT_PRICE f
join DIM_COMMODITY c
  on c.COMMODITY_ID = f.COMMODITY_ID
join DIM_METRICS m
  on m.CURRENCY_ID = f.CURRENCY_ID
where f.ASSESSMENT_MID is not null
  and m.CURRENCY_CODE = 'CNY'
group by c.COMMODITY_
having count(*) >= 10
order by CV_RISK desc
limi<img width="1388" height="700" alt="graf6" src="https://github.com/user-attachments/assets/3127048c-88da-4032-99ce-0dcf10ca7fbb" />
t 10;