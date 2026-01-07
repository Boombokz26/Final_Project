# ELT proces datasetu Chemical Price Assessments datasetom
Tento repozitár predstavuje ukážkovú implementáciu ELT procesu v Snowflake a vytvorenie dátového skladu so schémou Star Schema. Projekt pracuje s Chemical Price Assessments datasetom. Projekt je zameraný na štúdium trhu so surovinami a chemickými výrobkami na základe cenových odhadov ICIS za určité obdobie. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

# 1 Chemical Price Assessments dataset
Vybrali sme tento dátový súbor, pretože odráža reálne obchodné procesy súvisiace s analýzou a monitorovaním komoditných trhov a môže byť použitý na podporu rozhodovania v oblasti nákupu, predaja a strategického plánovania. Štruktúra dátového súboru zahŕňa kvantitatívne ukazovatele aj popisné atribúty, čo ho robí vhodným na vytvorenie relačnej databázy.

# 1.1 Podporovaný obchodný proces
- sledovanie trhových trendov a zmien cien v čase
- komparatívna analýza trhov, regiónov a obchodných podmienok

# 1.2 Typy údajov v dátovom súbore  

Hlavné typy údajov v tabuľke sú Number, Varchar, Date, Timestamp_NTZ, Variant.

Zdrojové dáta pochádzajú z Snowflake datasetu dostupného [tu](https://app.snowflake.com/marketplace/listing/GZSVZ9FU7N/icis-independent-commodity-intelligence-services-chemical-price-assessments?search=chemical). Dataset obsahuje dve hlavných tabuliek:
- `CHEMICAL_PRICE_ASSESSMENTS` - Táto tabuľka obsahuje transakčné záznamy pre každé ocenenie ceny vytvorené pre cenovú sériu
- `CHEMICAL_PRICE_SPECIFICATIONS` - Táto tabuľka obsahuje vysokoúrovňové popisné informácie alebo metadáta o cenových radoch/kotáciách, ktoré pokrýva ICIS. Na každý cenový rad/kotáciu pripadá jeden riadok.


# 1.3 V tomto príklade analyzujeme cenové a trhové údaje o surovinách a chemických výrobkoch. Cieľom je porozumieť:
- Ako sa menia ceny
- Ktoré výrobky sú najdrahšie
- Aké sú najvolatilnejšie
- Hlavné metódy dodávky tovaru
- Hlavné spôsoby platby


Účelom ELT procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

### 1.4 Dátová architektúra

# Uvodny diagram

<img width="651" height="914" alt="stating" src="https://github.com/user-attachments/assets/ecb8e344-2241-43d3-83c2-36db06d4e3c7" />


# ERD diagram

Surové dáta sú usporiadané v relačnom modeli, ktorý je znázornený na entitno-relačnom diagrame (ERD):

![5395643886870401602](https://github.com/user-attachments/assets/a60b94a1-ca98-4485-aec9-efaa9e1fea75)
<p align="center">
  <em>Obrázok 1 Entitno-relačná schéma Chemical Price Assessments</em>
</p>

***
# 2 Dimenzionálny model

V ukážke bola navrhnutá schéma hviezdy (star schema) podľa Kimballovej metodológie, ktorá obsahuje 1 tabuľku faktov FACT_PRICE, ktorá je prepojená s nasledujúcimi 7 dimenziami:
- `DIM_COMMODITY`: Obsahuje podrobné informácie o komoditách a ich hierarchii(hlavná komodita, úroveň P0, P1, P2 a ich identifikátory).
- `DIM_TIME`: Obsahuje podrobné časové údaje(deň, mesiac, názov mesiaca, quartet, rok).
- `DIM_METRICS`: Obsahuje informácie o jednotkách merania a menách.(mena, kód meny, symbol meny, merná jednotka, symbol jednotky).
- `DIM_SERIES`: Obsahuje metadáta cenových sérií (kľúč série, názov série, typ cenovej položky, stav publikácie, dátumy platnosti, frekvencia, dátum vytvorenia a zverejnenia).
- `DIM_LOGISTIC`: Obsahuje logistické informácie súvisiace s dodávkou komodít(výrobný závod, spôsob dopravy, typ dopravy).
- `DIM_TRADE`: Obsahuje obchodné a kontraktačné podmienky hodnotenia cien(obchodné podmienky, typ transakcie, spôsob kotácie, dodacia lehota, kontraktné obdobie, typ delty).
- `DIM_LOCATION`: Obsahuje informácie o geografickej lokalite (názov lokality, typ lokality).

![5390944595648122413](https://github.com/user-attachments/assets/583245a2-bae6-422e-8817-1be43779099c)
<p align="center">
  <em>Obrázok 2 Schéma hviezdy pre Chemical Price Assessments</em>
</p>

***
# 3. ELT proces v Snowflake

ETL proces pozostáva z troch hlavných fáz: extrahovanie (Extract), načítanie (Load) a transformácia (Transform). Tento proces bol implementovaný v Snowflake s cieľom pripraviť zdrojové dáta zo staging vrstvy do viacdimenzionálneho modelu vhodného na analýzu a vizualizáciu.
***

### 3.1 Extract (Extrahovanie dát)
Dáta zo zdrojového datasetu (formát .csv) boli najprv nahraté do Snowflake prostredníctvom interného stage úložiska s názvom STAGE_STATING. Stage v Snowflake slúži ako dočasné úložisko na import alebo export dát. Vytvorenie stage bolo zabezpečené príkazom:

<strong>Príklad kódu:</strong>
```sql
CREATE OR REPLACE STAGE STAGE_STATING;
```
***

### 3.2 Load (Načítanie dát)
Do stage boli následne nahraté súbory obsahujúce údaje o cenových hodnoteniach chemických komodít. Dáta zahŕňajú metadáta o produktoch, regiónoch, menách, obchodných a logistických podmienkach, ako aj časové atribúty a cenové metriky (assessment low, mid a high), ktoré slúžia ako základ pre analytické spracovanie v dátovom sklade.
Dáta boli importované do staging tabuliek pomocou príkazu COPY INTO. Pre každú tabuľku sa použil podobný príkaz:

<strong>Príklad kódu:</strong>
```sql
COPY INTO chemical_price_assessments_staging
FROM '@STAGE_STATING/Chemical Price Assessments - Examples_2026-01-04-1315.csv'
FILE_FORMAT = my_csv_format
ON_ERROR = 'ABORT_STATEMENT';
```
V prípade nesprávnych záznamov bol použitý parameter `ON_ERROR = 'ABORT_STATEMENT'`, ktorý okamžite preruší proces načítania údajov a zabráni vloženiu nesprávnych záznamov do cieľovej tabuľky.
***

### 3.3 Transfor (Transformácia dát)
V tejto fáze boli dáta zo staging tabuliek vyčistené, transformované a obohatené. Hlavným cieľom bolo pripraviť dimenzie a faktovú tabuľku, ktoré umožnia jednoduchú a efektívnu analýzu.

### 3.3.1

Dimenzie boli navrhnuté na poskytnutie kontextu faktovej tabuľke. Dimenzia DIM_COMMODITY obsahuje informácie o chemickom produkte, ktorý hodnotíme. Táto tabuľka má tiež určitú hierarchiu, ktorá sa delí na samotný hodnotený tovar → rodičovský tovar → tovar‑predok, ktoré používame na hodnotenie ceny. Transformácia zahŕňala vyhýbanie sa duplicite textových údajov, zachovanie hierarchie tovarov a tiež možnosť analýzy na rôznych úrovniach agregácie (tovar → skupina → podskupina). Táto dimenzia je realizovaná ako SCD typu 0, čo je statické riešenie, keďže v našom datasete sú údaje stabilné a nevyžadujú historické zmeny. V tomto datasete je klasifikácia tovarov stabilná. V reálnom prípade by táto tabuľka bola s najväčšou pravdepodobnosťou typu SCD typu 2, avšak ide len o možné použitie.

Príklad kódu:

```sql
create or replace table DIM_COMMODITY as
select distinct
    commodity_id::varchar(60)                   as COMMODITY_ID,
    commodity::varchar(60)                      as COMMODITY_,
    commodity_p1_id::varchar(60)                as COMMODITY_P1_ID,
    commodity_p1::varchar(60)                   as COMMODITY_P1,
    commodity_p2_id::varchar(60)                as COMMODITY_P2_ID,
    commodity_p2::varchar(60)                   as COMMODITY_P2
    
from GECKO_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging;
```
***

### 3.3.2

Dimenzia DIM_TIME bola navrhnutá ako bežný „kalendár“ alebo časová dimenzná tabuľka. Bola vytvorená s cieľom odpovedať na otázku: „kedy bola cena zaznamenaná?“. Dôležitou súčasťou je aj to, že táto tabuľka transformuje „jednoduchý dátum“ z poľa created_for, kde created_for nie je NULL, na kalendárne atribúty, ktoré je následne možné pohodlne využívať v analytike. Obsahuje odvodené atribúty, ako sú deň, mesiac (v číselnom aj textovom formáte), rok a štvrťrok. Štruktúra tejto dimenzie umožňuje vykonávať časovú analýzu, najmä analýzu hodnotenia chemických tovarov podľa dní, mesiacov alebo rokov.

V tejto tabuľke je typ SCD klasifikovaný ako SCD 0, existujúce záznamy sú nemenné a uchovávajú statické informácie, pričom hodnota „kalendár“ sa historicky nemení (deň/mesiac/rok sú vždy rovnaké). V prípade potreby sledovania zmien súvisiacich s odvodenými atribútmi (napríklad pracovné dni a sviatky) by bolo možné prehodnotiť klasifikáciu na SCD typu 1 (aktualizácia hodnôt) alebo SCD typu 2 (uchovávanie histórie zmien). V aktuálnom modeli takáto potreba neexistuje, preto je dimenzia DIM_TIME realizovaná ako SCD typu 0 s možnosťou rozšírenia o nové záznamy podľa potreby.

Príklad kódu:

```sql
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
```
***

### 3.3.3

Dimenzia DIM_METRICS je dimenznou tabuľkou a uchováva informácie o mene a merných jednotkách cien.
Táto dimenzia uchováva atribúty currency, currency_code, currency_symbol, measure_unit, measure_unit_symbol. Patrí do typu SCD 0, ktorý je taktiež statický, pričom mena a merné jednotky sa v rámci datasetu historicky nemenia. Štruktúra tejto dimenzie umožňuje vykonávať multimenové a viacrozmerné analýzy.
Zabezpečuje správnu interpretáciu číselných hodnôt vo faktovej tabuľke.

Príklad kódu:

```sql
INSERT INTO DIM_METRICS (CURRENCY, CURRENCY_CODE, CURRENCY_SYMBOL, MEASURE_UNIT, MEASURE_UNIT_SYMBOL)
select distinct
    currency::varchar(100)                as CURRENCY,
    currency_code::varchar(20)            as CURRENCY_CODE,
    currency_symbol::varchar(20)          as CURRENCY_SYMBOL,
    measure_unit::varchar(50)             as MEASURE_UNIT,
    measure_unit_symbol::varchar(20)      as MEASURE_UNIT_SYMBOL
    
from GECKO_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging;
```
Dôležitá poznámka: v tomto projekte sme najprv vytvorili prázdnu tabuľku, v ktorej sa ID generovalo automaticky, keďže v datasete neboli použité žiadne špeciálne ID.

Príklad kódu:

```sql
create or replace table DIM_METRICS (
    CURRENCY_ID  INT autoincrement start 1 primary key,
    CURRENCY varchar(100),              
    CURRENCY_CODE varchar(20),          
    CURRENCY_SYMBOL varchar(20),        
    MEASURE_UNIT varchar(50),           
    MEASURE_UNIT_SYMBOL varchar(20)
);
```
***

### 3.3.4

Dimenzia DIM_SERIES je tabuľka, ktorá opisuje sériu cenových hodnotení a odpovedá na väčšinu biznis otázok: „Čo je to za séria? Kto ju vytvára? Ako a s akou frekvenciou sa publikuje? Je aktívna alebo ukončená?“. Uchováva atribúty series_name, originator, price_item_type, publish_status, launch_date, start_date, end_date, terminated, frequency. Tieto atribúty sa nenachádzajú vo faktovej tabuľke, pretože sa opakujú pre všetky záznamy danej série.
V našej implementácii je táto tabuľka typu SCD 0, avšak v reálnych prípadoch je najčastejšie typu SCD typu 2, aby sa uchovávala história zmien stavu a frekvencie.

Príklad kódu: 

```sql
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
```
***

### 3.3.5

Dimenzia DIM_LOCATION je dimenzná tabuľka, ktorá opisuje geografické miesto cenovej kategórie. Má iba dva atribúty: location a location_type.
Umožňuje analyzovať regionálne rozdiely cien a porovnávať trhy medzi sebou.
Taktiež má typ SCD 0, keďže nie je historicky meniteľná.

```sql
create or replace table DIM_LOCATION as
select distinct
    location_id::varchar      as LOCATION_ID,
    location::varchar         as LOCATION,
    location_type::varchar    as LOCATION_TYPE
    
from GECKO_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging;

```
***

### 3.3.6

Dimenzia DIM_LOGISTIC – táto tabuľka opisuje logistický kontext dodávky, pre ktorý sa vytvára cena. Táto tabuľka existuje na to, aby bolo možné analyzovať ceny v závislosti od dopravy a rôznych typov dodávok. Uchováva atribúty factory, transport, transport_type.
Táto tabuľka má taktiež typ SCD 0.

Príklad kódu: 

```sql
insert into DIM_LOGISTIC (FACTORY, TRANSPORT, TRANSPORT_TYPE)
select distinct
    factory::varchar(200),
    transport::varchar(100),
    transport_type::varchar(100)

from GECKO_DB.SHEMA_CHEMICAL_PRICE_ASSESSMENTS_STATING.chemical_price_assessments_staging;
```
Dôležitá poznámka: v tomto projekte sme najprv vytvorili prázdnu tabuľku, v ktorej sa ID generovalo automaticky, keďže v datasete neboli použité žiadne špeciálne ID.

Príklad kódu: 

```sql
create or replace table DIM_LOGISTIC (
    LOGISTIC_ID    INT autoincrement start 1 primary key,
    FACTORY        varchar(200),
    TRANSPORT      varchar(100),
    TRANSPORT_TYPE varchar(100)
);
```
***

### 3.3.7

Dimenzia DIM_TRADE je tabuľka, ktorá opisuje obchodné a zmluvné podmienky, za ktorých sa vytvára cena. Táto tabuľka umožňuje vykonávať analýzu dynamiky cien a oddeliť komerčný kontext od faktických hodnôt.
Taktiež má typ SCD 0.

Príklad kódu:

```sql
INSERT INTO DIM_TRADE (TRADE_TERMS, TRADE_TERMS_DESCRIPTION, TRANSACTION_TYPE, QUOTE_APPROACH, QUOTE_MEASUREMENT_STYLE, DELIVERY_TIMEFRAME, CONTRACT_PERIOD, DELTA_TYPE)
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
```
Dôležitá poznámka: v tomto projekte sme najprv vytvorili prázdnu tabuľku, v ktorej sa ID generovalo automaticky, keďže v datasete neboli použité žiadne špeciálne ID.

Príklad kódu:

```sql
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
```
***

### 3.3.8

FACT_PRICE uchováva cenové hodnotenia ako udalosti, ktoré sú viazané na konkrétnu sériu, tovar, lokalitu, čas a obchodné podmienky.
Tabuľka obsahuje kľúčové cenové metriky a vypočítané ukazovatele dynamiky, ktoré umožňujú analyzovať trendy, volatilitu a zmeny cien v čase. Je centrálnym prvkom schémy typu star schema.
Taktiež sme v tejto tabuľke pridali niekoľko okenných funkcií:
1. RN_IN_SERIES (row_number) – čísluje záznamy v rámci každej series_key.
2. PREV_MID (lag) – pre každý záznam berie predchádzajúcu hodnotu MID ceny v rovnakej sérii.
3. MID_CHANGE (rozdiel oproti predchádzajúcej) – vypočítava absolútnu zmenu MID ceny medzi aktuálnym a predchádzajúcim záznamom v rámci série.

Príklad kódu:
```sql
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
```


ELT proces v Snowflake umožnil transformovať pôvodné údaje z formátu .csv na viacrozmerný model typu „hviezda“. Tento proces zahŕňal čistenie, obohatenie a reorganizáciu údajov. Výsledný model zabezpečuje možnosť analýzy čitateľských preferencií a správania používateľov a slúži ako základ pre tvorbu vizualizácií a reportov.

Po úspešnom vytvorení dimenzií a faktovej tabuľky boli údaje nahrané do finálnej štruktúry. Na záver boli staging tabuľky odstránené s cieľom optimalizácie využitia úložiska.

Príklad kódu:

```sql
DROP TABLE IF EXISTS chemical_price_assessments_staging;
```
***

# 4 Vizualizácia dát

Dashboard obsahuje `6 vizualizácií`,ktorý poskytuje základný prehľad kľúčových ukazovateľov a trendov týkajúcich sa surovín a chemických výrobkov. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť trh so surovinami a chemickými výrobkami a ich trendy.
***
# Graf 1: Najpopulárnejší čas doručenia
Táto vizualizácia ukazuje najpopulárnejší čas doručenia. Umožňuje určiť, kedy sa tovar najčastejšie dodáva kupujúcemu. Graf ukazuje, že najobľúbenejším termínom dodania je približne jeden mesiac. Tieto údaje môžu byť užitočné pri plánovaní logistiky, tvorbe cenovej stratégie a optimalizácii prevádzkových procesov.
```sql
select
  tr.DELIVERY_TIMEFRAME,
  count(*) as CNT
from FACT_PRICE f
left join DIM_TRADE tr on tr.TRADE_ID = f.TRADE_ID
where tr.DELIVERY_TIMEFRAME is not null
group by 1
order by CNT desc;
```

<img width="1347" height="556" alt="graf1" src="https://github.com/user-attachments/assets/dafbf439-a461-420c-abc4-882c848d0be4" />

***

# Graf 2: Najbežnejšie podmienky zmlúv

Táto vizualizácia zobrazuje najbežnejšie podmienky zmlúv. Z údajov je zrejmé, že najbežnejšou podmienkou zmluvy je FOB. Tieto údaje môžu byť užitočné pre analýzu trhových štandardov, optimalizáciu zmluvných podmienok a prijímanie rozhodnutí v oblasti logistiky a cenotvorby.

```sql
select
  tr.TRADE_TERMS,
  count(*) as CNT
from FACT_PRICE f
left join DIM_TRADE tr
  on tr.TRADE_ID = f.TRADE_ID
where tr.TRADE_TERMS is not null
group by tr.TRADE_TERMS
order by CNT desc;
```

<img width="1343" height="592" alt="graf2" src="https://github.com/user-attachments/assets/06baaf84-8358-4902-8e49-5d3cf5bc4907" />

# Graf 3: Najbežnejšie meny pri platbe za tovar

Táto vizualizácia ukazuje najbežnejšie meny používané pri platbe za tovar. Pomôže vám pochopiť, ktoré meny zákazníci používajú pri platbe najčastejšie. Ako vidíme, najbežnejšie meny používané pri platbe sú čínsky jüan a americký dolár.  

```sql
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
```

<img width="1357" height="622" alt="graf3" src="https://github.com/user-attachments/assets/10dfe7c1-14f2-4f33-bf70-f225688d7bb4" />


# Graf 4: Najobľúbenejšie regióny pre nákupy(Top 10 regionov)

Táto vizualizácia ukazuje 10 regiónov, v ktorých sa najčastejšie objednávajú tovary. Ako vidíme, najrozšírenejším regiónom je Čína. Táto vizualizácia pomôže identifikovať regióny s najväčšou nákupnou aktivitou, aby bolo možné uprednostniť kľúčové trhy.

```sql
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
```

<img width="1373" height="662" alt="graf4" src="https://github.com/user-attachments/assets/71388bc6-2b90-480a-b69a-c5e3e8baa231" />


# Graf 5: Zmeny ceny ropy Brent

Táto vizualizácia ukazuje zmeny ceny ropy za 1 mesiac. Z grafu je vidieť, že cena ropy je nestabilná, stúpa a klesá. Na základe týchto údajov je možné pochopiť trhový trend a volatilitu ropy ako komodity, vďaka čomu je možné prijímať strategické rozhodnutia.

```sql
select
  f.CREATED_FOR,
  f.MID_CHANGE
from FACT_PRICE f
where f.SERIES_KEY = 'petchem_1901011'
  and f.MID_CHANGE is not null
order by f.CREATED_FOR, f.RELEASED_ON;
```

<img width="1678" height="695" alt="graf5" src="https://github.com/user-attachments/assets/2c247d53-5682-41e7-b86e-f2fea5281a3e" />


# Graf 6: TOP 10 najrizikovejších komodít iba v juanoch (CNY)

Táto vizualizácia ukazuje 10 najrizikovejších komodít na trhu. Napríklad butadién má najvyšší koeficient zmeny ceny. To pomôže spoločnostiam zistiť najrizikovejšie komodity a vypočítať riziká pri ich obchodovaní.

```sql
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
limit 10;
```
<img width="1388" height="700" alt="graf6" src="https://github.com/user-attachments/assets/e7a5896d-f92d-41fc-b270-103d36415ca5" />


