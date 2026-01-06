# ELT proces datasetu Chemical Price Assessments
Tento repozitár predstavuje ukážkovú implementáciu ELT procesu v Snowflake a vytvorenie dátového skladu so schémou Star Schema. Projekt pracuje s Chemical Price Assessments datasetom. Projekt je zameraný na štúdium trhu so surovinami a chemickými výrobkami na základe cenových odhadov ICIS za určité obdobie. Výsledný dátový model umožňuje multidimenzionálnu analýzu a vizualizáciu kľúčových metrik.

# 1. Úvod a popis zdrojových dát
V tomto príklade analyzujeme cenové a trhové údaje o surovinách a chemických výrobkoch. Cieľom je porozumieť:
- Ako sa menia ceny
- Ktoré výrobky sú najdrahšie
- Aké sú najvolatilnejšie
- Hlavné metódy dodávky tovaru
- Hlavné spôsoby platby

Zdrojové dáta pochádzajú z Snowflake datasetu dostupného [tu](https://app.snowflake.com/marketplace/listing/GZSVZ9FU7N/icis-independent-commodity-intelligence-services-chemical-price-assessments?search=chemical). Dataset obsahuje dve hlavných tabuliek:
- `CHEMICAL_PRICE_ASSESSMENTS` - Táto tabuľka obsahuje transakčné záznamy pre každé ocenenie ceny vytvorené pre cenovú sériu
- `CHEMICAL_PRICE_SPECIFICATIONS` - Táto tabuľka obsahuje vysokoúrovňové popisné informácie alebo metadáta o cenových radoch/kotáciách, ktoré pokrýva ICIS. Na každý cenový rad/kotáciu pripadá jeden riadok.

Účelom ELT procesu bolo tieto dáta pripraviť, transformovať a sprístupniť pre viacdimenzionálnu analýzu.

### 1.1 Dátová architektúra

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




***
#4 Vizualizácia dát
Dashboard obsahuje `11 vizualizácií`,ktorý poskytuje základný prehľad kľúčových ukazovateľov a trendov týkajúcich sa surovín a chemických výrobkov. Tieto vizualizácie odpovedajú na dôležité otázky a umožňujú lepšie pochopiť trh so surovinami a chemickými výrobkami a ich trendy.
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
<img width="1347" height="556" alt="graf1" src="https://github.com/user-attachments/assets/dce1049f-bfbd-46c8-8ae4-16bbe87ce856" />
***
# Graf 2: Najpopulárnejší čas doručenia
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

