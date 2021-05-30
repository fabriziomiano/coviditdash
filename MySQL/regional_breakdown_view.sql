with
     rank_regional as (
         select *,
                ROW_NUMBER() over (PARTITION BY (denominazione_regione) order by data desc) AS rn
         from REGIONAL
        ),
     nuovi_positivi as (
         select 'nuovi_positivi' as id,
                denominazione_regione as area,
                nuovi_positivi   as count,
                CONCAT('/regions/', denominazione_regione) as url
         from rank_regional
         where rn=1
     ),
     ingressi_terapia_intensiva as (
         select 'ingressi_terapia_intensiva' as id,
                denominazione_regione as area,
                ingressi_terapia_intensiva   as count,
                CONCAT('/regions/', denominazione_regione) as url
         from rank_regional
         where rn=1
     ),
     deceduti_g as (
         select 'deceduti_g' as id,
                denominazione_regione as area,
                deceduti_g   as count,
                CONCAT('/regions/', denominazione_regione) as url
         from rank_regional
         where rn=1
     ),
     tamponi_g as (
         select 'tamponi_g' as id,
                denominazione_regione as area,
                tamponi_g   as count,
                CONCAT('/regions/', denominazione_regione) as url
         from rank_regional
         where rn=1
     ),
     totale_ospedalizzati_g as (
         select 'totale_ospedalizzati_g' as id,
                denominazione_regione as area,
                totale_ospedalizzati_g   as count,
                CONCAT('/regions/', denominazione_regione) as url
         from rank_regional
         where rn=1
     ),
     totale_positivi as (
         select 'totale_positivi' as id,
                denominazione_regione as area,
                totale_positivi   as count,
                CONCAT('/regions/', denominazione_regione) as url
         from rank_regional
         where rn=1
     ),
     terapia_intensiva as (
         select 'terapia_intensiva' as id,
                denominazione_regione as area,
                terapia_intensiva   as count,
                CONCAT('/regions/', denominazione_regione) as url
         from rank_regional
         where rn=1
     ),
     ricoverati_con_sintomi as (
         select 'ricoverati_con_sintomi' as id,
                denominazione_regione as area,
                ricoverati_con_sintomi   as count,
                CONCAT('/regions/', denominazione_regione) as url
         from rank_regional
         where rn=1
     ),
     totale_ospedalizzati as (
         select 'totale_ospedalizzati' as id,
                denominazione_regione as area,
                totale_ospedalizzati   as count,
                CONCAT('/regions/', denominazione_regione) as url
         from rank_regional
         where rn=1
     ),
     isolamento_domiciliare as (
         select 'isolamento_domiciliare' as id,
                denominazione_regione as area,
                isolamento_domiciliare   as count,
                CONCAT('/regions/', denominazione_regione) as url
         from rank_regional
         where rn=1
     ),
     totale_casi as (
         select 'totale_casi' as id,
                denominazione_regione as area,
                totale_casi   as count,
                CONCAT('/regions/', denominazione_regione) as url
         from rank_regional
         where rn=1
     ),
     deceduti as (
         select 'deceduti' as id,
                denominazione_regione as area,
                deceduti   as count,
                CONCAT('/regions/', denominazione_regione) as url
         from rank_regional
         where rn=1
     ),
     tamponi as (
         select 'tamponi' as id,
                denominazione_regione as area,
                tamponi   as count,
                CONCAT('/regions/', denominazione_regione) as url
         from rank_regional
         where rn=1
     ),
     dimessi_guariti as (
         select 'dimessi_guariti' as id,
                denominazione_regione as area,
                dimessi_guariti   as count,
                CONCAT('/regions/', denominazione_regione) as url
         from rank_regional
         where rn=1
     )
select *
from nuovi_positivi
union
select *
from ingressi_terapia_intensiva
union
select *
from deceduti_g
union
select *
from tamponi_g
union
select *
from totale_ospedalizzati_g
union
select *
from totale_positivi
union
select *
from terapia_intensiva
union
select *
from ricoverati_con_sintomi
union
select *
from totale_ospedalizzati
union
select *
from isolamento_domiciliare
union
select *
from totale_casi
union
select *
from deceduti
union
select *
from tamponi
union
select *
from dimessi_guariti