with
     nuovi_positivi as (
         select 'nuovi_positivi'                           AS id,
                cast(nuovi_positivi as signed)             AS count,
                cast(lag(nuovi_positivi) OVER w as signed) AS yesterday_count,
                cast(nuovi_positivi_perc as signed)        AS percentage_difference,
                (case
                    when ((nuovi_positivi - lag(nuovi_positivi) OVER w) > 0) then 'increase'
                    when ((nuovi_positivi - lag(nuovi_positivi) OVER w) < 0) then 'decrease'
                    else 'stable'
                    end)                                   AS status,
                'daily'                                    AS type,
                denominazione_regione                      AS denominazione_regione,
                data                                       AS data
         from covid.REGIONAL
         window w AS (PARTITION BY (denominazione_regione) ORDER BY data)
         ),
     rank_nuovi_positivi as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_regione) order by data desc) AS rn
         from nuovi_positivi
        ),
     ingressi_terapia_intensiva as (
         select 'ingressi_terapia_intensiva'                           AS id,
                cast(ingressi_terapia_intensiva as signed)             AS count,
                cast(lag(ingressi_terapia_intensiva) OVER w as signed) AS yesterday_count,
                cast(ingressi_terapia_intensiva_perc as signed)        AS percentage_difference,
                (case
                    when ((ingressi_terapia_intensiva - lag(ingressi_terapia_intensiva) OVER w) > 0) then 'increase'
                    when ((ingressi_terapia_intensiva - lag(ingressi_terapia_intensiva) OVER w) < 0) then 'decrease'
                    else 'stable'
                    end)                                   AS status,
                'daily'                                    AS type,
                denominazione_regione                      AS denominazione_regione,
                data                                       AS data
         from covid.REGIONAL
         window w AS (PARTITION BY (denominazione_regione) ORDER BY data)
         ),
     rank_ingressi_terapia_intensiva as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_regione) order by data desc) AS rn
         from ingressi_terapia_intensiva
        ),
     deceduti_g as (
         select 'deceduti_g'                           AS id,
                cast(deceduti_g as signed)             AS count,
                cast(lag(deceduti_g) OVER w as signed) AS yesterday_count,
                cast(deceduti_g_perc as signed)        AS percentage_difference,
                (case
                    when ((deceduti_g - lag(deceduti_g) OVER w) > 0) then 'increase'
                    when ((deceduti_g - lag(deceduti_g) OVER w) < 0) then 'decrease'
                    else 'stable'
                    end)                                   AS status,
                'daily'                                    AS type,
                denominazione_regione                      AS denominazione_regione,
                data                                       AS data
         from covid.REGIONAL
         window w AS (PARTITION BY (denominazione_regione) ORDER BY data)
         ),
     rank_deceduti_g as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_regione) order by data desc) AS rn
         from deceduti_g
        ),
     tamponi_g as (
         select 'tamponi_g'                           AS id,
                cast(tamponi_g as signed)             AS count,
                cast(lag(tamponi_g) OVER w as signed) AS yesterday_count,
                cast(tamponi_g_perc as signed)        AS percentage_difference,
                (case
                    when ((tamponi_g - lag(tamponi_g) OVER w) > 0) then 'increase'
                    when ((tamponi_g - lag(tamponi_g) OVER w) < 0) then 'decrease'
                    else 'stable'
                    end)                                   AS status,
                'daily'                                    AS type,
                denominazione_regione                      AS denominazione_regione,
                data                                       AS data
         from covid.REGIONAL
         window w AS (PARTITION BY (denominazione_regione) ORDER BY data)
         ),
     rank_tamponi_g as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_regione) order by data desc) AS rn
         from tamponi_g
        ),
     totale_ospedalizzati_g as (
         select 'totale_ospedalizzati_g'                           AS id,
                cast(totale_ospedalizzati_g as signed)             AS count,
                cast(lag(totale_ospedalizzati_g) OVER w as signed) AS yesterday_count,
                cast(totale_ospedalizzati_g_perc as signed)        AS percentage_difference,
                (case
                    when ((totale_ospedalizzati_g - lag(totale_ospedalizzati_g) OVER w) > 0) then 'increase'
                    when ((totale_ospedalizzati_g - lag(totale_ospedalizzati_g) OVER w) < 0) then 'decrease'
                    else 'stable'
                    end)                                   AS status,
                'daily'                                    AS type,
                denominazione_regione                      AS denominazione_regione,
                data                                       AS data
         from covid.REGIONAL
         window w AS (PARTITION BY (denominazione_regione) ORDER BY data)
         ),
     rank_totale_ospedalizzati_g as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_regione) order by data desc) AS rn
         from totale_ospedalizzati_g
        ),
     totale_positivi as (
         select 'totale_positivi'                           AS id,
                cast(totale_positivi as signed)             AS count,
                cast(lag(totale_positivi) OVER w as signed) AS yesterday_count,
                cast(totale_positivi_perc as signed)        AS percentage_difference,
                (case
                    when ((totale_positivi - lag(totale_positivi) OVER w) > 0) then 'increase'
                    when ((totale_positivi - lag(totale_positivi) OVER w) < 0) then 'decrease'
                    else 'stable'
                    end)                                   AS status,
                'current'                                  AS type,
                denominazione_regione                      AS denominazione_regione,
                data                                       AS data
         from covid.REGIONAL
         window w AS (PARTITION BY (denominazione_regione) ORDER BY data)
         ),
     rank_totale_positivi as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_regione) order by data desc) AS rn
         from totale_positivi
         ),
     terapia_intensiva as (
         select 'terapia_intensiva'                           AS id,
                cast(terapia_intensiva as signed)             AS count,
                cast(lag(terapia_intensiva) OVER w as signed) AS yesterday_count,
                cast(terapia_intensiva_perc as signed)        AS percentage_difference,
                (case
                    when ((terapia_intensiva - lag(terapia_intensiva) OVER w) > 0) then 'increase'
                    when ((terapia_intensiva - lag(terapia_intensiva) OVER w) < 0) then 'decrease'
                    else 'stable'
                    end)                                   AS status,
                'current'                                  AS type,
                denominazione_regione                      AS denominazione_regione,
                data                                       AS data
         from covid.REGIONAL
         window w AS (PARTITION BY (denominazione_regione) ORDER BY data)
         ),
     rank_terapia_intensiva as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_regione) order by data desc) AS rn
         from terapia_intensiva
         ),
     ricoverati_con_sintomi as (
         select 'ricoverati_con_sintomi'                           AS id,
                cast(ricoverati_con_sintomi as signed)             AS count,
                cast(lag(ricoverati_con_sintomi) OVER w as signed) AS yesterday_count,
                cast(ricoverati_con_sintomi_perc as signed)        AS percentage_difference,
                (case
                    when ((ricoverati_con_sintomi - lag(ricoverati_con_sintomi) OVER w) > 0) then 'increase'
                    when ((ricoverati_con_sintomi - lag(ricoverati_con_sintomi) OVER w) < 0) then 'decrease'
                    else 'stable'
                    end)                                   AS status,
                'current'                                  AS type,
                denominazione_regione                      AS denominazione_regione,
                data                                       AS data
         from covid.REGIONAL
         window w AS (PARTITION BY (denominazione_regione) ORDER BY data)
         ),
     rank_ricoverati_con_sintomi as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_regione) order by data desc) AS rn
         from ricoverati_con_sintomi
         ),
     totale_ospedalizzati as (
         select 'totale_ospedalizzati'                           AS id,
                cast(totale_ospedalizzati as signed)             AS count,
                cast(lag(totale_ospedalizzati) OVER w as signed) AS yesterday_count,
                cast(totale_ospedalizzati_perc as signed)        AS percentage_difference,
                (case
                    when ((totale_ospedalizzati - lag(totale_ospedalizzati) OVER w) > 0) then 'increase'
                    when ((totale_ospedalizzati - lag(totale_ospedalizzati) OVER w) < 0) then 'decrease'
                    else 'stable'
                    end)                                   AS status,
                'current'                                  AS type,
                denominazione_regione                      AS denominazione_regione,
                data                                       AS data
         from covid.REGIONAL
         window w AS (PARTITION BY (denominazione_regione) ORDER BY data)
         ),
     rank_totale_ospedalizzati as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_regione) order by data desc) AS rn
         from totale_ospedalizzati
         ),
     isolamento_domiciliare as (
         select 'isolamento_domiciliare'                           AS id,
                cast(isolamento_domiciliare as signed)             AS count,
                cast(lag(isolamento_domiciliare) OVER w as signed) AS yesterday_count,
                cast(isolamento_domiciliare_perc as signed)        AS percentage_difference,
                (case
                    when ((isolamento_domiciliare - lag(isolamento_domiciliare) OVER w) > 0) then 'increase'
                    when ((isolamento_domiciliare - lag(isolamento_domiciliare) OVER w) < 0) then 'decrease'
                    else 'stable'
                    end)                                   AS status,
                'current'                                  AS type,
                denominazione_regione                      AS denominazione_regione,
                data                                       AS data
         from covid.REGIONAL
         window w AS (PARTITION BY (denominazione_regione) ORDER BY data)
         ),
     rank_isolamento_domiciliare as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_regione) order by data desc) AS rn
         from isolamento_domiciliare
         ),
     totale_casi as (
         select 'totale_casi'                           AS id,
                cast(totale_casi as signed)             AS count,
                cast(lag(totale_casi) OVER w as signed) AS yesterday_count,
                cast(totale_casi_perc as signed)        AS percentage_difference,
                (case
                    when ((totale_casi - lag(totale_casi) OVER w) > 0) then 'increase'
                    when ((totale_casi - lag(totale_casi) OVER w) < 0) then 'decrease'
                    else 'stable'
                    end)                                   AS status,
                'cum'                                  AS type,
                denominazione_regione                      AS denominazione_regione,
                data                                       AS data
         from covid.REGIONAL
         window w AS (PARTITION BY (denominazione_regione) ORDER BY data)
         ),
     rank_totale_casi as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_regione) order by data desc) AS rn
         from totale_casi
     ),
     deceduti as (
         select 'deceduti'                           AS id,
                cast(deceduti as signed)             AS count,
                cast(lag(deceduti) OVER w as signed) AS yesterday_count,
                cast(deceduti_perc as signed)        AS percentage_difference,
                (case
                    when ((deceduti - lag(deceduti) OVER w) > 0) then 'increase'
                    when ((deceduti - lag(deceduti) OVER w) < 0) then 'decrease'
                    else 'stable'
                    end)                                   AS status,
                'cum'                                  AS type,
                denominazione_regione                      AS denominazione_regione,
                data                                       AS data
         from covid.REGIONAL
         window w AS (PARTITION BY (denominazione_regione) ORDER BY data)
         ),
     rank_deceduti as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_regione) order by data desc) AS rn
         from deceduti
     ),
     tamponi as (
         select 'tamponi'                           AS id,
                cast(tamponi as signed)             AS count,
                cast(lag(tamponi) OVER w as signed) AS yesterday_count,
                cast(tamponi_perc as signed)        AS percentage_difference,
                (case
                    when ((tamponi - lag(tamponi) OVER w) > 0) then 'increase'
                    when ((tamponi - lag(tamponi) OVER w) < 0) then 'decrease'
                    else 'stable'
                    end)                                   AS status,
                'cum'                                  AS type,
                denominazione_regione                      AS denominazione_regione,
                data                                       AS data
         from covid.REGIONAL
         window w AS (PARTITION BY (denominazione_regione) ORDER BY data)
         ),
     rank_tamponi as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_regione) order by data desc) AS rn
         from tamponi
     ),
     dimessi_guariti as (
         select 'dimessi_guariti'                           AS id,
                cast(dimessi_guariti as signed)             AS count,
                cast(lag(dimessi_guariti) OVER w as signed) AS yesterday_count,
                cast(dimessi_guariti_perc as signed)        AS percentage_difference,
                (case
                    when ((dimessi_guariti - lag(dimessi_guariti) OVER w) > 0) then 'increase'
                    when ((dimessi_guariti - lag(dimessi_guariti) OVER w) < 0) then 'decrease'
                    else 'stable'
                    end)                                   AS status,
                'cum'                                  AS type,
                denominazione_regione                      AS denominazione_regione,
                data                                       AS data
         from covid.REGIONAL
         window w AS (PARTITION BY (denominazione_regione) ORDER BY data)
         ),
     rank_dimessi_guariti as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_regione) order by data desc) AS rn
         from dimessi_guariti
     )

select id, count, yesterday_count, percentage_difference, status, type, denominazione_regione
from rank_nuovi_positivi
where rn=1
union
select id, count, yesterday_count, percentage_difference, status, type, denominazione_regione
from rank_ingressi_terapia_intensiva
where rn=1
union
select id, count, yesterday_count, percentage_difference, status, type, denominazione_regione
from rank_deceduti_g
where rn=1
union
select id, count, yesterday_count, percentage_difference, status, type, denominazione_regione
from rank_tamponi_g
where rn=1
union
select id, count, yesterday_count, percentage_difference, status, type, denominazione_regione
from rank_totale_ospedalizzati_g
where rn=1
union
select id, count, yesterday_count, percentage_difference, status, type, denominazione_regione
from rank_totale_positivi
where rn=1
union
select id, count, yesterday_count, percentage_difference, status, type, denominazione_regione
from rank_terapia_intensiva
where rn=1
union
select id, count, yesterday_count, percentage_difference, status, type, denominazione_regione
from rank_ricoverati_con_sintomi
where rn=1
union
select id, count, yesterday_count, percentage_difference, status, type, denominazione_regione
from rank_totale_ospedalizzati
where rn=1
union
select id, count, yesterday_count, percentage_difference, status, type, denominazione_regione
from rank_isolamento_domiciliare
where rn=1
union
select id, count, yesterday_count, percentage_difference, status, type, denominazione_regione
from rank_totale_casi
where rn=1
union
select id, count, yesterday_count, percentage_difference, status, type, denominazione_regione
from rank_deceduti
where rn=1
union
select id, count, yesterday_count, percentage_difference, status, type, denominazione_regione
from rank_tamponi
where rn=1
union
select id, count, yesterday_count, percentage_difference, status, type, denominazione_regione
from rank_dimessi_guariti
where rn=1
