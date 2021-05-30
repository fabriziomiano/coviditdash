with
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
                'daily'                                    AS type,
                denominazione_provincia                      AS denominazione_provincia,
                data                                       AS data
         from covid.PROVINCIAL
         window w AS (PARTITION BY (denominazione_provincia) ORDER BY data)
         ),
     rank_totale_casi as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_provincia) order by data desc) AS rn
         from totale_casi
        ),
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
                denominazione_provincia                      AS denominazione_provincia,
                data                                       AS data
         from covid.PROVINCIAL
         window w AS (PARTITION BY (denominazione_provincia) ORDER BY data)
         ),
     rank_nuovi_positivi as (
         select *, ROW_NUMBER() over (PARTITION BY (denominazione_provincia) order by data desc) AS rn
         from nuovi_positivi
        )
select id, count, yesterday_count, percentage_difference, status, type, denominazione_provincia
from rank_totale_casi
where rn=1
union
select id, count, yesterday_count, percentage_difference, status, type, denominazione_provincia
from rank_nuovi_positivi
where rn=1