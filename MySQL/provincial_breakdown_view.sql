alter view covid.v_PROVINCIAL_BREAKDOWN AS
    with
     rank_provincial as (
         select P.*,
                ROW_NUMBER() over (PARTITION BY (denominazione_provincia) order by P.data desc) AS rn
         from PROVINCIAL P
         join REGIONAL R on P.denominazione_regione = R.denominazione_regione
         and P.data = R.data
        ),
     nuovi_positivi as (
         select 'nuovi_positivi' as id,
                denominazione_regione,
                denominazione_provincia as area,
                nuovi_positivi   as count,
                CONCAT('/provinces/', denominazione_provincia) as url
         from rank_provincial
         where rn=1
     ),
     totale_casi as (
         select 'totale_casi' as id,
                denominazione_regione,
                denominazione_provincia as area,
                totale_casi   as count,
                CONCAT('/provinces/', denominazione_provincia) as url
         from rank_provincial
         where rn=1
     )
select *
from nuovi_positivi
union
select *
from totale_casi