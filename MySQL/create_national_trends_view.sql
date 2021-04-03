create
or replace
definer = root@localhost view v_NATIONAL_TRENDS as
	with `nuovi_positivi` as (select 'nuovi_positivi'                                            AS `id`,
                                 cast(
                                         `covid`.`NATIONAL`.`nuovi_positivi` as signed)      AS `count`,
                                 cast(lag(`covid`.`NATIONAL`.`nuovi_positivi`)
                                          OVER `w` as signed)                                AS `yesterday_count`,
                                 cast(
                                         `covid`.`NATIONAL`.`nuovi_positivi_perc` as signed) AS `percentage_difference`,
                                 (case
                                      when ((`covid`.`NATIONAL`.`nuovi_positivi` -
                                             lag(
                                                     `covid`.`NATIONAL`.`nuovi_positivi`)
                                                     OVER `w`) > 0)
                                          then 'increase'
                                      when ((`covid`.`NATIONAL`.`nuovi_positivi` -
                                             lag(
                                                     `covid`.`NATIONAL`.`nuovi_positivi`)
                                                     OVER `w`) < 0)
                                          then 'decrease'
                                      else 'stable' end)                                     AS `status`
                          from `covid`.`NATIONAL` window `w` AS (ORDER BY `covid`.`NATIONAL`.`data` )
                          order by `covid`.`NATIONAL`.`data` desc
                          limit 1),
     `ingressi_terapia_intensiva`
         as (select 'ingressi_terapia_intensiva'                                            AS `id`,
                    cast(
                            `covid`.`NATIONAL`.`ingressi_terapia_intensiva` as signed)      AS `count`,
                    cast(lag(`covid`.`NATIONAL`.`ingressi_terapia_intensiva`)
                             OVER `w` as signed)                                            AS `yesterday_count`,
                    cast(
                            `covid`.`NATIONAL`.`ingressi_terapia_intensiva_perc` as signed) AS `percentage_difference`,
                    (case
                         when ((`covid`.`NATIONAL`.`ingressi_terapia_intensiva` -
                                lag(
                                        `covid`.`NATIONAL`.`ingressi_terapia_intensiva`)
                                        OVER `w`) > 0) then 'increase'
                         when ((`covid`.`NATIONAL`.`ingressi_terapia_intensiva` -
                                lag(
                                        `covid`.`NATIONAL`.`ingressi_terapia_intensiva`)
                                        OVER `w`) < 0) then 'decrease'
                         else 'stable' end)                                                 AS `status`
             from `covid`.`NATIONAL` window `w` AS (ORDER BY `covid`.`NATIONAL`.`data` )
             order by `covid`.`NATIONAL`.`data` desc
             limit 1),
     `deceduti_g` as (select 'deceduti_g' AS                                            `id`,
                             cast(
                                     `covid`.`NATIONAL`.`deceduti_g` as signed) AS      `count`,
                             cast(lag(`covid`.`NATIONAL`.`deceduti_g`)
                                      OVER `w` as signed) AS                            `yesterday_count`,
                             cast(
                                     `covid`.`NATIONAL`.`deceduti_g_perc` as signed) AS `percentage_difference`,
                             (case
                                  when ((`covid`.`NATIONAL`.`deceduti_g` -
                                         lag(`covid`.`NATIONAL`.`deceduti_g`)
                                             OVER `w`) > 0) then 'increase'
                                  when ((`covid`.`NATIONAL`.`deceduti_g` -
                                         lag(`covid`.`NATIONAL`.`deceduti_g`)
                                             OVER `w`) < 0) then 'decrease'
                                  else 'stable' end) AS                                 `status`
                      from `covid`.`NATIONAL` window `w` AS (ORDER BY `covid`.`NATIONAL`.`data` )
                      order by `covid`.`NATIONAL`.`data` desc
                      limit 1),
     `tamponi_g` as (select 'tamponi_g' AS                                                     `id`,
                            cast(
                                    `covid`.`NATIONAL`.`tamponi_g` as signed) AS               `count`,
                            cast(
                                    lag(`covid`.`NATIONAL`.`tamponi_g`) OVER `w` as signed) AS `yesterday_count`,
                            cast(
                                    `covid`.`NATIONAL`.`tamponi_g_perc` as signed) AS          `percentage_difference`,
                            (case
                                 when ((`covid`.`NATIONAL`.`tamponi_g` -
                                        lag(`covid`.`NATIONAL`.`tamponi_g`) OVER `w`) >
                                       0) then 'increase'
                                 when ((`covid`.`NATIONAL`.`tamponi_g` -
                                        lag(`covid`.`NATIONAL`.`tamponi_g`) OVER `w`) <
                                       0) then 'decrease'
                                 else 'stable' end) AS                                         `status`
                     from `covid`.`NATIONAL` window `w` AS (ORDER BY `covid`.`NATIONAL`.`data` )
                     order by `covid`.`NATIONAL`.`data` desc
                     limit 1),
     `totale_ospedalizzati_g` as (select 'totale_ospedalizzati_g'                                            AS `id`,
                                         cast(
                                                 `covid`.`NATIONAL`.`totale_ospedalizzati_g` as signed)      AS `count`,
                                         cast(lag(
                                                 `covid`.`NATIONAL`.`totale_ospedalizzati_g`)
                                                 OVER `w` as signed)                                         AS `yesterday_count`,
                                         cast(
                                                 `covid`.`NATIONAL`.`totale_ospedalizzati_g_perc` as signed) AS `percentage_difference`,
                                         (case
                                              when ((`covid`.`NATIONAL`.`totale_ospedalizzati_g` -
                                                     lag(
                                                             `covid`.`NATIONAL`.`totale_ospedalizzati_g`)
                                                             OVER `w`) > 0)
                                                  then 'increase'
                                              when ((`covid`.`NATIONAL`.`totale_ospedalizzati_g` -
                                                     lag(
                                                             `covid`.`NATIONAL`.`totale_ospedalizzati_g`)
                                                             OVER `w`) < 0)
                                                  then 'decrease'
                                              else 'stable' end)                                             AS `status`
                                  from `covid`.`NATIONAL` window `w` AS (ORDER BY `covid`.`NATIONAL`.`data` )
                                  order by `covid`.`NATIONAL`.`data` desc
                                  limit 1),
     `totale_positivi` as (select 'totale_positivi'                                            AS `id`,
                                  cast(
                                          `covid`.`NATIONAL`.`totale_positivi` as signed)      AS `count`,
                                  cast(lag(
                                          `covid`.`NATIONAL`.`totale_positivi`)
                                          OVER `w` as signed)                                  AS `yesterday_count`,
                                  cast(
                                          `covid`.`NATIONAL`.`totale_positivi_perc` as signed) AS `percentage_difference`,
                                  (case
                                       when ((`covid`.`NATIONAL`.`totale_positivi` -
                                              lag(
                                                      `covid`.`NATIONAL`.`totale_positivi`)
                                                      OVER `w`) > 0)
                                           then 'increase'
                                       when ((`covid`.`NATIONAL`.`totale_positivi` -
                                              lag(
                                                      `covid`.`NATIONAL`.`totale_positivi`)
                                                      OVER `w`) < 0)
                                           then 'decrease'
                                       else 'stable' end)                                      AS `status`
                           from `covid`.`NATIONAL` window `w` AS (ORDER BY `covid`.`NATIONAL`.`data` )
                           order by `covid`.`NATIONAL`.`data` desc
                           limit 1),
     `terapia_intensiva` as (select 'terapia_intensiva'                                            AS `id`,
                                    cast(
                                            `covid`.`NATIONAL`.`terapia_intensiva` as signed)      AS `count`,
                                    cast(lag(
                                            `covid`.`NATIONAL`.`terapia_intensiva`)
                                            OVER `w` as signed)                                    AS `yesterday_count`,
                                    cast(
                                            `covid`.`NATIONAL`.`terapia_intensiva_perc` as signed) AS `percentage_difference`,
                                    (case
                                         when ((`covid`.`NATIONAL`.`terapia_intensiva` -
                                                lag(
                                                        `covid`.`NATIONAL`.`terapia_intensiva`)
                                                        OVER `w`) > 0)
                                             then 'increase'
                                         when ((`covid`.`NATIONAL`.`terapia_intensiva` -
                                                lag(
                                                        `covid`.`NATIONAL`.`terapia_intensiva`)
                                                        OVER `w`) < 0)
                                             then 'decrease'
                                         else 'stable' end)                                        AS `status`
                             from `covid`.`NATIONAL` window `w` AS (ORDER BY `covid`.`NATIONAL`.`data` )
                             order by `covid`.`NATIONAL`.`data` desc
                             limit 1),
     `ricoverati_con_sintomi` as (select 'ricoverati_con_sintomi'                                            AS `id`,
                                         cast(
                                                 `covid`.`NATIONAL`.`ricoverati_con_sintomi` as signed)      AS `count`,
                                         cast(lag(
                                                 `covid`.`NATIONAL`.`ricoverati_con_sintomi`)
                                                 OVER `w` as signed)                                         AS `yesterday_count`,
                                         cast(
                                                 `covid`.`NATIONAL`.`ricoverati_con_sintomi_perc` as signed) AS `percentage_difference`,
                                         (case
                                              when ((`covid`.`NATIONAL`.`ricoverati_con_sintomi` -
                                                     lag(
                                                             `covid`.`NATIONAL`.`ricoverati_con_sintomi`)
                                                             OVER `w`) > 0)
                                                  then 'increase'
                                              when ((`covid`.`NATIONAL`.`ricoverati_con_sintomi` -
                                                     lag(
                                                             `covid`.`NATIONAL`.`ricoverati_con_sintomi`)
                                                             OVER `w`) < 0)
                                                  then 'decrease'
                                              else 'stable' end)                                             AS `status`
                                  from `covid`.`NATIONAL` window `w` AS (ORDER BY `covid`.`NATIONAL`.`data` )
                                  order by `covid`.`NATIONAL`.`data` desc
                                  limit 1),
     `totale_ospedalizzati` as (select 'totale_ospedalizzati'                                            AS `id`,
                                       cast(
                                               `covid`.`NATIONAL`.`totale_ospedalizzati` as signed)      AS `count`,
                                       cast(lag(
                                               `covid`.`NATIONAL`.`totale_ospedalizzati`)
                                               OVER `w` as signed)                                       AS `yesterday_count`,
                                       cast(
                                               `covid`.`NATIONAL`.`totale_ospedalizzati_perc` as signed) AS `percentage_difference`,
                                       (case
                                            when ((`covid`.`NATIONAL`.`totale_ospedalizzati` -
                                                   lag(
                                                           `covid`.`NATIONAL`.`totale_ospedalizzati`)
                                                           OVER `w`) > 0)
                                                then 'increase'
                                            when ((`covid`.`NATIONAL`.`totale_ospedalizzati` -
                                                   lag(
                                                           `covid`.`NATIONAL`.`totale_ospedalizzati`)
                                                           OVER `w`) < 0)
                                                then 'decrease'
                                            else 'stable' end)                                           AS `status`
                                from `covid`.`NATIONAL` window `w` AS (ORDER BY `covid`.`NATIONAL`.`data` )
                                order by `covid`.`NATIONAL`.`data` desc
                                limit 1),
     `isolamento_domiciliare` as (select 'isolamento_domiciliare'                                            AS `id`,
                                         cast(
                                                 `covid`.`NATIONAL`.`isolamento_domiciliare` as signed)      AS `count`,
                                         cast(lag(
                                                 `covid`.`NATIONAL`.`isolamento_domiciliare`)
                                                 OVER `w` as signed)                                         AS `yesterday_count`,
                                         cast(
                                                 `covid`.`NATIONAL`.`isolamento_domiciliare_perc` as signed) AS `percentage_difference`,
                                         (case
                                              when ((`covid`.`NATIONAL`.`isolamento_domiciliare` -
                                                     lag(
                                                             `covid`.`NATIONAL`.`isolamento_domiciliare`)
                                                             OVER `w`) > 0)
                                                  then 'increase'
                                              when ((`covid`.`NATIONAL`.`isolamento_domiciliare` -
                                                     lag(
                                                             `covid`.`NATIONAL`.`isolamento_domiciliare`)
                                                             OVER `w`) < 0)
                                                  then 'decrease'
                                              else 'stable' end)                                             AS `status`
                                  from `covid`.`NATIONAL` window `w` AS (ORDER BY `covid`.`NATIONAL`.`data` )
                                  order by `covid`.`NATIONAL`.`data` desc
                                  limit 1),
     `totale_casi` as (select 'totale_casi'                                            AS `id`,
                              cast(
                                      `covid`.`NATIONAL`.`totale_casi` as signed)      AS `count`,
                              cast(lag(`covid`.`NATIONAL`.`totale_casi`)
                                       OVER `w` as signed)                             AS `yesterday_count`,
                              cast(
                                      `covid`.`NATIONAL`.`totale_casi_perc` as signed) AS `percentage_difference`,
                              (case
                                   when ((`covid`.`NATIONAL`.`totale_casi` -
                                          lag(`covid`.`NATIONAL`.`totale_casi`)
                                              OVER `w`) > 0) then 'increase'
                                   when ((`covid`.`NATIONAL`.`totale_casi` -
                                          lag(`covid`.`NATIONAL`.`totale_casi`)
                                              OVER `w`) < 0) then 'decrease'
                                   else 'stable' end)                                  AS `status`
                       from `covid`.`NATIONAL` window `w` AS (ORDER BY `covid`.`NATIONAL`.`data` )
                       order by `covid`.`NATIONAL`.`data` desc
                       limit 1),
     `deceduti` as (select 'deceduti' AS                                                     `id`,
                           cast(
                                   `covid`.`NATIONAL`.`deceduti` as signed) AS               `count`,
                           cast(
                                   lag(`covid`.`NATIONAL`.`deceduti`) OVER `w` as signed) AS `yesterday_count`,
                           cast(
                                   `covid`.`NATIONAL`.`deceduti_perc` as signed) AS          `percentage_difference`,
                           (case
                                when ((`covid`.`NATIONAL`.`deceduti` -
                                       lag(`covid`.`NATIONAL`.`deceduti`) OVER `w`) >
                                      0) then 'increase'
                                when ((`covid`.`NATIONAL`.`deceduti` -
                                       lag(`covid`.`NATIONAL`.`deceduti`) OVER `w`) <
                                      0) then 'decrease'
                                else 'stable' end) AS                                        `status`
                    from `covid`.`NATIONAL` window `w` AS (ORDER BY `covid`.`NATIONAL`.`data` )
                    order by `covid`.`NATIONAL`.`data` desc
                    limit 1),
     `tamponi` as (select 'tamponi'                                                     AS `id`,
                          cast(`covid`.`NATIONAL`.`tamponi` as signed)                  AS `count`,
                          cast(
                                  lag(`covid`.`NATIONAL`.`tamponi`) OVER `w` as signed) AS `yesterday_count`,
                          cast(
                                  `covid`.`NATIONAL`.`tamponi_perc` as signed)          AS `percentage_difference`,
                          (case
                               when ((`covid`.`NATIONAL`.`tamponi` -
                                      lag(`covid`.`NATIONAL`.`tamponi`) OVER `w`) >
                                     0) then 'increase'
                               when ((`covid`.`NATIONAL`.`tamponi` -
                                      lag(`covid`.`NATIONAL`.`tamponi`) OVER `w`) <
                                     0) then 'decrease'
                               else 'stable' end)                                       AS `status`
                   from `covid`.`NATIONAL` window `w` AS (ORDER BY `covid`.`NATIONAL`.`data` )
                   order by `covid`.`NATIONAL`.`data` desc
                   limit 1),
     `dimessi_guariti` as (select 'dimessi_guariti'                                            AS `id`,
                                  cast(
                                          `covid`.`NATIONAL`.`dimessi_guariti` as signed)      AS `count`,
                                  cast(lag(
                                          `covid`.`NATIONAL`.`dimessi_guariti`)
                                          OVER `w` as signed)                                  AS `yesterday_count`,
                                  cast(
                                          `covid`.`NATIONAL`.`dimessi_guariti_perc` as signed) AS `percentage_difference`,
                                  (case
                                       when ((`covid`.`NATIONAL`.`dimessi_guariti` -
                                              lag(
                                                      `covid`.`NATIONAL`.`dimessi_guariti`)
                                                      OVER `w`) > 0)
                                           then 'increase'
                                       when ((`covid`.`NATIONAL`.`dimessi_guariti` -
                                              lag(
                                                      `covid`.`NATIONAL`.`dimessi_guariti`)
                                                      OVER `w`) < 0)
                                           then 'decrease'
                                       else 'stable' end)                                      AS `status`
                           from `covid`.`NATIONAL` window `w` AS (ORDER BY `covid`.`NATIONAL`.`data` )
                           order by `covid`.`NATIONAL`.`data` desc
                           limit 1)
select `nuovi_positivi`.`id`                    AS `id`,
       `nuovi_positivi`.`count`                 AS `count`,
       `nuovi_positivi`.`yesterday_count`       AS `yesterday_count`,
       `nuovi_positivi`.`percentage_difference` AS `percentage_difference`,
       `nuovi_positivi`.`status`                AS `status`
from `nuovi_positivi`
union
select `ingressi_terapia_intensiva`.`id`                    AS `id`,
       `ingressi_terapia_intensiva`.`count`                 AS `count`,
       `ingressi_terapia_intensiva`.`yesterday_count`       AS `yesterday_count`,
       `ingressi_terapia_intensiva`.`percentage_difference` AS `percentage_difference`,
       `ingressi_terapia_intensiva`.`status`                AS `status`
from `ingressi_terapia_intensiva`
union
select `deceduti_g`.`id`                    AS `id`,
       `deceduti_g`.`count`                 AS `count`,
       `deceduti_g`.`yesterday_count`       AS `yesterday_count`,
       `deceduti_g`.`percentage_difference` AS `percentage_difference`,
       `deceduti_g`.`status`                AS `status`
from `deceduti_g`
union
select `tamponi_g`.`id`                    AS `id`,
       `tamponi_g`.`count`                 AS `count`,
       `tamponi_g`.`yesterday_count`       AS `yesterday_count`,
       `tamponi_g`.`percentage_difference` AS `percentage_difference`,
       `tamponi_g`.`status`                AS `status`
from `tamponi_g`
union
select `totale_ospedalizzati_g`.`id`                    AS `id`,
       `totale_ospedalizzati_g`.`count`                 AS `count`,
       `totale_ospedalizzati_g`.`yesterday_count`       AS `yesterday_count`,
       `totale_ospedalizzati_g`.`percentage_difference` AS `percentage_difference`,
       `totale_ospedalizzati_g`.`status`                AS `status`
from `totale_ospedalizzati_g`
union
select `totale_positivi`.`id`                    AS `id`,
       `totale_positivi`.`count`                 AS `count`,
       `totale_positivi`.`yesterday_count`       AS `yesterday_count`,
       `totale_positivi`.`percentage_difference` AS `percentage_difference`,
       `totale_positivi`.`status`                AS `status`
from `totale_positivi`
union all
select `terapia_intensiva`.`id`                    AS `id`,
       `terapia_intensiva`.`count`                 AS `count`,
       `terapia_intensiva`.`yesterday_count`       AS `yesterday_count`,
       `terapia_intensiva`.`percentage_difference` AS `percentage_difference`,
       `terapia_intensiva`.`status`                AS `status`
from `terapia_intensiva`
union all
select `ricoverati_con_sintomi`.`id`                    AS `id`,
       `ricoverati_con_sintomi`.`count`                 AS `count`,
       `ricoverati_con_sintomi`.`yesterday_count`       AS `yesterday_count`,
       `ricoverati_con_sintomi`.`percentage_difference` AS `percentage_difference`,
       `ricoverati_con_sintomi`.`status`                AS `status`
from `ricoverati_con_sintomi`
union all
select `totale_ospedalizzati`.`id`                    AS `id`,
       `totale_ospedalizzati`.`count`                 AS `count`,
       `totale_ospedalizzati`.`yesterday_count`       AS `yesterday_count`,
       `totale_ospedalizzati`.`percentage_difference` AS `percentage_difference`,
       `totale_ospedalizzati`.`status`                AS `status`
from `totale_ospedalizzati`
union all
select `isolamento_domiciliare`.`id`                    AS `id`,
       `isolamento_domiciliare`.`count`                 AS `count`,
       `isolamento_domiciliare`.`yesterday_count`       AS `yesterday_count`,
       `isolamento_domiciliare`.`percentage_difference` AS `percentage_difference`,
       `isolamento_domiciliare`.`status`                AS `status`
from `isolamento_domiciliare`
union all
select `totale_casi`.`id`                    AS `id`,
       `totale_casi`.`count`                 AS `count`,
       `totale_casi`.`yesterday_count`       AS `yesterday_count`,
       `totale_casi`.`percentage_difference` AS `percentage_difference`,
       `totale_casi`.`status`                AS `status`
from `totale_casi`
union all
select `deceduti`.`id`                    AS `id`,
       `deceduti`.`count`                 AS `count`,
       `deceduti`.`yesterday_count`       AS `yesterday_count`,
       `deceduti`.`percentage_difference` AS `percentage_difference`,
       `deceduti`.`status`                AS `status`
from `deceduti`
union all
select `tamponi`.`id`                    AS `id`,
       `tamponi`.`count`                 AS `count`,
       `tamponi`.`yesterday_count`       AS `yesterday_count`,
       `tamponi`.`percentage_difference` AS `percentage_difference`,
       `tamponi`.`status`                AS `status`
from `tamponi`
union all
select `dimessi_guariti`.`id`                    AS `id`,
       `dimessi_guariti`.`count`                 AS `count`,
       `dimessi_guariti`.`yesterday_count`       AS `yesterday_count`,
       `dimessi_guariti`.`percentage_difference` AS `percentage_difference`,
       `dimessi_guariti`.`status`                AS `status`
from `dimessi_guariti`;

