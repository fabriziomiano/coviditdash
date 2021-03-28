-----------------------------------------
-- Create v_national
-----------------------------------------
CREATE OR ALTER VIEW [dbo].[v_national] AS 
WITH nat AS (
    SELECT [data]
         , [ricoverati_con_sintomi]
         , [terapia_intensiva]
         , [totale_ospedalizzati]
         , [isolamento_domiciliare]
         , [totale_positivi]
         , [nuovi_positivi]
         , [dimessi_guariti]
         , [deceduti]
         , [totale_casi]
         , [tamponi]
         , [note]
         , [ingressi_terapia_intensiva]
    FROM [dbo].[nazione]
)
,
diff AS (
    SELECT *
         , [ricoverati_con_sintomi] - LAG([ricoverati_con_sintomi], 1, 0) OVER(ORDER BY [data])         AS [ricoverati_con_sintomi_g]
         , [terapia_intensiva] - LAG([terapia_intensiva], 1, 0) OVER(ORDER BY [data])                   AS [terapia_intensiva_g]
         , [totale_ospedalizzati] - LAG([totale_ospedalizzati], 1, 0) OVER(ORDER BY [data])             AS [totale_ospedalizzati_g]
         , [isolamento_domiciliare] - LAG([isolamento_domiciliare], 1, 0) OVER(ORDER BY [data])         AS [isolamento_domiciliare_g]
         , [totale_positivi] - LAG([totale_positivi], 1, 0) OVER(ORDER BY [data])                       AS [totale_positivi_g]
         , [nuovi_positivi] - LAG([nuovi_positivi], 1, 0) OVER(ORDER BY [data])                         AS [nuovi_positivi_g]
         , [dimessi_guariti] - LAG([dimessi_guariti], 1, 0) OVER(ORDER BY [data])                       AS [dimessi_guariti_g]
         , [deceduti] - LAG([deceduti], 1, 0) OVER(ORDER BY [data])                                     AS [deceduti_g]
         , [totale_casi] - LAG([totale_casi], 1, 0) OVER(ORDER BY [data])                               AS [totale_casi_g]
         , [tamponi] - LAG([tamponi], 1, 0) OVER(ORDER BY [data])                                       AS [tamponi_g]
         , [ingressi_terapia_intensiva] - LAG([ingressi_terapia_intensiva], 1, 0) OVER(ORDER BY [data]) AS [ingressi_terapia_intensiva_g]
    FROM nat
)
,
perc AS (
    SELECT *
         , [ricoverati_con_sintomi_g] / CAST([ricoverati_con_sintomi] AS decimal) * 100         AS [ricoverati_con_sintomi_perc]
         , [terapia_intensiva_g] / CAST([terapia_intensiva] AS decimal) * 100                   AS [terapia_intensiva_perc]
         , [totale_ospedalizzati_g] / CAST([totale_ospedalizzati] AS decimal) * 100             AS [totale_ospedalizzati_perc]
         , [isolamento_domiciliare_g] / CAST([isolamento_domiciliare] AS decimal) * 100         AS [isolamento_domiciliare_perc]
         , [totale_positivi_g] / CAST([totale_positivi] AS decimal) * 100                       AS [totale_positivi_perc]
         , [nuovi_positivi_g] / CAST([nuovi_positivi] AS decimal) * 100                         AS [nuovi_positivi_perc]
         , [dimessi_guariti_g] / CAST([dimessi_guariti] AS decimal) * 100                       AS [dimessi_guariti_perc]
         , [deceduti_g] / CAST([deceduti] AS decimal) * 100                                     AS [deceduti_perc]
         , [totale_casi_g] / CAST([totale_casi] AS decimal) * 100                               AS [totale_casi_perc]
         , [tamponi_g] / CAST([tamponi] AS decimal) * 100                                       AS [tamponi_perc]
         , [ingressi_terapia_intensiva_g] / CAST([ingressi_terapia_intensiva] AS decimal) * 100 AS [ingressi_terapia_intensiva_perc]
    FROM diff
)
SELECT * FROM perc
GO
-----------------------------------------
-- Create v_regional
-----------------------------------------
CREATE OR ALTER VIEW [dbo].[v_regional] AS 
WITH reg AS (
    SELECT [data]
         , [denominazione_regione]
         , [ricoverati_con_sintomi]
         , [terapia_intensiva]
         , [totale_ospedalizzati]
         , [isolamento_domiciliare]
         , [totale_positivi]
         , [nuovi_positivi]
         , [dimessi_guariti]
         , [deceduti]
         , [totale_casi]
         , [tamponi]
         , [note]
         , [ingressi_terapia_intensiva]
    FROM [dbo].[regioni]
)
,
diff AS (
    SELECT *
         , [ricoverati_con_sintomi] - LAG([ricoverati_con_sintomi], 1, 0) OVER(PARTITION BY [denominazione_regione] ORDER BY [data])         AS [ricoverati_con_sintomi_g]
         , [terapia_intensiva] - LAG([terapia_intensiva], 1, 0) OVER(PARTITION BY [denominazione_regione] ORDER BY [data])                   AS [terapia_intensiva_g]
         , [totale_ospedalizzati] - LAG([totale_ospedalizzati], 1, 0) OVER(PARTITION BY [denominazione_regione] ORDER BY [data])             AS [totale_ospedalizzati_g]
         , [isolamento_domiciliare] - LAG([isolamento_domiciliare], 1, 0) OVER(PARTITION BY [denominazione_regione] ORDER BY [data])         AS [isolamento_domiciliare_g]
         , [totale_positivi] - LAG([totale_positivi], 1, 0) OVER(PARTITION BY [denominazione_regione] ORDER BY [data])                       AS [totale_positivi_g]
         , [nuovi_positivi] - LAG([nuovi_positivi], 1, 0) OVER(PARTITION BY [denominazione_regione] ORDER BY [data])                         AS [nuovi_positivi_g]
         , [dimessi_guariti] - LAG([dimessi_guariti], 1, 0) OVER(PARTITION BY [denominazione_regione] ORDER BY [data])                       AS [dimessi_guariti_g]
         , [deceduti] - LAG([deceduti], 1, 0) OVER(PARTITION BY [denominazione_regione] ORDER BY [data])                                     AS [deceduti_g]
         , [totale_casi] - LAG([totale_casi], 1, 0) OVER(PARTITION BY [denominazione_regione] ORDER BY [data])                               AS [totale_casi_g]
         , [tamponi] - LAG([tamponi], 1, 0) OVER(PARTITION BY [denominazione_regione] ORDER BY [data])                                       AS [tamponi_g]
         , [ingressi_terapia_intensiva] - LAG([ingressi_terapia_intensiva], 1, 0) OVER(PARTITION BY [denominazione_regione] ORDER BY [data]) AS [ingressi_terapia_intensiva_g]
    FROM reg
)
,
perc AS (
    SELECT *
         , CASE 
            WHEN [ricoverati_con_sintomi] <> 0 THEN [ricoverati_con_sintomi_g] / CAST([ricoverati_con_sintomi] AS decimal) * 100
            ELSE 0
            END [ricoverati_con_sintomi_perc]
         , CASE 
            WHEN [terapia_intensiva] <> 0 THEN [terapia_intensiva_g] / CAST([terapia_intensiva] AS decimal) * 100
            ELSE 0
            END [terapia_intensiva_perc]
         , CASE 
            WHEN [totale_ospedalizzati] <> 0 THEN [totale_ospedalizzati_g] / CAST([totale_ospedalizzati] AS decimal) * 100
            ELSE 0
            END [totale_ospedalizzati_perc]
         , CASE 
            WHEN [isolamento_domiciliare] <> 0 THEN [isolamento_domiciliare_g] / CAST([isolamento_domiciliare] AS decimal) * 100
            ELSE 0
            END [isolamento_domiciliare_perc]
         , CASE 
            WHEN [totale_positivi] <> 0 THEN [totale_positivi_g] / CAST([totale_positivi] AS decimal) * 100
            ELSE 0
            END [totale_positivi_perc]
         , CASE 
            WHEN [nuovi_positivi] <> 0 THEN [nuovi_positivi_g] / CAST([nuovi_positivi] AS decimal) * 100
            ELSE 0
            END [nuovi_positivi_perc]
         , CASE 
            WHEN [dimessi_guariti] <> 0 THEN [dimessi_guariti_g] / CAST([dimessi_guariti] AS decimal) * 100
            ELSE 0
            END [dimessi_guariti_perc]
         , CASE 
            WHEN [deceduti] <> 0 THEN [deceduti_g] / CAST([deceduti] AS decimal) * 100
            ELSE 0
            END [deceduti_perc]
         , CASE 
            WHEN [totale_casi] <> 0 THEN [totale_casi_g] / CAST([totale_casi] AS decimal) * 100
            ELSE 0
            END [totale_casi_perc]
         , CASE 
            WHEN [tamponi] <> 0 THEN [tamponi_g] / CAST([tamponi] AS decimal) * 100
            ELSE 0
            END [tamponi_perc]
         , CASE 
            WHEN [ingressi_terapia_intensiva] <> 0 THEN [ingressi_terapia_intensiva_g] / CAST([ingressi_terapia_intensiva] AS decimal) * 100 
            ELSE 0
            END [ingressi_terapia_intensiva_perc]
    FROM diff
)
SELECT * FROM perc
GO
-----------------------------------------
-- Create v_provincial
-----------------------------------------
CREATE OR ALTER VIEW [dbo].[v_provincial] AS 
WITH reg AS (
    SELECT [data]
         , [denominazione_regione]
         , [denominazione_provincia]
         , [totale_casi]
         , [note]
    FROM [dbo].[province]
)
,
new AS (
    SELECT *
         , [totale_casi] - LAG([totale_casi], 1, 0) OVER(PARTITION BY [denominazione_regione], [denominazione_provincia] ORDER BY [data]) AS [nuovi_positivi]
    FROM reg
)
,
new_d AS (
    SELECT *
         , [nuovi_positivi] - LAG([nuovi_positivi], 1, 0) OVER(PARTITION BY [denominazione_regione], [denominazione_provincia] ORDER BY [data]) AS [nuovi_positivi_g]
    FROM new    
)
,
perc AS (
    SELECT *
         ,  CASE 
                WHEN [totale_casi] <> 0 THEN [nuovi_positivi] / CAST([totale_casi] AS decimal) * 100 
                ELSE 0
            END [totale_positivi_perc]
         ,  CASE
                WHEN [nuovi_positivi] <> 0 THEN [nuovi_positivi_g] / CAST([nuovi_positivi] AS decimal) * 100
                ELSE 0
            END [nuovi_positivi_perc]
    FROM new_d
)
SELECT * FROM perc
GO