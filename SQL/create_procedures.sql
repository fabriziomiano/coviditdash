----------------------------------------------------------------------------
-- Create a new stored procedure called 'usp_national_data' in schema 'dbo'
-- Drop the stored procedure if it already exists
----------------------------------------------------------------------------
-- Drop the stored procedure if it already exists
IF EXISTS (
SELECT *
    FROM INFORMATION_SCHEMA.ROUTINES
WHERE SPECIFIC_SCHEMA = N'dbo'
    AND SPECIFIC_NAME = N'usp_national_data'
    AND ROUTINE_TYPE = N'PROCEDURE'
)
DROP PROCEDURE dbo.usp_national_data
GO
-- Create the stored procedure in the specified schema
CREATE PROCEDURE dbo.usp_national_data
AS
BEGIN
    DROP TABLE IF EXISTS #national
    CREATE TABLE #national (
        [data] [datetime2](7) NOT NULL,
        [stato] [nvarchar](50) NULL,
        [ricoverati_con_sintomi] [bigint] NULL,
        [terapia_intensiva] [bigint] NULL,
        [totale_ospedalizzati] [bigint] NULL,
        [isolamento_domiciliare] [bigint] NULL,
        [totale_positivi] [bigint] NULL,
        [variazione_totale_positivi] [bigint] NULL,
        [nuovi_positivi] [bigint] NULL,
        [dimessi_guariti] [bigint] NULL,
        [deceduti] [bigint] NULL,
        [casi_da_sospetto_diagnostico] [bigint] NULL,
        [casi_da_screening] [bigint] NULL,
        [totale_casi] [bigint] NULL,
        [tamponi] [bigint] NULL,
        [casi_testati] [bigint] NULL,
        [note] [text] NULL,
        [ingressi_terapia_intensiva] [bigint] NULL,
        [note_test] [text] NULL,
        [note_casi] [text] NULL,
        [totale_positivi_test_molecolare] [bigint] NULL,
        [totale_positivi_test_antigenico_rapido] [bigint] NULL,
        [tamponi_test_molecolare] [bigint] NULL,
        [tamponi_test_antigenico_rapido] [bigint] NULL
    );

    BULK INSERT #national
        FROM 'C:\Users\fabri\repos\COVID-19\dati-andamento-nazionale\dpc-covid19-ita-andamento-nazionale.csv'
        WITH
        (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',  --CSV field delimiter
        ROWTERMINATOR = '\n',   --Use to shift the control to next row
        -- ERRORFILE = 'C:\Users\fabri\sqlerror.log',
        TABLOCK,
        KEEPNULLS
        )

    INSERT INTO [dbo].[nazione] (
        [data]
      , [stato]
      , [ricoverati_con_sintomi]
      , [terapia_intensiva]
      , [totale_ospedalizzati]
      , [isolamento_domiciliare]
      , [totale_positivi]
      , [variazione_totale_positivi]
      , [nuovi_positivi]
      , [dimessi_guariti]
      , [deceduti]
      , [casi_da_sospetto_diagnostico]
      , [casi_da_screening]
      , [totale_casi]
      , [tamponi]
      , [casi_testati]
      , [note]
      , [ingressi_terapia_intensiva]
      , [note_test]
      , [note_casi]
      , [totale_positivi_test_molecolare]
      , [totale_positivi_test_antigenico_rapido]
      , [tamponi_test_molecolare]
      , [tamponi_test_antigenico_rapido]    
    )
    SELECT [data]
         , [stato]
         , [ricoverati_con_sintomi]
         , [terapia_intensiva]
         , [totale_ospedalizzati]
         , [isolamento_domiciliare]
         , [totale_positivi]
         , [variazione_totale_positivi]
         , [nuovi_positivi]
         , [dimessi_guariti]
         , [deceduti]
         , [casi_da_sospetto_diagnostico]
         , [casi_da_screening]
         , [totale_casi]
         , [tamponi]
         , [casi_testati]
         , [note]
         , [ingressi_terapia_intensiva]
         , [note_test]
         , [note_casi]
         , [totale_positivi_test_molecolare]
         , [totale_positivi_test_antigenico_rapido]
         , [tamponi_test_molecolare]
         , [tamponi_test_antigenico_rapido]   
    from #national
    where [data] not in (
        select [data]
        from [dbo].[nazione]
    )
END
GO
----------------------------------------------------------------------------
-- Create a new stored procedure called 'usp_regional_data' in schema 'dbo'
-- Drop the stored procedure if it already exists
----------------------------------------------------------------------------
-- Drop the stored procedure if it already exists
IF EXISTS (
SELECT *
    FROM INFORMATION_SCHEMA.ROUTINES
WHERE SPECIFIC_SCHEMA = N'dbo'
    AND SPECIFIC_NAME = N'usp_regional_data'
    AND ROUTINE_TYPE = N'PROCEDURE'
)
DROP PROCEDURE dbo.usp_regional_data
GO
-- Create the stored procedure in the specified schema
CREATE PROCEDURE [dbo].[usp_regional_data]
AS
BEGIN
    DROP TABLE IF EXISTS #regional
    CREATE TABLE #regional (
        [data] [datetime2](7) NOT NULL,
        [stato] [nvarchar](50) NULL,
        [codice_regione] [tinyint] NULL,
        [denominazione_regione] [nvarchar](50) NULL,
        [lat] [float] NULL,
        [long] [float] NULL,
        [ricoverati_con_sintomi] [bigint] NULL,
        [terapia_intensiva] [bigint] NULL,
        [totale_ospedalizzati] [bigint] NULL,
        [isolamento_domiciliare] [bigint] NULL,
        [totale_positivi] [bigint] NULL,
        [variazione_totale_positivi] [bigint] NULL,
        [nuovi_positivi] [bigint] NULL,
        [dimessi_guariti] [bigint] NULL,
        [deceduti] [bigint] NULL,
        [casi_da_sospetto_diagnostico] [bigint] NULL,
        [casi_da_screening] [bigint] NULL,
        [totale_casi] [bigint] NULL,
        [tamponi] [bigint] NULL,
        [casi_testati] [bigint] NULL,
        [note] [text] NULL,
        [ingressi_terapia_intensiva] [bigint] NULL,
        [note_test] [text] NULL,
        [note_casi] [text] NULL,
        [totale_positivi_test_molecolare] [bigint] NULL,
        [totale_positivi_test_antigenico_rapido] [bigint] NULL,
        [tamponi_test_molecolare] [bigint] NULL,
        [tamponi_test_antigenico_rapido] [bigint] NULL,
        [codice_nuts_1] [nvarchar](10) NULL,
        [codice_nuts_2] [nvarchar](10) NULL
    )
    ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];


    BULK INSERT #regional
        FROM 'C:\Users\fabri\repos\COVID-19\dati-regioni\dpc-covid19-ita-regioni.csv'
        WITH
        (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',  --CSV field delimiter
        ROWTERMINATOR = '\n',   --Use to shift the control to next row
        TABLOCK,
        KEEPNULLS
        )

    INSERT INTO [dbo].[regioni] (
        [data]
      , [stato]
      , [codice_regione]
      , [denominazione_regione]
      , [lat]
      , [long]
      , [ricoverati_con_sintomi]
      , [terapia_intensiva]
      , [totale_ospedalizzati]
      , [isolamento_domiciliare]
      , [totale_positivi]
      , [variazione_totale_positivi]
      , [nuovi_positivi]
      , [dimessi_guariti]
      , [deceduti]
      , [casi_da_sospetto_diagnostico]
      , [casi_da_screening]
      , [totale_casi]
      , [tamponi]
      , [casi_testati]
      , [note]
      , [ingressi_terapia_intensiva]
      , [note_test]
      , [note_casi]
      , [totale_positivi_test_molecolare]
      , [totale_positivi_test_antigenico_rapido]
      , [tamponi_test_molecolare]
      , [tamponi_test_antigenico_rapido]
      , [codice_nuts_1]
      , [codice_nuts_2]
    )
    SELECT [data]
         , [stato]
         , [codice_regione]
         , [denominazione_regione]
         , [lat]
         , [long]
         , [ricoverati_con_sintomi]
         , [terapia_intensiva]
         , [totale_ospedalizzati]
         , [isolamento_domiciliare]
         , [totale_positivi]
         , [variazione_totale_positivi]
         , [nuovi_positivi]
         , [dimessi_guariti]
         , [deceduti]
         , [casi_da_sospetto_diagnostico]
         , [casi_da_screening]
         , [totale_casi]
         , [tamponi]
         , [casi_testati]
         , [note]
         , [ingressi_terapia_intensiva]
         , [note_test]
         , [note_casi]
         , [totale_positivi_test_molecolare]
         , [totale_positivi_test_antigenico_rapido]
         , [tamponi_test_molecolare]
         , [tamponi_test_antigenico_rapido]
         , [codice_nuts_1]
         , [codice_nuts_2]
    from #regional
    where [data] not in (
        select [data]
        from [dbo].[regioni]
    )
END
GO
----------------------------------------------------------------------------
-- Create a new stored procedure called 'usp_provincial_data' in schema 'dbo'
-- Drop the stored procedure if it already exists
----------------------------------------------------------------------------
-- Drop the stored procedure if it already exists
IF EXISTS (
SELECT *
    FROM INFORMATION_SCHEMA.ROUTINES
WHERE SPECIFIC_SCHEMA = N'dbo'
    AND SPECIFIC_NAME = N'usp_provincial_data'
    AND ROUTINE_TYPE = N'PROCEDURE'
)
DROP PROCEDURE dbo.usp_provincial_data
GO
-- Create the stored procedure in the specified schema
CREATE PROCEDURE dbo.usp_provincial_data
AS
BEGIN
    DROP TABLE IF EXISTS #provincial
    CREATE TABLE #provincial (
        [data] [datetime2](7) NOT NULL,
        [stato] [text] NULL,
        [codice_regione] [tinyint] NULL,
        [denominazione_regione] [nvarchar](50) NULL,
        [codice_provincia] [smallint] NOT NULL,
        [denominazione_provincia] [nvarchar](50) NULL,
        [sigla_provincia] [nvarchar](50) NULL,
        [lat] [float] NULL,
        [long] [float] NULL,
        [totale_casi] [int] NULL,
        [note] [text] NULL,
        [codice_nuts_1] [text] NULL,
        [codice_nuts_2] [text] NULL,
        [codice_nuts_3] [text] NULL
    );

    BULK INSERT #provincial
        FROM 'C:\Users\fabri\repos\COVID-19\dati-province\dpc-covid19-ita-province.csv'
        WITH
        (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',  --CSV field delimiter
        ROWTERMINATOR = '\n',   --Use to shift the control to next row
        TABLOCK,
        KEEPNULLS
        )

    INSERT INTO [dbo].[province] (
        [data]
      , [stato]
      , [codice_regione]
      , [denominazione_regione]
      , [codice_provincia]
      , [denominazione_provincia]
      , [sigla_provincia]
      , [lat]
      , [long]
      , [totale_casi]
      , [note]
      , [codice_nuts_1]
      , [codice_nuts_2]
      , [codice_nuts_3]
    )
    SELECT [data]
         , [stato]
         , [codice_regione]
         , [denominazione_regione]
         , [codice_provincia]
         , [denominazione_provincia]
         , [sigla_provincia]
         , [lat]
         , [long]
         , [totale_casi]
         , [note]
         , [codice_nuts_1]
         , [codice_nuts_2]
         , [codice_nuts_3]
    from #provincial
    where [data] not in (
        select [data]
        from [dbo].[province]
    )
END
GO