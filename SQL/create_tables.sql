-----------------------
-- create Table nazione
-----------------------
DROP TABLE IF EXISTS  [dbo].[nazione]
CREATE TABLE [dbo].[nazione](
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
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
-----------------------
-- create Table regioni
-----------------------
DROP TABLE [dbo].[regioni]
CREATE TABLE [dbo].[regioni](
	[data] [datetime2](7) NOT NULL,
	[stato] [nvarchar](50) NULL,
	[codice_regione] [tinyint] NULL,
	[denominazione_regione] [nvarchar](50) NULL,
	[lat] [float] NULL,
	[long] [float] NULL,
	[ricoverati_con_sintomi] [numeric](18, 0) NULL,
	[terapia_intensiva] [numeric](18, 0) NULL,
	[totale_ospedalizzati] [numeric](18, 0) NULL,
	[isolamento_domiciliare] [numeric](18, 0) NULL,
	[totale_positivi] [numeric](18, 0) NULL,
	[variazione_totale_positivi] [numeric](18, 0) NULL,
	[nuovi_positivi] [numeric](18, 0) NULL,
	[dimessi_guariti] [numeric](18, 0) NULL,
	[deceduti] [numeric](18, 0) NULL,
	[casi_da_sospetto_diagnostico] [numeric](18, 0) NULL,
	[casi_da_screening] [numeric](18, 0) NULL,
	[totale_casi] [numeric](18, 0) NULL,
	[tamponi] [numeric](18, 0) NULL,
	[casi_testati] [numeric](18, 0) NULL,
	[note] [text] NULL,
	[ingressi_terapia_intensiva] [numeric](18, 0) NULL,
	[note_test] [text] NULL,
	[note_casi] [text] NULL,
	[totale_positivi_test_molecolare] [numeric](18, 0) NULL,
	[totale_positivi_test_antigenico_rapido] [numeric](18, 0) NULL,
	[tamponi_test_molecolare] [numeric](18, 0) NULL,
	[tamponi_test_antigenico_rapido] [numeric](18, 0) NULL,
	[codice_nuts_1] [text] NULL,
	[codice_nuts_2] [text] NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
-----------------------
-- create Table province
-----------------------
DROP TABLE [dbo].[province]
CREATE TABLE [dbo].[province](
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
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
