-----------------------
-- create Table nazione
-----------------------
DROP TABLE IF EXISTS  [dbo].[nazione]
CREATE TABLE [dbo].[nazione]
(
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
CREATE TABLE [dbo].[regioni]
(
	[data] [datetime2](7) NOT NULL,
	[stato] [nvarchar](50) NULL,
	[codice_regione] [tinyint] NOT NULL,
	[denominazione_regione] [nvarchar](50) NOT NULL,
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
	[codice_nuts_2] [nvarchar](10) NULL,
	CONSTRAINT [PK_regioni] PRIMARY KEY CLUSTERED 
(
	[data] ASC,
	[codice_regione] ASC,
	[denominazione_regione] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
-----------------------
-- create Table province
-----------------------
DROP TABLE [dbo].[province]
CREATE TABLE [dbo].[province]
(
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
