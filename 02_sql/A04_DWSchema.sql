/*
   GRUPPO 13 - Assignment 4: Data Warehouse Schema Definition
   Description: Script DDL completo per la creazione del Data Warehouse.
   
   Workflow:
   1. Cleanup: DROP di tutte le tabelle se esistenti (ordine inverso alle dipendenze).
   2. Creation: CREATE delle Dimensioni, Bridge e Fact Table.
*/

-- =============================================================================
-- SECTION 1: CLEANUP (DROP EXISTING OBJECTS)
-- Eseguiamo il drop in ordine inverso per evitare conflitti FK.
-- =============================================================================

-- 1.1 Tabelle Fact e Bridge (Livello più basso della gerarchia FK)
IF OBJECT_ID('FactSongStreams', 'U') IS NOT NULL DROP TABLE FactSongStreams;
IF OBJECT_ID('BridgeFeatured', 'U') IS NOT NULL DROP TABLE BridgeFeatured;

-- 1.2 Tabelle Dimensionali con Dipendenze (Mid-Level)
IF OBJECT_ID('DimTrack', 'U') IS NOT NULL DROP TABLE DimTrack;
IF OBJECT_ID('DimArtist', 'U') IS NOT NULL DROP TABLE DimArtist;

-- 1.3 Tabelle Dimensionali Indipendenti (Top-Level)
IF OBJECT_ID('DimLyrics', 'U') IS NOT NULL DROP TABLE DimLyrics;
IF OBJECT_ID('DimAudio', 'U') IS NOT NULL DROP TABLE DimAudio;
IF OBJECT_ID('DimGeography', 'U') IS NOT NULL DROP TABLE DimGeography;
IF OBJECT_ID('DimCategory', 'U') IS NOT NULL DROP TABLE DimCategory;
IF OBJECT_ID('DimDate', 'U') IS NOT NULL DROP TABLE DimDate;

GO


-- =============================================================================
-- SECTION 2: DIMENSION TABLES CREATION
-- =============================================================================

-- 2.1 Dimensioni (Senza Foreign Keys)

CREATE TABLE DimDate (
    date_song_pk    INT NOT NULL PRIMARY KEY, -- format: YYYYMMDD
    full_date       DATE NULL,
    [day]           TINYINT NOT NULL,
    [month]         TINYINT NOT NULL,
    [year]          SMALLINT NOT NULL,
    [weekday]       TINYINT NULL,
    season          NVARCHAR(20) NULL
);
GO

CREATE TABLE DimCategory (
    category_pk     INT IDENTITY(1,1) PRIMARY KEY,
    category_name   NVARCHAR(50) NOT NULL UNIQUE -- Es. 'POP', 'RAP', 'DANCE'
);
GO

CREATE TABLE DimGeography (
    geo_pk          INT PRIMARY KEY, 
    birth_place     NVARCHAR(200) NULL,
    province        NVARCHAR(100) NULL,
    region          NVARCHAR(100) NULL,
    country         NVARCHAR(100) NULL
);
GO

CREATE TABLE DimAudio (
    audio_pk                INT PRIMARY KEY,
    bpm                     FLOAT NULL,
    loudness                FLOAT NULL,
    flatness                FLOAT NULL,
    rolloff                 FLOAT NULL,
    flux                    FLOAT NULL,
    rms                     FLOAT NULL,
    spectral_complexity     FLOAT NULL,
    pitch                   FLOAT NULL
);
GO

CREATE TABLE DimLyrics (
    lyrics_pk               INT PRIMARY KEY,
    n_tokens                INT NULL,
    n_sentences             INT NULL,
    avg_token_per_clause    FLOAT NULL,
    char_per_tok            FLOAT NULL,
    swear_IT                INT NULL,
    swear_EN                INT NULL
);
GO

-- 2.2 Dimensioni Principali (Con Foreign Keys)

CREATE TABLE DimArtist (
    artist_pk       INT IDENTITY(1,1) PRIMARY KEY,
    id_artist       VARCHAR(50) NOT NULL, 
    geo_fk          INT NOT NULL,
    name            NVARCHAR(200) NOT NULL,
    gender          NVARCHAR(50) NULL,
    birth_date      DATE NULL,
    nationality     NVARCHAR(100) NULL,
    description     NVARCHAR(MAX) NULL,

    CONSTRAINT FK_Artist_Geo FOREIGN KEY (geo_fk) REFERENCES DimGeography(geo_pk)
);
GO

CREATE TABLE DimTrack (
    track_pk            INT IDENTITY(1,1) PRIMARY KEY,
    id_track            VARCHAR(50) NOT NULL, 
    title               NVARCHAR(400) NOT NULL,
    duration_ms         INT NULL,
    explicit            BIT NULL,
    track_number        INT NULL,
    disc_number         INT NULL,
    original_source_id  VARCHAR(50) NULL,
    album_release_date  DATE NULL,

    audio_fk            INT NOT NULL,
    lyrics_fk           INT NOT NULL,

    CONSTRAINT FK_Track_Audio FOREIGN KEY (audio_fk) REFERENCES DimAudio(audio_pk),
    CONSTRAINT FK_Track_Lyrics FOREIGN KEY (lyrics_fk) REFERENCES DimLyrics(lyrics_pk)
);
GO


-- =============================================================================
-- SECTION 3: BRIDGE TABLES
-- Gestione relazione Many-to-Many tra Brani e Artisti (Main vs Featured)
-- =============================================================================

CREATE TABLE BridgeFeatured (
    track_fk        INT NOT NULL,
    artist_fk       INT NOT NULL,
    role            BIT NOT NULL, -- 1 Main Artist, 0 Featured Artist

    CONSTRAINT PK_BridgeFeatured PRIMARY KEY (track_fk, artist_fk),
    CONSTRAINT FK_Bridge_Track FOREIGN KEY (track_fk) REFERENCES DimTrack(track_pk),
    CONSTRAINT FK_Bridge_Artist FOREIGN KEY (artist_fk) REFERENCES DimArtist(artist_pk)
);
GO


-- =============================================================================
-- SECTION 4: FACT TABLE
-- =============================================================================

CREATE TABLE FactSongStreams (
    fact_pk         INT IDENTITY(1,1) PRIMARY KEY,
    date_song_fk    INT NOT NULL,
    track_fk        INT NOT NULL,
    category_fk     INT NOT NULL,
    streams_1month  INT NULL,
    popularity      INT NULL,

    CONSTRAINT FK_Fact_Date FOREIGN KEY (date_song_fk) REFERENCES DimDate(date_song_pk),
    CONSTRAINT FK_Fact_Track FOREIGN KEY (track_fk) REFERENCES DimTrack(track_pk),
    CONSTRAINT FK_Fact_Category FOREIGN KEY (category_fk) REFERENCES DimCategory(category_pk)
);
GO