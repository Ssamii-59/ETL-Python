create database DWH;

-- On supprime d'abord les tables de faits et de liaison
DROP TABLE IF EXISTS SALES;
DROP TABLE IF EXISTS BR_PLAYLIST_TRACK;

-- Puis on supprime les dimensions
DROP TABLE IF EXISTS Dim_CUSTOMER;
DROP TABLE IF EXISTS Dim_TRACK;
DROP TABLE IF EXISTS Dim_DATE_;
DROP TABLE IF EXISTS Dim_PLAYLIST;

-- =================================================================
-- 1. DIMENSIONS INDÉPENDANTES
-- =================================================================

-- Table PLAYLIST
CREATE TABLE Dim_PLAYLIST (
    nk_playlist_id INT PRIMARY KEY,
    Name NVARCHAR(200)
);

-- Table DATE_
CREATE TABLE Dim_DATE_ (
    InvoiceDate DATE, -- Modifié pour correspondre au tk_date de la table SALES
    Day INT,              -- Remplacement de NUMBER par INT
    Month INT,            -- Remplacement de NUMBER par INT
    Year_ INT,            -- Remplacement de NUMBER par INT
    Hour_ INT,            -- Remplacement de NUMBER par INT
    weekday NVARCHAR(20), -- Remplacement de VARCHAR2 par NVARCHAR
    CONSTRAINT PK_DATE_ PRIMARY KEY (InvoiceDate)
);

-- =================================================================
-- 2. DIMENSIONS AVEC CLÉS TECHNIQUES (IDENTITY)
-- =================================================================

-- Table TRACK
CREATE TABLE Dim_TRACK (
    tk_track INT IDENTITY(1,1) PRIMARY KEY, 
    nk_track_id INT,
    nk_album_id INT,
    nk_artist_id INT,
    nk_mediatype_id INT,
    nk_genre_id INT,
    TRACK_Name NVARCHAR(200),
    ALBUM_TITLE NVARCHAR(200),
    ARTIST_NAME NVARCHAR(200),
    MEDIATYPE_NAME NVARCHAR(200),
    GENRE_NAME NVARCHAR(200),
    TRACK_UNITPRICE DECIMAL(10,2),
    TRACK_BYTES BIGINT,
    TRACK_MILLISECONDS BIGINT,
    Composer NVARCHAR(200),
    ALBUM_NAME NVARCHAR(200) 
);

-- Table CUSTOMER
CREATE TABLE Dim_CUSTOMER (
    tk_customer INT IDENTITY(1,1) PRIMARY KEY,
    nk_customer_id INT,
    nk_employee_id INT,
    FirstName NVARCHAR(50),
    LastName NVARCHAR(50),
    Company NVARCHAR(100),
    ADDRESS_LINE NVARCHAR(200),
    City NVARCHAR(50),
    STATE_PROV NVARCHAR(50),
    Country NVARCHAR(50),
    POSTAL_CODE NVARCHAR(20),
    Phone NVARCHAR(30),
    Fax NVARCHAR(30),
    Email NVARCHAR(100),
    Employee_FirstName NVARCHAR(50),
    Employee_LastName NVARCHAR(50),
    Employee_Title NVARCHAR(100)
);

-- =================================================================
-- 3. TABLES DÉPENDANTES (FAITS ET BRIDGES)
-- =================================================================

-- Table PLAYLIST_TRACK (Table de liaison)
CREATE TABLE BR_PLAYLIST_TRACK (
    nk_playlist_id INT,
    tk_track INT
);

-- Table SALES (Table des faits)
CREATE TABLE SALES (
    tk_track INT,
    tk_customer INT,
    date_ DATE, 
    nk_invoice_id INT,
    nk_invoice_line_id INT,
    BillingAddress NVARCHAR(200),
    BillingCity NVARCHAR(50),
    BillingCountry NVARCHAR(50),
    BillingPostalCode NVARCHAR(20),
    AMOUNT DECIMAL(10,2),
    QUANTITY INT
);