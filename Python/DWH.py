import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime


# --- Connexion  
dwh = create_engine('DWH path')
ods = create_engine('ODS path')

# --- Création des tables DWH si elles n'existent pas ---
with dwh.connect() as conn:
    conn.execute(text("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Dim_DATE_')
        CREATE TABLE Dim_DATE_ (
            InvoiceDate DATE,
            Day INT,
            Month INT,
            Year_ INT,
            Hour_ INT,
            weekday NVARCHAR(20)
        )
    """))
    conn.execute(text("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Dim_PLAYLIST')
        CREATE TABLE Dim_PLAYLIST (
            nk_playlist_id INT,
            Name NVARCHAR(120)
        )
    """))
    conn.execute(text("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Dim_TRACK')
        CREATE TABLE Dim_TRACK (
            tk_track INT IDENTITY(1,1) PRIMARY KEY,
            nk_track_id INT,
            nk_album_id INT,
            nk_artist_id INT,
            nk_mediatype_id INT,
            nk_genre_id INT,
            TRACK_Name NVARCHAR(200),
            ALBUM_TITLE NVARCHAR(160),
            ARTIST_NAME NVARCHAR(120),
            MEDIATYPE_NAME NVARCHAR(120),
            GENRE_NAME NVARCHAR(120),
            TRACK_UNITPRICE DECIMAL(10,2),
            TRACK_BYTES INT,
            TRACK_MILLISECONDS INT,
            Composer NVARCHAR(220),
            ALBUM_NAME NVARCHAR(160),
            start_date DATE,
            end_date DATE,
            is_active INT DEFAULT 1
        )
    """))
    conn.execute(text("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Dim_CUSTOMER')
        CREATE TABLE Dim_CUSTOMER (
            tk_customer INT IDENTITY(1,1) PRIMARY KEY,
            nk_customer_id INT,
            nk_employee_id INT,
            FirstName NVARCHAR(40),
            LastName NVARCHAR(20),
            Company NVARCHAR(80),
            ADDRESS_LINE NVARCHAR(70),
            City NVARCHAR(40),
            STATE_PROV NVARCHAR(40),
            Country NVARCHAR(40),
            POSTAL_CODE NVARCHAR(10),
            Phone NVARCHAR(24),
            Fax NVARCHAR(24),
            Email NVARCHAR(60),
            Employee_FirstName NVARCHAR(20),
            Employee_LastName NVARCHAR(20),
            Employee_Title NVARCHAR(30)
        )
    """))
    conn.execute(text("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'BR_PLAYLIST_TRACK')
        CREATE TABLE BR_PLAYLIST_TRACK (
            nk_playlist_id INT,
            tk_track INT
        )
    """))
    conn.execute(text("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SALES')
        CREATE TABLE SALES (
            tk_sales INT IDENTITY(1,1) PRIMARY KEY,
            tk_track INT,
            tk_customer INT,
            date_ DATE,
            nk_invoice_id INT,
            nk_invoice_line_id INT,
            BillingAddress NVARCHAR(70),
            BillingCity NVARCHAR(40),
            BillingCountry NVARCHAR(40),
            BillingPostalCode NVARCHAR(10),
            AMOUNT DECIMAL(10,2),
            QUANTITY INT,
            canal_vente NVARCHAR(50)
        )
    """))
    conn.commit()
    print("Tables DWH vérifiées/créées.")

# --- Dim date
def load_dim_date():
    print(">>> [1/6] Remplissage de Dim_DATE_...")
    # Récupération depuis l'ODS
    df = pd.read_sql("SELECT DISTINCT InvoiceDate FROM Invoice", ods)
    df['InvoiceDate_dt'] = pd.to_datetime(df['InvoiceDate'], format="mixed")
    
    # Création des colonnes exactement comme dans ton SQL
    df_date = pd.DataFrame()
    df_date['InvoiceDate'] = df['InvoiceDate_dt'].dt.date  # Format YYYY-MM-DD
    df_date['Day'] = df['InvoiceDate_dt'].dt.day
    df_date['Month'] = df['InvoiceDate_dt'].dt.month
    df_date['Year_'] = df['InvoiceDate_dt'].dt.year
    df_date['Hour_'] = df['InvoiceDate_dt'].dt.hour
    df_date['weekday'] = df['InvoiceDate_dt'].dt.day_name()
    
    # Suppression des doublons potentiels sur la date pure
    df_date = df_date.drop_duplicates(subset=['InvoiceDate'])

    with dwh.begin() as connexion:
        connexion.execute(text("TRUNCATE TABLE Dim_DATE_"))
        df_date.to_sql('Dim_DATE_', con=connexion, if_exists='append', index=False)

def load_dim_playlist():
    print(">>> [2/6] Remplissage de Dim_PLAYLIST...")
    df = pd.read_sql("SELECT PlaylistId as nk_playlist_id, Name, date_integration FROM Playlist", ods)
    df['date_integration'] = pd.to_datetime(df['date_integration'], format='mixed')
    df = df.sort_values(by=['nk_playlist_id', 'date_integration']).drop_duplicates('nk_playlist_id', keep='last')
    
    # On ne garde que les colonnes de ton SQL
    df_final = df[['nk_playlist_id', 'Name']]
    
    with dwh.begin() as connexion:
        connexion.execute(text("TRUNCATE TABLE Dim_PLAYLIST"))
        df_final.to_sql('Dim_PLAYLIST', con=connexion, if_exists='append', index=False)


# =================================================================
# 2. DIMENSIONS AVEC CLÉS TECHNIQUES (IDENTITY)
# =================================================================

def load_dim_track():
    print(">>> [3/6] Remplissage de Dim_TRACK (Mode Historisation / SCD2)...")
    
    now_date = datetime.now().strftime('%Y-%m-%d')
    end_date_default = '2099-12-31'
    
    # 1. On récupère les données fraîches de l'ODS
    query = """
    SELECT t.TrackId as nk_track_id, t.AlbumId as nk_album_id, a.ArtistId as nk_artist_id, 
           t.MediaTypeId as nk_mediatype_id, t.GenreId as nk_genre_id, t.Name as TRACK_Name, 
           a.Title as ALBUM_TITLE, art.Name as ARTIST_NAME, m.Name as MEDIATYPE_NAME, 
           g.Name as GENRE_NAME, t.UnitPrice as TRACK_UNITPRICE, t.Bytes as TRACK_BYTES, 
           t.Milliseconds as TRACK_MILLISECONDS, t.Composer, t.date_integration
    FROM Track t
    LEFT JOIN Album a ON t.AlbumId = a.AlbumId
    LEFT JOIN Artist art ON a.ArtistId = art.ArtistId
    LEFT JOIN Genre g ON t.GenreId = g.GenreId
    LEFT JOIN MediaType m ON t.MediaTypeId = m.MediaTypeId
    """
    df_ods = pd.read_sql(query, ods)
    df_ods['date_integration'] = pd.to_datetime(df_ods['date_integration'], format='mixed')
    df_ods = df_ods.sort_values(by=['nk_track_id', 'date_integration']).drop_duplicates('nk_track_id', keep='last')
    df_ods['ALBUM_NAME'] = df_ods['ALBUM_TITLE']
    df_ods = df_ods.drop(columns=['date_integration'])

    # 2. On récupère uniquement les pistes actuellement ACTIVES dans le DWH
    try:
        df_dwh = pd.read_sql("SELECT tk_track, nk_track_id, TRACK_UNITPRICE FROM Dim_TRACK WHERE is_active = 1", dwh)
    except Exception:
        # Si la table vient d'être créée
        df_dwh = pd.DataFrame(columns=['tk_track', 'nk_track_id', 'TRACK_UNITPRICE'])

    # 3. La logique de comparaison et de mise à jour (Sans TRUNCATE)
    if df_dwh.empty:
        # S'il n'y a rien dans le DWH, on insère tout pour la première fois
        df_insert = df_ods.copy()
        df_insert['start_date'] = now_date
        df_insert['end_date'] = end_date_default
        df_insert['is_active'] = 1
        with dwh.begin() as connexion:
            df_insert.to_sql('Dim_TRACK', con=connexion, if_exists='append', index=False)
        print(f"    -> {len(df_insert)} pistes insérées (Initialisation).")
    else:
        # Sinon, on compare l'ancien et le nouveau prix
        merged = df_ods.merge(df_dwh, on='nk_track_id', how='left', suffixes=('', '_dwh'))
        
        # Pour les nouvelles pistes pas dans le DWH
        new_tracks = merged[merged['tk_track'].isna()].copy()
        
        # Ce qui existe mais avec un PRIX DIFFÉRENT (Mise à jour)
        changed_tracks = merged[(merged['tk_track'].notna()) & 
                                (merged['TRACK_UNITPRICE'] != merged['TRACK_UNITPRICE_dwh'])].copy()

        # A. Mettre en inactive les anciennes lignes
        if not changed_tracks.empty:
            tk_tracks_to_update = changed_tracks['tk_track'].astype(int).tolist()
            tk_tracks_str = ','.join(map(str, tk_tracks_to_update))
            update_query = text(f"UPDATE Dim_TRACK SET is_active = 0, end_date = '{now_date}' WHERE tk_track IN ({tk_tracks_str})")
            
            with dwh.begin() as connexion:
                connexion.execute(update_query)
            print(f"    -> {len(changed_tracks)} ancien(s) prix désactivé(s).")
        
        # B. Insertion des nouvelles lignes (nouvelles pistes + nouveaux prix)
        cols_to_insert = list(df_ods.columns)
        df_to_insert = pd.concat([new_tracks[cols_to_insert], changed_tracks[cols_to_insert]])
        
        if not df_to_insert.empty:
            df_to_insert['start_date'] = now_date
            df_to_insert['end_date'] = end_date_default
            df_to_insert['is_active'] = 1
            with dwh.begin() as connexion:
                df_to_insert.to_sql('Dim_TRACK', con=connexion, if_exists='append', index=False)
            print(f"    -> {len(df_to_insert)} nouvelle(s) ligne(s) active(s) insérée(s).")
        else:
            print("    -> Aucun changement détecté. La dimension est à jour.")

def load_dim_customer():
    print(">>> [4/6] Remplissage de Dim_CUSTOMER...")
    query = """
    SELECT c.CustomerId as nk_customer_id, c.SupportRepId as nk_employee_id, c.FirstName, c.LastName, 
           c.Company, c.Address as ADDRESS_LINE, c.City, c.State as STATE_PROV, c.Country, 
           c.PostalCode as POSTAL_CODE, c.Phone, c.Fax, c.Email, e.FirstName as Employee_FirstName, 
           e.LastName as Employee_LastName, e.Title as Employee_Title, c.date_integration
    FROM Customer c
    LEFT JOIN Employee e ON c.SupportRepId = e.EmployeeId
    """
    df = pd.read_sql(query, ods)
    df['date_integration'] = pd.to_datetime(df['date_integration'], format='mixed')
    df = df.sort_values(by=['nk_customer_id', 'date_integration']).drop_duplicates('nk_customer_id', keep='last')
    
    cols = ['nk_customer_id', 'nk_employee_id', 'FirstName', 'LastName', 'Company', 
            'ADDRESS_LINE', 'City', 'STATE_PROV', 'Country', 'POSTAL_CODE', 'Phone', 
            'Fax', 'Email', 'Employee_FirstName', 'Employee_LastName', 'Employee_Title']
    df_final = df[cols]
    
    with dwh.begin() as connexion:
        connexion.execute(text("TRUNCATE TABLE Dim_CUSTOMER"))
        df_final.to_sql('Dim_CUSTOMER', con=connexion, if_exists='append', index=False)


# =================================================================
# 3. TABLES DÉPENDANTES (FAITS ET BRIDGES)
# =================================================================

def load_bridge_playlist_track():
    print(">>> [5/6] Remplissage de BR_PLAYLIST_TRACK...")
    df_ods = pd.read_sql("SELECT PlaylistId as nk_playlist_id, TrackId as nk_track_id FROM PlaylistTrack", ods)
    df_tk = pd.read_sql("SELECT tk_track, nk_track_id FROM Dim_TRACK", dwh)
    
    df_final = pd.merge(df_ods, df_tk, on='nk_track_id', how='inner')[['nk_playlist_id', 'tk_track']]
    
    with dwh.begin() as connexion:
        connexion.execute(text("TRUNCATE TABLE BR_PLAYLIST_TRACK"))
        df_final.to_sql('BR_PLAYLIST_TRACK', con=connexion, if_exists='append', index=False)

def load_fact_sales():
    print(">>> [6/6] Remplissage de SALES...")
    
    # --- 1. VÉRIFICATION ET CRÉATION INTELLIGENTE ---
    # On vérifie et on ajoute canal_vente ET UNIT_PRICE si elles n'existent pas
    check_column_sql = """
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'SALES' AND COLUMN_NAME = 'canal_vente'
    )
    BEGIN
        ALTER TABLE SALES ADD canal_vente NVARCHAR(50);
    END;

    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'SALES' AND COLUMN_NAME = 'UNIT_PRICE'
    )
    BEGIN
        ALTER TABLE SALES ADD UNIT_PRICE DECIMAL(10,2);
    END;
    """
    with dwh.begin() as connexion:
        connexion.execute(text(check_column_sql))
    
    # --- 2. EXTRACTION DES DONNÉES ---
    # J'ai ajouté "il.UnitPrice as UNIT_PRICE" pour capturer le prix exact de la facture
    query = """
    SELECT il.TrackId as nk_track_id, i.CustomerId as nk_customer_id, 
           i.InvoiceDate, i.InvoiceId as nk_invoice_id, il.InvoiceLineId as nk_invoice_line_id, 
           i.BillingAddress, i.BillingCity, i.BillingCountry, i.BillingPostalCode,
           il.UnitPrice as UNIT_PRICE,
           (il.UnitPrice * il.Quantity) as AMOUNT, il.Quantity as QUANTITY  
    FROM InvoiceLine il JOIN Invoice i ON il.InvoiceId = i.InvoiceId
    """
    df_sales = pd.read_sql(query, ods)
    
    df_sales['date_'] = pd.to_datetime(df_sales['InvoiceDate']).dt.date
    
    # Récupération des clés DWH (avec sécurité anti-doublons)
    df_tk_track = pd.read_sql("SELECT tk_track, nk_track_id FROM Dim_TRACK", dwh)
    df_tk_track = df_tk_track.drop_duplicates(subset=['nk_track_id'], keep='last')
    
    df_tk_cust = pd.read_sql("SELECT tk_customer, nk_customer_id FROM Dim_CUSTOMER", dwh)
    df_tk_cust = df_tk_cust.drop_duplicates(subset=['nk_customer_id'], keep='last')
    
    df_final = pd.merge(df_sales, df_tk_track, on='nk_track_id', how='inner')
    df_final = pd.merge(df_final, df_tk_cust, on='nk_customer_id', how='inner')
    
    # --- 3. TRANSFORMATION ---
    df_final['canal_vente'] = 'web'
    
    # Alignement avec la nouvelle colonne UNIT_PRICE
    cols = ['tk_track', 'tk_customer', 'date_', 'nk_invoice_id', 'nk_invoice_line_id', 
            'BillingAddress', 'BillingCity', 'BillingCountry', 'BillingPostalCode', 
            'UNIT_PRICE', 'AMOUNT', 'QUANTITY', 'canal_vente']
    df_final = df_final[cols]
    
    # --- 4. INSERTION ---
    with dwh.begin() as connexion:
        connexion.execute(text("TRUNCATE TABLE SALES"))
        df_final.to_sql('SALES', con=connexion, if_exists='append', index=False)

# --- EXÉCUTION ORDONNÉE ---
if __name__ == "__main__":
    try:
        load_dim_date()
        load_dim_playlist()
        load_dim_track()    
        load_dim_customer() 
        load_bridge_playlist_track()
        load_fact_sales()
        print("\n✅ PROJET DWH TERMINÉ : Toutes les tables sont alimentées avec succès.")
    except Exception as e:
        print(f"\n❌ ERREUR GÉNÉRALE : {e}")