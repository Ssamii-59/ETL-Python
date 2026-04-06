import pandas as pd
from sqlalchemy import create_engine, text

# --- Connexion  ---
dwh = create_engine('DWH path')
ods = create_engine('ODS magasin path')

# =================================================================
# 1. MISE À JOUR DE LA DIMENSION TEMPS (Incrémentale)
# =================================================================
def load_dim_date_magasin():
    print(">>> [1/2] Mise à jour de Dim_DATE_ (Vérification des nouvelles dates)...")
    
    df_ods = pd.read_sql("SELECT DISTINCT InvoiceDate FROM Invoice", ods)
    df_ods['InvoiceDate_dt'] = pd.to_datetime(df_ods['InvoiceDate'], format="mixed")
    
    df_new_dates = pd.DataFrame()
    df_new_dates['InvoiceDate'] = df_ods['InvoiceDate_dt'].dt.date
    df_new_dates['Day'] = df_ods['InvoiceDate_dt'].dt.day
    df_new_dates['Month'] = df_ods['InvoiceDate_dt'].dt.month
    df_new_dates['Year_'] = df_ods['InvoiceDate_dt'].dt.year
    df_new_dates['Hour_'] = df_ods['InvoiceDate_dt'].dt.hour
    df_new_dates['weekday'] = df_ods['InvoiceDate_dt'].dt.day_name()
    df_new_dates = df_new_dates.drop_duplicates(subset=['InvoiceDate'])

    try:
        df_dwh = pd.read_sql("SELECT InvoiceDate FROM Dim_DATE_", dwh)
        df_dwh['InvoiceDate'] = pd.to_datetime(df_dwh['InvoiceDate']).dt.date
        df_to_insert = df_new_dates[~df_new_dates['InvoiceDate'].isin(df_dwh['InvoiceDate'])]
    except Exception:
        df_to_insert = df_new_dates

    if not df_to_insert.empty:
        with dwh.begin() as connexion:
            df_to_insert.to_sql('Dim_DATE_', con=connexion, if_exists='append', index=False)
        print(f"    -> {len(df_to_insert)} nouvelle(s) date(s) ajoutée(s).")
    else:
        print("    -> Aucune nouvelle date à ajouter. Dimension à jour.")

# =================================================================
# 2. AJOUT DES FAITS (Alimentation incrémentale stricte)
# =================================================================
def load_fact_sales_magasin():
    print(">>> [2/2] Intégration des ventes dans SALES (Mode Anti-Doublons)...")
    
    check_column_sql = """
    IF NOT EXISTS (
        SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'SALES' AND COLUMN_NAME = 'canal_vente'
    )
    BEGIN
        ALTER TABLE SALES ADD canal_vente NVARCHAR(50);
    END
    """
    with dwh.begin() as connexion:
        connexion.execute(text(check_column_sql))
        
    # 1. Extraction brute de l'ODS
    query = """
    SELECT il.TrackId as nk_track_id, i.CustomerId as nk_customer_id, 
           i.InvoiceDate, i.InvoiceId as nk_invoice_id, il.InvoiceLineId as nk_invoice_line_id, 
           i.BillingAddress, i.BillingCity, i.BillingCountry, i.BillingPostalCode,
           (il.UnitPrice * il.Quantity) as AMOUNT, il.Quantity as QUANTITY
    FROM InvoiceLine il JOIN Invoice i ON il.InvoiceId = i.InvoiceId
    """
    df_sales = pd.read_sql(query, ods)
    df_sales['date_'] = pd.to_datetime(df_sales['InvoiceDate']).dt.date
    
    # Sécurité intra-ODS (au cas où l'ODS lui-même a des doublons)
    df_sales = df_sales.drop_duplicates(subset=['nk_invoice_line_id'], keep='last')

    # --- ÉTAPE ANTI-DOUBLON (DELTA) ---
    # 2. On récupère les identifiants de ventes magasins qui sont DÉJÀ dans le DWH
    try:
        query_existing = "SELECT nk_invoice_line_id FROM SALES WHERE canal_vente = 'magasin'"
        df_existing = pd.read_sql(query_existing, dwh)
        existing_ids = df_existing['nk_invoice_line_id'].tolist()
    except Exception:
        existing_ids = [] # Si la table SALES est vide

    # 3. Le Filtre Magique : On ne garde que les lignes de df_sales dont l'ID n'est pas dans le DWH
    df_sales = df_sales[~df_sales['nk_invoice_line_id'].isin(existing_ids)]
    
    nb_ventes_a_integrer = len(df_sales)
    
    if nb_ventes_a_integrer == 0:
        print("    -> Aucune nouvelle vente détectée. Le DWH possède déjà toutes ces données.")
        return # On arrête la fonction ici, pas besoin de faire les jointures
        
    print(f"    -> {nb_ventes_a_integrer} nouvelle(s) vente(s) à intégrer...")

    # 4. Récupération des clés techniques du DWH (Avec sécurité anti-doublons)
    
    # Si tu as la colonne is_active, tu devrais utiliser la requête commentée ci-dessous :
    # df_tk_track = pd.read_sql("SELECT tk_track, nk_track_id FROM Dim_TRACK WHERE is_active = 1", dwh)
    
    # Sinon, on récupère tout et on force la suppression des doublons pour être sûr à 100% :
    df_tk_track = pd.read_sql("SELECT tk_track, nk_track_id FROM Dim_TRACK", dwh)
    df_tk_track = df_tk_track.drop_duplicates(subset=['nk_track_id'], keep='last')
    
    df_tk_cust = pd.read_sql("SELECT tk_customer, nk_customer_id FROM Dim_CUSTOMER", dwh)
    df_tk_cust = df_tk_cust.drop_duplicates(subset=['nk_customer_id'], keep='last')
    
    # 5. Jointures
    df_final = pd.merge(df_sales, df_tk_track, on='nk_track_id', how='inner')
    df_final = pd.merge(df_final, df_tk_cust, on='nk_customer_id', how='inner')
    nb_ventes_finales = len(df_final)
    
    if nb_ventes_a_integrer != nb_ventes_finales:
        print(f"    ⚠️ ATTENTION : {nb_ventes_a_integrer - nb_ventes_finales} ventes ignorées ! " 
              f"Leur CustomerId ou TrackId n'existe pas dans le DWH.")
    
    # 6. Ajout du canal de vente
    df_final['canal_vente'] = 'magasin'
    
    cols = ['tk_track', 'tk_customer', 'date_', 'nk_invoice_id', 'nk_invoice_line_id', 
            'BillingAddress', 'BillingCity', 'BillingCountry', 'BillingPostalCode', 'AMOUNT', 'QUANTITY', 'canal_vente']
    df_final = df_final[cols]
    
    # 7. Insertion
    if not df_final.empty:
        with dwh.begin() as connexion:
            df_final.to_sql('SALES', con=connexion, if_exists='append', index=False)
        print(f"    -> {len(df_final)} nouvelles lignes 'magasin' ajoutées proprement à la table des faits.")


# --- EXÉCUTION ---
if __name__ == "__main__":
    try:
        print("\n=== DÉBUT DE L'INTÉGRATION : ODS_MAGASIN -> DWH ===")
        load_dim_date_magasin()
        load_fact_sales_magasin()
        print("✅ INTÉGRATION TERMINÉE SANS DOUBLONS.\n")
    except Exception as e:
        print(f"\n❌ ERREUR LORS DE L'INTÉGRATION : {e}")