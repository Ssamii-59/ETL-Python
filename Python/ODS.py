import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime


# --- CONNEXIONS ---

dsa = create_engine('DSA path')
ods = create_engine('ODS path')


tables = [
    'Artist', 'Album', 'Track', 'Genre', 'MediaType', 
    'Customer', 'Employee', 'Invoice', 'InvoiceLine', 
    'Playlist', 'PlaylistTrack'
]

def dsa_to_ods():
    print(">>> Début de l'alimentation ODS (Mode Ajout)...")
    
    # Format : 2026-03-05 09:37 (plus précis pour différencier les imports)
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        for table in tables:
            print(f"Traitement de la table : {table}...")
            
            # 1. Charger les données du DSA
            df = pd.read_sql(f"SELECT * FROM [{table}]", dsa)
            
            # 2. Ajouter la colonne de traçabilité (Audit)
            # Cela permet de savoir quand chaque ligne a été ajoutée
            df['date_integration'] = now
            
            # 3. Envoyer vers l'ODS
            # 'append' : Si la table existe, ajoute à la suite. 
            # Si elle n'existe pas, elle est créée.
            # index=False : évite de créer une colonne d'index inutile dans SQL Server.
            df.to_sql(table, con=ods, if_exists='append', index=False)
            
        print(f">>> ODS alimenté avec succès à {now}.")
        
    except Exception as e:
        print(f"❌ Erreur pendant le transfert ODS : {e}")

if __name__ == "__main__":
    # Vérification que la base ODS est accessible
    try:
        with ods.connect() as connexion:
            dsa_to_ods()
    except Exception as e:
        print(f"❌ Impossible de se connecter à la base ODS. Vérifiez qu'elle est créée dans Azure Data Studio : {e}")