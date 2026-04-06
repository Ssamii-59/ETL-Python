import pandas as pd
from sqlalchemy import create_engine, text

# --- CONNEXIONS ---

source = create_engine('source path')
dsa = create_engine('DSA path')


tables = ['Artist', 'Album', 'Track', 
          'Genre', 'MediaType', 'Customer', 'Employee', 
          'Invoice', 'InvoiceLine', 'Playlist', 'PlaylistTrack']

# --- ÉTAPE 1 : SOURCE -> DSA ---
def source_to_dsa():
    print(">>> Alimentation DSA en cours...")
    for table in tables:
        df = pd.read_sql(f"SELECT * FROM {table}", source)
        df.to_sql(table, con=dsa, if_exists='replace', index=False)
    print(">>> DSA OK.")


# --- LANCEMENT TOTAL ---
if __name__ == "__main__":
    source_to_dsa()


