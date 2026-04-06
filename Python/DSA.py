import pandas as pd
from sqlalchemy import create_engine, text

# --- CONNEXIONS ordi de l'iut---

#source = create_engine('mssql+pyodbc://ul7261/Chinook?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')
#dsa = create_engine('mssql+pyodbc://ul7261/DSA?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')

# --- CONNEXIONS ordi de l'iut sale 501---

source = create_engine('mssql+pyodbc://IUTC-S501-16/Chinook?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')
dsa = create_engine('mssql+pyodbc://IUTC-S501-16/DSA?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes')


# --- Connexion à la base Chinook Ordi maison ---
#source = create_engine('mssql+pyodbc://sa:Momosami.2006@localhost:1433/Chinook?driver=/opt/homebrew/lib/libmsodbcsql.18.dylib&TrustServerCertificate=yes')

# --- Connexion à la base DSA ---
#dsa = create_engine('mssql+pyodbc://sa:Momosami.2006@localhost:1433/dsa?driver=/opt/homebrew/lib/libmsodbcsql.18.dylib&TrustServerCertificate=yes')
    
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


