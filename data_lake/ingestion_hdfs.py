# import pandas as pd
# import requests
# import mysql.connector
# from pymongo import MongoClient
# from hdfs import InsecureClient
# from io import BytesIO


# # 1 Connexion HDFS

# hdfs_client = InsecureClient('http://localhost:9870', user='root')

# # 2️ Ingestion depuis MongoDB
# def read_mongodb():
#     client = MongoClient("mongodb://admin:admin@localhost:27025/")
#     db = client["sante_db"]
#     collection = db["patients"]
#     data = list(collection.find({}, {'_id': 0}))  # Retire le champ _id
#     return pd.DataFrame(data)


# # 3️ Ingestion depuis MySQL
# def read_mysql():
#     conn = mysql.connector.connect(
#         host="localhost",
#         user="root",
#         password="admin",
#         database="mysql_db_collecte",
#         port=3310
#     )
#     query = "SELECT * FROM patients;"
#     df = pd.read_sql(query, conn)
#     conn.close()
#     return df


# # 4 Ingestion depuis CSV

# def read_csv():
#     return pd.read_csv("./data_collection/donnee_sante.csv")


# # 5 Ingestion depuis Excel

# def read_excel():
#     return pd.read_excel("./data_collection/donnees_sante.xlsx")


# # 6️Ingestion depuis API

# def read_api():
#     url = "https://disease.sh/v3/covid-19/countries"  # exemple d’API publique
#     response = requests.get(url)
#     data = response.json()["countries"]
#     return pd.DataFrame(data)

# # 7️Envoi vers HDFS
# def write_to_hdfs(df, path):
#     with hdfs_client.write(path, encoding='utf-8', overwrite=True) as writer:
#         df.to_csv(writer, index=False)
#     print(f"Données envoyées vers HDFS : {path}")

# # 8️ Pipeline complet
# if __name__ == "__main__":
#     try:
#         print(" Lecture des données MongoDB...")
#         mongo_df = read_mongodb()
#         write_to_hdfs(mongo_df, '/data_collection/base_nosql.py')

#         print("Lecture des données MySQL...")
#         mysql_df = read_mysql()
#         write_to_hdfs(mysql_df, '/data_collection/base_sql.py')

#         print(" Lecture du fichier CSV...")
#         csv_df = read_csv()
#         write_to_hdfs(csv_df, '/data_collection/donnee_sante.csv')

#         print("Lecture du fichier Excel...")
#         excel_df = read_excel()
#         write_to_hdfs(excel_df, '/data_collection/fichier_xls.py')

#         print(" Lecture depuis API...")
#         api_df = read_api()
#         write_to_hdfs(api_df, '/data_collection/donnees_sanitaires_api.csv')

#         print(" Ingestion terminée avec succès dans HDFS !")

#     except Exception as e:
#         print(f" Erreur : {e}")
       
import pandas as pd
import requests
from pymysql import connect
from pymongo import MongoClient
import io

# -------------------- HDFS --------------------
HDFS_HOST = 'localhost'
HDFS_PORT = 9870

# -------------------- MongoDB --------------------
MONGO_HOST = 'localhost'
MONGO_PORT = 27025
MONGO_USER = 'admin'
MONGO_PASSWORD = 'admin'
MONGO_DB = 'sante_db'
MONGO_COLLECTION = 'patients'

def read_mongodb():
    client = MongoClient(f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/")
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    data = list(collection.find({}, {'_id': 0}))
    if not data:
        print("Attention : aucune donnée MongoDB trouvée !")
    return pd.DataFrame(data)

# -------------------- MySQL --------------------
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3310
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'admin'
MYSQL_DB = 'sante'

def read_mysql():
    conn = connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        port=MYSQL_PORT
    )
    query = "SELECT * FROM patients;"
    df = pd.read_sql(query, conn)
    conn.close()
    if df.empty:
        print("Attention : aucune donnée MySQL trouvée !")
    return df

# -------------------- CSV --------------------
def read_csv():
    try:
        return pd.read_csv("../data_collection/donnee_sante.csv")
    except FileNotFoundError:
        print("Fichier CSV non trouvé !")
        return pd.DataFrame()

# -------------------- Excel --------------------
def read_excel():
    try:
        return pd.read_excel("../data_collection/donnees_sante.xlsx")
    except FileNotFoundError:
        print("Fichier Excel non trouvé !")
        return pd.DataFrame()

# -------------------- API --------------------
def read_api():
    try:
        url = "https://disease.sh/v3/covid-19/countries"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Erreur lors de la lecture de l'API : {e}")
        return pd.DataFrame()

# -------------------- Écriture HDFS avec WebHDFS --------------------
def write_to_hdfs(df, path):
    if df.empty:
        print(f"⚠ Aucune donnée à écrire pour {path}")
        return
    
    try:
        # Convertir le DataFrame en CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()
        
        # URL WebHDFS
        url = f'http://{HDFS_HOST}:{HDFS_PORT}/webhdfs/v1{path}'
        
        # Étape 1 : Créer le fichier (obtenir la redirection)
        params = {
            'op': 'CREATE',
            'user.name': 'root',
            'overwrite': 'true'
        }
        
        print(f"  → Création du fichier HDFS : {path}")
        response = requests.put(url, params=params, allow_redirects=False, timeout=10)
        
        if response.status_code == 307:
            # Étape 2 : Suivre la redirection en remplaçant le hostname
            redirect_url = response.headers['Location']
            print(f"  → Redirection vers : {redirect_url}")
            
            # Remplacer le hostname du datanode par localhost
            # Le datanode utilise généralement le port 9864
            import re
            redirect_url = re.sub(r'http://[^:]+:', 'http://localhost:', redirect_url)
            
            print(f"  → URL modifiée : {redirect_url}")
            
            # Envoyer les données
            response = requests.put(
                redirect_url, 
                data=csv_data.encode('utf-8'),
                headers={'Content-Type': 'application/octet-stream'},
                timeout=30
            )
            
            if response.status_code == 201:
                print(f"✓ Données envoyées vers HDFS : {path} ({len(df)} lignes)")
            else:
                print(f"✗ Erreur lors de l'écriture : {response.status_code}")
                print(f"  Réponse : {response.text}")
        else:
            print(f"✗ Erreur lors de la création : {response.status_code}")
            print(f"  Réponse : {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Erreur de connexion HDFS : {e}")
    except Exception as e:
        print(f"✗ Erreur inattendue : {e}")

# -------------------- Pipeline complet --------------------
if __name__ == "__main__":
    print("=" * 60)
    print("DÉMARRAGE DE L'INGESTION VERS HDFS")
    print("=" * 60)
    
    try:
        print("\n[1/5] Lecture des données MongoDB...")
        mongo_df = read_mongodb()
        write_to_hdfs(mongo_df, '/data_collection/base_nosql.csv')
        
        print("\n[2/5] Lecture des données MySQL...")
        mysql_df = read_mysql()
        write_to_hdfs(mysql_df, '/data_collection/base_sql.csv')
        
        print("\n[3/5] Lecture du fichier CSV...")
        csv_df = read_csv()
        write_to_hdfs(csv_df, '/data_collection/donnee_sante.csv')
        
        print("\n[4/5] Lecture du fichier Excel...")
        excel_df = read_excel()
        write_to_hdfs(excel_df, '/data_collection/donnees_sante.xlsx')
        
        print("\n[5/5] Lecture depuis API...")
        api_df = read_api()
        write_to_hdfs(api_df, '/data_collection/donnees_sanitaires_api.csv')
        
        print("\n" + "=" * 60)
        print("✓ INGESTION TERMINÉE AVEC SUCCÈS !")
        print("=" * 60)
        
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"✗ ERREUR CRITIQUE : {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()