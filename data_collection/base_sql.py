from pymysql import connect
from faker import Faker
import random
from datetime import datetime, timedelta

# -----------------------
# CONFIGURATION
# -----------------------connector
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = "admin"  # ton mot de passe MySQL
DB_NAME = "sante"
TABLE_NAME = "patients"
N_PATIENTS = 100  # nombre de patients à générer

# Liste de maladies possibles
MALADIES = ['Paludisme', 'Grippe', 'Diabète', 'Hypertension', 'Anémie', 
            'Covid-19', 'Tuberculose', 'Asthme', 'Gastro-entérite', 'Choléra', 'VIH']

# -----------------------
# Connexion à MySQL
# -----------------------
conn = connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    port=3310, 
    password=MYSQL_PASSWORD
)
cursor = conn.cursor()

# -----------------------
# Création de la base et de la table
# -----------------------
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
cursor.execute(f"USE {DB_NAME}")

cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nom VARCHAR(100),
    age INT,
    sexe VARCHAR(10),
    maladie VARCHAR(100),
    date_consultation DATE
)
""")

# -----------------------
# Génération et insertion de données
# -----------------------
faker = Faker('fr_FR')  # pour générer des noms français
patients = []

for _ in range(N_PATIENTS):
    nom = faker.first_name() + " " + faker.last_name()
    age = random.randint(1, 90)
    sexe = random.choice(['M', 'F'])
    maladie = random.choice(MALADIES)
    # date aléatoire dans les 2 dernières années
    date_consultation = faker.date_between(start_date='-2y', end_date='today')
    
    patients.append((nom, age, sexe, maladie, date_consultation))

# Insertion dans la table
insert_query = f"INSERT INTO {TABLE_NAME} (nom, age, sexe, maladie, date_consultation) VALUES (%s, %s, %s, %s, %s)"
cursor.executemany(insert_query, patients)
conn.commit()

print(f"{N_PATIENTS} patients insérés dans la base {DB_NAME}.")

# -----------------------
# Vérification (affiche les 5 premières lignes)
# -----------------------
cursor.execute(f"SELECT * FROM {TABLE_NAME} LIMIT 5")
for row in cursor.fetchall():
    print(row)

# -----------------------
# Fermeture de la connexion
# -----------------------
cursor.close()
conn.close()
