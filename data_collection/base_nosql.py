from pymongo import MongoClient

# 1️ Connexion à MongoDB (par défaut en local sur le port 27025)s
client = MongoClient("mongodb://admin:admin@localhost:27025/")

# 2️Création de la base de données "sante_db"
db = client["sante_db"]

# 3️Création d’une collection "patients"
collection = db["patients"]

# 4️Quelques données sanitaires (documents à insérer)
patients = [
    {
        "nom": "Diop",
        "prenom": "Awa",
        "age": 32,
        "sexe": "F",
        "ville": "Dakar",
        "maladie": "Diabète",
        "hopital": "Hôpital Principal",
        "date_consultation": "2025-10-10",
        "medecin": "Dr. Ndiaye"
    },
    {
        "nom": "Fall",
        "prenom": "Mamadou",
        "age": 45,
        "sexe": "M",
        "ville": "Thiès",
        "maladie": "Hypertension",
        "hopital": "Hôpital Régional de Thiès",
        "date_consultation": "2025-09-22",
        "medecin": "Dr. Diallo"
    },
    {
        "nom": "Sow",
        "prenom": "Fatou",
        "age": 28,
        "sexe": "F",
        "ville": "Saint-Louis",
        "maladie": "Paludisme",
        "hopital": "Centre de Santé de Sor",
        "date_consultation": "2025-10-02",
        "medecin": "Dr. Sy"
    }
]

# 5️Insertion des données
result = collection.insert_many(patients)

# 6️Confirmation
print("Données insérées avec succès !")
print("Identifiants des documents :", result.inserted_ids)
