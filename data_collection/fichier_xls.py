import pandas as pd
import random
from datetime import datetime, timedelta

# Listes de valeurs possibles
noms_centres = [
    "Centre Médical Dakar", "Clinique Espoir", "Hôpital Régional Thiès",
    "Poste de Santé Rufisque", "Clinique Almadies", "Centre de Santé Pikine",
    "Clinique Médina", "Hôpital Ziguinchor", "Centre Médical Kaolack",
    "Centre de Santé Louga"
]

vaccins = ["Pfizer", "AstraZeneca", "Moderna", "Johnson&Johnson", "Sinopharm"]
responsables = ["Dr. Mbaye", "Dr. Sow", "Dr. Ndour", "Dr. Ba", "Dr. Diouf", "Dr. Diallo", "Dr. Faye"]
regions = ["Dakar", "Thiès", "Kaolack", "Louga", "Saint-Louis", "Ziguinchor", "Fatick"]

# Génération de 100 enregistrements
data = []
for i in range(1, 101):
    code = f"VAC{i:03d}"
    nom = random.choice(noms_centres)
    vaccin = random.choice(vaccins)
    doses = random.randint(100, 1000)
    taux = round(random.uniform(40, 90), 1)
    date_campagne = (datetime(2025, 9, 1) + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
    responsable = random.choice(responsables)
    region = random.choice(regions)
    stock = random.randint(50, 300)
    
    data.append([
        code, nom, vaccin, doses, taux, date_campagne, responsable, region, stock
    ])

# Création du DataFrame
df_vaccination = pd.DataFrame(data, columns=[
    "Code_Centre", "Nom_Centre", "Vaccin", "Nombre_Doses_Administrees",
    "Taux_Couverture(%)", "Date_Dernière_Campagne", "Responsable_Centre",
    "Région", "Stock_Restant"
])

# Enregistrement dans un fichier Excel
df_vaccination.to_excel("donnees_sante.xlsx", index=False)

print(df_vaccination.head(50))  
