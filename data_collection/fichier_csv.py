import pandas as pd
import random
from datetime import datetime, timedelta

# Données de base
regions = ["Dakar", "Thiès", "Kaolack", "Saint-Louis", "Ziguinchor", "Tambacounda"]
hopitaux = [
    "Hôpital Principal",
    "Hôpital de Thiès",
    "Hôpital Régional",
    "Hôpital Aristide Le Dantec",
    "Hôpital de Ziguinchor",
    "Centre de Santé Tambacounda"
]
maladies = ["Paludisme", "Grippe", "Diabète", "Covid-19", "Tuberculose", "Hypertension"]
traitements = ["Paracétamol", "Quinimax", "Insuline", "Antibiotiques", "Repos", "Chloroquine"]

# Génération des données
data = []
for i in range(1, 101):
    region = random.choice(regions)
    hopital = random.choice(hopitaux)
    sexe = random.choice(["M", "F"])
    age = random.randint(1, 90)
    maladie = random.choice(maladies)
    traitement = random.choice(traitements)
    date_consultation = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 300))
    cout_consultation = random.randint(2000, 20000)
    
    data.append({
        "id": i,
        "region": region,
        "hopital": hopital,
        "sexe": sexe,
        "age": age,
        "maladie": maladie,
        "date_consultation": date_consultation.strftime("%Y-%m-%d"),
        "traitement": traitement,
        "cout_consultation": cout_consultation
    })

# Création du DataFrame
df = pd.DataFrame(data)

# Sauvegarde du CSV
df.to_csv("donnee_sante.csv", index=False, encoding="utf-8")
print(df.head(50))  

