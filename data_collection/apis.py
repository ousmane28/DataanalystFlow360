import requests
import pandas as pd

# API publique et gratuite de données COVID
url = "https://disease.sh/v3/covid-19/countries"

# 1. Envoi de la requête GET
response = requests.get(url)

# 2. Vérifier la réponse
if response.status_code == 200:
    data = response.json()
    print("Données récupérées avec succès depuis l’API disease.sh\n")
else:
    print(" Erreur lors de la récupération :", response.status_code)
    exit()

#  3. Transformer la réponse en DataFrame
pays = []
cas_confirmes = []
deces = []
population = []
tests = []
vaccins = []

for item in data:
    pays.append(item["country"])
    cas_confirmes.append(item["cases"])
    deces.append(item["deaths"])
    population.append(item["population"])
    tests.append(item.get("tests", 0))
    vaccins.append(item.get("vaccinated", "N/A"))

df = pd.DataFrame({
    "Pays": pays,
    "Cas confirmés": cas_confirmes,
    "Décès": deces,
    "Tests effectués": tests,
    "Population": population,
    "Vaccinés": vaccins
})

#  4. Afficher les 10 premières lignes
print(df.head(10))

#  5. Sauvegarder les données dans un CSV
df.to_csv("donnees_sanitaires_api.csv", index=False)
print("\n Fichier 'donnees_sanitaires_api.csv' généré avec succès !")
