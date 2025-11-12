from pyspark.sql import SparkSession

# spark.version
# spark.stop()
# 1 Création de la session Spark
spark = SparkSession.builder \
    .appName("NettoyageDonneesSante") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()
#
# 2 Lecture des fichiers depuis HDFS
base_nosql = spark.read.option("header", "true").csv("hdfs://namenode:8020/data_collection/base_nosql.csv")
base_sql = spark.read.option("header", "true").csv("hdfs://namenode:8020/data_collection/base_sql.csv")
donnee_sante = spark.read.option("header", "true").csv("hdfs://namenode:8020/data_collection/donnee_sante.csv")
donnees_sanitaires_api = spark.read.option("header", "true").csv("hdfs://namenode:8020/data_collection/donnees_sanitaires_api.csv")

# # 3 Lecture du fichier Excel (si supporté)
# donnees_sante_excel = spark.read \
#     .format("com.crealytics.spark.excel") \
#     .option("header", "true") \
#     .load("hdfs://namenode:8020/data_collection/donnees_sante.xlsx")

# 4 Vérification rapide
print("=== Base SQL ===")
base_sql.show(5)

print("=== Données API ===")
donnees_sanitaires_api.show(5)
