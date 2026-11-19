"""TP J3 - test de l'environnement : session Spark locale, lecture HDFS (Bronze), ecriture et relecture Parquet de test."""
import os, sys
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("TP3-Retail-Intelligence-test")
         .master("local[*]")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
print("Version Spark :", spark.version)
print("Java utilise  :", os.environ.get("JAVA_HOME", "(java par defaut du systeme)"))

# 1. Petit jeu de test (programme du TP)
df = spark.createDataFrame([("Magasin A", 1200.0), ("Magasin B", 850.0), ("Magasin C", 1600.0)], ["magasin", "chiffre_affaires"])
df.show()

# 2. Lecture d'un fichier reel dans HDFS (zone Bronze)
chemin = "hdfs://localhost:9000/retail-lake/bronze/tpcxbb/store/lot=initial/store.dat"
try:
    st = spark.read.option("header", False).option("sep", "|").csv(chemin)
    print("Lecture HDFS OK :", chemin)
    print("Lignes :", st.count(), "| colonnes :", len(st.columns))
    st.select(st.columns[:6]).show(3, truncate=False)
except Exception as e:
    print("Lecture HDFS ECHEC :", type(e).__name__, str(e).splitlines()[0][:300])

# 3. Ecriture et relecture Parquet de test, dans un dossier dedie au TP
sortie = os.path.expanduser("~/retail-intelligence/tp3/data/output/test_parquet")
df.write.mode("overwrite").parquet(sortie)
relu = spark.read.parquet(sortie)
relu.printSchema()
print("Parquet relu :", relu.count(), "lignes depuis", sortie)
spark.stop()
