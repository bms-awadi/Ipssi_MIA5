"""UC-06 Etape 1 : verification de l'environnement de traitement.

Releve les versions (Python, PySpark, Spark, Java, client Hadoop), le mode d'execution,
les ressources, puis verifie : session Spark, lecture d'un fichier reel (HDFS Bronze),
affichage du schema, ecriture et relecture d'un resultat de test (local et HDFS).
Usage : python scripts/01_verif_environnement.py
"""
import os, platform, sys, time
from datetime import datetime
import pyspark
import config as C

print("=" * 70)
print("UC-06 Etape 1 - Verification de l'environnement")
print("Date :", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
print("=" * 70)

print("\n[1] Python")
print("  version   :", platform.python_version())
print("  executable:", sys.executable)
print("  systeme   :", platform.platform())

t0 = time.time()
spark = C.get_spark("UC06-etape1-environnement")
sc = spark.sparkContext
jvm = sc._jvm
print(f"\n[2] Session Spark creee en {time.time() - t0:.1f} s")
print("  PySpark           :", pyspark.__version__)
print("  Spark             :", spark.version)
print("  Java (JVM Spark)  :", jvm.System.getProperty("java.version"), "-", jvm.System.getProperty("java.home"))
print("  Client Hadoop     :", jvm.org.apache.hadoop.util.VersionInfo.getVersion())
print("  Mode (master)     :", sc.master)
print("  Parallelisme      :", sc.defaultParallelism, "coeurs")
print("  Memoire driver    :", spark.conf.get("spark.driver.memory"))
print("  Fuseau horaire    :", spark.conf.get("spark.sql.session.timeZone"))
print("  Interface Spark   :", sc.uiWebUrl)

print("\n[3] Chemins utilises (config.py)")
for k in ("BRONZE", "SILVER", "GOLD", "QUALITY", "OUTPUT_LOCAL", "KIT_LOCAL"):
    print(f"  {k:<13}: {getattr(C, k)}")

print("\n[4] Lecture d'un fichier reel dans HDFS (zone Bronze)")
chemin = C.bronze("store")
df = spark.read.option("header", False).option("sep", "|").option("inferSchema", True).csv(chemin)
print("  fichier :", chemin)
print("  lignes  :", df.count(), "| colonnes :", len(df.columns))
print("  schema (types proposes par inferSchema, a valider a l'etape 3) :")
df.printSchema()
df.select(df.columns[:8]).show(3, truncate=False)

print("[5] Ecriture et relecture d'un resultat de test")
test = df.groupBy().count().withColumnRenamed("count", "nb_magasins")
local = f"{C.OUTPUT_LOCAL}/test_etape1"
test.write.mode("overwrite").parquet(local)
print("  local  :", local, "->", spark.read.parquet(local).collect()[0]["nb_magasins"], "relu")
hdfs_test = f"{C.HDFS}/tmp/uc06_test_etape1"
test.write.mode("overwrite").parquet(hdfs_test)
print("  HDFS   :", hdfs_test, "->", spark.read.parquet(hdfs_test).collect()[0]["nb_magasins"], "relu")
fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(C.HDFS), sc._jsc.hadoopConfiguration())
fs.delete(jvm.org.apache.hadoop.fs.Path(hdfs_test), True)
print("  test HDFS supprime apres verification :", not fs.exists(jvm.org.apache.hadoop.fs.Path(hdfs_test)))

print("\n[6] Zones de sortie presentes dans HDFS")
for z in (C.SILVER, C.GOLD, C.REJETS, C.CONTROLES):
    print(f"  {z:<55} existe : {fs.exists(jvm.org.apache.hadoop.fs.Path(z))}")

spark.stop()
print("\nVerification terminee.")
