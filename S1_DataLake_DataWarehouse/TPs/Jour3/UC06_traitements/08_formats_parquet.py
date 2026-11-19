"""UC-06 Etape 8 : utilisation de Parquet et comparaison des formats CSV, JSON et Parquet.

1. Relecture verifiee d'un resultat Gold en Parquet (schema, lignes, valeurs, chemin).
2. Comparaison sur les memes donnees : les 667 579 ventes du lot initial, 23 colonnes metier,
   ecrites en CSV, JSON et Parquet dans un dossier temporaire HDFS (supprime a la fin).
   Mesures : taille, temps d'ecriture, temps de lecture complete, lecture de 2 colonnes, types relus.
Usage : ./run.sh 08_formats_parquet.py
"""
import csv, os, time
from datetime import datetime
from pyspark.sql import functions as F
import config as C
import schemas as SC

PREUVES = C.preuves(8)
spark = C.get_spark("UC06-etape8-parquet")
jvm, sc = spark.sparkContext._jvm, spark.sparkContext
fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(C.HDFS), sc._jsc.hadoopConfiguration())
P = jvm.org.apache.hadoop.fs.Path
print("Date d'execution :", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

print("\n=== 1. Relecture d'un resultat Gold en Parquet")
chemin = f"{C.GOLD}/kpi_ventes_magasin_mois"
k = spark.read.parquet(chemin)
k.printSchema()
print("  chemin :", chemin)
print("  lignes relues :", k.count(), "(attendu 720 = 12 magasins x 60 mois)")
print("  valeurs exploitables : CA nul ou vide =", k.filter(F.col("chiffre_affaires").isNull() | (F.col("chiffre_affaires") <= 0)).count(),
      "| panier vide =", k.filter(F.col("panier_moyen").isNull()).count())
k.orderBy("annee_mois", "magasin_sk").select("annee_mois", "magasin", "chiffre_affaires", "panier_moyen", "taux_marge").show(3)

print("=== 2. Comparaison des formats sur les memes donnees")
noms = SC.noms("store_sales")
ventes = spark.read.parquet(f"{C.SILVER}/store_sales").filter("lot = 'initial'").select(*noms).cache()
n = ventes.count()
ref = ventes.agg(F.sum("ss_ext_sales_price").alias("ca"), F.sum("ss_quantity").alias("q")).collect()[0]
print(f"  jeu de test : {n} lignes, {len(noms)} colonnes | CA de reference {ref.ca}")
base = f"{C.HDFS}/tmp/uc06_formats"
taille = lambda p: fs.getContentSummary(P(p)).getLength()
resultats = []
src = f"{C.bronze('store_sales')}"
resultats.append({"format": "Bronze .dat (texte |, origine)", "octets": taille(src), "ecriture_s": "", "lecture_complete_s": "",
                  "lecture_2_colonnes_s": "", "types_relus": "texte (sans en-tete, aucun type)", "ca_relu_ok": ""})
for fmt in ("csv", "json", "parquet"):
    p = f"{base}/{fmt}"
    t0 = time.perf_counter()
    w = ventes.coalesce(1).write.mode("overwrite")
    {"csv": lambda: w.option("header", True).csv(p), "json": lambda: w.json(p), "parquet": lambda: w.parquet(p)}[fmt]()
    t_ecr = time.perf_counter() - t0
    lire = {"csv": lambda: spark.read.option("header", True).option("inferSchema", True).csv(p),
            "json": lambda: spark.read.json(p), "parquet": lambda: spark.read.parquet(p)}[fmt]
    # lecture complete mesuree dans Spark (empreinte de toutes les colonnes), sans transfert vers Python
    t0 = time.perf_counter(); df = lire(); df.agg(F.max(F.xxhash64(*df.columns))).collect(); t_lec = time.perf_counter() - t0
    t0 = time.perf_counter(); r = lire().agg(F.sum("ss_ext_sales_price").alias("ca"), F.sum("ss_quantity").alias("q")).collect()[0]; t_2c = time.perf_counter() - t0
    types = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    resume_types = f"montants en {types['ss_ext_sales_price']}, quantite en {types['ss_quantity']}"
    ok = abs(float(r.ca) - float(ref.ca)) < 0.005 and int(r.q) == int(ref.q)
    resultats.append({"format": fmt, "octets": taille(p), "ecriture_s": round(t_ecr, 1), "lecture_complete_s": round(t_lec, 1),
                      "lecture_2_colonnes_s": round(t_2c, 1), "types_relus": resume_types, "ca_relu_ok": ok})
    print(f"  {fmt:<8} {taille(p) / 1048576:7.2f} Mo | ecriture {t_ecr:5.1f} s | lecture complete {t_lec:5.1f} s | "
          f"somme de 2 colonnes {t_2c:5.1f} s | {resume_types} | CA relu = reference : {ok} ({r.ca})")

print("\n=== 3. Ce que Parquet garde : schema relu sans aucune option")
spark.read.parquet(f"{base}/parquet").select("ss_sold_date_sk", "ss_quantity", "ss_ext_sales_price", "ss_net_profit").printSchema()
print("=== et ce que CSV perd : schema relu avec inferSchema")
spark.read.option("header", True).option("inferSchema", True).csv(f"{base}/csv").select("ss_sold_date_sk", "ss_quantity", "ss_ext_sales_price", "ss_net_profit").printSchema()

print("=== 4. Taille de la cle de ligne SHA-256 dans silver/store_sales")
avec = spark.read.parquet(f"{C.SILVER}/store_sales").filter("lot = 'initial'")
p2 = f"{base}/parquet_avec_cle"
avec.coalesce(1).write.mode("overwrite").parquet(p2)
print(f"  23 colonnes metier : {taille(f'{base}/parquet') / 1048576:.2f} Mo | avec ss_ligne_id et provenance : {taille(p2) / 1048576:.2f} Mo")
resultats.append({"format": "parquet avec ss_ligne_id + provenance", "octets": taille(p2), "ecriture_s": "", "lecture_complete_s": "",
                  "lecture_2_colonnes_s": "", "types_relus": "identiques", "ca_relu_ok": ""})

fs.delete(P(base), True)
print("\n  dossier temporaire supprime :", not fs.exists(P(base)))
with open(os.path.join(PREUVES, "comparaison_formats.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, delimiter=";", fieldnames=list(resultats[0].keys())); w.writeheader(); w.writerows(resultats)
print("Preuves :", PREUVES)
spark.stop()
