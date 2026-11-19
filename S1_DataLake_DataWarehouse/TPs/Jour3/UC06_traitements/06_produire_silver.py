"""UC-06 Etape 6 : production de la zone Silver (Parquet, HDFS) et controles apres ecriture.

Ecrit dans hdfs:///retail-lake/silver :
  store_sales/        ventes nettoyees, noms TPC-DS, partitionnees par lot
  item/, store/, date_dim/   dimensions nettoyees
  ventes_detaillees/  ventes renommees et enrichies (etape 5), partitionnees par annee
et dans quality/rejets/<table>/ les lignes rejetees avec leurs motifs (meme si aucune).
Un journal d'execution JSON est ecrit dans logs/traitements/.
Le traitement est relancable : chaque sortie est remplacee (mode overwrite) dans son dossier dedie.
Usage : ./run.sh 06_produire_silver.py
"""
import csv, json, os
from datetime import datetime
from pyspark.sql import functions as F
import config as C
import nettoyage as N
import transformations as T
import dictionnaire as DI

PREUVES = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "UC06_preuves", "etape6")
os.makedirs(PREUVES, exist_ok=True)
spark = C.get_spark("UC06-etape6-silver")
jvm, sc = spark.sparkContext._jvm, spark.sparkContext
fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(C.HDFS), sc._jsc.hadoopConfiguration())
Path = jvm.org.apache.hadoop.fs.Path
debut = datetime.now()

# --- 1. Calcul (etapes 4 et 5) ---------------------------------------------------------------
item_ok, item_rej = N.nettoyer_item(spark)
store_ok, store_rej = N.nettoyer_store(spark)
date_ok, date_rej = N.nettoyer_date_dim(spark)
ventes_ok, ventes_rej = N.nettoyer_store_sales(spark, item_ok, store_ok, date_ok)
detail = T.preparer_ventes(ventes_ok)
for nom, cle, _ in T.JOINTURES:
    dim = {"dates": T.preparer_dates(date_ok), "produits": T.preparer_produits(item_ok), "magasins": T.preparer_magasins(store_ok)}[nom]
    detail, _r = T.joindre_verifie(detail, dim, cle, nom)

SORTIES = [  # (nom, DataFrame, partition, rejets, cle)
    ("store_sales", ventes_ok, "lot", ventes_rej, "ss_ligne_id"),
    ("item", item_ok, None, item_rej, "i_item_sk"),
    ("store", store_ok, None, store_rej, "s_store_sk"),
    ("date_dim", date_ok, None, date_rej, "d_date_sk"),
    ("ventes_detaillees", detail, "annee", None, "ligne_id"),
]

# --- 2. Ecriture -----------------------------------------------------------------------------
attendus = {}
for nom, df, part, rej, cle in SORTIES:
    df = df.cache()
    attendus[nom] = {"lignes": df.count(), "schema": [(f.name, f.dataType.simpleString()) for f in df.schema.fields]}
    # Eviter les petits fichiers : un fichier par partition (ou un seul pour une petite table).
    # Sans cela, Spark ecrit un fichier par tache (200 par defaut apres une jointure) : 1 000 fichiers au 1er essai.
    df = df.repartition(part) if part else df.coalesce(1)
    w = df.write.mode("overwrite").option("compression", "snappy")
    (w.partitionBy(part) if part else w).parquet(f"{C.SILVER}/{nom}")
    print(f"ecrit : {C.SILVER}/{nom} ({attendus[nom]['lignes']} lignes{', partitions par ' + part if part else ''})")
    if rej is not None:
        rej.withColumn("motifs", F.concat_ws(",", "motifs")).coalesce(1).write.mode("overwrite").parquet(f"{C.REJETS}/{nom}")
        print(f"ecrit : {C.REJETS}/{nom} ({rej.count()} lignes rejetees)")

# --- 3. Controles apres ecriture -------------------------------------------------------------
controles = []
print("\n=== Controles apres ecriture (relecture depuis HDFS)")
for nom, _, part, _, cle in SORTIES:
    chemin = f"{C.SILVER}/{nom}"
    it = fs.listFiles(Path(chemin), True)
    nb_parquet, taille, succes = 0, 0, False
    while it.hasNext():
        s = it.next(); n = s.getPath().getName()
        if n.endswith(".parquet"): nb_parquet += 1; taille += s.getLen()
        if n == "_SUCCESS": succes = True
    relu = spark.read.parquet(chemin)
    n_relu = relu.count()
    schema_relu = sorted((f.name, f.dataType.simpleString()) for f in relu.schema.fields)
    schema_ok = schema_relu == sorted(attendus[nom]["schema"])
    cle_unique = relu.select(cle).distinct().count() == n_relu
    parts = [r[0] for r in relu.select(part).distinct().orderBy(part).collect()] if part else []
    ligne = {"table": nom, "chemin": chemin, "fichier_succes": succes, "fichiers_parquet": nb_parquet, "octets": taille,
             "lignes_attendues": attendus[nom]["lignes"], "lignes_relues": n_relu, "lignes_ok": n_relu == attendus[nom]["lignes"],
             "colonnes": len(relu.columns), "schema_identique": schema_ok, "cle": cle, "cle_unique": cle_unique,
             "partitions": "|".join(map(str, parts))}
    controles.append(ligne)
    print(f"  {nom:<18} _SUCCESS={succes} | {nb_parquet} fichiers parquet, {taille / 1048576:.2f} Mo | lignes {n_relu}/{attendus[nom]['lignes']} | "
          f"{len(relu.columns)} colonnes, schema identique={schema_ok} | cle {cle} unique={cle_unique}" + (f" | partitions {part}={parts}" if part else ""))
    with open(os.path.join(PREUVES, f"schema_{nom}.txt"), "w", encoding="utf-8") as f:
        f.write(relu._jdf.schema().treeString())

# Coherence metier : totaux identiques entre la table Silver de ventes et la table detaillee
v = spark.read.parquet(f"{C.SILVER}/store_sales").agg(F.sum("ss_ext_sales_price").alias("ca"), F.sum("ss_quantity").alias("q")).collect()[0]
d = spark.read.parquet(f"{C.SILVER}/ventes_detaillees").agg(F.sum("chiffre_affaires").alias("ca"), F.sum("quantite").alias("q")).collect()[0]
print(f"\n  coherence : CA store_sales {v.ca} = CA ventes_detaillees {d.ca} : {v.ca == d.ca} ; quantites {v.q} = {d.q} : {v.q == d.q}")
print("  lecture d'une seule partition (annee=2005) :", spark.read.parquet(f"{C.SILVER}/ventes_detaillees").filter("annee = 2005").count(), "lignes")
spark.read.parquet(f"{C.SILVER}/ventes_detaillees").select("ligne_id", "date_vente", "magasin", "categorie", "chiffre_affaires", "lot", "annee").show(3, truncate=16)

# --- 4. Dictionnaire des colonnes et journal d'execution -------------------------------------
with open(os.path.join(PREUVES, "dictionnaire_silver.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f, delimiter=";"); w.writerow(["table", "colonne", "type", "description"])
    for nom, _, _, _, _ in SORTIES:
        for col, typ in attendus[nom]["schema"]:
            w.writerow([nom, col, typ, DI.DESCRIPTIONS.get(nom, {}).get(col) or DI.PROVENANCE.get(col, "")])
with open(os.path.join(PREUVES, "controle_silver.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, delimiter=";", fieldnames=list(controles[0].keys())); w.writeheader(); w.writerows(controles)
journal = {"traitement": "06_produire_silver.py", "debut": debut.isoformat(timespec="seconds"), "fin": datetime.now().isoformat(timespec="seconds"),
           "spark": spark.version, "entrees": f"{C.BRONZE}/<table>/lot=initial|refresh", "regles": [r[0] for r in N.REGLES],
           "sorties": controles, "coherence_ca": str(v.ca == d.ca)}
nom_journal = f"{C.LAKE}/logs/traitements/silver_{debut.strftime('%Y%m%d_%H%M%S')}.json"
spark.sparkContext.parallelize([json.dumps(journal, ensure_ascii=False, default=str)], 1).saveAsTextFile(nom_journal)
with open(os.path.join(PREUVES, "journal_execution_silver.json"), "w", encoding="utf-8") as f:
    json.dump(journal, f, ensure_ascii=False, indent=2, default=str)
print("journal d'execution ecrit :", nom_journal)
spark.stop()
