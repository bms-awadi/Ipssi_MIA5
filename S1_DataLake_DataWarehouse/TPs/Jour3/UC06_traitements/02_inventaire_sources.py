"""UC-06 Etape 2 : inventaire technique des sources retenues (lecture depuis la zone Bronze HDFS).

Pour chaque source et chaque lot : emplacement, format, taille, lignes, colonnes, types proposes
par Spark (inferSchema) compares aux types cibles, valeurs nulles, unicite de la cle candidate,
granularite, et presence du caractere de fin de ligne Windows (\\r) dans la derniere colonne.
Sorties : UC06_preuves/etape2/inventaire_sources.csv et colonnes_sources.csv
Usage : ./run.sh 02_inventaire_sources.py
"""
import csv, os
from pyspark.sql import functions as F
import config as C
import schemas as SC

PREUVES = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "UC06_preuves", "etape2")
os.makedirs(PREUVES, exist_ok=True)

spark = C.get_spark("UC06-etape2-inventaire")
jvm, sc = spark.sparkContext._jvm, spark.sparkContext
fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(C.HDFS), sc._jsc.hadoopConfiguration())

lignes_inv, lignes_col = [], []
for table, info in SC.SOURCES.items():
    for lot in ("initial", "refresh"):
        chemin = C.bronze(table, lot)
        p = jvm.org.apache.hadoop.fs.Path(chemin)
        if not fs.exists(p):
            print(f"\n=== {table} / lot {lot} : absent de Bronze")
            continue
        taille = fs.getFileStatus(p).getLen()
        noms = SC.noms(table)
        # 1) lecture avec inferSchema : types proposes par Spark
        brut = spark.read.option("sep", "|").option("inferSchema", True).csv(chemin)
        if len(brut.columns) != len(noms):
            print(f"!!! {table} {lot} : {len(brut.columns)} colonnes lues, {len(noms)} attendues")
        df = brut.toDF(*noms[:len(brut.columns)])
        n = df.count()
        # 2) lecture texte : caractere \r en fin de ligne, valeurs vides
        txt = spark.read.option("sep", "|").schema(SC.schema_texte(table)).csv(chemin)
        derniere = noms[-1]
        nb_cr = txt.filter(F.col(derniere).endswith("\r")).count()
        nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0].asDict()
        cle = info["cle"]
        nb_cles = df.select(*cle).distinct().count()
        print(f"\n=== {table} / lot {lot}")
        print(f"  chemin    : {chemin}")
        print(f"  taille    : {taille} octets ({taille / 1048576:.2f} Mo)")
        print(f"  lignes    : {n} | colonnes : {len(df.columns)} (attendues : {len(noms)})")
        print(f"  cle {'+'.join(cle)} : {nb_cles} valeurs distinctes pour {n} lignes -> {'unique' if nb_cles == n else f'{n - nb_cles} doublons de cle'}")
        print(f"  fin de ligne Windows (\\r) dans '{derniere}' : {nb_cr} lignes")
        print(f"  colonnes avec valeurs nulles : {sum(1 for v in nulls.values() if v)} sur {len(nulls)}")
        df.printSchema()
        attendu = dict(SC.SOURCES[table]["colonnes"])
        for f in df.schema.fields:
            lignes_col.append([table, lot, f.name, f.dataType.simpleString(), attendu[f.name].simpleString(), nulls[f.name],
                               round(100 * nulls[f.name] / n, 2) if n else 0])
        lignes_inv.append([table, lot, chemin, "texte delimite |, sans en-tete", taille, n, len(df.columns),
                           "+".join(cle), "unique" if nb_cles == n else f"{n - nb_cles} doublons", info["granularite"], nb_cr,
                           sum(1 for v in nulls.values() if v)])

        # Constats propres a chaque table
        if table == "store_sales":
            df.agg(F.min("ss_sold_date_sk").alias("date_sk_min"), F.max("ss_sold_date_sk").alias("date_sk_max"),
                   F.countDistinct("ss_ticket_number").alias("tickets"), F.countDistinct("ss_store_sk").alias("magasins"),
                   F.countDistinct("ss_item_sk").alias("produits"),
                   F.sum((F.col("ss_quantity") <= 0).cast("int")).alias("quantites_<=0"),
                   F.sum((F.col("ss_net_profit") < 0).cast("int")).alias("marges_negatives")).show(truncate=False)
        if table == "date_dim":
            df.agg(F.min("d_date"), F.max("d_date")).show()

with open(os.path.join(PREUVES, "inventaire_sources.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f, delimiter=";")
    w.writerow(["table", "lot", "chemin_hdfs", "format", "taille_octets", "lignes", "colonnes", "cle_candidate",
                "unicite_cle", "granularite", "lignes_avec_cr_final", "colonnes_avec_nulls"])
    w.writerows(lignes_inv)
with open(os.path.join(PREUVES, "colonnes_sources.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f, delimiter=";")
    w.writerow(["table", "lot", "colonne", "type_infere_spark", "type_cible", "nulls", "pct_nulls"])
    w.writerows(lignes_col)
print("\nFichiers ecrits :", os.path.realpath(PREUVES))
spark.stop()
