"""UC-06 Etape 4 : application des regles de nettoyage (nettoyage.py) et mesure de leur effet.

N'ecrit rien dans le Data Lake (l'ecriture Silver est faite a l'etape 6) : produit le catalogue
des regles et le bilan chiffre par regle dans UC06_preuves/etape4/.
Usage : ./run.sh 04_appliquer_nettoyage.py
"""
import csv, os
from pyspark.sql import functions as F
import config as C
import nettoyage as N

PREUVES = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "UC06_preuves", "etape4")
os.makedirs(PREUVES, exist_ok=True)
spark = C.get_spark("UC06-etape4-nettoyage")

with open(os.path.join(PREUVES, "regles_nettoyage.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f, delimiter=";"); w.writerow(["code", "table", "anomalie", "regle", "action", "justification"]); w.writerows(N.REGLES)

bilan = []
def mesurer(table, entree, ok, rej):
    n_in, n_ok, n_rej = entree.count(), ok.count(), rej.count()
    print(f"\n=== {table} : {n_in} lignes en entree (lots reunis), {n_ok} conservees, {n_rej} rejetees")
    par_lot = entree.groupBy("lot").count().collect()
    print("   par lot :", {r["lot"]: r["count"] for r in par_lot})
    bilan.append([table, "entree", n_in]); bilan.append([table, "conservees", n_ok]); bilan.append([table, "rejetees", n_rej])
    if n_rej:
        motifs = rej.select(F.explode("motifs").alias("motif")).groupBy("motif").count().orderBy(F.desc("count")).collect()
        for m in motifs:
            print(f"   motif {m['motif']} : {m['count']}"); bilan.append([table, f"motif {m['motif']}", m["count"]])
    else:
        print("   aucun motif de rejet declenche")
    return n_in

item_ok, item_rej = N.nettoyer_item(spark)
mesurer("item", N.lire_bronze(spark, "item"), item_ok, item_rej)
store_ok, store_rej = N.nettoyer_store(spark)
mesurer("store", N.lire_bronze(spark, "store"), store_ok, store_rej)
print("   colonnes retirees (N15) :", N.COLONNES_RETIREES["store"], "| colonnes restantes :", len(store_ok.columns))
date_ok, date_rej = N.nettoyer_date_dim(spark)
mesurer("date_dim", N.lire_bronze(spark, "date_dim"), date_ok, date_rej)
print("   colonnes retirees (N16) :", N.COLONNES_RETIREES["date_dim"], "| colonnes restantes :", len(date_ok.columns))

ventes_ok, ventes_rej = N.nettoyer_store_sales(spark, item_ok, store_ok, date_ok)
mesurer("store_sales", N.lire_bronze(spark, "store_sales"), ventes_ok, ventes_rej)

print("\n=== store_sales : effets des regles de conservation")
r = ventes_ok.agg(
    F.countDistinct("ss_ligne_id").alias("ss_ligne_id_distincts"), F.count("*").alias("lignes"),
    *[F.sum(F.col(x).isNull().cast("int")).alias(f"N04_{x}_vide") for x in ("ss_customer_sk", "ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_promo_sk")],
    F.sum((F.col("ss_sales_price") == 0).cast("int")).alias("N12_prix_vente_0"),
    F.sum((F.col("ss_net_paid") == 0).cast("int")).alias("N12_net_paye_0"),
    F.sum((F.col("ss_net_profit") < 0).cast("int")).alias("N12_marge_negative"),
).collect()[0].asDict()
for k, v in r.items():
    print(f"   {k:<26}: {v}"); bilan.append(["store_sales", k, v])
print("   N06 : cle de ligne unique :", r["ss_ligne_id_distincts"] == r["lignes"])
print("\nApercu des ventes nettoyees :")
ventes_ok.select("ss_ligne_id", "ss_sold_date_sk", "ss_store_sk", "ss_item_sk", "ss_ticket_number", "ss_net_paid", "lot").show(3, truncate=20)
ventes_ok.printSchema()

with open(os.path.join(PREUVES, "bilan_nettoyage.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f, delimiter=";"); w.writerow(["table", "mesure", "valeur"]); w.writerows(bilan)
print("Fichiers ecrits :", os.path.realpath(PREUVES))
spark.stop()
