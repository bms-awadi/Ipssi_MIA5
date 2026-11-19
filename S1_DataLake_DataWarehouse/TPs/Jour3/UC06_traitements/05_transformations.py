"""UC-06 Etape 5 : transformations, jointures verifiees et agregations (sans ecriture dans le Data Lake).

Enchaine : nettoyage (etape 4) -> preparation (renommage, colonnes calculees) -> jointures controlees
-> agregations par magasin x mois et par categorie x mois. Les controles et resultats sont ecrits
dans UC06_preuves/etape5/. L'ecriture Silver et Gold est faite aux etapes 6 et 7.
Usage : ./run.sh 05_transformations.py
"""
import csv, os
from pyspark.sql import functions as F
import config as C
import nettoyage as N
import transformations as T

PREUVES = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "UC06_preuves", "etape5")
os.makedirs(PREUVES, exist_ok=True)
spark = C.get_spark("UC06-etape5-transformations")

item_ok, _ = N.nettoyer_item(spark)
store_ok, _ = N.nettoyer_store(spark)
date_ok, _ = N.nettoyer_date_dim(spark)
ventes_ok, _ = N.nettoyer_store_sales(spark, item_ok, store_ok, date_ok)

print("=== 1. Preparation : renommage et colonnes calculees")
ventes = T.preparer_ventes(ventes_ok).cache()
dims = {"dates": T.preparer_dates(date_ok), "produits": T.preparer_produits(item_ok), "magasins": T.preparer_magasins(store_ok)}
ventes.printSchema()
c = F.col
ventes.agg(F.sum((F.abs(c("remise") - (c("montant_catalogue") - c("chiffre_affaires"))) > 0.01).cast("int")).alias("remise != catalogue - CA"),
           F.sum(c("avec_promotion").cast("int")).alias("lignes_avec_promotion"),
           F.sum(c("vente_gratuite").cast("int")).alias("lignes_gratuites"),
           F.min("taux_remise_ligne").alias("taux_remise_min"), F.max("taux_remise_ligne").alias("taux_remise_max")).show(truncate=False)

print("=== 2. Granularite du ticket : un ticket = un magasin et un jour ?")
g = ventes.groupBy("ticket").agg(F.countDistinct("magasin_sk").alias("m"), F.countDistinct("date_sk").alias("d"), F.countDistinct("heure_sk").alias("h"), F.count("*").alias("lignes"))
g.agg(F.count("*").alias("tickets"), F.sum((c("m") > 1).cast("int")).alias("tickets_multi_magasins"),
      F.sum((c("d") > 1).cast("int")).alias("tickets_multi_jours"), F.sum((c("h") > 1).cast("int")).alias("tickets_multi_heures"),
      F.min("lignes").alias("lignes_min"), F.round(F.avg("lignes"), 2).alias("lignes_moy"), F.max("lignes").alias("lignes_max")).show()

print("=== 3. Jointures verifiees")
detail, rapports = ventes, []
for nom, cle, relation in T.JOINTURES:
    detail, r = T.joindre_verifie(detail, dims[nom], cle, nom)
    r["relation"] = relation; rapports.append(r)
    print(f"  {nom:<9} cle {cle:<11} | dimension {r['lignes_dimension']} lignes, cle unique : {r['cle_unique_dimension']} | "
          f"ventes avant {r['lignes_avant']} apres {r['lignes_apres']} | non rapprochees {r['non_rapprochees']}")
detail = detail.cache()
print(f"  colonnes de la table detaillee : {len(detail.columns)}")
detail.select("ligne_id", "date_vente", "magasin", "ville", "categorie", "quantite", "chiffre_affaires", "marge", "lot").show(5, truncate=16)
with open(os.path.join(PREUVES, "controle_jointures.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, delimiter=";", fieldnames=list(rapports[0].keys())); w.writeheader(); w.writerows(rapports)

print("=== 4. Agregation par magasin et par mois")
pm = T.ventes_par_magasin_mois(detail).cache()
print("  lignes (magasins x mois) :", pm.count(), "| magasins :", pm.select("magasin_sk").distinct().count(), "| mois :", pm.select("annee_mois").distinct().count())
pm.orderBy("annee_mois", "magasin_sk").select("annee_mois", "magasin", "ville", "chiffre_affaires", "quantite", "nb_tickets", "panier_moyen", "taux_remise", "taux_marge").show(5)

print("=== 5. Agregation par categorie et par mois")
pc = T.ventes_par_categorie_mois(detail).cache()
print("  lignes (categories x mois) :", pc.count())
pc.groupBy("categorie").agg(F.round(F.sum("chiffre_affaires"), 2).alias("ca_total"), F.sum("quantite").alias("quantite")).orderBy(F.desc("ca_total")).show(truncate=False)

print("=== 6. Controles de conservation des totaux (detail = agregats)")
tot = lambda df: df.agg(F.sum("chiffre_affaires").alias("ca"), F.sum("quantite").alias("q"), F.sum("marge").alias("m")).collect()[0]
d, a, b = tot(detail), tot(pm), tot(pc)
print(f"  CA       detail {d.ca} | magasin x mois {a.ca} | categorie x mois {b.ca} | egal : {d.ca == a.ca == b.ca}")
print(f"  quantite detail {d.q} | magasin x mois {a.q} | categorie x mois {b.q} | egal : {d.q == a.q == b.q}")
print(f"  marge    detail {d.m} | magasin x mois {a.m} | categorie x mois {b.m} | egal : {d.m == a.m == b.m}")
tickets_total = detail.select("ticket").distinct().count()
print(f"  tickets distincts : {tickets_total} | somme des tickets par magasin x mois : {pm.agg(F.sum('nb_tickets')).collect()[0][0]}")

print("=== 7. Decembre 2005 : apport du lot refresh")
detail.filter((c("annee") == 2005) & (c("mois") == 12)).groupBy("lot").agg(F.count("*").alias("lignes"), F.min("date_vente"), F.max("date_vente"),
    F.round(F.sum("chiffre_affaires"), 2).alias("ca")).orderBy("lot").show()
detail.filter(c("annee") == 2005).groupBy("mois").agg(F.round(F.sum("chiffre_affaires"), 2).alias("ca_2005")).orderBy("mois").show(12)

# Resultats agreges (petits) gardes comme preuves
for df, nom in ((pm, "ventes_par_magasin_mois"), (pc, "ventes_par_categorie_mois")):
    rows = df.orderBy("annee_mois").collect()
    with open(os.path.join(PREUVES, f"{nom}.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";"); w.writerow(df.columns); w.writerows([[str(x) for x in r] for r in rows])
print("Fichiers ecrits :", os.path.realpath(PREUVES))
spark.stop()
