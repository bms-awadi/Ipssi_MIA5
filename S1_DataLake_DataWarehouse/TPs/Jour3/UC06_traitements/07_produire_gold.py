"""UC-06 Etape 7 : production de la zone Gold a partir de Silver, puis controles.

Lit silver/ (etape 6) et ecrit dans gold/ : dim_date, dim_produit, dim_magasin, fait_ventes (partition annee),
kpi_ventes_magasin_mois, kpi_ventes_categorie_mois. Documente chaque jeu (gold.DEFINITIONS).
Usage : ./run.sh 07_produire_gold.py        (UC06_ESSAI=1 pour un essai hors zones officielles)
"""
import csv, json, os
from datetime import datetime
from pyspark.sql import functions as F
import config as C
import gold as G

PREUVES = C.preuves(7)
spark = C.get_spark("UC06-etape7-gold")
debut = datetime.now()
print("Date d'execution :", debut.strftime("%Y-%m-%d %H:%M:%S"), "| sortie :", C.GOLD)

s = lambda t: spark.read.parquet(f"{C.SILVER}/{t}")
dd, dp, dm = G.dim_date(s("date_dim")).cache(), G.dim_produit(s("item")).cache(), G.dim_magasin(s("store")).cache()
fv = G.fait_ventes(s("ventes_detaillees")).cache()
km, kc = G.kpi_magasin_mois(fv, dd, dp, dm).cache(), G.kpi_categorie_mois(fv, dd, dp, dm).cache()

JEUX = [("dim_date", dd, None), ("dim_produit", dp, None), ("dim_magasin", dm, None),
        ("fait_ventes", fv, "annee"), ("kpi_ventes_magasin_mois", km, None), ("kpi_ventes_categorie_mois", kc, None)]
bilan = []
for nom, df, part in JEUX:
    out = df.repartition(part) if part else df.coalesce(1)
    w = out.write.mode("overwrite")
    (w.partitionBy(part) if part else w).parquet(f"{C.GOLD}/{nom}")
    relu = spark.read.parquet(f"{C.GOLD}/{nom}")
    n, n_relu = df.count(), relu.count()
    bilan.append({"jeu": nom, "lignes_ecrites": n, "lignes_relues": n_relu, "colonnes": len(relu.columns),
                  "schema_identique": sorted(x.simpleString() for x in [f.dataType for f in relu.schema.fields]) ==
                                      sorted(x.simpleString() for x in [f.dataType for f in df.schema.fields])})
    print(f"  {nom:<26} {n_relu:>7} lignes relues / {n} ecrites, {len(relu.columns)} colonnes")
    with open(os.path.join(PREUVES, f"schema_{nom}.txt"), "w", encoding="utf-8") as f:
        f.write(relu._jdf.schema().treeString())

print("\n=== Controles du modele en etoile")
g = lambda t: spark.read.parquet(f"{C.GOLD}/{t}")
f_, d_, p_, m_ = g("fait_ventes"), g("dim_date"), g("dim_produit"), g("dim_magasin")
for cle, dim, nom in (("date_sk", d_, "dim_date"), ("produit_sk", p_, "dim_produit"), ("magasin_sk", m_, "dim_magasin")):
    orph = f_.join(dim.select(cle), cle, "left_anti").count()
    uniq = dim.select(cle).distinct().count() == dim.count()
    print(f"  fait_ventes.{cle} -> {nom}.{cle} : {orph} ventes orphelines | cle unique dans {nom} : {uniq}")
    bilan.append({"jeu": f"integrite fait_ventes->{nom}", "lignes_ecrites": orph, "lignes_relues": "", "colonnes": "", "schema_identique": uniq})
ca = lambda df: df.agg(F.sum("chiffre_affaires")).collect()[0][0]
ca_s, ca_f, ca_m, ca_c = ca(s("ventes_detaillees")), ca(f_), ca(g("kpi_ventes_magasin_mois")), ca(g("kpi_ventes_categorie_mois"))
print(f"  CA Silver {ca_s} | fait_ventes {ca_f} | kpi magasin x mois {ca_m} | kpi categorie x mois {ca_c} | egal : {ca_s == ca_f == ca_m == ca_c}")
bilan.append({"jeu": "coherence CA Silver = Gold", "lignes_ecrites": str(ca_f), "lignes_relues": "", "colonnes": "", "schema_identique": ca_s == ca_f == ca_m == ca_c})
print(f"  dim_date : {d_.agg(F.min('date'), F.max('date')).collect()[0]} ({d_.count()} jours)")

print("\n=== Apercu des indicateurs")
k = g("kpi_ventes_magasin_mois")
k.filter("annee = 2005 AND mois = 12").orderBy(F.desc("chiffre_affaires")).select(
    "magasin", "ville", "chiffre_affaires", "nb_tickets", "panier_moyen", "taux_remise", "taux_marge", "ca_meme_mois_n_1", "evolution_ca_pct").show(12)
print("  lignes sans evolution (2001, pas d'annee precedente) :", k.filter(F.col("evolution_ca_pct").isNull()).count())
k.groupBy("annee").agg(F.round(F.sum("chiffre_affaires"), 2).alias("ca"), F.round(F.avg("evolution_ca_pct"), 2).alias("evolution_moyenne_pct")).orderBy("annee").show()
g("kpi_ventes_categorie_mois").filter("annee_mois = '2005-12'").orderBy(F.desc("part_ca_mois_pct")).select(
    "categorie", "chiffre_affaires", "quantite", "taux_marge", "part_ca_mois_pct").show(truncate=False)

with open(os.path.join(PREUVES, "controle_gold.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, delimiter=";", fieldnames=list(bilan[0].keys())); w.writeheader(); w.writerows(bilan)
with open(os.path.join(PREUVES, "definitions_gold.csv"), "w", newline="", encoding="utf-8") as f:
    cols = ["jeu", "finalite", "sources", "granularite", "mesures", "regles", "limites"]
    w = csv.writer(f, delimiter=";"); w.writerow(cols + ["colonnes"])
    for nom, df, _ in JEUX:
        w.writerow([nom] + [G.DEFINITIONS[nom][c] for c in cols[1:]] + [", ".join(df.columns)])
for nom in ("kpi_ventes_magasin_mois", "kpi_ventes_categorie_mois"):
    rows = g(nom).orderBy("annee_mois").collect()
    with open(os.path.join(PREUVES, f"{nom}.csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";"); w.writerow(rows[0].__fields__); w.writerows([[str(x) for x in r] for r in rows])
journal = {"traitement": "07_produire_gold.py", "debut": debut.isoformat(timespec="seconds"), "fin": datetime.now().isoformat(timespec="seconds"),
           "entree": C.SILVER, "sortie": C.GOLD, "jeux": bilan}
with open(os.path.join(PREUVES, "journal_execution_gold.json"), "w", encoding="utf-8") as f:
    json.dump(journal, f, ensure_ascii=False, indent=2, default=str)
if not C.ESSAI:
    spark.sparkContext.parallelize([json.dumps(journal, ensure_ascii=False, default=str)], 1).saveAsTextFile(
        f"{C.LAKE}/logs/traitements/gold_{debut.strftime('%Y%m%d_%H%M%S')}.json")
print("Preuves :", PREUVES)
spark.stop()
