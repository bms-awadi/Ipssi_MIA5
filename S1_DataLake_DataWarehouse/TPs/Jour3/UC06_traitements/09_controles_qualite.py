"""UC-06 Etape 9 : controles qualite des jeux Silver et Gold.

Familles de controles demandees par le TP : completude, unicite, validite des types, coherence des valeurs,
volumes, cles candidates, integrite referentielle. Chaque controle produit : regle, resultat, statut
(OK, ALERTE = anomalie connue et acceptee, KO = a corriger).
Sorties : rapport dans quality/controles/<date> (Parquet) et UC06_preuves/etape9/rapport_qualite.csv.
Usage : ./run.sh 09_controles_qualite.py
"""
import csv, os
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, DateType, StringType
import config as C
import schemas as SC
import nettoyage as N

PREUVES = C.preuves(9)
spark = C.get_spark("UC06-etape9-qualite")
debut = datetime.now()
print("Date d'execution :", debut.strftime("%Y-%m-%d %H:%M:%S"))
S = lambda t: spark.read.parquet(f"{C.SILVER}/{t}")
G = lambda t: spark.read.parquet(f"{C.GOLD}/{t}")
rapport = []


def ctrl(famille, jeu, regle, valeur, ok, alerte=False, detail=""):
    statut = "OK" if ok else ("ALERTE" if alerte else "KO")
    rapport.append([famille, jeu, regle, str(valeur), statut, detail])
    print(f"  [{statut:<6}] {famille:<22} {jeu:<26} {regle} : {valeur}")


ventes, item, store, dates = S("store_sales"), S("item"), S("store"), S("date_dim")
fait, dd, dp, dm = G("fait_ventes"), G("dim_date"), G("dim_produit"), G("dim_magasin")
km, kc = G("kpi_ventes_magasin_mois"), G("kpi_ventes_categorie_mois")

print("=== Completude")
for t, df in (("store_sales", ventes), ("item", item), ("store", store), ("date_dim", dates)):
    nul = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in N.OBLIGATOIRES[t]]).collect()[0].asDict()
    ctrl("Completude", f"silver/{t}", "champs obligatoires non vides (" + ", ".join(N.OBLIGATOIRES[t]) + ")", sum(nul.values()), sum(nul.values()) == 0)
sec = ventes.select([F.round(100 * F.avg(F.col(c).isNull().cast("int")), 3).alias(c) for c in ("ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_promo_sk")]).collect()[0].asDict()
ctrl("Completude", "silver/store_sales", "cles secondaires vides (%, toleres si < 1 %)", sec, max(sec.values()) < 1, alerte=True)
nk = fait.select([F.sum(F.col(c).isNull().cast("int")) for c in ("date_sk", "produit_sk", "magasin_sk", "chiffre_affaires", "quantite")]).collect()[0]
ctrl("Completude", "gold/fait_ventes", "cles et mesures principales non vides", sum(nk), sum(nk) == 0)
mq = dp.filter(F.col("marque") == "Inconnu").count()
ctrl("Completude", "gold/dim_produit", "produits a marque inconnue (affichage Inconnu)", mq, mq < 0.01 * dp.count(), alerte=True)

print("=== Unicite et cles candidates")
for jeu, df, cle in (("silver/store_sales", ventes, ["ss_ligne_id"]), ("silver/item", item, ["i_item_sk"]), ("silver/store", store, ["s_store_sk"]),
                     ("silver/date_dim", dates, ["d_date_sk"]), ("gold/fait_ventes", fait, ["ligne_id"]), ("gold/dim_date", dd, ["date_sk"]),
                     ("gold/dim_produit", dp, ["produit_sk"]), ("gold/dim_magasin", dm, ["magasin_sk"]),
                     ("gold/kpi_ventes_magasin_mois", km, ["magasin_sk", "annee_mois"]), ("gold/kpi_ventes_categorie_mois", kc, ["categorie", "annee_mois"])):
    n, d = df.count(), df.select(*cle).distinct().count()
    ctrl("Unicite", jeu, f"cle {'+'.join(cle)} unique", f"{n - d} doublon(s) sur {n}", n == d)
dup = ventes.groupBy("ss_ticket_number", "ss_item_sk").count().filter("count > 1").count()
ctrl("Cle candidate", "silver/store_sales", "ticket + produit (n'est pas une cle : ventes repetees conservees)", f"{dup} couples repetes", True, detail="cle de ligne ss_ligne_id creee a la place (N06)")

print("=== Validite des types")
for t, df in (("store_sales", ventes), ("item", item), ("store", store), ("date_dim", dates)):
    attendu = {n: ty.simpleString() for n, ty in SC.SOURCES[t]["colonnes"]}
    ecarts = [f.name for f in df.schema.fields if f.name in attendu and f.dataType.simpleString() != attendu[f.name]]
    ctrl("Validite", f"silver/{t}", "types conformes au schema cible (schemas.py)", f"{len(ecarts)} ecart(s) {ecarts or ''}", not ecarts)
tm = {f.name: f.dataType for f in fait.schema.fields}
ok = all(isinstance(tm[m], DecimalType) for m in ("chiffre_affaires", "marge", "remise")) and isinstance(tm["quantite"], IntegerType)
ctrl("Validite", "gold/fait_ventes", "montants en decimal, quantite en entier", f"chiffre_affaires {tm['chiffre_affaires'].simpleString()}, quantite {tm['quantite'].simpleString()}", ok)

print("=== Coherence des valeurs")
c = F.col
r = ventes.agg(F.sum(((c("ss_quantity") < 1) | (c("ss_quantity") > 100)).cast("int")).alias("q"),
               F.sum((c("ss_sales_price") > c("ss_list_price")).cast("int")).alias("p"),
               F.sum((F.abs(c("ss_ext_sales_price") - c("ss_sales_price") * c("ss_quantity")) > 0.01).cast("int")).alias("m"),
               F.sum((F.abs(c("ss_net_profit") - (c("ss_net_paid") - c("ss_ext_wholesale_cost"))) > 0.01).cast("int")).alias("g"),
               F.sum((c("ss_sales_price") == 0).cast("int")).alias("z"), F.sum((c("ss_net_profit") < 0).cast("int")).alias("neg"), F.count("*").alias("n")).collect()[0]
ctrl("Coherence", "silver/store_sales", "quantite entre 1 et 100", f"{r.q} hors domaine", r.q == 0)
ctrl("Coherence", "silver/store_sales", "prix de vente <= prix catalogue", f"{r.p} lignes", r.p == 0)
ctrl("Coherence", "silver/store_sales", "montant = prix x quantite (1 centime)", f"{r.m} lignes", r.m == 0)
ctrl("Coherence", "silver/store_sales", "marge = net paye - cout (1 centime)", f"{r.g} lignes", r.g == 0)
ctrl("Coherence", "silver/store_sales", "prix de vente a 0 (atypique conserve)", f"{r.z} lignes ({100 * r.z / r.n:.2f} %)", False, alerte=True)
ctrl("Coherence", "silver/store_sales", "marge negative (atypique du jeu synthetique)", f"{r.neg} lignes ({100 * r.neg / r.n:.1f} %)", False, alerte=True)
cat = dp.select("categorie").distinct().count()
ctrl("Coherence", "gold/dim_produit", "9 categories attendues", cat, cat == 9)
pe = dd.agg(F.min("date"), F.max("date")).collect()[0]
ctrl("Coherence", "gold/dim_date", "periode 2001-01-01 a 2005-12-31", f"{pe[0]} a {pe[1]}", str(pe[0]) == "2001-01-01" and str(pe[1]) == "2005-12-31")
parts = kc.groupBy("annee_mois").agg(F.round(F.sum("part_ca_mois_pct"), 1).alias("p")).filter("abs(p - 100) > 0.1").count()
ctrl("Coherence", "gold/kpi_ventes_categorie_mois", "parts de CA des categories = 100 % chaque mois", f"{parts} mois en ecart", parts == 0)

print("=== Volumes")
bronze = {"store_sales": 667579 + 6632, "item": 17820 + 180, "store": 12, "date_dim": 109573}
for t, df in (("store_sales", ventes), ("item", item), ("store", store), ("date_dim", dates)):
    n = df.count()
    ctrl("Volume", f"silver/{t}", f"lignes = Bronze ({bronze[t]}) - rejets", n, n == bronze[t])
rej = {t: spark.read.parquet(f"{C.REJETS}/{t}").count() for t in bronze}
ctrl("Volume", "quality/rejets", "lignes mises a l'ecart par les regles de nettoyage", rej, sum(rej.values()) == 0)
for jeu, df, att in (("gold/fait_ventes", fait, 674211), ("gold/dim_date", dd, 1826), ("gold/kpi_ventes_magasin_mois", km, 720), ("gold/kpi_ventes_categorie_mois", kc, 540)):
    n = df.count()
    ctrl("Volume", jeu, f"lignes attendues : {att}", n, n == att)
ca = lambda df, col: df.agg(F.sum(col)).collect()[0][0]
v = (ca(ventes, "ss_ext_sales_price"), ca(fait, "chiffre_affaires"), ca(km, "chiffre_affaires"), ca(kc, "chiffre_affaires"))
ctrl("Volume", "silver -> gold", "chiffre d'affaires identique a chaque niveau", v[0], len(set(v)) == 1)

print("=== Integrite referentielle")
for cle_f, dim, cle_d, nom in (("ss_item_sk", item, "i_item_sk", "silver item"), ("ss_store_sk", store, "s_store_sk", "silver store"), ("ss_sold_date_sk", dates, "d_date_sk", "silver date_dim")):
    o = ventes.join(dim.select(F.col(cle_d).alias(cle_f)), cle_f, "left_anti").count()
    ctrl("Integrite", "silver/store_sales", f"{cle_f} existe dans {nom}", f"{o} orphelines", o == 0)
for cle, dim, nom in (("date_sk", dd, "dim_date"), ("produit_sk", dp, "dim_produit"), ("magasin_sk", dm, "dim_magasin")):
    o = fait.join(dim.select(cle), cle, "left_anti").count()
    ctrl("Integrite", "gold/fait_ventes", f"{cle} existe dans {nom}", f"{o} orphelines", o == 0)
nv = dp.join(fait.select("produit_sk").distinct(), "produit_sk", "left_anti").count()
ctrl("Integrite", "gold/dim_produit", "produits sans aucune vente (information)", nv, True, detail="un produit peut ne jamais etre vendu")

nb = {s: sum(1 for x in rapport if x[4] == s) for s in ("OK", "ALERTE", "KO")}
print(f"\nBilan : {len(rapport)} controles | OK {nb['OK']} | ALERTE {nb['ALERTE']} | KO {nb['KO']}")
cols = ["famille", "jeu", "regle", "resultat", "statut", "detail"]
with open(os.path.join(PREUVES, "rapport_qualite.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f, delimiter=";"); w.writerow(cols); w.writerows(rapport)
out = f"{C.CONTROLES}/controles_{debut.strftime('%Y%m%d_%H%M%S')}"
spark.createDataFrame(rapport, cols).withColumn("date_controle", F.lit(debut.strftime("%Y-%m-%d %H:%M:%S"))).coalesce(1).write.mode("overwrite").parquet(out)
print("Rapport ecrit :", out, "| preuves :", PREUVES)
spark.stop()
