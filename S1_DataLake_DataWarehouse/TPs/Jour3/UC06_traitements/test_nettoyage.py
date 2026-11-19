"""Test des regles de rejet de store_sales sur des lignes SYNTHETIQUES (fabriquees pour le test).

Sur les donnees reelles, aucune regle de rejet ne se declenche (etape 4) : ce test verifie que les
regles detectent bien chaque anomalie. Les lignes sont creees en memoire, jamais ecrites dans le
Data Lake, et ne doivent pas etre confondues avec les donnees TPCx-BB.
Usage : ./run.sh test_nettoyage.py
"""
from pyspark.sql import functions as F
import config as C
import schemas as SC
import nettoyage as N

spark = C.get_spark("UC06-test-nettoyage")
noms = SC.noms("store_sales")

# Une vente valide (ticket 0, produit 10001 du lot initial), coherente avec les regles de calcul
base = dict(zip(noms, ["36890", "44093", "10001", "44404", "493944", "2898", "44093", "10", "294", "0", "31",
                       "18.00", "30.78", "23.08", "238.70", "715.48", "558.00", "954.18", "21.46", "0.00",
                       "715.48", "736.94", "157.48"]))
cas = {  # motif attendu : modifications appliquees a la vente valide
    "aucun (ligne valide)": {},
    "type_invalide:ss_quantity": {"ss_quantity": "trente"},
    "obligatoire_vide:ss_store_sk": {"ss_store_sk": None},
    "produit_inconnu": {"ss_item_sk": "999999"},
    "magasin_inconnu": {"ss_store_sk": "99"},
    "date_inconnue": {"ss_sold_date_sk": "999999"},
    "date_hors_periode": {"ss_sold_date_sk": "36000"},
    "identifiant_negatif": {"ss_ticket_number": "-5"},
    "quantite_invalide": {"ss_quantity": "0", "ss_ext_sales_price": "0.00", "ss_ext_list_price": "0.00"},
    "montant_negatif": {"ss_coupon_amt": "-1.00", "ss_net_paid": "716.48", "ss_net_paid_inc_tax": "737.94", "ss_net_profit": "158.48"},
    "calcul_incoherent": {"ss_net_paid": "700.00"},
}
lignes = []
for i, (attendu, modif) in enumerate(cas.items()):
    r = dict(base, **modif)
    r["ss_ticket_number"] = modif.get("ss_ticket_number", str(900000 + i))   # tickets distincts
    lignes.append([r[n] for n in noms] + [attendu])
brut = (spark.createDataFrame(lignes, SC.schema_texte("store_sales").add("attendu", "string"))
        .withColumn("lot", F.lit("test")).withColumn("fichier_source", F.lit("synthetique"))
        .withColumn("date_traitement", F.current_timestamp()))

item_ok, _ = N.nettoyer_item(spark)
store_ok, _ = N.nettoyer_store(spark)
date_ok, _ = N.nettoyer_date_dim(spark)
ok, rej = N.nettoyer_store_sales(spark, item_ok, store_ok, date_ok, brut=brut.drop("attendu"))

# Rapprochement par numero de ticket
attendus = {int(l[noms.index("ss_ticket_number")]): l[-1] for l in lignes}
obtenus = {r["ss_ticket_number"]: r["motifs"] for r in rej.select("ss_ticket_number", "motifs").collect()}
conserves = {r["ss_ticket_number"] for r in ok.select("ss_ticket_number").collect()}
echecs = 0
print(f"{'cas teste':<32} {'resultat':<12} motifs obtenus")
for ticket, attendu in attendus.items():
    if attendu.startswith("aucun"):
        bon = ticket in conserves; res = "conservee"; m = []
    else:
        m = obtenus.get(ticket, []); bon = attendu in m; res = "rejetee" if m else "CONSERVEE"
    echecs += not bon
    print(f"{attendu:<32} {res:<12} {', '.join(m) if m else '-'}  {'OK' if bon else 'ECHEC'}")
print(f"\n{len(attendus) - echecs} cas sur {len(attendus)} conformes")
spark.stop()
