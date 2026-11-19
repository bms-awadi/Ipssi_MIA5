"""Transformations de l'UC-06 (etape 5) : renommage, colonnes calculees, jointures verifiees, agregations.

Entrees : les DataFrames nettoyes par nettoyage.py (ou relus depuis Silver).
Les definitions d'indicateurs reprennent le dictionnaire des KPI du Jour 1 (UC01_dictionnaire_KPI) :
  CA = SOMME(ss_ext_sales_price) : montant apres remise, hors taxe, retours non deduits
  nombre de transactions = tickets distincts ; panier moyen = CA / tickets
  taux de remise = SOMME(ss_ext_discount_amt) / SOMME(ss_ext_list_price) x 100
"""
from pyspark.sql import functions as F

# --- 1. Renommage et colonnes calculees -----------------------------------------------------
RENOMMAGE_VENTES = {
    "ss_ligne_id": "ligne_id", "ss_ticket_number": "ticket", "ss_sold_date_sk": "date_sk", "ss_sold_time_sk": "heure_sk",
    "ss_store_sk": "magasin_sk", "ss_item_sk": "produit_sk", "ss_customer_sk": "client_sk", "ss_promo_sk": "promotion_sk",
    "ss_quantity": "quantite", "ss_list_price": "prix_catalogue_unitaire", "ss_sales_price": "prix_vente_unitaire",
    "ss_wholesale_cost": "cout_unitaire", "ss_ext_list_price": "montant_catalogue", "ss_ext_discount_amt": "remise",
    "ss_ext_sales_price": "chiffre_affaires", "ss_coupon_amt": "coupon", "ss_net_paid": "net_paye", "ss_ext_tax": "taxe",
    "ss_net_paid_inc_tax": "net_paye_ttc", "ss_ext_wholesale_cost": "cout", "ss_net_profit": "marge",
}


def preparer_ventes(ventes_ok):
    """Noms metier en francais, colonnes calculees ; les colonnes non utiles a l'analyse
    (demographie, menage, adresse) restent dans la table Silver store_sales."""
    v = ventes_ok.select(*[F.col(a).alias(b) for a, b in RENOMMAGE_VENTES.items()], "lot")
    return (v.withColumn("avec_promotion", F.col("promotion_sk").isNotNull())
             .withColumn("vente_gratuite", F.col("chiffre_affaires") == 0)
             .withColumn("taux_remise_ligne", F.when(F.col("montant_catalogue") > 0,
                         F.round(100 * F.col("remise") / F.col("montant_catalogue"), 2))))


def preparer_produits(item_ok):
    return item_ok.select(F.col("i_item_sk").alias("produit_sk"), F.col("i_item_id").alias("produit_id"),
                          F.col("i_category").alias("categorie"), F.col("i_class").alias("classe"),
                          F.coalesce(F.col("i_brand"), F.lit("Inconnu")).alias("marque"),
                          F.col("i_current_price").alias("prix_courant"))


def preparer_magasins(store_ok):
    return store_ok.select(F.col("s_store_sk").alias("magasin_sk"), F.col("s_store_id").alias("magasin_id"),
                           F.col("s_store_name").alias("magasin"), F.col("s_city").alias("ville"),
                           F.col("s_state").alias("etat"), F.col("s_number_employees").alias("employes"))


def preparer_dates(date_ok):
    return date_ok.select(F.col("d_date_sk").alias("date_sk"), F.col("d_date").alias("date_vente"),
                          F.col("d_year").alias("annee"), F.col("d_moy").alias("mois"), F.col("d_qoy").alias("trimestre"),
                          F.date_format("d_date", "yyyy-MM").alias("annee_mois"), F.col("d_day_name").alias("jour_semaine"),
                          (F.col("d_weekend") == "Y").alias("week_end"))


# --- 2. Jointures verifiees ------------------------------------------------------------------
JOINTURES = [
    # (dimension, cle, description de la relation)
    ("dates", "date_sk", "ventes.ss_sold_date_sk = date_dim.d_date_sk : plusieurs ventes par jour, un jour par vente (N vers 1)"),
    ("produits", "produit_sk", "ventes.ss_item_sk = item.i_item_sk : plusieurs ventes par produit, un produit par vente (N vers 1)"),
    ("magasins", "magasin_sk", "ventes.ss_store_sk = store.s_store_sk : plusieurs ventes par magasin, un magasin par vente (N vers 1)"),
]


def joindre_verifie(fait, dim, cle, nom):
    """Jointure gauche controlee : la cle doit etre unique dans la dimension, aucune ligne du fait
    ne doit etre perdue ni dupliquee. Renvoie (resultat, rapport)."""
    n_dim, n_cles = dim.count(), dim.select(cle).distinct().count()
    avant = fait.count()
    marque = dim.withColumn(f"_trouve_{nom}", F.lit(True))
    res = fait.join(marque, cle, "left")
    apres = res.count()
    non_rapproches = res.filter(F.col(f"_trouve_{nom}").isNull()).count()
    rapport = {"dimension": nom, "cle": cle, "lignes_dimension": n_dim, "cle_unique_dimension": n_dim == n_cles,
               "lignes_avant": avant, "lignes_apres": apres, "non_rapprochees": non_rapproches}
    return res.drop(f"_trouve_{nom}"), rapport


# --- 3. Agregations --------------------------------------------------------------------------
def _mesures():
    return [F.sum("chiffre_affaires").alias("chiffre_affaires"), F.sum("quantite").alias("quantite"),
            F.countDistinct("ticket").alias("nb_tickets"), F.count("*").alias("nb_lignes"),
            F.sum("marge").alias("marge"), F.sum("remise").alias("remise"), F.sum("montant_catalogue").alias("montant_catalogue")]


def _ratios(df):
    return (df.withColumn("panier_moyen", F.round(F.col("chiffre_affaires") / F.col("nb_tickets"), 2))
              .withColumn("taux_remise", F.round(100 * F.col("remise") / F.col("montant_catalogue"), 2))
              .withColumn("taux_marge", F.round(100 * F.col("marge") / F.col("chiffre_affaires"), 2)))


def ventes_par_magasin_mois(detail):
    """Granularite : un magasin x un mois. Profil P2 (responsable de magasin) et P1."""
    return _ratios(detail.groupBy("magasin_sk", "magasin", "ville", "etat", "annee", "mois", "annee_mois").agg(*_mesures()))


def ventes_par_categorie_mois(detail):
    """Granularite : une categorie de produit x un mois. Profil P1 (direction commerciale)."""
    return _ratios(detail.groupBy("categorie", "annee", "mois", "annee_mois").agg(*_mesures()))
