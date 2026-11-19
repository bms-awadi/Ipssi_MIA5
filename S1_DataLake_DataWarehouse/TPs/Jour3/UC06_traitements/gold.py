"""Jeux Gold de l'UC-06 (etape 7) : modele en etoile du Jour 1 (FAIT_VENTES et ses dimensions)
et agregats d'indicateurs repondant aux questions des profils P1 et P2.

Chaque jeu est decrit dans DEFINITIONS : finalite, sources, granularite, mesures, regles, limites.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import transformations as T

DEBUT, FIN = "2001-01-01", "2005-12-31"

DEFINITIONS = {
    "dim_date": {
        "finalite": "Analyser les ventes par jour, mois, trimestre, annee, jour de semaine",
        "sources": "silver/date_dim", "granularite": "un jour, du 01/01/2001 au 31/12/2005",
        "mesures": "aucune (dimension)", "regles": "Calendrier limite a la periode des ventes ; colonne d_holiday retiree en Silver",
        "limites": "Pas de jours feries exploitables dans la source"},
    "dim_produit": {
        "finalite": "Analyser les ventes par categorie, classe et marque",
        "sources": "silver/item (lots initial et refresh)", "granularite": "un produit (18 000)",
        "mesures": "prix_courant", "regles": "Marque vide affichee Inconnu",
        "limites": "Marques et noms de produits generes aleatoirement : seule la categorie se prete a l'analyse"},
    "dim_magasin": {
        "finalite": "Analyser les ventes par magasin, ville et Etat",
        "sources": "silver/store", "granularite": "un magasin (12)",
        "mesures": "employes, surface", "regles": "Colonne s_closed_date_sk retiree en Silver",
        "limites": "Un seul pays (Etats-Unis) ; pas d'historique des magasins"},
    "fait_ventes": {
        "finalite": "Table de faits du modele en etoile : base de tous les indicateurs de vente en magasin",
        "sources": "silver/ventes_detaillees", "granularite": "un article d'un ticket de caisse (674 211 lignes)",
        "mesures": "quantite, montant_catalogue, remise, chiffre_affaires, coupon, net_paye, taxe, cout, marge",
        "regles": "Cles vers dim_date, dim_produit, dim_magasin ; chiffre_affaires = ss_ext_sales_price (apres remise, hors taxe)",
        "limites": "Retours non deduits ; ventes web non incluses ; montants issus d'un jeu synthetique (marge souvent negative)"},
    "kpi_ventes_magasin_mois": {
        "finalite": "P2 (responsable de magasin) : suivre son magasin mois par mois ; P1 : comparer les magasins",
        "sources": "fait_ventes, dim_magasin, dim_date", "granularite": "un magasin x un mois (12 x 60 = 720 lignes)",
        "mesures": "chiffre_affaires, quantite, nb_tickets, panier_moyen, taux_remise, taux_marge, ca_meme_mois_n_1, evolution_ca_pct",
        "regles": "Sommes par magasin et mois ; taux recalcules sur les sommes ; panier = CA / tickets distincts ; evolution = (CA - CA du meme mois de l'annee precedente) / CA N-1 x 100",
        "limites": "Pas d'evolution pour 2001 (pas d'annee precedente) ; panier moyen tres eleve (environ 7,5 lignes et 50 articles par ticket)"},
    "kpi_ventes_categorie_mois": {
        "finalite": "P1 (direction commerciale) : poids et evolution de chaque categorie de produits",
        "sources": "fait_ventes, dim_produit, dim_date", "granularite": "une categorie x un mois (9 x 60 = 540 lignes)",
        "mesures": "chiffre_affaires, quantite, nb_tickets, panier_moyen, taux_remise, taux_marge, part_ca_mois_pct",
        "regles": "Sommes par categorie et mois ; part = CA de la categorie / CA total du mois x 100",
        "limites": "nb_tickets non additif entre categories (un ticket contient plusieurs categories)"},
}


def dim_date(date_silver):
    return (T.preparer_dates(date_silver).filter((F.col("date_vente") >= DEBUT) & (F.col("date_vente") <= FIN))
            .withColumnRenamed("date_vente", "date"))


def dim_produit(item_silver):
    return T.preparer_produits(item_silver)


def dim_magasin(store_silver):
    return T.preparer_magasins(store_silver).join(
        store_silver.select(F.col("s_store_sk").alias("magasin_sk"), F.col("s_floor_space").alias("surface")), "magasin_sk")


MESURES_FAIT = ["quantite", "montant_catalogue", "remise", "chiffre_affaires", "coupon", "net_paye", "taxe", "cout", "marge"]


def fait_ventes(ventes_detaillees):
    return ventes_detaillees.select("ligne_id", "ticket", "date_sk", "magasin_sk", "produit_sk", "promotion_sk",
                                    *MESURES_FAIT, "avec_promotion", "lot", "annee")


def _detail(fait, dim_d, dim_p, dim_m):
    return (fait.drop("annee").join(dim_d, "date_sk").join(dim_p.select("produit_sk", "categorie"), "produit_sk")
                .join(dim_m.select("magasin_sk", "magasin", "ville", "etat"), "magasin_sk"))


def kpi_magasin_mois(fait, dim_d, dim_p, dim_m):
    agg = T.ventes_par_magasin_mois(_detail(fait, dim_d, dim_p, dim_m))
    w = Window.partitionBy("magasin_sk", "mois").orderBy("annee")
    return (agg.withColumn("ca_meme_mois_n_1", F.lag("chiffre_affaires").over(w))
               .withColumn("evolution_ca_pct", F.round(100 * (F.col("chiffre_affaires") - F.col("ca_meme_mois_n_1")) / F.col("ca_meme_mois_n_1"), 2)))


def kpi_categorie_mois(fait, dim_d, dim_p, dim_m):
    agg = T.ventes_par_categorie_mois(_detail(fait, dim_d, dim_p, dim_m))
    total = Window.partitionBy("annee_mois")
    return agg.withColumn("part_ca_mois_pct", F.round(100 * F.col("chiffre_affaires") / F.sum("chiffre_affaires").over(total), 2))
