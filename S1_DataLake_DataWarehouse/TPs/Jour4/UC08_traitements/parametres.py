"""Parametres du TP 4 (UC-08) : chemins des donnees, des preuves et session Spark.

Les donnees analysees sont celles produites au Jour 3 (UC-06) : les chemins HDFS et la
creation de la session Spark sont repris du module config.py du J3, pour qu'il n'existe
qu'une seule definition des zones du Data Lake.
"""
import os
import sys

REPO_J4 = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
REPO = os.path.dirname(REPO_J4)
sys.path.insert(0, os.path.join(REPO, "J3", "UC06_traitements"))

import config as J3  # noqa: E402  (chemins et session Spark du Jour 3)

HDFS, SILVER, GOLD, QUALITY, CONTROLES, LOGS = (
    J3.HDFS, J3.SILVER, J3.GOLD, J3.QUALITY, J3.CONTROLES, f"{J3.LAKE}/logs/traitements")

# Jeux de donnees retenus pour l'analyse (etape 1) :
# nom -> zone, chemin, cle de granularite, description de la granularite
JEUX = {
    "silver/ventes_detaillees": ("Silver", f"{SILVER}/ventes_detaillees", ["ligne_id"],
                                 "un article d'un ticket de caisse, avec produit, magasin et date"),
    "gold/fait_ventes": ("Gold", f"{GOLD}/fait_ventes", ["ligne_id"],
                         "un article d'un ticket de caisse (table de faits)"),
    "gold/dim_date": ("Gold", f"{GOLD}/dim_date", ["date_sk"], "un jour"),
    "gold/dim_produit": ("Gold", f"{GOLD}/dim_produit", ["produit_sk"], "un produit"),
    "gold/dim_magasin": ("Gold", f"{GOLD}/dim_magasin", ["magasin_sk"], "un magasin"),
    "gold/kpi_ventes_magasin_mois": ("Gold", f"{GOLD}/kpi_ventes_magasin_mois", ["magasin_sk", "annee_mois"],
                                     "un magasin x un mois"),
    "gold/kpi_ventes_categorie_mois": ("Gold", f"{GOLD}/kpi_ventes_categorie_mois", ["categorie", "annee_mois"],
                                       "une categorie x un mois"),
}


ESSAI = os.environ.get("UC08_ESSAI") == "1"   # mise au point : preuves dans ~/retail-intelligence/tp4/essai


def preuves(etape):
    """Dossier des preuves d'une etape : J4/UC08_preuves/etapeN (ou dossier d'essai)."""
    base = os.path.expanduser("~/retail-intelligence/tp4/essai/preuves") if ESSAI else os.path.join(REPO_J4, "UC08_preuves")
    d = os.path.join(base, f"etape{etape}")
    os.makedirs(d, exist_ok=True)
    return d


def get_spark(app="TP4-Retail-Intelligence"):
    return J3.get_spark(app)
