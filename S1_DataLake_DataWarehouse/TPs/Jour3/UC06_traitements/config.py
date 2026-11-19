"""Parametres communs du TP 3 (UC-06) : chemins et creation de la session Spark.

Tous les scripts importent ce module : pour changer d'environnement (autre HDFS, lecture
locale de repli, Azure au Jour 4), seules les valeurs de ce fichier sont a modifier.
"""
import os
from pyspark.sql import SparkSession

# --- Racines -------------------------------------------------------------------
HDFS = "hdfs://localhost:9000"
LAKE = f"{HDFS}/retail-lake"

# --- Zones du Data Lake (arborescence creee au Jour 2, UC-03 etape 3) ----------
BRONZE = f"{LAKE}/bronze/tpcxbb"          # entree : <table>/lot=initial|refresh/<table>.dat
SILVER = f"{LAKE}/silver"                 # sortie : tables nettoyees (Parquet)
GOLD = f"{LAKE}/gold"                     # sortie : jeux analytiques (Parquet)
QUALITY = f"{LAKE}/quality"               # sortie : controles et rejets
REJETS = f"{QUALITY}/rejets"
CONTROLES = f"{QUALITY}/controles"

# --- Mode essai : UC06_ESSAI=1 ---------------------------------------------------
# Pendant la mise au point d'un traitement, ses sorties vont dans un espace d'essai
# (HDFS /tmp/uc06_essai et dossier local essai/) ; les executions officielles ecrivent
# dans les zones du Data Lake et dans UC06_preuves.
ESSAI = os.environ.get("UC06_ESSAI") == "1"
if ESSAI:
    GOLD = f"{HDFS}/tmp/uc06_essai/gold"
    QUALITY = f"{HDFS}/tmp/uc06_essai/quality"
    CONTROLES = f"{QUALITY}/controles"
REPO_J3 = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))


def preuves(etape):
    """Dossier des preuves d'une etape (UC06_preuves/etapeN, ou dossier d'essai local)."""
    base = os.path.expanduser("~/retail-intelligence/tp3/data/output/essai") if ESSAI else os.path.join(REPO_J3, "UC06_preuves")
    d = os.path.join(base, f"etape{etape}")
    os.makedirs(d, exist_ok=True)
    return d


# --- Dossier de travail local (WSL) ---------------------------------------------
TP = os.path.expanduser("~/retail-intelligence/tp3")
OUTPUT_LOCAL = f"{TP}/data/output"        # resultats de test et exports locaux

# --- Repli si HDFS est indisponible : lecture directe du kit local --------------
KIT_LOCAL = os.path.expanduser("~/kit_tpcxbb")   # lien vers J1/retail_project/retail_project


def bronze(table, lot="initial"):
    """Chemin HDFS d'une table Bronze pour un lot donne."""
    return f"{BRONZE}/{table}/lot={lot}/{table}.dat"


def get_spark(app="TP3-Retail-Intelligence"):
    """Session Spark locale, utilisant tous les coeurs ; 4 Go pour le driver
    (la table inventory fait 23 millions de lignes)."""
    spark = (SparkSession.builder
             .appName(app)
             .master("local[*]")
             .config("spark.driver.memory", "4g")
             .config("spark.sql.session.timeZone", "Europe/Paris")
             # Spark ne lit pas /opt/hadoop/etc/hadoop : sans ce reglage il ecrit avec la replication
             # par defaut (3) alors que notre HDFS n'a qu'un DataNode (blocs sous-repliques).
             .config("spark.hadoop.dfs.replication", "1")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark
