"""UC-06 Etape 10 : verification de l'organisation de la chaine de traitement.

Verifie que chaque sortie attendue existe dans HDFS avec son volume, que les journaux d'execution
sont presents, que les parametres sont centralises, et relance le test des regles de rejet.
Usage : ./run.sh 10_verifier_chaine.py
"""
import glob, os, subprocess, sys
from datetime import datetime
import config as C

PREUVES = C.preuves(10)
ICI = os.path.dirname(os.path.realpath(__file__))
spark = C.get_spark("UC06-etape10-verification")
jvm, sc = spark.sparkContext._jvm, spark.sparkContext
fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(C.HDFS), sc._jsc.hadoopConfiguration())
P = jvm.org.apache.hadoop.fs.Path
print("Date d'execution :", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
ko = 0

print("\n=== 1. Organisation des traitements (ordre d'execution)")
roles = {"config.py": "parametres et chemins, session Spark", "schemas.py": "schemas et types cibles", "nettoyage.py": "regles de nettoyage",
         "transformations.py": "renommage, jointures, agregations", "gold.py": "definition des jeux Gold", "dictionnaire.py": "description des colonnes"}
for f, r in roles.items():
    present = os.path.exists(os.path.join(ICI, f)); ko += not present
    print(f"  module   {f:<22} {r:<40} {'present' if present else 'ABSENT'}")
for f in sorted(glob.glob(os.path.join(ICI, "[0-9][0-9]_*.py"))):
    print(f"  etape    {os.path.basename(f)}")

print("\n=== 2. Sorties attendues dans HDFS")
sorties = [("bronze (entree)", C.BRONZE), *[(f"silver/{t}", f"{C.SILVER}/{t}") for t in ("store_sales", "item", "store", "date_dim", "ventes_detaillees")],
           *[(f"gold/{t}", f"{C.GOLD}/{t}") for t in ("dim_date", "dim_produit", "dim_magasin", "fait_ventes", "kpi_ventes_magasin_mois", "kpi_ventes_categorie_mois")],
           ("quality/rejets", C.REJETS), ("quality/controles", C.CONTROLES), ("logs/traitements", f"{C.LAKE}/logs/traitements")]
for nom, chemin in sorties:
    existe = fs.exists(P(chemin)); ko += not existe
    if existe:
        cs = fs.getContentSummary(P(chemin))
        lignes = spark.read.parquet(chemin).count() if nom.startswith(("silver", "gold")) else ""
        print(f"  {nom:<30} {cs.getFileCount():>4} fichiers {cs.getLength() / 1048576:9.2f} Mo  {lignes}")
    else:
        print(f"  {nom:<30} ABSENT")

print("\n=== 3. Journaux d'execution")
it = fs.listStatus(P(f"{C.LAKE}/logs/traitements"))
for st in sorted(it, key=lambda s: s.getPath().getName()):
    print("  ", st.getPath().getName())
for e in range(1, 10):
    j = sorted(glob.glob(os.path.join(C.REPO_J3, "UC06_preuves", f"etape{e}", "journal*")))
    print(f"   etape {e} : {', '.join(os.path.basename(x) for x in j) or 'notebook ou aucune'}")

print("\n=== 4. Parametres centralises dans config.py")
for k in ("HDFS", "BRONZE", "SILVER", "GOLD", "QUALITY", "REJETS", "CONTROLES", "OUTPUT_LOCAL", "KIT_LOCAL"):
    print(f"  {k:<13}= {getattr(C, k)}")
spark.stop()

print("\n=== 5. Test des regles de rejet (test_nettoyage.py)")
r = subprocess.run([sys.executable, os.path.join(ICI, "test_nettoyage.py")], capture_output=True, text=True)
fin = [l for l in r.stdout.splitlines() if "conformes" in l]
print("  ", fin[-1] if fin else "ECHEC du test")
ko += not (fin and fin[-1].startswith("11 cas sur 11"))
print(f"\nVerification : {'aucun probleme' if ko == 0 else f'{ko} probleme(s)'}")
