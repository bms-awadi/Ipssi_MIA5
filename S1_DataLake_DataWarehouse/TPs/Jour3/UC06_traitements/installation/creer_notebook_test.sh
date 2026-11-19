#!/usr/bin/env bash
# Cree et execute (sans interface) un notebook de test : preuve que le noyau Jupyter du venv pilote Spark.
TP=$HOME/retail-intelligence/tp3; cd $TP && source .venv/bin/activate
python - <<'PY'
import nbformat as nbf
nb = nbf.v4.new_notebook()
nb.metadata["kernelspec"] = {"name": "tp3-retail", "display_name": "Python (TP3 Retail)", "language": "python"}
nb.cells = [
  nbf.v4.new_markdown_cell("# UC-06 Etape 1 - Test de l'environnement dans Jupyter\nTeam MaxMag : Maxime DANINO - Awadi BEDJA - Gurvan Godin\n\nObjectif : verifier que le noyau du notebook utilise le venv du TP et pilote Spark."),
  nbf.v4.new_code_cell("import os, sys\nsys.path.insert(0, os.path.expanduser('~/retail-intelligence/tp3/scripts'))\nimport config as C\nprint('Python :', sys.executable)"),
  nbf.v4.new_code_cell("spark = C.get_spark('UC06-notebook-test')\nprint('Version Spark :', spark.version, '| mode :', spark.sparkContext.master)"),
  nbf.v4.new_code_cell("df = spark.read.option('sep', '|').csv(C.bronze('store'))\nprint('Lignes :', df.count(), '| colonnes :', len(df.columns))\ndf.select(df.columns[:6]).show(3, truncate=False)"),
  nbf.v4.new_code_cell("spark.stop()"),
  nbf.v4.new_markdown_cell("Resultat : la session Spark est creee depuis le notebook et lit la zone Bronze dans HDFS."),
]
nbf.write(nb, "notebooks/00_test_environnement.ipynb")
PY
cd notebooks && jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.kernel_name=tp3-retail 00_test_environnement.ipynb 2>&1 | grep -vE 'WARN|incubator' | tail -2
python - <<'PY'
import nbformat as nbf
nb = nbf.read("00_test_environnement.ipynb", as_version=4)
for c in nb.cells:
    if c.cell_type == "code":
        for o in c.get("outputs", []):
            t = o.get("text") or "".join(o.get("data", {}).get("text/plain", ""))
            if t: print(t.strip()[:400])
PY
