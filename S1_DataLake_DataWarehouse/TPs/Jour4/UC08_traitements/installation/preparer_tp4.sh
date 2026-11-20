#!/usr/bin/env bash
# Prepare l'environnement du TP 4 a partir de celui du TP 3 (pas de reinstallation de Spark).
# 1. bibliotheques de visualisation dans le venv du TP 3 ; 2. noyau Jupyter du TP 4 ;
# 3. dossier de travail ~/retail-intelligence/tp4 relie au depot Git (J4/).
set -e
source $HOME/retail-intelligence/tp3/.venv/bin/activate
python -m pip install -q --upgrade pip
# pandas < 3 : PySpark 4.3 avertit qu'il ne prend pas encore entierement en charge pandas 3
python -m pip install -q matplotlib "pandas<3" pyarrow jupyterlab ipykernel
python -m ipykernel install --user --name retail-tp4 --display-name "Python - Retail TP4"

TP=$HOME/retail-intelligence/tp4
REPO="/mnt/c/xampp/htdocs/dashboard/Perso/Cours/Architecture DataLake, DataWarehouse, Lakehous/J4"
mkdir -p "$TP"
[ -L "$TP/scripts" ] || ln -s "$REPO/UC08_traitements" "$TP/scripts"
[ -L "$TP/notebooks" ] || ln -s "$REPO/UC08_notebooks" "$TP/notebooks"
ls -la "$TP"
python -m pip freeze | grep -iE '^(pandas|matplotlib|pyarrow|pyspark|numpy|jupyterlab|ipykernel)=='
