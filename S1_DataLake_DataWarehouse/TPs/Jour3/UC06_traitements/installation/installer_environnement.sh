#!/usr/bin/env bash
# TP J3 : environnement Python isole + PySpark + JupyterLab (section 1.3 du TP). Aucune commande sudo.
set -euo pipefail
TP=$HOME/retail-intelligence/tp3
mkdir -p "$TP"/{data/input,data/output,notebooks,scripts,documentation}
cd "$TP"
[ -d .venv ] || python3 -m venv .venv
source .venv/bin/activate
python -m pip install --quiet --upgrade pip
python -m pip install --quiet pyspark jupyterlab ipykernel
python -m ipykernel install --user --name tp3-retail --display-name "Python (TP3 Retail)" >/dev/null
python -m pip freeze > "$TP/documentation/requirements_observes.txt"
echo "--- versions"
which python; python --version
python -c "import pyspark; print('PySpark', pyspark.__version__)"
jupyter lab --version | sed 's/^/JupyterLab /'
jupyter kernelspec list
echo "=== INSTALLATION TERMINEE ==="
