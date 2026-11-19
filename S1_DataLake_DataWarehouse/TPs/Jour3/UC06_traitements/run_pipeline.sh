#!/usr/bin/env bash
# Chaine complete de l'UC-06, dans l'ordre d'execution. Chaque etape ecrit son journal dans UC06_preuves/etapeN.
# Usage : ./run_pipeline.sh            (toutes les etapes)
#         ./run_pipeline.sh 6 7        (seulement les etapes 6 et 7)
# Prerequis : venv ~/retail-intelligence/tp3/.venv, HDFS demarre (run.sh le demarre si besoin), Bronze depose (Jour 2).
set -e
cd "$(dirname "$(readlink -f "$0")")"
P=../UC06_preuves
declare -A ETAPES=(
  [1]="01_verif_environnement.py|$P/etape1/journal_verif_environnement.txt"
  [2]="02_inventaire_sources.py|$P/etape2/journal_inventaire.txt"
  [4]="04_appliquer_nettoyage.py|$P/etape4/journal_nettoyage.txt"
  [44]="test_nettoyage.py|$P/etape4/journal_test_regles.txt"
  [5]="05_transformations.py|$P/etape5/journal_transformations.txt"
  [6]="06_produire_silver.py|$P/etape6/journal_silver.txt"
  [7]="07_produire_gold.py|$P/etape7/journal_gold.txt"
  [8]="08_formats_parquet.py|$P/etape8/journal_parquet.txt"
  [9]="09_controles_qualite.py|$P/etape9/journal_qualite.txt"
  [10]="10_verifier_chaine.py|$P/etape10/journal_verification_chaine.txt"
)
ORDRE=(1 2 4 44 5 6 7 8 9 10)
[ $# -gt 0 ] && ORDRE=("$@")
for e in "${ORDRE[@]}"; do
  IFS='|' read -r script journal <<< "${ETAPES[$e]}"
  mkdir -p "$(dirname "$journal")"
  echo "=== $(date '+%H:%M:%S') etape $e : $script"
  ./run.sh "$script" "$journal" > /dev/null
  echo "    termine, journal : $journal"
done
# L'exploration (etape 3) est un notebook : ./run_notebook.sh 01_exploration.ipynb
