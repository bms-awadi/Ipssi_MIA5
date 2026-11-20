#!/usr/bin/env bash
# Construit puis execute le notebook du TP 4 avec le noyau « Python - Retail TP4 ».
# Usage : ./run_notebook.sh [derniere etape]     (depuis ~/retail-intelligence/tp4/scripts)
#   sans argument : toutes les etapes ; avec 4 : etapes 1 a 4 seulement.
# UC08_ESSAI=1 : notebook, HTML et preuves dans ~/retail-intelligence/tp4/essai (mise au point),
#   le notebook officiel UC08_notebooks/ et UC08_preuves/ ne sont pas touches.
# Demarre HDFS s'il est arrete (cas d'un redemarrage de WSL) et attend la fin du mode sans echec.
set -e
TP=$HOME/retail-intelligence/tp4
source $HOME/retail-intelligence/tp3/.venv/bin/activate
export PATH=$PATH:/opt/hadoop/bin:/opt/hadoop/sbin
jps | grep -qw NameNode || { echo "HDFS arrete : demarrage"; start-dfs.sh >/dev/null 2>&1; }
hdfs dfsadmin -safemode wait 2>/dev/null | grep -v WARN
if [ "${UC08_ESSAI:-}" = "1" ]; then NB_DIR=$TP/essai; HTML_DIR=$TP/essai; mkdir -p "$NB_DIR"
else NB_DIR=$TP/notebooks; HTML_DIR="$TP/scripts/../UC08_preuves"; fi
python "$TP/scripts/construire_notebook.py" "$NB_DIR/TP4_Retail_Intelligence.ipynb" ${1:-99}
cd "$NB_DIR"
jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=1800 \
  --ExecutePreprocessor.kernel_name=retail-tp4 TP4_Retail_Intelligence.ipynb 2>&1 | grep -E 'Writing|Error|error' | tail -5
jupyter nbconvert --to html TP4_Retail_Intelligence.ipynb --output-dir "$HTML_DIR" 2>&1 | grep -E 'Writing' | tail -1
