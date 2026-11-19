#!/usr/bin/env bash
# Construit (si un constructeur est donne) puis execute un notebook du TP 3 avec le noyau du venv.
# Usage : j3_nb_run.sh <notebook.ipynb> [constructeur.py]
TP=$HOME/retail-intelligence/tp3; cd $TP && source .venv/bin/activate
export PATH=$PATH:/opt/hadoop/bin:/opt/hadoop/sbin
jps | grep -qw NameNode || { echo "HDFS arrete : demarrage"; start-dfs.sh >/dev/null 2>&1; }
hdfs dfsadmin -safemode wait 2>/dev/null | grep -v WARN
[ -n "${2:-}" ] && python "$2" "notebooks/$1"
cd notebooks && jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=1800 --ExecutePreprocessor.kernel_name=tp3-retail "$1" 2>&1 | grep -E 'Writing|Error|error' | grep -v WARN | tail -5
