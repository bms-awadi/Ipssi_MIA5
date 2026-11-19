#!/usr/bin/env bash
# Lance un script du TP 3 dans le venv. Usage : run.sh <script.py> [journal]
# Demarre HDFS s'il est arrete (cas d'un redemarrage de WSL) et attend la fin du mode sans echec.
TP=$HOME/retail-intelligence/tp3; cd $TP && source .venv/bin/activate
export PATH=$PATH:/opt/hadoop/bin:/opt/hadoop/sbin
jps | grep -qw NameNode || { echo "HDFS arrete : demarrage"; start-dfs.sh >/dev/null 2>&1; }
hdfs dfsadmin -safemode wait 2>/dev/null | grep -v WARN
cd scripts
F='WARN NativeCodeLoader|incubator|WARN Utils|log4j|default log level|adjust logging'
if [ -n "${2:-}" ]; then python "$1" 2>&1 | grep -vE "$F" | tee "$2"; else python "$1" 2>&1 | grep -vE "$F"; fi
