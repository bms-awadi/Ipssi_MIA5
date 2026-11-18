#!/usr/bin/env bash
# UC-03 Etape 3 : creation de l'arborescence du Data Lake dans HDFS. Journal date.
export HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop PATH=$PATH:/opt/hadoop/bin
J=$1/journal_arborescence.txt
{ echo "# UC-03 Etape 3 - Creation de l'arborescence du Data Lake"
  echo "# Machine : $(hostname) ($(lsb_release -ds)), utilisateur Linux : $(whoami), Hadoop $(hadoop version | head -1 | cut -d' ' -f2)"
  echo; } > "$J"
n=0
run() { n=$((n+1)); local titre=$1; shift
  { echo "=================================================================="
    printf '[%02d] %s\n' $n "$titre"; echo "Date : $(date '+%Y-%m-%d %H:%M:%S')"; echo "\$ $*"
    echo "------------------------------------------------------------------"
    bash -c "$*" 2>&1 | grep -v 'WARN util.NativeCodeLoader'; echo "(code retour : ${PIPESTATUS[0]})"; echo; } >> "$J"; }
run "Etat avant creation" "hdfs dfs -ls -R /retail-lake"
run "Zone RAW : copie exacte des fichiers livres, par lot, et depots NiFi (UC-04)" \
    "hdfs dfs -mkdir -p /retail-lake/raw/tpcxbb/initial /retail-lake/raw/tpcxbb/refresh /retail-lake/raw/nifi"
run "Zone BRONZE : fichiers ranges par table et par lot" "hdfs dfs -mkdir -p /retail-lake/bronze/tpcxbb"
run "Zones SILVER et GOLD (alimentees au Jour 3)" "hdfs dfs -mkdir -p /retail-lake/silver /retail-lake/gold"
run "Zone QUALITY : resultats des controles et lignes rejetees" "hdfs dfs -mkdir -p /retail-lake/quality/controles /retail-lake/quality/rejets"
run "Zone LOGS : journaux et manifestes d'ingestion" "hdfs dfs -mkdir -p /retail-lake/logs/ingestion"
run "Arborescence obtenue (liste recursive)" "hdfs dfs -ls -R /retail-lake"
run "Vue en arbre" "hdfs dfs -ls -R /retail-lake | awk '{print \$8}' | sed -e 's#^/retail-lake##' -e 's#[^/]*/#   #g' | sed '1i /retail-lake'"
run "Nombre de dossiers et de fichiers" "hdfs dfs -count /retail-lake"
run "Relance de la creation (doit etre sans effet ni erreur)" "hdfs dfs -mkdir -p /retail-lake/raw/tpcxbb/initial && hdfs dfs -count /retail-lake"
echo "$n commandes journalisees"
