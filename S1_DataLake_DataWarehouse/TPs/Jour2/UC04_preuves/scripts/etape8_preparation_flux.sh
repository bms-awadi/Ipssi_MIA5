#!/usr/bin/env bash
# UC-04 Preparation du flux NiFi (section 12.3.2 du TP) : copie de travail + destination HDFS.
# Prerequis (sudo, fait a la main) : sudo install -d -o abedja -g abedja -m 755 /data/nifi/input /data/nifi/archive /data/nifi/error
# Usage : preparation_flux_nifi.sh <dossier_preuves>
set -uo pipefail
export PATH=$PATH:/opt/hadoop/bin
OUT=$1; mkdir -p "$OUT"; KIT=$HOME/kit_tpcxbb
SRCF=$KIT/data/store/store.dat
export SRCF
J=$OUT/journal_preparation.txt
{ echo "# UC-04 - Preparation du flux GetFile vers PutHDFS"; echo "# Machine : $(hostname), utilisateur Linux : $(whoami)"; echo; } > "$J"
n=0
run() { n=$((n+1)); local titre=$1; shift
  { echo "=================================================================="
    printf '[%02d] %s\n' $n "$titre"; echo "Date : $(date '+%Y-%m-%d %H:%M:%S')"; echo "\$ $*"
    echo "------------------------------------------------------------------"
    bash -c "$*" 2>&1 | grep -v 'WARN util.NativeCodeLoader'; echo "(code retour : ${PIPESTATUS[0]})"; echo; } >> "$J"; }
run "Dossiers locaux de NiFi (crees avec sudo, proprietaire abedja)" "ls -ld /data /data/nifi /data/nifi/input /data/nifi/archive /data/nifi/error"
run "Fichier choisi pour le premier test : store.dat du lot initial (petit fichier reel du kit)" "ls -l \"\$SRCF\"; awk 'END{print NR \" lignes\"}' \"\$SRCF\"; sha256sum \"\$SRCF\""
run "Copie de travail dans le dossier d'entree (l'original du kit n'est pas touche)" "cp \"\$SRCF\" /data/nifi/input/ && ls -l /data/nifi/input && sha256sum /data/nifi/input/store.dat"
run "Le dossier d'entree ne contient que le fichier de test" "find /data/nifi/input -type f | wc -l"
run "NiFi (compte abedja) peut lire et supprimer dans le dossier d'entree" "ps -o user= -p \$(pgrep -f 'org.apache.nifi.NiFi' | head -1); [ -r /data/nifi/input/store.dat ] && echo 'lecture : oui'; [ -w /data/nifi/input ] && echo 'ecriture dans le dossier (suppression apres lecture) : oui'"
run "Destination HDFS distincte de l'UC-03 : /retail-lake/raw/nifi" "hdfs dfs -mkdir -p /retail-lake/raw/nifi && hdfs dfs -ls -d /retail-lake/raw/nifi && hdfs dfs -count /retail-lake/raw/nifi"
run "Fichiers de configuration Hadoop a donner a PutHDFS" "ls -l /opt/hadoop/etc/hadoop/core-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml"
echo "$n commandes journalisees"
