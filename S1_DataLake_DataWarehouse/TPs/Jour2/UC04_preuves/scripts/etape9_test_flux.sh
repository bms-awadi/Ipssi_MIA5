#!/usr/bin/env bash
# UC-04 Etape 9 : controles avant et apres l'execution du flux GetFile -> PutHDFS.
# Usage : etape9_test_flux.sh avant|apres <dossier_preuves>
set -uo pipefail
export PATH=$PATH:/opt/hadoop/bin
PHASE=$1; OUT=$2; mkdir -p "$OUT"; KIT=$HOME/kit_tpcxbb; DEST=/retail-lake/raw/nifi
export KIT DEST
J=$OUT/journal_test_flux_$PHASE.txt
{ echo "# UC-04 Etape 9 - Controles $PHASE l'execution du flux"; echo "# Machine : $(hostname), utilisateur Linux : $(whoami)"; echo; } > "$J"
n=0
run() { n=$((n+1)); local titre=$1; shift
  { echo "=================================================================="
    printf '[%02d] %s\n' $n "$titre"; echo "Date : $(date '+%Y-%m-%d %H:%M:%S')"; echo "\$ $*"
    echo "------------------------------------------------------------------"
    bash -c "$*" 2>&1 | grep -v 'WARN util.NativeCodeLoader'; echo "(code retour : ${PIPESTATUS[0]})"; echo; } >> "$J"; }
if [ "$PHASE" = avant ]; then
  run "Fichier de test present dans le dossier d'entree" "ls -l /data/nifi/input; sha256sum /data/nifi/input/*"
  run "Destination HDFS existante et vide" "hdfs dfs -ls -d $DEST; hdfs dfs -count $DEST"
else
  run "Dossier d'entree apres execution (Keep Source File = false : le fichier doit avoir ete consomme)" "ls -la /data/nifi/input; echo -n 'fichiers restants : '; find /data/nifi/input -type f | wc -l"
  run "Resultat dans HDFS : liste (commande du TP)" "hdfs dfs -ls -h $DEST"
  run "Resultat dans HDFS : taille (commande du TP)" "hdfs dfs -du -h $DEST"
  run "Aucun fichier temporaire laisse par l'ecriture (Write and rename)" "hdfs dfs -ls -R $DEST | grep -c '^-' | sed 's/^/fichiers : /'; hdfs dfs -ls -R $DEST | grep -iE 'tmp|\\.part|_COPYING_' || echo 'aucun fichier temporaire'"
  run "Integrite : contenu ecrit par NiFi contre fichier original du kit" "echo -n 'taille HDFS  : '; hdfs dfs -stat %b $DEST/store.dat; echo -n 'taille kit   : '; stat -c %s \$KIT/data/store/store.dat; echo -n 'SHA-256 HDFS : '; hdfs dfs -cat $DEST/store.dat | sha256sum | cut -d' ' -f1; echo -n 'SHA-256 kit  : '; sha256sum \$KIT/data/store/store.dat | cut -d' ' -f1"
  run "Lecture des deux premieres lignes depuis HDFS" "hdfs dfs -cat $DEST/store.dat | head -2 | cut -c1-120"
  run "Proprietaire et date du fichier ecrit par NiFi" "hdfs dfs -stat 'proprietaire=%u groupe=%g droits=%A ecrit=%y' $DEST/store.dat"
  run "Le depot de l'UC-03 n'a pas ete touche" "hdfs dfs -ls /retail-lake/raw/tpcxbb/initial/store"
  run "Original du kit toujours present" "ls -l \$KIT/data/store/store.dat"
  run "Traces de PutHDFS dans le journal de NiFi" "grep -hE 'PutHDFS|GetFile' /opt/nifi/logs/nifi-app.log | grep -vi password | tail -5 | cut -c1-220"
fi
echo "$n commandes journalisees"
