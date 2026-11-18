#!/usr/bin/env bash
# UC-04 Etape 12 : test du flux enrichi (UpdateAttribute, RouteOnAttribute, PutHDFS par table, PutFile pour les rejets).
# Usage : etape12_flux_enrichi.sh avant|apres <dossier_preuves>
set -uo pipefail
export PATH=$PATH:/opt/hadoop/bin
PHASE=$1; OUT=$2; mkdir -p "$OUT"; KIT=$HOME/kit_tpcxbb; DEST=/retail-lake/raw/nifi
export KIT DEST
J=$OUT/journal_flux_enrichi_$PHASE.txt
{ echo "# UC-04 Etape 12 - Flux enrichi ($PHASE)"; echo "# Machine : $(hostname), utilisateur Linux : $(whoami)"; echo; } > "$J"
n=0
run() { n=$((n+1)); local titre=$1; shift
  { echo "=================================================================="
    printf '[%02d] %s\n' $n "$titre"; echo "Date : $(date '+%Y-%m-%d %H:%M:%S')"; echo "\$ $*"
    echo "------------------------------------------------------------------"
    bash -c "$*" 2>&1 | grep -v 'WARN util.NativeCodeLoader'; echo "(code retour : ${PIPESTATUS[0]})"; echo; } >> "$J"; }
if [ "$PHASE" = avant ]; then
  date '+%Y-%m-%d %H:%M:%S' > /tmp/uc04_e12_debut
  run "Etat initial : HDFS raw/nifi et dossier de rejets" "hdfs dfs -ls -R $DEST; echo; ls -la /data/nifi/error"
  run "Trois fichiers reels du kit copies dans le dossier d'entree (2 .dat attendus en HDFS, 1 .csv attendu en rejet)" \
    "cp \$KIT/data/reason/reason.dat \$KIT/data/ship_mode/ship_mode.dat \$KIT/data/item_marketprices/item_marketprices_audit.csv /data/nifi/input/ && ls -l /data/nifi/input && sha256sum /data/nifi/input/*"
else
  DEB=$(cat /tmp/uc04_e12_debut 2>/dev/null || echo '2026-11-17 00:00:00')
  run "Dossier d'entree vide (les 3 fichiers ont ete lus)" "echo -n 'fichiers restants : '; find /data/nifi/input -type f | wc -l"
  run "HDFS : les .dat ranges par table sous raw/nifi" "hdfs dfs -ls -R $DEST"
  run "HDFS : integrite des .dat deposes par NiFi" "for t in reason ship_mode; do printf '%-10s HDFS %s  kit %s\n' \$t \"\$(hdfs dfs -cat $DEST/\$t/\$t.dat | sha256sum | cut -c1-16)\" \"\$(sha256sum \$KIT/data/\$t/\$t.dat | cut -c1-16)\"; done"
  run "Rejets : le .csv est dans /data/nifi/error et pas dans HDFS" "ls -l /data/nifi/error; echo -n 'SHA-256 rejet : '; sha256sum /data/nifi/error/item_marketprices_audit.csv | cut -c1-16; echo -n 'SHA-256 kit   : '; sha256sum \$KIT/data/item_marketprices/item_marketprices_audit.csv | cut -c1-16; echo -n 'present dans HDFS : '; hdfs dfs -ls -R $DEST | grep -c audit || true"
  run "Le fichier de l'etape 9 est inchange" "hdfs dfs -stat 'ecrit=%y taille=%b' $DEST/store.dat"
  run "Journal d'ingestion : attributs ecrits par LogAttribute pour les 3 FlowFiles" \
    "python3 \"$(dirname "$0")/etape12_journal_ingestion.py\" '$DEB' '$OUT/journal_ingestion_logattribute.csv'"
  run "Erreurs ou avertissements de NiFi pendant le test" "cat /opt/nifi/logs/nifi-app*.log | awk -v d='$DEB' 'substr(\$0,1,19) >= d' | grep -cE ' (ERROR|WARN) ' | sed 's/^/lignes ERROR ou WARN : /'"
fi
echo "$n commandes journalisees"
