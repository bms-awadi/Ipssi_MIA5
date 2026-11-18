#!/usr/bin/env bash
# UC-03 Etape 4 : verifications apres depot (presence, taille, lecture, chemin, source intacte).
# Usage : etape4_verification.sh <dossier_kit> <dossier_preuves>
set -uo pipefail
export HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop PATH=$PATH:/opt/hadoop/bin
SRC=$1; OUT=$2; LAKE=/retail-lake
MF=$(ls -t "$OUT"/manifeste_depot_*.csv | head -1)
J=$OUT/journal_verification_depot.txt
{ echo "# UC-03 Etape 4 - Verifications apres depot"; echo "# Manifeste utilise : $(basename "$MF")"; echo; } > "$J"
n=0
run() { n=$((n+1)); local titre=$1; shift
  { echo "=================================================================="
    printf '[%02d] %s\n' $n "$titre"; echo "Date : $(date '+%Y-%m-%d %H:%M:%S')"; echo "\$ $*"
    echo "------------------------------------------------------------------"
    bash -c "$*" 2>&1 | grep -v 'WARN util.NativeCodeLoader'; echo "(code retour : ${PIPESTATUS[0]})"; echo; } >> "$J"; }
export SRC LAKE

# Tailles HDFS en un seul appel par zone (ordre du manifeste)
mapfile -t RAWP < <(tail -n +2 "$MF" | cut -d';' -f5)
mapfile -t BRZP < <(tail -n +2 "$MF" | cut -d';' -f6)
mapfile -t RAWS < <(hdfs dfs -stat '%b' "${RAWP[@]}" 2>/dev/null)
mapfile -t BRZS < <(hdfs dfs -stat '%b' "${BRZP[@]}" 2>/dev/null)
V=$OUT/verification_depot.csv
echo "lot;table;fichier;taille_locale;taille_raw;taille_bronze;tailles_identiques;sha256_local_inchange" > "$V"
i=0
while IFS=';' read -r lot t b loc praw pbrz taille lignes sha rest; do
  shaap=$(sha256sum "$SRC/$loc" | cut -d' ' -f1)
  [ "${RAWS[$i]:-x}" = "$taille" ] && [ "${BRZS[$i]:-x}" = "$taille" ] && ok=oui || ok=NON
  [ "$shaap" = "$sha" ] && inch=oui || inch=NON
  echo "$lot;$t;$b;$taille;${RAWS[$i]:-absent};${BRZS[$i]:-absent};$ok;$inch" >> "$V"
  i=$((i+1))
done < <(tail -n +2 "$MF")

run "Arborescence obtenue (dossiers et fichiers, tailles lisibles)" "hdfs dfs -ls -R -h $LAKE | grep -v '^d' | awk '{print \$5, \$6, \$1, \$3, \$8}' | sed -n '1,80p'"
run "Nombre de dossiers, fichiers et octets par zone" "for z in raw/tpcxbb bronze/tpcxbb logs/ingestion; do hdfs dfs -count $LAKE/\$z; done"
run "Espace occupe par zone" "hdfs dfs -du -s -h $LAKE/raw $LAKE/bronze $LAKE/logs"
run "Presence : fichiers attendus (manifeste) contre fichiers trouves dans HDFS" \
  "echo -n 'attendus RAW : '; tail -n +2 '$MF' | wc -l; echo -n 'trouves RAW  : '; hdfs dfs -ls -R $LAKE/raw/tpcxbb | grep -c '^-'; echo -n 'trouves BRONZE : '; hdfs dfs -ls -R $LAKE/bronze/tpcxbb | grep -c '^-'; echo -n 'chemins du manifeste absents de HDFS : '; comm -23 <(tail -n +2 '$MF' | cut -d';' -f5,6 | tr ';' '\n' | sort) <(hdfs dfs -ls -R $LAKE/raw/tpcxbb $LAKE/bronze/tpcxbb | grep '^-' | awk '{print \$8}' | sort) | wc -l"
run "Tailles : comparaison locale, RAW et BRONZE (fichier par fichier)" "column -t -s';' '$V'"
run "Synthese des comparaisons" "tail -n +2 '$V' | awk -F';' '{n++; if(\$7==\"oui\") t++; if(\$8==\"oui\") s++} END{printf \"%d fichiers : %d avec tailles identiques, %d sources locales inchangees\\n\", n, t, s}'"
run "Lecture depuis HDFS : debut et fin de store_sales.dat (RAW, lot initial)" \
  "hdfs dfs -cat $LAKE/raw/tpcxbb/initial/store_sales/store_sales.dat | head -2; echo '...'; hdfs dfs -tail $LAKE/raw/tpcxbb/initial/store_sales/store_sales.dat | tail -1"
run "Lecture depuis HDFS : nombre de lignes de store_sales.dat en BRONZE (lot initial)" \
  "hdfs dfs -cat '$LAKE/bronze/tpcxbb/store_sales/lot=initial/store_sales.dat' | awk 'END{print NR}'"
run "Recuperation controlee : copie HDFS vers un dossier temporaire et comparaison octet par octet" \
  "rm -rf /tmp/verif_depot && mkdir /tmp/verif_depot && hdfs dfs -get $LAKE/raw/tpcxbb/refresh/store_sales/store_sales.dat /tmp/verif_depot/ && cmp \"\$SRC/data_refresh/store_sales/store_sales.dat\" /tmp/verif_depot/store_sales.dat && echo 'identique octet par octet'; rm -rf /tmp/verif_depot"
run "Permissions apres depot" "hdfs dfs -ls -d $LAKE/raw/tpcxbb/initial $LAKE/raw/tpcxbb/initial/store_sales $LAKE/bronze/tpcxbb/store_sales '$LAKE/bronze/tpcxbb/store_sales/lot=initial'; hdfs dfs -ls $LAKE/raw/tpcxbb/initial/store_sales '$LAKE/bronze/tpcxbb/store_sales/lot=initial'"
run "Source originale conservee sur le disque local" "ls -l \"\$SRC/data/store_sales\" \"\$SRC/data_refresh/store_sales\"; echo; find \"\$SRC\" -type f | wc -l"
echo "$n commandes journalisees"
