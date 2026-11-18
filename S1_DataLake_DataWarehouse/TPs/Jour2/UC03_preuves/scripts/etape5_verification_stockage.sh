#!/usr/bin/env bash
# UC-03 Etape 5 : verification du stockage HDFS.
#   repertoires attendus, fichiers, tailles, permissions, lecture et recuperation,
#   integrite : SHA-256 du contenu lu dans HDFS compare a l'empreinte locale du manifeste.
# Usage : etape5_verification_stockage.sh <dossier_preuves_etape4> <dossier_preuves_etape5>
set -uo pipefail
export HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop PATH=$PATH:/opt/hadoop/bin
E4=$1; OUT=$2; mkdir -p "$OUT"; LAKE=/retail-lake; KIT=$HOME/kit_tpcxbb
MF=$(ls -t "$E4"/manifeste_depot_*.csv | head -1)
J=$OUT/journal_verification_stockage.txt
{ echo "# UC-03 Etape 5 - Verification du stockage HDFS"
  echo "# Machine : $(hostname) ($(lsb_release -ds)), utilisateur Linux : $(whoami), Hadoop $(hadoop version | head -1 | cut -d' ' -f2)"
  echo "# Manifeste de reference : $(basename "$MF")"; echo; } > "$J"
n=0
run() { n=$((n+1)); local titre=$1; shift
  { echo "=================================================================="
    printf '[%02d] %s\n' $n "$titre"; echo "Date : $(date '+%Y-%m-%d %H:%M:%S')"; echo "\$ $*"
    echo "------------------------------------------------------------------"
    bash -c "$*" 2>&1 | grep -v 'WARN util.NativeCodeLoader'; echo "(code retour : ${PIPESTATUS[0]})"; echo; } >> "$J"; }
export LAKE KIT

# ---------- 1. Repertoires attendus ----------
ATT=$OUT/repertoires_attendus.txt
{ for d in raw raw/tpcxbb raw/tpcxbb/initial raw/tpcxbb/refresh raw/nifi bronze bronze/tpcxbb silver gold \
           quality quality/controles quality/rejets logs logs/ingestion; do echo "$LAKE/$d"; done
  tail -n +2 "$MF" | while IFS=';' read -r lot t rest; do
    echo "$LAKE/raw/tpcxbb/$lot/$t"; echo "$LAKE/bronze/tpcxbb/$t"; echo "$LAKE/bronze/tpcxbb/$t/lot=$lot"; done
} | sort -u > "$ATT"
run "Presence des repertoires attendus (etape 3 + dossiers des tables)" \
  "echo -n 'repertoires attendus : '; wc -l < '$ATT'; hdfs dfs -ls -R $LAKE | grep '^d' | awk '{print \$8}' | sort > /tmp/rep_trouves.txt; echo -n 'repertoires trouves  : '; wc -l < /tmp/rep_trouves.txt; echo -n 'attendus absents     : '; comm -23 '$ATT' /tmp/rep_trouves.txt | wc -l; echo 'trouves non prevus   :'; comm -13 '$ATT' /tmp/rep_trouves.txt; rm -f /tmp/rep_trouves.txt"

# ---------- 2. Fichiers et tailles ----------
run "Presence des fichiers deposes et tailles (Raw et Bronze contre manifeste)" \
  "tail -n +2 '$MF' | cut -d';' -f5,7 | tr ';' ' ' | sort > /tmp/a.txt; tail -n +2 '$MF' | cut -d';' -f6,7 | tr ';' ' ' | sort >> /tmp/a.txt; sort -o /tmp/a.txt /tmp/a.txt; hdfs dfs -ls -R $LAKE/raw/tpcxbb $LAKE/bronze/tpcxbb | grep '^-' | awk '{print \$8, \$5}' | sort > /tmp/t.txt; echo -n 'fichiers attendus : '; wc -l < /tmp/a.txt; echo -n 'fichiers trouves  : '; wc -l < /tmp/t.txt; echo -n 'chemin et taille identiques : '; comm -12 /tmp/a.txt /tmp/t.txt | wc -l; echo 'differences :'; diff /tmp/a.txt /tmp/t.txt && echo 'aucune'; rm -f /tmp/a.txt /tmp/t.txt"
run "Volumes par zone" "hdfs dfs -du -h $LAKE; echo; hdfs dfs -count -h $LAKE/raw/tpcxbb $LAKE/bronze/tpcxbb $LAKE/logs/ingestion"

# ---------- 3. Etat des blocs ----------
run "Sante des blocs de tout le Data Lake (fsck)" "hdfs fsck $LAKE | sed -n '/Status/,/Number of racks/p'"
run "Blocs du plus gros fichier : inventory.dat (Raw, lot initial)" \
  "hdfs fsck $LAKE/raw/tpcxbb/initial/inventory/inventory.dat -files -blocks -locations | grep -E 'inventory.dat|blk_|Total blocks' | sed -e 's/DatanodeInfoWithStorage/DN/g' | cut -c1-170"

# ---------- 4. Permissions ----------
run "Permissions : Raw en lecture seule, Bronze modifiable par le proprietaire" \
  "hdfs dfs -ls -d $LAKE/raw/tpcxbb $LAKE/raw/tpcxbb/initial $LAKE/raw/tpcxbb/initial/store_sales $LAKE/bronze/tpcxbb '$LAKE/bronze/tpcxbb/store_sales/lot=initial'; hdfs dfs -ls $LAKE/raw/tpcxbb/initial/store_sales '$LAKE/bronze/tpcxbb/store_sales/lot=initial'; echo; echo -n 'fichiers Raw sans droit ecriture : '; hdfs dfs -ls -R $LAKE/raw/tpcxbb | grep '^-' | grep -c '^-r--r--r--'"
run "Test : un autre utilisateur peut lire Raw" \
  "HADOOP_USER_NAME=invite hdfs dfs -cat $LAKE/raw/tpcxbb/initial/store/store.dat 2>&1 | head -1 | cut -c1-80"
run "Test : un autre utilisateur ne peut pas supprimer un fichier de Raw (doit etre refuse)" \
  "HADOOP_USER_NAME=invite hdfs dfs -rm -skipTrash $LAKE/raw/tpcxbb/initial/store/store.dat"
run "Test : un autre utilisateur ne peut pas ecrire dans Bronze (doit etre refuse)" \
  "HADOOP_USER_NAME=invite hdfs dfs -touchz '$LAKE/bronze/tpcxbb/store/lot=initial/essai.txt'"
run "Verification : le fichier de Raw est toujours la, aucun fichier ajoute dans Bronze" \
  "hdfs dfs -ls $LAKE/raw/tpcxbb/initial/store '$LAKE/bronze/tpcxbb/store/lot=initial'"

# ---------- 5. Lecture et recuperation controlee ----------
run "Lecture : 3 premieres lignes de customer.dat (Bronze, lot refresh)" \
  "hdfs dfs -cat '$LAKE/bronze/tpcxbb/customer/lot=refresh/customer.dat' 2>/dev/null | head -3 | cut -d'|' -f1-7"
run "Recuperation controlee de web_sales.dat (Raw, lot initial) dans /tmp puis comparaison avec le kit" \
  "rm -rf /tmp/recup && mkdir /tmp/recup && hdfs dfs -get $LAKE/raw/tpcxbb/initial/web_sales/web_sales.dat /tmp/recup/ && ls -l /tmp/recup/web_sales.dat \$KIT/data/web_sales/web_sales.dat && sha256sum /tmp/recup/web_sales.dat \$KIT/data/web_sales/web_sales.dat && cmp /tmp/recup/web_sales.dat \$KIT/data/web_sales/web_sales.dat && echo 'identique octet par octet'; rm -rf /tmp/recup"

# ---------- 6. Integrite : SHA-256 de tous les fichiers ----------
IN=$OUT/integrite_sha256.csv
echo "lot;table;fichier;sha256_local_manifeste;sha256_raw_hdfs;sha256_bronze_hdfs;resultat" > "$IN"
debut=$(date '+%H:%M:%S')
tail -n +2 "$MF" | while IFS=';' read -r lot t b loc praw pbrz taille lignes sha rest; do
  sr=$(hdfs dfs -cat "$praw" 2>/dev/null | sha256sum | cut -d' ' -f1)
  sb=$(hdfs dfs -cat "$pbrz" 2>/dev/null | sha256sum | cut -d' ' -f1)
  [ "$sr" = "$sha" ] && [ "$sb" = "$sha" ] && res=identique || res=DIFFERENT
  echo "$lot;$t;$b;$sha;$sr;$sb;$res"
done >> "$IN"
run "Integrite : SHA-256 du contenu lu dans HDFS (Raw et Bronze) contre SHA-256 local du manifeste (calcul de $debut a $(date '+%H:%M:%S'))" \
  "tail -n +2 '$IN' | awk -F';' '{n++; if(\$7==\"identique\") ok++; else print \"DIFFERENT :\", \$1, \$3} END{printf \"%d fichiers compares (x2 : Raw et Bronze), %d identiques, %d differents\\n\", n, ok, n-ok}'; echo; echo 'Exemple :'; grep ';store_sales.dat;' '$IN' | head -1 | tr ';' '\n' | paste -d' ' <(printf 'lot\ntable\nfichier\nlocal\nraw\nbronze\nresultat\n') -"
run "Empreinte interne HDFS (MD5 des CRC des blocs) : Raw et Bronze de store_sales.dat" \
  "hdfs dfs -checksum $LAKE/raw/tpcxbb/initial/store_sales/store_sales.dat '$LAKE/bronze/tpcxbb/store_sales/lot=initial/store_sales.dat'"
echo "$n commandes journalisees"
