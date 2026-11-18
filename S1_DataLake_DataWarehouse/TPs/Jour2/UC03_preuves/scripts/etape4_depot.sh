#!/usr/bin/env bash
# UC-03 Etape 4 : depot des donnees TPCx-BB dans HDFS.
#   1. controle des fichiers locaux avant depot
#   2. depot de la livraison telle quelle en RAW (hdfs dfs -put)
#   3. rangement par table et par lot en BRONZE (hdfs dfs -cp)
#   4. manifeste de depot (local + HDFS logs/ingestion), RAW passe en lecture seule
# Usage : etape4_depot.sh <dossier_kit_contenant_data_et_data_refresh> <dossier_preuves>
set -uo pipefail
export HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop PATH=$PATH:/opt/hadoop/bin
SRC=$1; OUT=$2; mkdir -p "$OUT"
# hdfs dfs -put refuse les chemins locaux contenant un espace (1er essai en echec,
# "No such file or directory") : on passe par un lien symbolique sans espace vers le kit.
KIT=$HOME/kit_tpcxbb; ln -sfn "$SRC" "$KIT"
LAKE=/retail-lake
J=$OUT/journal_depot.txt
STAMP=$(date '+%Y%m%d_%H%M%S')
{ echo "# UC-03 Etape 4 - Depot des donnees TPCx-BB"
  echo "# Machine : $(hostname) ($(lsb_release -ds)), utilisateur Linux : $(whoami), Hadoop $(hadoop version | head -1 | cut -d' ' -f2)"
  echo "# Source locale : $SRC"; echo; } > "$J"
n=0
run() { n=$((n+1)); local titre=$1; shift
  { echo "=================================================================="
    printf '[%02d] %s\n' $n "$titre"; echo "Date : $(date '+%Y-%m-%d %H:%M:%S')"; echo "\$ $*"
    echo "------------------------------------------------------------------"
    bash -c "$*" 2>&1 | grep -v 'WARN util.NativeCodeLoader'; echo "(code retour : ${PIPESTATUS[0]})"; echo; } >> "$J"; }
export SRC LAKE OUT KIT

lot_of() { [ "$1" = data ] && echo initial || echo refresh; }

# ---------- 1. Controle avant depot ----------
AV=$OUT/controle_avant_depot.csv
echo "lot;table;fichier;chemin_local;lisible;taille_octets;lignes;colonnes_1re_ligne;encodage;sha256" > "$AV"
for jeu in data data_refresh; do
  for f in "$SRC/$jeu"/*/*; do
    t=$(basename "$(dirname "$f")"); b=$(basename "$f")
    [ -r "$f" ] && lis=oui || lis=non
    taille=$(stat -c %s "$f")
    lignes=$(awk 'END{print NR}' "$f")
    if [[ $b == *.csv ]]; then sep=','; else sep='|'; fi
    cols=$(head -1 "$f" | awk -F"$sep" '{print NF}')
    enc=$(file -b --mime-encoding "$f")
    sha=$(sha256sum "$f" | cut -d' ' -f1)
    echo "$(lot_of $jeu);$t;$b;$jeu/$t/$b;$lis;$taille;$lignes;$cols;$enc;$sha" >> "$AV"
  done
done
run "Controle avant depot : nombre de fichiers, lisibilite, taille totale" \
  "tail -n +2 '$AV' | awk -F';' '{n++; if(\$5==\"oui\") r++; s+=\$6} END{printf \"%d fichiers, %d lisibles, %d octets\\n\", n, r, s}'"
run "Exemple detaille avant depot : store_sales.dat (lot initial)" \
  "f=\"\$SRC/data/store_sales/store_sales.dat\"; ls -l \"\$f\"; file \"\$f\"; echo; echo 'Deux premieres lignes :'; head -2 \"\$f\"; echo; echo -n 'Colonnes (separateur |) : '; head -1 \"\$f\" | awk -F'|' '{print NF}'; echo -n 'Lignes : '; awk 'END{print NR}' \"\$f\""

# ---------- 2. Depot RAW ----------
run "Lien symbolique vers le kit (chemin sans espace pour Hadoop)" "ls -l \"\$KIT\"; ls \"\$KIT\""
run "Etat de RAW avant depot (droits d'ecriture du proprietaire retablis apres le 1er essai)" "hdfs dfs -chmod -R u+w $LAKE/raw/tpcxbb && hdfs dfs -ls -R $LAKE/raw"
for jeu in data data_refresh; do
  lot=$(lot_of $jeu)
  run "Depot RAW du lot $lot : copie de tout le dossier $jeu du kit" \
    "if hdfs dfs -test -e $LAKE/raw/tpcxbb/$lot/store_sales; then echo 'deja depose : rien a faire'; else hdfs dfs -put \$KIT/$jeu/* $LAKE/raw/tpcxbb/$lot/; fi"
done

# ---------- 3. Rangement BRONZE ----------
for jeu in data data_refresh; do
  lot=$(lot_of $jeu)
  tables=$(ls "$SRC/$jeu")
  mk=""; for t in $tables; do mk="$mk $LAKE/bronze/tpcxbb/$t/lot=$lot"; done
  run "Creation des dossiers BRONZE du lot $lot ($(echo $tables | wc -w) tables)" "hdfs dfs -mkdir -p$mk"
  for t in $tables; do
    d=$LAKE/bronze/tpcxbb/$t/lot=$lot
    run "BRONZE : $t, lot $lot" \
      "if [ \$(hdfs dfs -count $d | awk '{print \$2}') -gt 0 ]; then echo 'deja range : rien a faire'; else hdfs dfs -cp '$LAKE/raw/tpcxbb/$lot/$t/*' $d/; fi"
  done
done

# ---------- 4. Manifeste ----------
MF=$OUT/manifeste_depot_$STAMP.csv
echo "lot;table;fichier;chemin_local;chemin_raw;chemin_bronze;taille_octets;lignes;sha256;date_depot;utilisateur;methode" > "$MF"
tail -n +2 "$AV" | while IFS=';' read -r lot t b loc lis taille lignes cols enc sha; do
  echo "$lot;$t;$b;$loc;$LAKE/raw/tpcxbb/$lot/$t/$b;$LAKE/bronze/tpcxbb/$t/lot=$lot/$b;$taille;$lignes;$sha;$(date '+%Y-%m-%d %H:%M:%S');$(whoami);hdfs dfs -put puis hdfs dfs -cp"
done >> "$MF"
run "Enregistrement du manifeste dans HDFS (logs/ingestion)" \
  "hdfs dfs -put - $LAKE/logs/ingestion/$(basename "$MF") < '$MF' && hdfs dfs -ls $LAKE/logs/ingestion"

# ---------- 5. RAW en lecture seule ----------
run "RAW en lecture seule : retrait du droit d'ecriture" "hdfs dfs -chmod -R a-w $LAKE/raw/tpcxbb && hdfs dfs -ls -d $LAKE/raw/tpcxbb $LAKE/raw/tpcxbb/initial $LAKE/raw/tpcxbb/initial/store_sales && hdfs dfs -ls $LAKE/raw/tpcxbb/initial/store_sales"
echo "$n commandes journalisees"
