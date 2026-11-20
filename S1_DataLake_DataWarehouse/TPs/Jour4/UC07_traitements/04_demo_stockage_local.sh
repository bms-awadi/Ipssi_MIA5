#!/usr/bin/env bash
# UC-07 etape 4 (parcours B, local) : realiser sur notre HDFS les operations prevues sur ADLS Gen2.
#   1. identifier le stockage et ses zones   2. verifier les caracteristiques attendues (hierarchie, ACL)
#   3. creer l'organisation des repertoires  4. deposer un fichier de demonstration autorise
#   5. verifier qu'il est accessible et intact  6. operation sur un chemin (renommage d'un dossier)
#   7. permissions par ACL (moindre privilege)  8. relever les chemins, puis nettoyer
# Usage : ./04_demo_stockage_local.sh [dossier de demonstration HDFS]   (defaut : /retail-lake/_demo_uc07)
set -eo pipefail
export PATH=$PATH:/opt/hadoop/bin:/opt/hadoop/sbin
jps | grep -qw NameNode || { echo "HDFS arrete : demarrage"; start-dfs.sh >/dev/null 2>&1; }
hdfs dfsadmin -safemode wait 2>/dev/null | grep -v WARN
H() { hdfs "$@" 2>&1 | { grep -v WARN || true; }; }
REPO="/mnt/c/xampp/htdocs/dashboard/Perso/Cours/Architecture DataLake, DataWarehouse, Lakehous"
DEMO=${1:-/retail-lake/_demo_uc07}
FICHIER="$REPO/J3/UC06_preuves/etape7/kpi_ventes_magasin_mois.csv"   # export Gold de 720 lignes, sans donnee personnelle

echo "UC-07 etape 4 - demonstration locale (parcours B) : HDFS dans le role d'ADLS Gen2"
echo "Date d'execution : $(date '+%Y-%m-%d %H:%M:%S') | Hadoop $(hadoop version 2>/dev/null | head -1 | cut -d' ' -f2)"

echo; echo "=== 1. Stockage utilise et zones existantes (equivalent des conteneurs ADLS)"
H dfs -count -h /retail-lake/raw /retail-lake/bronze /retail-lake/silver /retail-lake/gold /retail-lake/quality /retail-lake/logs \
  | awk '{printf "  %-24s %6s dossiers %6s fichiers %8s %s\n", $5, $1, $2, $3, $4}'

echo; echo "=== 2. Caracteristiques attendues d'un stockage de type ADLS Gen2"
echo "  espace de noms hierarchique : natif dans HDFS (vrais dossiers, gere par le NameNode)"
echo "  ACL activees (dfs.namenode.acls.enabled) : $(hdfs getconf -confKey dfs.namenode.acls.enabled 2>/dev/null)"
echo "  replication par defaut (dfs.replication) : $(hdfs getconf -confKey dfs.replication 2>/dev/null)"

echo; echo "=== 3. Organisation des repertoires (modele du conteneur gold)"
H dfs -rm -r -f -skipTrash "$DEMO" >/dev/null || true
H dfs -mkdir -p "$DEMO/gold/indicateurs" "$DEMO/gold/analyses" "$DEMO/gouvernance/documentation"
H dfs -ls -R "$DEMO" | awk '{print "  " $1, $3, $4, $8}'

echo; echo "=== 4. Depot d'un fichier de demonstration autorise"
echo "  fichier : J3/UC06_preuves/etape7/kpi_ventes_magasin_mois.csv (indicateurs agreges, aucune donnee personnelle)"
# hdfs dfs -put refuse les chemins locaux contenant des espaces (constat du Jour 2) : copie temporaire
TMP=$(mktemp --suffix=.csv); cp "$FICHIER" "$TMP"
H dfs -put -f "$TMP" "$DEMO/gold/indicateurs/kpi_ventes_magasin_mois.csv"; rm -f "$TMP"
H dfs -ls "$DEMO/gold/indicateurs" | awk 'NF>=8 {print "  " $1, $5, "octets", $8}'

echo; echo "=== 5. Le fichier est-il accessible et intact ?"
LOC=$(wc -l < "$FICHIER"); DIST=$(hdfs dfs -cat "$DEMO/gold/indicateurs/kpi_ventes_magasin_mois.csv" 2>/dev/null | wc -l)
M1=$(md5sum < "$FICHIER" | cut -d' ' -f1); M2=$(hdfs dfs -cat "$DEMO/gold/indicateurs/kpi_ventes_magasin_mois.csv" 2>/dev/null | md5sum | cut -d' ' -f1)
echo "  lignes : local $LOC | relues dans HDFS $DIST"
echo "  empreinte MD5 : local $M1 | HDFS $M2 -> $([ "$M1" = "$M2" ] && echo identique || echo DIFFERENTE)"
echo "  premieres lignes relues :"; hdfs dfs -cat "$DEMO/gold/indicateurs/kpi_ventes_magasin_mois.csv" 2>/dev/null | head -3 | cut -c1-110 | sed 's/^/    /' || true

echo; echo "=== 6. Operation sur un chemin : renommage d'un dossier entier"
H dfs -mv "$DEMO/gold/indicateurs" "$DEMO/gold/indicateurs_archive"
echo "  gold/indicateurs renomme en gold/indicateurs_archive : une seule operation sur le NameNode (metadonnees),"
echo "  aucune copie des fichiers ; sans espace de noms hierarchique, il faudrait copier puis supprimer chaque objet"
echo "  apres renommage :"; H dfs -ls -R "$DEMO/gold" | awk '{print "    " $8}'
H dfs -mv "$DEMO/gold/indicateurs_archive" "$DEMO/gold/indicateurs"
echo "  apres retour au nom d'origine :"; H dfs -ls -R "$DEMO/gold" | awk '{print "    " $8}'

echo; echo "=== 7. Permissions par ACL (moindre privilege)"
H dfs -chmod -R 750 "$DEMO"
for p in "$DEMO" "$DEMO/gold" "$DEMO/gold/indicateurs"; do H dfs -setfacl -m group:analystes:r-x "$p"; done
H dfs -setfacl -m group:analystes:r-- "$DEMO/gold/indicateurs/kpi_ventes_magasin_mois.csv"
H dfs -setfacl -m default:group:analystes:r-x "$DEMO/gold/indicateurs"
echo "  regles posees : groupe analystes en lecture sur gold/indicateurs (et x sur les dossiers parents pour les parcourir) ;"
echo "  rien sur gouvernance/ ; les autres utilisateurs n'ont aucun droit (750)"
for p in "$DEMO/gold/indicateurs" "$DEMO/gouvernance/documentation"; do echo "  -- getfacl ${p#$DEMO/}"; H dfs -getfacl "$p" | sed 's/^/     /'; done

echo; echo "=== 8. Chemins releves et correspondance ADLS Gen2 (compte stretailmaxmag, a creer en parcours A)"
echo "  HDFS : hdfs://localhost:9000$DEMO/gold/indicateurs/kpi_ventes_magasin_mois.csv"
echo "  ADLS : abfss://gold@stretailmaxmag.dfs.core.windows.net/indicateurs/kpi_ventes_magasin_mois.csv"
H dfs -rm -r -skipTrash "$DEMO" | sed 's/^/  nettoyage : /'
echo "Fin de la demonstration : $(date '+%Y-%m-%d %H:%M:%S')"
