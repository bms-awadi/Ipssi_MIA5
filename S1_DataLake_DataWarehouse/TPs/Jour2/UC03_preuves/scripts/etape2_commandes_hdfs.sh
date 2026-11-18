#!/usr/bin/env bash
# UC-03 Etape 2 : decouverte des commandes HDFS. Journal date avec code retour de chaque commande.
export HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop PATH=$PATH:/opt/hadoop/bin
J=$1/journal_commandes_hdfs.txt
{ echo "# UC-03 Etape 2 - Journal des commandes HDFS"
  echo "# Machine : $(hostname) ($(lsb_release -ds)), utilisateur Linux : $(whoami), Hadoop $(hadoop version | head -1 | cut -d' ' -f2)"
  echo; } > "$J"
n=0
run() { # titre, commande
  n=$((n+1)); local titre=$1; shift
  { echo "=================================================================="
    printf '[%02d] %s\n' $n "$titre"
    echo "Date : $(date '+%Y-%m-%d %H:%M:%S')"
    echo "\$ $*"
    echo "------------------------------------------------------------------"
    bash -c "$*" 2>&1 | grep -v 'WARN util.NativeCodeLoader'; echo "(code retour : ${PIPESTATUS[0]})"; echo; } >> "$J"
}
# 1. Etat du systeme HDFS
run "Etat general du systeme HDFS" "hdfs dfsadmin -report | sed -n '1,8p'"
run "Mode sans echec" "hdfs dfsadmin -safemode get"
run "Controle de coherence du systeme de fichiers" "hdfs fsck / | tail -n 14"
# 2. Informations sur les noeuds
run "DataNodes vivants" "hdfs dfsadmin -report -live | sed -n '/Live datanodes/,\$p' | sed -n '1,16p'"
run "DataNodes morts" "hdfs dfsadmin -report -dead | grep -E 'Dead datanodes'"
run "Topologie (rack de chaque DataNode)" "hdfs dfsadmin -printTopology"
run "Adresse du NameNode" "hdfs getconf -namenodes"
# 3. Lister les repertoires
run "Contenu de la racine (avant creation)" "hdfs dfs -ls /"
# 4. Creer un repertoire de travail
run "Creation du repertoire de travail du projet" "hdfs dfs -mkdir -p /retail-lake"
run "Contenu de la racine (apres creation)" "hdfs dfs -ls /"
run "Contenu du repertoire de travail" "hdfs dfs -ls /retail-lake"
# 5. Permissions
run "Proprietaire et droits du repertoire de travail" "hdfs dfs -ls -d /retail-lake"
run "Detail des droits (ACL)" "hdfs dfs -getfacl /retail-lake"
run "Statistiques du repertoire (type, droits, proprietaire, date)" "hdfs dfs -stat 'type=%F droits=%A proprietaire=%u groupe=%g modifie=%y' /retail-lake"
run "Test volontaire : ecriture par un autre utilisateur (doit etre refusee)" "HADOOP_USER_NAME=invite hdfs dfs -mkdir /retail-lake/test_invite"
run "Verification : le dossier de test n'a pas ete cree" "hdfs dfs -test -e /retail-lake/test_invite && echo 'present' || echo 'absent'"
# 6. Taille des donnees stockees
run "Taille du repertoire de travail" "hdfs dfs -du -h /retail-lake"
run "Taille totale (resume)" "hdfs dfs -du -s -h /retail-lake"
run "Nombre de dossiers, fichiers et octets" "hdfs dfs -count -h /retail-lake"
run "Espace disponible dans HDFS" "hdfs dfs -df -h /"
# Erreur volontaire de chemin
run "Test volontaire : chemin inexistant (doit echouer)" "hdfs dfs -ls /retail-lake/inexistant"
echo "$n commandes journalisees dans $J"
