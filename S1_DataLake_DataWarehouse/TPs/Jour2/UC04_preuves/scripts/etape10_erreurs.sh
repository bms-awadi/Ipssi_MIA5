#!/usr/bin/env bash
# UC-04 Etape 10 : erreurs controlees.
#   test 1 : dossier source inexistant dans GetFile (manipulation dans l'interface)
#   test 2 : nouvel envoi de store.dat alors qu'il existe deja dans HDFS (strategie "fail" de PutHDFS)
# Usage : etape10_erreurs.sh avant|apres <dossier_preuves>
set -uo pipefail
export PATH=$PATH:/opt/hadoop/bin
PHASE=$1; OUT=$2; mkdir -p "$OUT"; KIT=$HOME/kit_tpcxbb; DEST=/retail-lake/raw/nifi
export KIT DEST
J=$OUT/journal_erreurs_$PHASE.txt
{ echo "# UC-04 Etape 10 - Erreurs controlees ($PHASE)"; echo "# Machine : $(hostname), utilisateur Linux : $(whoami)"; echo; } > "$J"
n=0
run() { n=$((n+1)); local titre=$1; shift
  { echo "=================================================================="
    printf '[%02d] %s\n' $n "$titre"; echo "Date : $(date '+%Y-%m-%d %H:%M:%S')"; echo "\$ $*"
    echo "------------------------------------------------------------------"
    bash -c "$*" 2>&1 | grep -v 'WARN util.NativeCodeLoader'; echo "(code retour : ${PIPESTATUS[0]})"; echo; } >> "$J"; }
if [ "$PHASE" = avant ]; then
  date '+%Y-%m-%d %H:%M:%S' > "$OUT/.debut_tests"
  run "Test 1 : le dossier qui sera indique dans GetFile n'existe pas" "ls -ld /data/nifi/dossier_inexistant"
  run "Test 2 : fichier deja present dans HDFS (depose a l'etape 9)" "hdfs dfs -ls $DEST; hdfs dfs -cat $DEST/store.dat | sha256sum"
  run "Test 2 : nouvelle copie de travail du meme fichier dans le dossier d'entree" "cp \$KIT/data/store/store.dat /data/nifi/input/ && ls -l /data/nifi/input"
else
  DEB=$(cat "$OUT/.debut_tests" 2>/dev/null || echo '2026-11-17 16:00:00')
  run "Erreurs et avertissements de NiFi depuis le debut des tests ($DEB)" \
    "cat /opt/nifi/logs/nifi-app*.log | awk -v d='$DEB' 'substr(\$0,1,19) >= d' | grep -E ' (ERROR|WARN) ' | grep -vi password | cut -c1-400"
  run "Attributs du FlowFile en echec journalises par LogAttribute" \
    "cat /opt/nifi/logs/nifi-app*.log | awk -v d='$DEB' 'substr(\$0,1,19) >= d' | grep -A 30 'LogAttribute\\[' | grep -E 'LogAttribute|filename|path|absolute.path|uuid|file.size|Key:|Value:|---' | head -40 | cut -c1-200"
  run "Dossier d'entree : la copie a ete lue par GetFile" "ls -la /data/nifi/input; echo -n 'fichiers restants : '; find /data/nifi/input -type f | wc -l"
  run "HDFS : le fichier de l'etape 9 n'a pas ete ecrase (meme date, meme empreinte)" "hdfs dfs -ls $DEST; hdfs dfs -stat 'ecrit=%y' $DEST/store.dat; hdfs dfs -cat $DEST/store.dat | sha256sum"
  run "GetFile a retrouve son dossier d'origine (configuration enregistree)" \
    "python3 -c \"import gzip,json; f=json.load(gzip.open('/opt/nifi/conf/flow.json.gz')); g=[x for x in f['rootGroup']['processGroups'] if x['name'].startswith('UC04')][0]; [print(p['name'], p['scheduledState'], p['properties'].get('Input Directory') or '') for p in g['processors']]\""
fi
echo "$n commandes journalisees"
