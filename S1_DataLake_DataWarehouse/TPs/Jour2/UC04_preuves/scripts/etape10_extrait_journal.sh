#!/usr/bin/env bash
# Extrait complet de l'echec PutHDFS et du bloc LogAttribute (etape 10, test 2)
L=$(ls /opt/nifi/logs/nifi-app*.log)
echo "# Avertissement PutHDFS (ligne complete)"; cat $L | grep 'penalizing' | grep 'PutHDFS' | tail -1 | sed 's/claim=StandardContentClaim \[[^]]*\]\]*,//'
echo; echo "# Bloc ecrit par LogAttribute"
cat $L | awk '/LogAttribute.*logging for FlowFile/{p=1} p{print} p&&/^-{10,}$/&&++n==2{exit}' | sed -n '1,60p'
