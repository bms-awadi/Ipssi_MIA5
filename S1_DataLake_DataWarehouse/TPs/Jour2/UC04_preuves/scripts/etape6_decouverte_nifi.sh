#!/usr/bin/env bash
# UC-04 Etape 6 : decouverte de l'environnement NiFi (sans identifiants : aucun secret n'est affiche).
# Usage : etape6_decouverte_nifi.sh <dossier_preuves>
set -uo pipefail
OUT=$1; mkdir -p "$OUT"; N=/opt/nifi; P=$N/conf/nifi.properties
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 N P
J=$OUT/journal_decouverte_nifi.txt
{ echo "# UC-04 Etape 6 - Decouverte de l'environnement NiFi"
  echo "# Machine : $(hostname) ($(lsb_release -ds)), utilisateur Linux : $(whoami)"
  echo "# Aucun mot de passe ni cle n'est affiche dans ce journal."; echo; } > "$J"
n=0
run() { n=$((n+1)); local titre=$1; shift
  { echo "=================================================================="
    printf '[%02d] %s\n' $n "$titre"; echo "Date : $(date '+%Y-%m-%d %H:%M:%S')"; echo "\$ $*"
    echo "------------------------------------------------------------------"
    bash -c "$*" 2>&1; echo "(code retour : ${PIPESTATUS[0]})"; echo; } >> "$J"; }

run "Version de NiFi" "ls \$N/lib | grep -E '^nifi-(api|framework-api|runtime)-' ; grep -m1 -oE 'NiFi [0-9.]+|Apache NiFi[^,]*2\.[0-9.]+' \$N/logs/nifi-app.log | head -1"
run "Etat du service (Java 21)" "\$N/bin/nifi.sh status 2>&1 | grep -E 'Status|Command Status' | sed 's/^.*INFO \[main\] //'; echo; jps -l | grep -iE 'nifi|bootstrap'"
run "Demarrage de l'application (journal)" "cat \$N/logs/nifi-app*.log | grep -E 'Started Application|Flow Controller started' | tail -2 | cut -c1-160"
run "Memoire allouee a NiFi (bootstrap.conf)" "grep -E '^java.arg.(2|3)=|preferIPv4' \$N/conf/bootstrap.conf"
run "Adresse d'acces a l'interface" "grep -E '^nifi.web.https?.(host|port)=' \$P; echo; ss -ltnp 2>/dev/null | grep -E ':8443' | awk '{print \$4}'; curl -sk -o /dev/null -w 'https://localhost:8443/nifi -> HTTP %{http_code}\n' https://localhost:8443/nifi/"
run "Securite : HTTPS, certificat et authentification" "grep -E '^nifi.security.(keystoreType|truststoreType)=|^nifi.security.user.(login.identity.provider|authorizer)=|^nifi.web.https.network.interface' \$P; echo; ls -l \$N/conf | grep -E 'keystore|truststore|login-identity|authorizers'; echo; echo 'Certificat du serveur :'; echo | openssl s_client -connect localhost:8443 2>/dev/null | openssl x509 -noout -subject -issuer -dates 2>/dev/null"
run "Securite : fournisseur d'identite configure" "grep -oE '<identifier>[^<]+</identifier>|<class>[^<]+</class>' \$N/conf/login-identity-providers.xml | head -4; grep -oE '<class>[^<]+</class>' \$N/conf/authorizers.xml | sort -u"
run "Depots internes de NiFi (FlowFile, contenu, provenance)" "grep -E '^nifi.(flowfile|content|provenance).repository.(implementation|directory[^=]*|max.storage.time|max.storage.size)=' \$P; echo; du -sh \$N/*_repository 2>/dev/null"
run "Flux enregistre (fichier de configuration du canvas)" "grep -E '^nifi.flow.configuration.file=' \$P; ls -l \$N/conf/flow.json.gz 2>/dev/null"
run "Modules (NAR) charges" "ls \$N/lib/*.nar | wc -l | sed 's/^/NAR dans lib : /'; ls \$N/extensions/*.nar 2>/dev/null | wc -l | sed 's/^/NAR ajoutes dans extensions : /'; ls \$N/extensions"

# Inventaire des composants a partir des manifestes des NAR deplies
CSV=$OUT/composants_nifi.csv
python3 - "$CSV" $N/work/nar/*/*-unpacked/META-INF/docs/extension-manifest.xml <<'PY'
import sys, re, xml.etree.ElementTree as ET
rows = set()
for m in sys.argv[2:]:
    nar = re.sub(r'.*/([^/]+)\.nar-unpacked/.*', r'\1', m)
    for e in ET.parse(m).getroot().iter('extension'):
        n, t = e.findtext('name'), e.findtext('type')
        if n and t: rows.add((t, n, nar))
with open(sys.argv[1], 'w') as f:
    f.write('type;composant;module\n')
    for r in sorted(rows): f.write(';'.join(r) + '\n')
PY
run "Composants disponibles par type (lus dans les manifestes des $(ls $N/work/nar/*/*-unpacked/META-INF/docs/extension-manifest.xml | wc -l) modules)" "tail -n +2 '$CSV' | cut -d';' -f1 | sort | uniq -c | sort -rn"
run "Processeurs utiles au TP : presents ?" "for p in GetFile PutHDFS LogAttribute UpdateAttribute RouteOnAttribute ListFile FetchFile PutFile GetHDFS ListHDFS FetchHDFS; do printf '%-18s ' \$p; grep -E '^PROCESSOR;org\.apache\.nifi\.processors\.[a-z.]+\.'\$p';' '$CSV' | head -1 | cut -d';' -f3 | sed 's/^/present, module /' | grep . || echo ABSENT; done"

run "Services necessaires a l'ingestion vers HDFS" "echo -n 'NameNode localhost:9000 : '; (exec 3<>/dev/tcp/127.0.0.1/9000) 2>/dev/null && echo joignable || echo INJOIGNABLE; echo -n 'core-site.xml lisible par abedja : '; [ -r /opt/hadoop/etc/hadoop/core-site.xml ] && echo oui || echo non; echo -n 'hdfs-site.xml lisible par abedja : '; [ -r /opt/hadoop/etc/hadoop/hdfs-site.xml ] && echo oui || echo non; echo -n 'Utilisateur qui execute NiFi : '; ps -o user= -p \$(pgrep -f 'org.apache.nifi.NiFi' | head -1); echo -n 'Destination HDFS /retail-lake/raw/nifi : '; /opt/hadoop/bin/hdfs dfs -ls -d /retail-lake/raw/nifi 2>/dev/null | awk '{print \$1, \$3, \$8}'"
echo "$n commandes journalisees"
