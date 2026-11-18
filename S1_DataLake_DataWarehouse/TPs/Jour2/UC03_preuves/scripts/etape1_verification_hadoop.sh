#!/usr/bin/env bash
# UC-03 Etape 1 : verification de l'environnement Hadoop. Chaque sortie est datee.
export HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop PATH=$PATH:/opt/hadoop/bin:/opt/hadoop/sbin
OUT=$1; mkdir -p "$OUT"
run() { # fichier, commande
  local f=$OUT/$1; shift
  { echo "# Date      : $(date '+%Y-%m-%d %H:%M:%S %Z')"; echo "# Machine   : $(hostname) ($(lsb_release -ds))"; echo "# Utilisateur : $(whoami)"; echo "\$ $*"; echo; bash -c "$*" 2>&1; } > "$f"
}
run 01_hadoop_version.txt "hadoop version"
run 02_jps.txt "jps"
run 03_dfsadmin_report.txt "hdfs dfsadmin -report"
run 04_safemode.txt "hdfs dfsadmin -safemode get"
run 05_configuration.txt "echo '--- core-site.xml'; cat \$HADOOP_CONF_DIR/core-site.xml; echo; echo '--- hdfs-site.xml'; cat \$HADOOP_CONF_DIR/hdfs-site.xml; echo; echo '--- JAVA_HOME (hadoop-env.sh)'; grep '^export JAVA_HOME' \$HADOOP_CONF_DIR/hadoop-env.sh; echo; echo '--- valeurs effectives'; for k in fs.defaultFS dfs.replication dfs.blocksize dfs.namenode.name.dir dfs.datanode.data.dir dfs.namenode.http-address dfs.datanode.address; do printf '%-28s %s\n' \$k \"\$(hdfs getconf -confKey \$k)\"; done; echo; echo '--- NameNode(s) declares'; hdfs getconf -namenodes; echo '--- SecondaryNameNode'; hdfs getconf -secondaryNameNodes"
run 06_ports.txt "ss -ltnp 2>/dev/null | grep -E ':(9000|9870|9864|9866|9867|9868)\b' | awk '{print \$4, \$6}'"
run 07_installation.txt "echo HADOOP_HOME=\$HADOOP_HOME; echo HADOOP_CONF_DIR=\$HADOOP_CONF_DIR; ls \$HADOOP_HOME; echo; echo '--- Java'; readlink -f /usr/bin/java; ls -d /usr/lib/jvm/*; echo; /usr/lib/jvm/java-17-openjdk-amd64/bin/java -version; /usr/lib/jvm/java-21-openjdk-amd64/bin/java -version; echo; echo '--- Python'; python3 --version; echo; echo '--- NiFi'; ls /opt/nifi/lib | grep -m1 -oE 'nifi-api-[0-9.]+' ; echo; echo '--- Systeme'; uname -srm; nproc; free -h | head -2; df -h \$HOME | tail -1"
run 08_journaux_erreurs.txt "for f in \$HADOOP_HOME/logs/*.log; do echo \"--- \$(basename \$f)\"; grep -cE ' (ERROR|FATAL) ' \$f | sed 's/^/lignes ERROR ou FATAL : /'; grep -E ' (WARN|ERROR|FATAL) ' \$f | cut -c1-200 | sort -u -k5 | head -5; done"
run 09_namenode_web.txt "curl -s 'http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus' ; echo; curl -s 'http://localhost:9870/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState' | grep -E 'NumLiveDataNodes|NumDeadDataNodes|FSState|CapacityTotal\"|BlocksTotal|FilesTotal'"
ls -1 "$OUT"
