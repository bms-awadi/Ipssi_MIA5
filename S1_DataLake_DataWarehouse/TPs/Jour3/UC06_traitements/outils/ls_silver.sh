export PATH=$PATH:/opt/hadoop/bin
jps | grep -qw NameNode || /opt/hadoop/sbin/start-dfs.sh >/dev/null 2>&1; hdfs dfsadmin -safemode wait >/dev/null 2>&1
hdfs dfs -ls -R /retail-lake/silver /retail-lake/quality/rejets /retail-lake/logs/traitements 2>/dev/null | awk '{print $5, $8}' | sed 's#/retail-lake/##' | grep -vE '^0 .*(_SUCCESS)$'
echo; hdfs dfs -du -s -h /retail-lake/silver /retail-lake/bronze 2>/dev/null
