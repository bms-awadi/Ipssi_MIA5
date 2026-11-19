export PATH=$PATH:/opt/hadoop/bin
jps | grep -qw NameNode || /opt/hadoop/sbin/start-dfs.sh >/dev/null 2>&1; hdfs dfsadmin -safemode wait >/dev/null 2>&1
hdfs dfs -setrep -w 1 /retail-lake/logs/traitements 2>/dev/null | tail -1
for z in silver quality/rejets logs/traitements; do echo "--- $z"; hdfs fsck /retail-lake/$z 2>/dev/null | grep -E 'Under-replicated blocks|Under replicated|Default replication|Average block replication|Status'; done
echo; hdfs dfs -du -s -h /retail-lake/silver 2>/dev/null
