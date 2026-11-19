#!/usr/bin/env bash
# Demarrage HDFS (section 1.8 du TP 2) : SSH, puis start-dfs.sh avec Java 17 (hadoop-env.sh)
export HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop PATH=$PATH:/opt/hadoop/bin:/opt/hadoop/sbin
systemctl is-active ssh
jps | grep -qw NameNode || start-dfs.sh 2>&1 | grep -v WARN
for i in $(seq 1 30); do hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF && break; sleep 2; done
jps | sort -k2
hdfs dfsadmin -safemode get 2>/dev/null
hdfs dfsadmin -report 2>/dev/null | grep -E 'Live datanodes'
hdfs dfs -count /retail-lake/raw/tpcxbb /retail-lake/bronze/tpcxbb 2>/dev/null
