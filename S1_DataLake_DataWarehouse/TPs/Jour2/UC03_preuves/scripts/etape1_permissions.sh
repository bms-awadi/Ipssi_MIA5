#!/usr/bin/env bash
export PATH=$PATH:/opt/hadoop/bin
{ echo "# Date      : $(date '+%Y-%m-%d %H:%M:%S %Z')"; echo "# Machine   : $(hostname) ($(lsb_release -ds))"; echo "# Utilisateur : $(whoami)"; echo
for k in dfs.permissions.enabled dfs.permissions.superusergroup hadoop.security.authentication dfs.namenode.checkpoint.period; do printf '%-34s %s\n' $k "$(hdfs getconf -confKey $k)"; done
echo; echo '$ hdfs dfs -ls -d /'; hdfs dfs -ls -d / 2>&1; } > "$1/10_permissions.txt"
