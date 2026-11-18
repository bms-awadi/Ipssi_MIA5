#!/usr/bin/env bash
cd ~/kit_tpcxbb
for f in */*/*; do
  tot=$(wc -l < "$f"); cr=$(tr -cd '\r' < "$f" | wc -c)
  echo "$f;$tot;$cr"
done | awk -F';' '{ if ($3==0) z++; else if ($3>=$2) a++; else p++; print } END {print "sans CR:",z+0," tout CRLF:",a+0," partiel:",p+0}'
