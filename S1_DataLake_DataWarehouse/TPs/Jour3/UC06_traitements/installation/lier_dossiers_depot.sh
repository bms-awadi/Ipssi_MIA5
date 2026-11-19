#!/usr/bin/env bash
# Les scripts et notebooks du TP 3 vivent dans le depot Git (J3/), le dossier de travail WSL y pointe.
set -e
TP=$HOME/retail-intelligence/tp3
REPO="/mnt/c/xampp/htdocs/dashboard/Perso/Cours/Architecture DataLake, DataWarehouse, Lakehous/J3"
[ -f "$TP/scripts/test_spark.py" ] && [ ! -L "$TP/scripts" ] && cp "$TP/scripts/test_spark.py" "$REPO/UC06_traitements/00_test_spark.py"
[ -L "$TP/scripts" ] || { rm -rf "$TP/scripts"; ln -s "$REPO/UC06_traitements" "$TP/scripts"; }
[ -L "$TP/notebooks" ] || { rmdir "$TP/notebooks" 2>/dev/null || true; ln -s "$REPO/UC06_notebooks" "$TP/notebooks"; }
ls -la "$TP"
