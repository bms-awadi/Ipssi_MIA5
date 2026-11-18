"""Lit les blocs ecrits par LogAttribute depuis une date et produit le journal d'ingestion (CSV).
Usage : python3 journal_ingestion.py "AAAA-MM-JJ HH:MM:SS" sortie.csv"""
import glob, re, sys, csv
debut, out = sys.argv[1], sys.argv[2]
lines = []
for f in sorted(glob.glob('/opt/nifi/logs/nifi-app*.log')): lines += open(f, encoding='utf-8', errors='replace').read().splitlines()
garder = ['filename', 'table', 'lot', 'source', 'flux', 'date_ingestion', 'RouteOnAttribute.Route', 'absolute.path', 'absolute.hdfs.path', 'fileSize', 'uuid']
blocs, cur, key = [], None, None
for l in lines:
    m = re.match(r'(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)', l)
    if m and 'LogAttribute' in l and 'logging for FlowFile' in l:
        cur = {'heure_log': m.group(1)} if m.group(1) >= debut else None
        if cur is not None: blocs.append(cur)
        continue
    if cur is None: continue
    k = re.match(r"Key: '(.+)'$", l.strip())
    v = re.match(r"Value: '(.*)'$", l.strip())
    if k: key = k.group(1)
    elif v and key: cur[key] = v.group(1); key = None
    elif m: cur = None
with open(out, 'w', newline='') as fh:
    w = csv.writer(fh, delimiter=';'); w.writerow(['heure_log'] + garder)
    for b in blocs: w.writerow([b.get('heure_log')] + [b.get(k, '') for k in garder])
for b in blocs:
    print(f"{b.get('heure_log')} | {b.get('filename'):<28} | route={b.get('RouteOnAttribute.Route', ''):<9} | table={b.get('table')} lot={b.get('lot')} source={b.get('source')} | date_ingestion={b.get('date_ingestion')} | hdfs={b.get('absolute.hdfs.path', '-')}")
