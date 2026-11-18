"""Construit UC03_inventaire_depot.xlsx (inventaire du Jour 1 mis a jour avec le depot HDFS).
Usage: python -I uc03_inventaire.py inventaire.json notes.json dossier_etape4 sortie.xlsx"""
import sys, json, csv, glob, os
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

inv = {(i['jeu'], i['fichier']): i for i in json.load(open(sys.argv[1], encoding='utf-8'))['infos']}
notes = json.load(open(sys.argv[2], encoding='utf-8'))
d4, out = sys.argv[3], sys.argv[4]
mf_path = sorted(glob.glob(os.path.join(d4, 'manifeste_depot_*.csv')))[-1]
rd = lambda p: list(csv.DictReader(open(p, encoding='utf-8'), delimiter=';'))
mf, av, ve = rd(mf_path), rd(os.path.join(d4, 'controle_avant_depot.csv')), rd(os.path.join(d4, 'verification_depot.csv'))
avk = {(r['lot'], r['fichier']): r for r in av}
vek = {(r['lot'], r['fichier']): r for r in ve}

HEAD = PatternFill('solid', fgColor='1F3864'); HF = Font(bold=True, color='FFFFFF', name='Arial', size=10)
BODY = Font(name='Arial', size=10); thin = Side(style='thin', color='BFBFBF')
BORDER = Border(top=thin, bottom=thin, left=thin, right=thin)
WARN = PatternFill('solid', fgColor='FCE4D6')
wb = Workbook()

def sheet(title, headers, rows, widths, first=False, warn=None):
    ws = wb.active if first else wb.create_sheet()
    ws.title = title
    ws.append(headers)
    for r in rows: ws.append(r)
    for c in ws[1]:
        c.fill = HEAD; c.font = HF; c.alignment = Alignment(wrap_text=True, vertical='center'); c.border = BORDER
    for row in ws.iter_rows(min_row=2):
        for c in row:
            c.font = BODY; c.alignment = Alignment(wrap_text=True, vertical='top'); c.border = BORDER
        if warn and warn([c.value for c in row]):
            for c in row: c.fill = WARN
    for i, w in enumerate(widths, 1): ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = 'A2'; ws.auto_filter.ref = ws.dimensions
    return ws

# 1. Matrice d'inventaire (section 8.3 du TP), completee du depot
rows = []
for m in mf:
    jeu = 'data' if m['lot'] == 'initial' else 'data_refresh'
    nom = m['fichier'].rsplit('.', 1)[0]
    i = inv.get((jeu, nom)) or inv.get((jeu, m['fichier']), {})
    a, v = avk[(m['lot'], m['fichier'])], vek[(m['lot'], m['fichier'])]
    fmt = 'CSV (virgule), avec en-tête' if m['fichier'].endswith('.csv') else 'Texte délimité « | », sans en-tête'
    rem = notes['remarques'].get(f"{jeu}/{nom}", notes['remarques'].get(nom, ''))
    rows.append([m['fichier'], m['lot'], m['table'], i.get('domaine', ''), fmt, a['encodage'], int(m['taille_octets']),
                 round(int(m['taille_octets']) / 1048576, 2), int(m['lignes']), int(a['colonnes_1re_ligne']), a['lisible'],
                 m['chemin_local'], m['chemin_raw'], m['chemin_bronze'], m['sha256'], m['date_depot'], m['utilisateur'],
                 v['tailles_identiques'], v['sha256_local_inchange'], 'TPCx-BB Tools 1.7.0 (Cours 2, slide 18), kit fourni par le formateur', rem])
sheet('Inventaire et dépôt', ['Fichier', 'Lot', 'Table', 'Domaine', 'Format', 'Encodage', 'Taille (octets)', 'Taille (Mo)', 'Lignes',
      'Colonnes (1re ligne)', 'Lisible', 'Chemin local (dans retail_project/)', 'Chemin HDFS RAW', 'Chemin HDFS BRONZE', 'SHA-256 (local)',
      'Date du dépôt', 'Déposé par', 'Taille HDFS = locale', 'Source locale inchangée', 'Provenance', 'Observations (Jour 1)'],
      rows, [26, 9, 22, 18, 26, 10, 14, 10, 11, 10, 8, 44, 58, 62, 30, 18, 10, 11, 11, 30, 60], first=True,
      warn=lambda r: r[17] != 'oui' or r[18] != 'oui')

# 2. Synthese par lot
syn = []
for lot in ('initial', 'refresh'):
    sel = [r for r in rows if r[1] == lot]
    syn.append([lot, len(sel), sum(r[6] for r in sel), round(sum(r[6] for r in sel) / 1048576, 1), sum(r[8] for r in sel),
                sum(1 for r in sel if r[17] == 'oui'), sum(1 for r in sel if r[18] == 'oui')])
syn.append(['Total', sum(s[1] for s in syn), sum(s[2] for s in syn), round(sum(s[2] for s in syn) / 1048576, 1), sum(s[4] for s in syn),
            sum(s[5] for s in syn), sum(s[6] for s in syn)])
sheet('Synthèse par lot', ['Lot', 'Fichiers', 'Octets', 'Mo', 'Lignes', 'Tailles identiques dans HDFS', 'Sources locales inchangées'],
      syn, [12, 10, 16, 10, 14, 18, 18])

# 3. Manifeste brut
sheet('Manifeste', list(mf[0].keys()), [list(r.values()) for r in mf], [9, 22, 26, 44, 58, 62, 14, 11, 66, 18, 10, 28])
wb.save(out); print('ok', out, len(rows), 'fichiers', os.path.basename(mf_path))
