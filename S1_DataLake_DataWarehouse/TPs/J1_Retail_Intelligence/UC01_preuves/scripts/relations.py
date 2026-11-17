"""Etape D : controle des relations. Usage: python -I relations.py <racine retail_project> <sortie.json>"""
import sys, os, json, collections

ROOT, OUT = sys.argv[1], sys.argv[2]

def path(jeu, t): return os.path.join(ROOT, jeu, t, t + '.dat')

def rows(jeu, t):
    p = path(jeu, t)
    if not os.path.exists(p): return
    with open(p, encoding='utf-8') as f:
        for line in f:
            yield line.rstrip('\r\n').split('|')

def keyset(t, col=0, jeux=('data', 'data_refresh')):
    s = set()
    for j in jeux:
        for r in rows(j, t): s.add(r[col])
    return s

# (enfant, col enfant 0-based, nom col enfant, parent, col parent, nom col parent, statut, commentaire)
REL = [
 ('store_sales', 0, 'ss_sold_date_sk', 'date_dim', 0, 'd_date_sk', ''),
 ('store_sales', 1, 'ss_sold_time_sk', 'time_dim', 0, 't_time_sk', ''),
 ('store_sales', 2, 'ss_item_sk', 'item', 0, 'i_item_sk', ''),
 ('store_sales', 3, 'ss_customer_sk', 'customer', 0, 'c_customer_sk', ''),
 ('store_sales', 4, 'ss_cdemo_sk', 'customer_demographics', 0, 'cd_demo_sk', ''),
 ('store_sales', 5, 'ss_hdemo_sk', 'household_demographics', 0, 'hd_demo_sk', ''),
 ('store_sales', 6, 'ss_addr_sk', 'customer_address', 0, 'ca_address_sk', ''),
 ('store_sales', 7, 'ss_store_sk', 'store', 0, 's_store_sk', ''),
 ('store_sales', 8, 'ss_promo_sk', 'promotion', 0, 'p_promo_sk', ''),
 ('web_sales', 0, 'ws_sold_date_sk', 'date_dim', 0, 'd_date_sk', ''),
 ('web_sales', 1, 'ws_sold_time_sk', 'time_dim', 0, 't_time_sk', ''),
 ('web_sales', 2, 'ws_ship_date_sk', 'date_dim', 0, 'd_date_sk', ''),
 ('web_sales', 3, 'ws_item_sk', 'item', 0, 'i_item_sk', ''),
 ('web_sales', 4, 'ws_bill_customer_sk', 'customer', 0, 'c_customer_sk', ''),
 ('web_sales', 12, 'ws_web_page_sk', 'web_page', 0, 'wp_web_page_sk', ''),
 ('web_sales', 13, 'ws_web_site_sk', 'web_site', 0, 'web_site_sk', ''),
 ('web_sales', 14, 'ws_ship_mode_sk', 'ship_mode', 0, 'sm_ship_mode_sk', ''),
 ('web_sales', 15, 'ws_warehouse_sk', 'warehouse', 0, 'w_warehouse_sk', ''),
 ('web_sales', 16, 'ws_promo_sk', 'promotion', 0, 'p_promo_sk', ''),
 ('store_returns', 0, 'sr_returned_date_sk', 'date_dim', 0, 'd_date_sk', ''),
 ('store_returns', 2, 'sr_item_sk', 'item', 0, 'i_item_sk', ''),
 ('store_returns', 3, 'sr_customer_sk', 'customer', 0, 'c_customer_sk', ''),
 ('store_returns', 7, 'sr_store_sk', 'store', 0, 's_store_sk', ''),
 ('store_returns', 8, 'sr_reason_sk', 'reason', 0, 'r_reason_sk', ''),
 ('web_returns', 0, 'wr_returned_date_sk', 'date_dim', 0, 'd_date_sk', ''),
 ('web_returns', 2, 'wr_item_sk', 'item', 0, 'i_item_sk', ''),
 ('web_returns', 3, 'wr_refunded_customer_sk', 'customer', 0, 'c_customer_sk', ''),
 ('web_returns', 12, 'wr_reason_sk', 'reason', 0, 'r_reason_sk', ''),
 ('inventory', 0, 'inv_date_sk', 'date_dim', 0, 'd_date_sk', ''),
 ('inventory', 1, 'inv_item_sk', 'item', 0, 'i_item_sk', ''),
 ('inventory', 2, 'inv_warehouse_sk', 'warehouse', 0, 'w_warehouse_sk', ''),
 ('web_clickstreams', 0, 'wcs_click_date_sk', 'date_dim', 0, 'd_date_sk', ''),
 ('web_clickstreams', 1, 'wcs_click_time_sk', 'time_dim', 0, 't_time_sk', ''),
 ('web_clickstreams', 3, 'wcs_item_sk', 'item', 0, 'i_item_sk', ''),
 ('web_clickstreams', 4, 'wcs_web_page_sk', 'web_page', 0, 'wp_web_page_sk', ''),
 ('web_clickstreams', 5, 'wcs_user_sk', 'customer', 0, 'c_customer_sk', ''),
 ('web_clickstreams', 2, 'wcs_sales_sk', 'web_sales', 17, 'ws_order_number', 'sens a confirmer'),
 ('product_reviews', 4, 'pr_item_sk', 'item', 0, 'i_item_sk', ''),
 ('product_reviews', 5, 'pr_user_sk', 'customer', 0, 'c_customer_sk', ''),
 ('product_reviews', 6, 'pr_order_sk', 'web_sales', 17, 'ws_order_number', 'sens a confirmer'),
 ('item_marketprices', 1, 'imp_item_sk', 'item', 0, 'i_item_sk', ''),
 ('promotion', 4, 'p_item_sk', 'item', 0, 'i_item_sk', ''),
 ('customer', 2, 'c_current_cdemo_sk', 'customer_demographics', 0, 'cd_demo_sk', ''),
 ('customer', 3, 'c_current_hdemo_sk', 'household_demographics', 0, 'hd_demo_sk', ''),
 ('customer', 4, 'c_current_addr_sk', 'customer_address', 0, 'ca_address_sk', ''),
 ('household_demographics', 1, 'hd_income_band_sk', 'income_band', 0, 'ib_income_band_sk', ''),
]

cache = {}
def parent(t, c):
    if (t, c) not in cache:
        cache[(t, c)] = keyset(t, c)
    return cache[(t, c)]

# group by child table to read each child file once per jeu
by_child = collections.defaultdict(list)
for r in REL: by_child[r[0]].append(r)

results = []
for child, rels in by_child.items():
    for jeu in ('data', 'data_refresh'):
        if not os.path.exists(path(jeu, child)): continue
        sets = [parent(r[3], r[4]) for r in rels]
        stats = [{'n': 0, 'null': 0, 'orph': 0, 'cnt': collections.Counter(), 'ex': ''} for _ in rels]
        for row in rows(jeu, child):
            for i, r in enumerate(rels):
                v = row[r[1]]; s = stats[i]; s['n'] += 1
                if v == '': s['null'] += 1; continue
                if v not in sets[i]:
                    s['orph'] += 1
                    if not s['ex']: s['ex'] = v
                s['cnt'][v] += 1
        for i, r in enumerate(rels):
            s = stats[i]; nn = s['n'] - s['null']
            used = len(s['cnt']); total_parent = len(sets[i])
            results.append({
                'jeu': jeu, 'enfant': child, 'colonne_enfant': r[2], 'parent': r[3], 'colonne_parent': r[5],
                'lignes': s['n'], 'vides': s['null'], 'pct_vides': round(100 * s['null'] / s['n'], 2) if s['n'] else 0,
                'orphelins': s['orph'], 'pct_orphelins': round(100 * s['orph'] / nn, 2) if nn else 0, 'exemple_orphelin': s['ex'],
                'cles_distinctes_enfant': used, 'cles_parent': total_parent,
                'pct_parent_utilise': round(100 * used / total_parent, 1) if total_parent else 0,
                'max_lignes_par_cle': max(s['cnt'].values()) if s['cnt'] else 0,
                'note': r[6],
            })
        print(jeu, child, 'ok', flush=True)

# ventes <-> retours (ticket/commande + produit), coherence quantite et date
def sales_index(t, kcol, icol, qcol, dcol):
    idx = {}
    for j in ('data', 'data_refresh'):
        for r in rows(j, t):
            k = (r[kcol], r[icol])
            q = int(r[qcol]) if r[qcol] else 0
            d = int(r[dcol]) if r[dcol] else None
            pq, pd = idx.get(k, (0, None))
            ds = [x for x in (pd, d) if x is not None]
            idx[k] = (pq + q, min(ds) if ds else None)
    return idx

retours = []
for (st, sk, si, sq, sd, rt, rk, ri, rq, rd) in [
        ('store_sales', 9, 2, 10, 0, 'store_returns', 9, 2, 10, 0),
        ('web_sales', 17, 3, 18, 0, 'web_returns', 13, 2, 14, 0)]:
    idx = sales_index(st, sk, si, sq, sd)
    for jeu in ('data', 'data_refresh'):
        n = orph = qty_gt = before = 0
        for r in rows(jeu, rt):
            n += 1
            k = (r[rk], r[ri])
            if k not in idx: orph += 1; continue
            q, d = idx[k]
            if r[rq] and int(r[rq]) > q: qty_gt += 1
            if r[rd] and d is not None and int(r[rd]) < d: before += 1
        retours.append({'jeu': jeu, 'retours': rt, 'ventes': st, 'lignes': n, 'sans_vente_origine': orph,
                        'qte_retour_sup_vendue': qty_gt, 'retour_avant_vente': before})
    print('retours', rt, 'ok', flush=True)

json.dump({'relations': results, 'retours': retours}, open(OUT, 'w', encoding='utf-8'), ensure_ascii=False)
print('done')
