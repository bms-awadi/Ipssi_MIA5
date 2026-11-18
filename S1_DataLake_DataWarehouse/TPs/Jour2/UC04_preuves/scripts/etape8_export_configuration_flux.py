"""Extrait la configuration du flux depuis /opt/nifi/conf/flow.json.gz (sans valeur sensible).
Usage : python3 export_flux.py <sortie.json>"""
import gzip, json, sys
flow = json.load(gzip.open('/opt/nifi/conf/flow.json.gz'))
root = flow['rootGroup']
def walk(g, path):
    yield path, g
    for c in g.get('processGroups', []): yield from walk(c, path + [c['name']])
out = {'source': '/opt/nifi/conf/flow.json.gz', 'groupes': []}
for path, g in walk(root, [root['name']]):
    if not g.get('processors'): continue
    names = {p['identifier']: p['name'] for p in g['processors']}
    out['groupes'].append({
        'groupe': ' / '.join(path),
        'processeurs': [{
            'nom': p['name'], 'type': p['type'].rsplit('.', 1)[-1], 'version': p['bundle']['version'], 'module': p['bundle']['artifact'],
            'etat': p['scheduledState'], 'planning': p['schedulingPeriod'],
            'relations_auto_terminees': p.get('autoTerminatedRelationships', []),
            'proprietes_renseignees': {k: v for k, v in p['properties'].items() if v is not None and k not in p.get('propertyDescriptors', {}) or (v is not None and not p['propertyDescriptors'].get(k, {}).get('sensitive'))},
        } for p in g['processors']],
        'connexions': [{'de': names.get(c['source']['id'], c['source']['name']), 'vers': names.get(c['destination']['id'], c['destination']['name']),
                        'relations': c['selectedRelationships'], 'seuil_objets': c['backPressureObjectThreshold'], 'seuil_taille': c['backPressureDataSizeThreshold']}
                       for c in g.get('connections', [])],
    })
json.dump(out, open(sys.argv[1], 'w'), indent=2, ensure_ascii=False)
for g in out['groupes']:
    print('Groupe :', g['groupe'])
    for p in g['processeurs']:
        print(f"  {p['nom']} ({p['type']} {p['version']}) etat={p['etat']} planning={p['planning']} auto-termine={p['relations_auto_terminees']}")
        for k in ('Input Directory', 'Keep Source File', 'Recurse Subdirectories', 'Hadoop Configuration Resources', 'Directory', 'Conflict Resolution Strategy', 'Writing Strategy'):
            if k in p['proprietes_renseignees']: print(f"      {k} = {p['proprietes_renseignees'][k]}")
    for c in g['connexions']: print(f"  connexion {c['de']} -> {c['vers']} {c['relations']} (seuils {c['seuil_objets']} objets / {c['seuil_taille']})")
