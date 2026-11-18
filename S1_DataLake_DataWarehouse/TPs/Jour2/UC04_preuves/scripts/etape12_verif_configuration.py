import gzip, json
f = json.load(gzip.open('/opt/nifi/conf/flow.json.gz'))
g = [x for x in f['rootGroup']['processGroups'] if x['name'].startswith('UC04')][0]
ids = {p['identifier']: p['name'] for p in g['processors']}
for p in g['processors']:
    props = {k: v for k, v in p['properties'].items() if v is not None and k in ('Input Directory', 'Directory', 'Conflict Resolution Strategy', 'Routing Strategy', 'Keep Source File', 'dat', 'source', 'lot', 'table', 'date_ingestion', 'flux')}
    print(f"{p['name']:<18} {p['type'].rsplit('.',1)[-1]:<18} {p['scheduledState']:<9} run={p['schedulingPeriod']:<7} auto={p.get('autoTerminatedRelationships', [])}")
    for k, v in props.items(): print(f"      {k} = {v}")
for c in g.get('connections', []):
    print(f"  {ids.get(c['source']['id'], c['source']['name'])} -[{','.join(c['selectedRelationships'])}]-> {ids.get(c['destination']['id'], c['destination']['name'])}")
