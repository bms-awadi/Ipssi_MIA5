"""Construit UC08_notebooks/TP4_Retail_Intelligence.ipynb (TP 4, UC-08).

Le notebook est reconstruit a chaque execution (run_notebook.sh), puis execute en entier :
toutes les cellules repartent des donnees Silver et Gold, le resultat est donc reproductible.
Usage : python construire_notebook.py <chemin du notebook> [derniere etape incluse]
Chaque etape commence par une cellule Markdown « ## Etape N » ; le second argument permet de
n'executer que les etapes deja terminees (par defaut : toutes).
"""
import re
import sys
import nbformat as nbf

md, code = nbf.v4.new_markdown_cell, nbf.v4.new_code_cell
C = []

# ---------------------------------------------------------------------------------------------
# 0. Titre, imports, session Spark
# ---------------------------------------------------------------------------------------------
C.append(md("""# TP 4 - UC-08 : Analyse des donnees et visualisation
Team **MaxMag** : Maxime DANINO - Awadi BEDJA - Gurvan Godin

**Objectif** : exploiter les donnees Silver et Gold produites au Jour 3 (UC-06) pour calculer les KPI definis
au Jour 1 (UC-01), produire des statistiques, des analyses metier et des visualisations, puis en tirer des
constats sans depasser ce que les donnees permettent d'affirmer.

**Questions metier traitees** (UC-01) : Q1 *quels produits ou categories generent le plus de chiffre d'affaires,
par mois ?* (direction commerciale) et Q3 *comment se situe mon magasin par rapport aux autres ?* (responsable de
magasin).

**Methode** : Spark fait tous les calculs sur les donnees completes ; seuls les petits resultats agreges sont
convertis en tableaux pandas pour etre affiches ou dessines avec Matplotlib.

{PLAN}"""))

C.append(code("""import os, sys, platform, subprocess
sys.path.insert(0, os.path.expanduser('~/retail-intelligence/tp4/scripts'))
import pandas as pd, matplotlib, pyspark
from pyspark.sql import functions as F
import parametres as P

pd.set_option('display.max_columns', 30); pd.set_option('display.width', 200)
pd.set_option('display.float_format', lambda x: f'{x:,.2f}'.replace(',', ' '))
java = subprocess.run(['java', '-version'], capture_output=True, text=True).stderr.splitlines()[0]
versions = {'Python': platform.python_version(), 'PySpark': pyspark.__version__, 'pandas': pd.__version__,
            'Matplotlib': matplotlib.__version__, 'Java': java}
for k, v in versions.items():
    print(f'{k:<11}: {v}')
with open(os.path.join(P.preuves(1), 'versions.txt'), 'w') as f:
    f.writelines(f'{k}: {v}\\n' for k, v in versions.items())"""))

C.append(code("""spark = P.get_spark('TP4-Retail-Intelligence')
print('Spark', spark.version, '|', spark.sparkContext.master, '|', spark.sparkContext.defaultParallelism, 'coeurs')"""))

# ---------------------------------------------------------------------------------------------
# Etape 1 : selectionner les donnees
# ---------------------------------------------------------------------------------------------
C.append(md("""## Etape 1 - Selectionner les donnees

Jeux retenus, tous produits au Jour 3 et stockes dans HDFS (`hdfs://localhost:9000/retail-lake`) :

- **Silver** : `ventes_detaillees`, les ventes nettoyees deja jointes a leurs produits, magasins et dates.
  Elle sert aux statistiques descriptives a la maille la plus fine (l'article de ticket) et au calcul par ticket.
- **Gold** : le modele en etoile (`fait_ventes` et ses 3 dimensions) pour recalculer les KPI, et les deux jeux
  d'indicateurs `kpi_ventes_magasin_mois` et `kpi_ventes_categorie_mois`, qui repondent directement a Q3 et Q1.

Pour chaque jeu, la suite verifie : nom exact, provenance, format, schema, granularite, valeurs vides et
controles qualite disponibles."""))

C.append(code("""jvm, hconf = spark._jvm, spark._jsc.hadoopConfiguration()

def fichiers_hdfs(chemin):
    \"\"\"Liste recursive des fichiers d'un dossier HDFS : (chemin, taille, date de modification).\"\"\"
    p = jvm.org.apache.hadoop.fs.Path(chemin)
    it = p.getFileSystem(hconf).listFiles(p, True)
    out = []
    while it.hasNext():
        s = it.next()
        out.append((s.getPath().toString(), s.getLen(), s.getModificationTime()))
    return out

def format_hdfs(chemin):
    fs = fichiers_hdfs(chemin)
    parquet = [f for f in fs if f[0].endswith('.parquet')]
    succes = [f for f in fs if f[0].endswith('/_SUCCESS')]
    parts = sorted({seg.split('=')[0] for f in parquet for seg in f[0][len(chemin):].split('/') if '=' in seg})
    return {
        'format': 'Parquet (' + ('Snappy' if all('snappy' in f[0] for f in parquet) else 'compression mixte') + ')',
        'fichiers': len(parquet),
        'taille_mo': round(sum(f[1] for f in parquet) / 2**20, 2),   # Mo de 1 048 576 octets, comme au J3
        'partitions': ', '.join(parts) or 'aucune',
        'succes': bool(succes),
        'ecrit_le': pd.to_datetime(max(f[2] for f in succes or parquet), unit='ms', utc=True)
                      .tz_convert('Europe/Paris').strftime('%Y-%m-%d %H:%M'),
    }

DF = {nom: spark.read.parquet(chemin) for nom, (zone, chemin, cle, grain) in P.JEUX.items()}
for nom, df in DF.items():
    print(f'{nom:<32} {len(df.columns):>3} colonnes')"""))

C.append(md("""### Provenance : journaux d'execution du Jour 3

Chaque traitement du Jour 3 a ecrit un journal JSON dans `logs/traitements`. On relit le dernier journal
Silver et le dernier journal Gold pour savoir quel script a produit chaque jeu, a partir de quoi, et si les
controles apres ecriture etaient bons."""))

C.append(code("""import json
# Chaque journal est un dossier <nom>.json ecrit par Spark (saveAsTextFile), contenant une seule ligne JSON.
journaux = sorted({f[0].split('/logs/traitements/')[1].split('/')[0] for f in fichiers_hdfs(P.LOGS)})
def lire_journal(prefixe):
    nom = [j for j in journaux if j.startswith(prefixe)][-1]
    texte = ''.join(r.value for r in spark.read.text(f'{P.LOGS}/{nom}').collect())
    return nom, json.loads(texte)

nom_s, js = lire_journal('silver_')
nom_g, jg = lire_journal('gold_')
print('Silver :', nom_s, '|', js['traitement'], '|', js['debut'], '->', js['fin'], '| entree', js['entrees'])
print('Gold   :', nom_g, '|', jg['traitement'], '|', jg['debut'], '->', jg['fin'], '| entree', jg['entree'])

sorties_silver = {s['table']: s for s in js['sorties']}
jeux_gold = {j['jeu']: j for j in jg['jeux']}
s = sorties_silver['ventes_detaillees']
print('\\nsilver/ventes_detaillees : relues', s['lignes_relues'], '/ attendues', s['lignes_attendues'],
      '| schema identique', s['schema_identique'], '| cle', s['cle'], 'unique :', s['cle_unique'])
for j in jeux_gold.values():
    if j['jeu'].startswith(('dim_', 'fait_', 'kpi_')):
        print(f\"gold/{j['jeu']:<26} ecrites {j['lignes_ecrites']:>7} | relues {j['lignes_relues']:>7} | schema identique {j['schema_identique']}\")
    else:   # controles enregistres dans le meme journal (integrite, coherence du CA)
        print(f\"controle {j['jeu']:<40} resultat {j['lignes_ecrites']}\")"""))

C.append(md("""### Controles qualite disponibles (rapport de l'etape 9 du Jour 3)"""))

C.append(code("""rapports = sorted({f[0].split('/controles/')[1].split('/')[0] for f in fichiers_hdfs(P.CONTROLES) if '/controles_' in f[0]})
rq = spark.read.parquet(f'{P.CONTROLES}/{rapports[-1]}').toPandas()
print('Rapport :', rapports[-1], '|', len(rq), 'controles |', rq['statut'].value_counts().to_dict())
synthese_qualite = (rq.assign(n=1).pivot_table(index='jeu', columns='statut', values='n', aggfunc='sum', fill_value=0))
synthese_qualite"""))

C.append(md("""### Verification de chaque jeu : volume, granularite, valeurs vides, periode"""))

C.append(code("""PROVENANCE = {
    'silver/ventes_detaillees': f\"{js['traitement']} ({js['debut'][:16].replace('T', ' ')}), Bronze store_sales + item + store + date_dim, lots initial et refresh\",
}
for jeu in ('fait_ventes', 'dim_date', 'dim_produit', 'dim_magasin'):
    PROVENANCE[f'gold/{jeu}'] = f\"{jg['traitement']} ({jg['debut'][:16].replace('T', ' ')}), depuis silver\"
for jeu in ('kpi_ventes_magasin_mois', 'kpi_ventes_categorie_mois'):
    PROVENANCE[f'gold/{jeu}'] = f\"{jg['traitement']} ({jg['debut'][:16].replace('T', ' ')}), depuis fait_ventes et ses dimensions\"

USAGE = {
    'silver/ventes_detaillees': 'Statistiques descriptives par ligne et par ticket ; KPI recalcules',
    'gold/fait_ventes': 'Calcul des KPI (CA, quantites, tickets, panier, remise)',
    'gold/dim_date': 'Axe temps des analyses (annee, mois)',
    'gold/dim_produit': 'Axe categorie de produits (Q1)',
    'gold/dim_magasin': 'Axe magasin (Q3)',
    'gold/kpi_ventes_magasin_mois': 'Analyse et visualisation Q3 : comparaison des magasins, evolution',
    'gold/kpi_ventes_categorie_mois': 'Analyse et visualisation Q1 : categories par mois',
}

def controles(nom):
    r = rq[rq['jeu'] == nom]['statut'].value_counts().to_dict()
    if nom == 'silver/ventes_detaillees':
        return 'controles apres ecriture (lignes, schema, cle unique) ; CA identique a Gold (rapport)'
    return ', '.join(f'{v} {k}' for k, v in sorted(r.items())) or 'aucun controle dedie'

lignes = []
for nom, (zone, chemin, cle, grain) in P.JEUX.items():
    df = DF[nom]
    n = df.count()
    distincts = df.select(*cle).distinct().count()
    vides = df.select([F.sum(F.col(c).isNull().cast('int')).alias(c) for c in df.columns]).first().asDict()
    vides = {c: v for c, v in vides.items() if v}
    if 'annee_mois' in df.columns:
        a = df.agg(F.min('annee_mois'), F.max('annee_mois')).first(); periode = f'{a[0]} a {a[1]}'
    elif 'date' in df.columns:
        a = df.agg(F.min('date'), F.max('date')).first(); periode = f'{a[0]} a {a[1]}'
    elif 'annee' in df.columns:
        a = df.agg(F.min('annee'), F.max('annee')).first(); periode = f'{a[0]} a {a[1]}'
    else:
        periode = ''
    ca = df.agg(F.sum('chiffre_affaires')).first()[0] if 'chiffre_affaires' in df.columns else None
    lignes.append({'jeu': nom, 'zone': zone, 'chemin': chemin.replace(P.HDFS, ''), **format_hdfs(chemin),
                   'lignes': n, 'colonnes': len(df.columns), 'granularite': grain, 'cle': ' + '.join(cle),
                   'cle_unique': distincts == n, 'colonnes_avec_vides': ', '.join(f'{c} ({v})' for c, v in vides.items()) or 'aucune',
                   'periode': periode, 'chiffre_affaires': float(ca) if ca is not None else None,
                   'provenance': PROVENANCE[nom], 'controles_qualite': controles(nom), 'usage_prevu': USAGE[nom]})
inventaire = pd.DataFrame(lignes)
inventaire.to_csv(os.path.join(P.preuves(1), 'inventaire_jeux.csv'), index=False, sep=';')
inventaire[['jeu', 'format', 'fichiers', 'taille_mo', 'partitions', 'lignes', 'colonnes', 'cle', 'cle_unique', 'periode']]"""))

C.append(code("""inventaire[['jeu', 'colonnes_avec_vides', 'chiffre_affaires', 'controles_qualite']]"""))

C.append(md("""### Schemas

Les schemas complets sont enregistres dans `UC08_preuves/etape1/schemas.txt`. Affichage du schema de la table
de faits et d'un jeu d'indicateurs :"""))

C.append(code("""with open(os.path.join(P.preuves(1), 'schemas.txt'), 'w') as f:
    for nom, df in DF.items():
        f.write(f'== {nom}\\n{df._jdf.schema().treeString()}\\n')
DF['gold/fait_ventes'].printSchema()
DF['gold/kpi_ventes_magasin_mois'].printSchema()"""))

C.append(md("""### Coherence entre les jeux

Les jeux Gold sont calcules a partir de Silver : le chiffre d'affaires, les quantites et le nombre de lignes
doivent etre identiques a chaque niveau. Le nombre de tickets n'est additif que par magasin (un ticket = un
magasin), pas par categorie (un ticket contient plusieurs categories)."""))

C.append(code("""def totaux(df, tickets=True):
    agg = [F.sum('chiffre_affaires').alias('ca'), F.sum('quantite').alias('quantite')]
    agg.append(F.countDistinct('ticket').alias('tickets') if 'ticket' in df.columns else F.sum('nb_tickets').alias('tickets'))
    agg.append(F.count('*').alias('lignes') if 'ligne_id' in df.columns else F.sum('nb_lignes').alias('lignes'))
    return df.agg(*agg).first().asDict()

coherence = pd.DataFrame({nom: totaux(DF[nom]) for nom in
    ['silver/ventes_detaillees', 'gold/fait_ventes', 'gold/kpi_ventes_magasin_mois', 'gold/kpi_ventes_categorie_mois']}).T
coherence['ca'] = coherence['ca'].astype(float).round(2)
coherence.to_csv(os.path.join(P.preuves(1), 'coherence_jeux.csv'), sep=';')
coherence"""))

C.append(code("""ref = coherence.loc['silver/ventes_detaillees']
for nom, r in coherence.iterrows():
    ecarts = [c for c in ('ca', 'quantite', 'lignes') if r[c] != ref[c]]
    if nom != 'gold/kpi_ventes_categorie_mois' and r['tickets'] != ref['tickets']:
        ecarts.append('tickets')
    print(f\"{nom:<32} {'identique a Silver' if not ecarts else 'ECART : ' + ', '.join(ecarts)}\")
print('\\nTickets distincts :', ref['tickets'], '| somme des tickets par categorie :', coherence.loc['gold/kpi_ventes_categorie_mois', 'tickets'],
      '(plus grand : un ticket compte dans chaque categorie qu il contient)')"""))

# ---------------------------------------------------------------------------------------------
# Etape 2 : verifier les KPI
# ---------------------------------------------------------------------------------------------
C.append(md("""## Etape 2 - Verifier les KPI

On reprend le dictionnaire des KPI du Jour 1 (UC-01, K1 a K8). Pour chaque indicateur : definition, formule,
champs de **nos** jeux Silver et Gold, filtres, granularite, unite et limites. Un KPI n'est retenu que si les
champs necessaires existent dans les donnees preparees au Jour 3.

Adaptation par rapport au Jour 1 : le Jour 3 n'a transforme que les **ventes en magasin**. Les formules du Jour 1
qui additionnaient magasin et web se limitent donc ici au canal magasin."""))

C.append(code("""PERIODE = 'du 01/01/2001 au 31/12/2005, lots initial et refresh reunis (decembre 2005 complet)'
DICO = pd.DataFrame([
    ('K1', \"Chiffre d'affaires (CA)\", \"Montant des ventes apres remise, hors taxe\", 'SUM(chiffre_affaires)',
     'chiffre_affaires (= ss_ext_sales_price)', 'aucun ; ventes a prix nul conservees', 'magasin, categorie, mois, annee', 'unite monetaire (dollar probable)',
     'CA brut : retours non deduits ; magasin seulement', 'Retenu'),
    ('K2', 'Quantite vendue', \"Nombre d'unites de produits vendues\", 'SUM(quantite)',
     'quantite', 'aucun', 'categorie, magasin, mois', 'unites',
     \"Additionner des produits differents n'a de sens qu'a l'interieur d'une categorie\", 'Retenu'),
    ('K3', 'Nombre de transactions', 'Nombre de passages en caisse (tickets distincts)', 'COUNT(DISTINCT ticket)',
     'ticket (= ss_ticket_number)', 'aucun', 'magasin, mois (un ticket = un magasin, un jour)', 'tickets',
     'Non additif entre categories : un ticket contient plusieurs categories', 'Retenu'),
    ('K4', 'Panier moyen', 'Montant moyen depense par ticket', 'K1 / K3',
     'chiffre_affaires, ticket', 'aucun', 'magasin, mois', 'unite monetaire par ticket',
     'Sensible aux gros tickets : a completer par la mediane ; ne jamais diviser par un nombre de lignes', 'Retenu'),
    ('K5', 'Taux de remise', 'Part du prix catalogue abandonnee en remise', 'SUM(remise) / SUM(montant_catalogue) x 100',
     'remise, montant_catalogue', 'aucun', 'categorie, magasin, mois', '%',
     \"Une remise n'est pas forcement liee a une promotion ; correlation n'est pas causalite\", 'Retenu'),
    ('K8', 'Evolution du CA', 'Variation du CA par rapport au meme mois de l annee precedente',
     '(K1 mois M - K1 meme mois N-1) / K1 meme mois N-1 x 100',
     'chiffre_affaires, annee, mois', 'periodes completes seulement', 'magasin, categorie, mois ou annee', '%',
     \"Pas de valeur pour 2001 (pas d'annee precedente) ; une hausse ne s'explique pas sans autres donnees\", 'Retenu'),
    ('K6', 'Taux de retour', 'Part des quantites vendues retournees', 'SUM(sr_return_quantity) / SUM(ss_quantity) x 100',
     'store_returns (absent de Silver et Gold)', '', '', '%',
     \"store_returns n'a pas ete nettoye ni transforme au Jour 3\", 'Non calculable avec Silver / Gold'),
    ('K7', 'Niveau de stock', \"Quantite disponible d'un produit dans un entrepot a une date\", 'inv_quantity_on_hand a la date choisie',
     'inventory (absent de Silver et Gold)', '', '', 'unites',
     \"inventory n'a pas ete transforme au Jour 3 ; stock connu par entrepot et par semaine seulement\", 'Non calculable avec Silver / Gold'),
    ('-', 'Taux de marge', 'Part du CA restant apres le cout des produits', 'SUM(marge) / SUM(chiffre_affaires) x 100',
     'marge, cout (= ss_net_profit, ss_wholesale_cost)', '', 'magasin, categorie, mois', '%',
     \"Champs presents mais non qualifies : 74,3 % des lignes ont une marge negative (jeu synthetique)\", 'Non retenu (affiche a titre informatif)'),
], columns=['code', 'nom', 'definition', 'formule', 'champs', 'filtres', 'granularite', 'unite', 'limites', 'statut'])
DICO['periode'] = PERIODE
DICO.to_csv(os.path.join(P.preuves(2), 'dictionnaire_kpi.csv'), index=False, sep=';')
DICO[['code', 'nom', 'formule', 'granularite', 'unite', 'statut']]"""))

C.append(md("""### Calcul des KPI retenus sur toute la periode

Calcul a partir de la table de faits Gold `fait_ventes`, sans passer par les jeux d'indicateurs : c'est le calcul
de reference qui servira a controler ces jeux."""))

C.append(code("""fait = DF['gold/fait_ventes']
k = fait.agg(F.sum('chiffre_affaires').alias('K1'), F.sum('quantite').alias('K2'),
             F.countDistinct('ticket').alias('K3'), F.sum('remise').alias('remise'),
             F.sum('montant_catalogue').alias('catalogue'), F.count('*').alias('lignes'),
             F.sum('marge').alias('marge')).first()
K = {'K1 Chiffre d affaires': float(k.K1), 'K2 Quantite vendue': k.K2, 'K3 Nombre de transactions (tickets)': k.K3,
     'K4 Panier moyen': round(float(k.K1) / k.K3, 2), 'K5 Taux de remise (%)': round(100 * float(k.remise) / float(k.catalogue), 2),
     '(non retenu) Taux de marge (%)': round(100 * float(k.marge) / float(k.K1), 2)}
kpi_globaux = pd.DataFrame({'kpi': list(K), 'valeur': list(K.values())})
kpi_globaux.to_csv(os.path.join(P.preuves(2), 'kpi_globaux.csv'), index=False, sep=';')
kpi_globaux"""))

C.append(md("""### Point de vigilance : lignes, articles et transactions

Le TP demande de ne pas confondre nombre de lignes, nombre d'articles et nombre de transactions. Le calcul ci-dessous
montre l'erreur que l'on commettrait en divisant le CA par le nombre de lignes au lieu du nombre de tickets."""))

C.append(code("""pieges = pd.DataFrame([
    ('Lignes de vente (articles distincts d un ticket)', k.lignes, round(float(k.K1) / k.lignes, 2)),
    ('Articles vendus (somme des quantites)', k.K2, round(float(k.K1) / k.K2, 2)),
    ('Transactions (tickets distincts)', k.K3, round(float(k.K1) / k.K3, 2)),
], columns=['denominateur', 'nombre', 'CA / nombre'])
pieges.to_csv(os.path.join(P.preuves(2), 'lignes_articles_tickets.csv'), index=False, sep=';')
print(f'Lignes par ticket : {k.lignes / k.K3:.2f} | articles par ticket : {k.K2 / k.K3:.1f}')
pieges"""))

C.append(md("""### K8 : evolution annuelle du CA

Le CA d'une annee n'est comparable a celui de l'annee precedente que si les deux annees sont completes. On verifie
d'abord le nombre de jours de vente par annee."""))

C.append(code("""dates = DF['gold/dim_date'].select('date_sk', 'annee', 'mois', 'annee_mois')
par_an = (fait.join(dates.select('date_sk', F.col('annee').alias('a')), 'date_sk')
              .groupBy('a').agg(F.sum('chiffre_affaires').alias('ca'), F.countDistinct('date_sk').alias('jours_de_vente'),
                                F.countDistinct('ticket').alias('tickets'))
              .orderBy('a').toPandas().rename(columns={'a': 'annee'}))
par_an['ca'] = par_an['ca'].astype(float)
par_an['evolution_pct'] = (100 * par_an['ca'].pct_change()).round(2)
par_an.to_csv(os.path.join(P.preuves(2), 'k8_evolution_annuelle.csv'), index=False, sep=';')
par_an"""))

C.append(md("""### Controle des jeux d'indicateurs Gold

Les KPI sont recalcules ici par magasin x mois et par categorie x mois, independamment du code du Jour 3, puis
compares aux colonnes des jeux `kpi_ventes_magasin_mois` et `kpi_ventes_categorie_mois`. Tolerance : 0,01 (arrondi
a 2 decimales)."""))

C.append(code("""from pyspark.sql import Window
detail = (fait.join(dates.drop('annee'), 'date_sk')
              .join(DF['gold/dim_produit'].select('produit_sk', 'categorie'), 'produit_sk'))

def recalcul(cles):
    r = (detail.groupBy(*cles).agg(F.sum('chiffre_affaires').alias('K1'), F.sum('quantite').alias('K2'),
                                  F.countDistinct('ticket').alias('K3'),
                                  (100 * F.sum('remise') / F.sum('montant_catalogue')).alias('K5'))
               .withColumn('K4', F.col('K1') / F.col('K3')))
    return r

ref_m = recalcul(['magasin_sk', 'annee_mois', 'mois'])
w = Window.partitionBy('magasin_sk', 'mois').orderBy('annee_mois')
ref_m = ref_m.withColumn('K8', 100 * (F.col('K1') - F.lag('K1').over(w)) / F.lag('K1').over(w))
ref_c = recalcul(['categorie', 'annee_mois'])

COLONNES = {'K1': 'chiffre_affaires', 'K2': 'quantite', 'K3': 'nb_tickets', 'K4': 'panier_moyen', 'K5': 'taux_remise', 'K8': 'evolution_ca_pct'}
def comparer(ref, gold, cles, kpis):
    j = ref.join(gold, cles, 'full_outer')
    ligne = {'lignes_recalculees': ref.count(), 'lignes_gold': gold.count(), 'lignes_rapprochees': j.filter(F.col('K1').isNotNull() & F.col('chiffre_affaires').isNotNull()).count()}
    for kk in kpis:
        ecart = F.abs(F.col(kk).cast('double') - F.col(COLONNES[kk]).cast('double'))
        ligne[f'{kk} ecart max'] = j.agg(F.max(ecart)).first()[0]
        ligne[f'{kk} lignes > 0,01'] = j.filter(ecart > 0.01).count()
    return ligne

controle_kpi = pd.DataFrame({
    'kpi_ventes_magasin_mois': comparer(ref_m, DF['gold/kpi_ventes_magasin_mois'], ['magasin_sk', 'annee_mois'], ['K1', 'K2', 'K3', 'K4', 'K5', 'K8']),
    'kpi_ventes_categorie_mois': comparer(ref_c, DF['gold/kpi_ventes_categorie_mois'], ['categorie', 'annee_mois'], ['K1', 'K2', 'K3', 'K4', 'K5']),
}).T
controle_kpi.to_csv(os.path.join(P.preuves(2), 'controle_kpi_gold.csv'), sep=';')
controle_kpi.T"""))

C.append(code("""ecarts = controle_kpi[[c for c in controle_kpi.columns if c.endswith('> 0,01')]].fillna(0).to_numpy().sum()
vides_k8 = ref_m.filter(F.col('K8').isNull()).count()
print('Lignes en ecart de plus de 0,01 :', int(ecarts))
print('K8 sans valeur (annee 2001, pas de N-1) :', vides_k8, '| dans le jeu Gold :',
      DF['gold/kpi_ventes_magasin_mois'].filter(F.col('evolution_ca_pct').isNull()).count())"""))

# ---------------------------------------------------------------------------------------------
# Etape 3 : statistiques descriptives
# ---------------------------------------------------------------------------------------------
C.append(md("""## Etape 3 - Statistiques descriptives

Jeu etudie : **silver/ventes_detaillees** (une ligne = un article d'un ticket), complete par une agregation par
ticket. Toutes les statistiques sont calculees par Spark sur les 674 211 lignes ; seuls les tableaux de resultats
passent dans pandas. Les medianes et quartiles sont exacts (fonction `percentile`), pas approches.

Statistiques produites : nombre d'enregistrements, minimum, maximum, moyenne, mediane, quartiles, sommes pour
les mesures additives, repartitions par categorie, par magasin et par jour de la semaine, evolution mensuelle."""))

C.append(code("""ventes = DF['silver/ventes_detaillees']

def stats(df, colonnes, additives):
    \"\"\"Une ligne par variable : n, min, Q1, mediane, moyenne, Q3, max, ecart-type et somme si la mesure est additive.\"\"\"
    lignes = []
    for c in colonnes:
        x = F.col(c).cast('double')
        r = df.agg(F.count(x).alias('n'), F.min(x).alias('min'), F.percentile(x, 0.25).alias('q1'),
                   F.percentile(x, 0.5).alias('mediane'), F.avg(x).alias('moyenne'), F.percentile(x, 0.75).alias('q3'),
                   F.max(x).alias('max'), F.stddev(x).alias('ecart_type'), F.sum(F.col(c)).alias('somme')).first().asDict()
        r['somme'] = float(r['somme']) if c in additives else None
        lignes.append({'variable': c, **r})
    return pd.DataFrame(lignes).set_index('variable')

MESURES_LIGNE = ['quantite', 'prix_catalogue_unitaire', 'prix_vente_unitaire', 'montant_catalogue', 'remise', 'chiffre_affaires', 'taux_remise_ligne']
stats_ligne = stats(ventes, MESURES_LIGNE, additives={'quantite', 'montant_catalogue', 'remise', 'chiffre_affaires'})
stats_ligne.round(2)"""))

C.append(md("""### Par ticket

Le panier moyen (K4) est une moyenne : on la compare a la mediane des montants par ticket pour voir si quelques
tres gros tickets la tirent vers le haut."""))

C.append(code("""tickets = (ventes.groupBy('ticket', 'magasin', 'date_vente')
                 .agg(F.sum('chiffre_affaires').alias('montant_ticket'), F.count('*').alias('lignes_par_ticket'),
                      F.sum('quantite').alias('articles_par_ticket'), F.countDistinct('categorie').alias('categories_par_ticket')))
print('Tickets :', tickets.count(), '| tickets sur plusieurs magasins ou plusieurs jours :',
      tickets.groupBy('ticket').count().filter('count > 1').count())
stats_ticket = stats(tickets, ['montant_ticket', 'lignes_par_ticket', 'articles_par_ticket', 'categories_par_ticket'], additives={'montant_ticket'})
stats_ticket.round(2)"""))

C.append(code("""stats_desc = pd.concat([stats_ligne.assign(granularite='ligne de vente'), stats_ticket.assign(granularite='ticket')])
stats_desc.round(4).to_csv(os.path.join(P.preuves(3), 'statistiques.csv'), sep=';')
m = stats_ticket.loc['montant_ticket']
print(f\"Panier moyen {m['moyenne']:.2f} | panier median {m['mediane']:.2f} | ecart {100 * (m['moyenne'] - m['mediane']) / m['mediane']:.1f} %\")
print('Lignes a prix de vente nul :', ventes.filter(F.col('prix_vente_unitaire') == 0).count(),
      '| tickets de montant nul :', tickets.filter(F.col('montant_ticket') == 0).count())"""))

C.append(md("""### Distribution des montants par ticket

Repartition des tickets par tranche de montant (calcul Spark, 10 tranches de 5 000)."""))

C.append(code("""tranches = (tickets.withColumn('tranche', F.least(F.floor(F.col('montant_ticket') / 5000), F.lit(9)))
                   .groupBy('tranche').agg(F.count('*').alias('tickets'), F.sum('montant_ticket').alias('ca'))
                   .orderBy('tranche').toPandas())
tranches['tranche'] = [f'{int(t) * 5} a {int(t) * 5 + 5} k' if t < 9 else '45 k et plus' for t in tranches['tranche']]
tranches['part_tickets_pct'] = (100 * tranches['tickets'] / tranches['tickets'].sum()).round(2)
tranches['ca'] = tranches['ca'].astype(float)
tranches.to_csv(os.path.join(P.preuves(3), 'distribution_montant_ticket.csv'), index=False, sep=';')
tranches"""))

C.append(md("""### Repartition par categorie et par magasin"""))

C.append(code("""def repartition(cle):
    r = (ventes.groupBy(cle).agg(F.sum('chiffre_affaires').alias('ca'), F.sum('quantite').alias('quantite'),
                                 F.count('*').alias('lignes'), F.countDistinct('ticket').alias('tickets'),
                                 (100 * F.sum('remise') / F.sum('montant_catalogue')).alias('taux_remise'))
               .toPandas())
    r['ca'] = r['ca'].astype(float); r['taux_remise'] = r['taux_remise'].astype(float).round(2)
    r['part_ca_pct'] = (100 * r['ca'] / r['ca'].sum()).round(2)
    return r.sort_values('ca', ascending=False).reset_index(drop=True)

par_categorie = repartition('categorie')
par_categorie.to_csv(os.path.join(P.preuves(3), 'repartition_categorie.csv'), index=False, sep=';')
par_categorie"""))

C.append(code("""par_magasin = repartition('magasin')
par_magasin['panier_moyen'] = (par_magasin['ca'] / par_magasin['tickets']).round(2)
par_magasin.to_csv(os.path.join(P.preuves(3), 'repartition_magasin.csv'), index=False, sep=';')
ecart = (par_magasin['ca'].max() - par_magasin['ca'].min()) / par_magasin['ca'].mean() * 100
print(f'Ecart entre le magasin le plus fort et le plus faible : {ecart:.1f} % du CA moyen par magasin')
par_magasin"""))

C.append(md("""### Repartition par jour de la semaine et evolution mensuelle"""))

C.append(code("""ORDRE_JOURS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
par_jour = repartition('jour_semaine')
par_jour['ordre'] = par_jour['jour_semaine'].map({j: i for i, j in enumerate(ORDRE_JOURS)})
par_jour = par_jour.sort_values('ordre').drop(columns='ordre').reset_index(drop=True)
par_jour.to_csv(os.path.join(P.preuves(3), 'repartition_jour_semaine.csv'), index=False, sep=';')
par_jour"""))

C.append(code("""par_mois = (ventes.groupBy('annee_mois', 'annee', 'mois').agg(F.sum('chiffre_affaires').alias('ca'),
                                                               F.countDistinct('ticket').alias('tickets'),
                                                               F.countDistinct('date_vente').alias('jours'))
                 .orderBy('annee_mois').toPandas())
par_mois['ca'] = par_mois['ca'].astype(float)
par_mois['ca_par_jour'] = (par_mois['ca'] / par_mois['jours']).round(2)
par_mois.to_csv(os.path.join(P.preuves(3), 'evolution_mensuelle.csv'), index=False, sep=';')
resume_mois = pd.DataFrame({
    'mois': len(par_mois), 'ca_min': par_mois['ca'].min(), 'mois_min': par_mois.loc[par_mois['ca'].idxmin(), 'annee_mois'],
    'ca_max': par_mois['ca'].max(), 'mois_max': par_mois.loc[par_mois['ca'].idxmax(), 'annee_mois'],
    'ca_moyen': par_mois['ca'].mean(), 'ca_median': par_mois['ca'].median(),
    'ca_par_jour_min': par_mois['ca_par_jour'].min(), 'ca_par_jour_max': par_mois['ca_par_jour'].max()}, index=['CA mensuel'])
resume_mois"""))

C.append(code("""saison = (par_mois.groupby('mois').agg(ca_moyen=('ca', 'mean'), ca_par_jour_moyen=('ca_par_jour', 'mean')).round(2))
saison['indice_vs_moyenne'] = (100 * saison['ca_par_jour_moyen'] / saison['ca_par_jour_moyen'].mean()).round(1)
saison.to_csv(os.path.join(P.preuves(3), 'saisonnalite_mois.csv'), sep=';')
saison"""))

# ---------------------------------------------------------------------------------------------
# Etape 4 : analyses metier
# ---------------------------------------------------------------------------------------------
C.append(md("""## Etape 4 - Analyses metier

Deux analyses, chacune issue d'une question metier du Jour 1 (UC-01) classee « indispensable » :

| Analyse | Question metier | Profil | Jeux utilises |
|---|---|---|---|
| A1 | Q1 : quels produits ou categories generent le plus de chiffre d'affaires, par mois ? | Direction commerciale | kpi_ventes_categorie_mois ; fait_ventes + dim_produit |
| A2 | Q3 : comment se situe mon magasin par rapport aux autres magasins de l'enseigne ? | Responsable de magasin | kpi_ventes_magasin_mois ; dim_magasin |"""))

C.append(md("""### A1 - Categories et produits qui generent le plus de chiffre d'affaires, par mois

**Methode** : a partir des 540 lignes categorie x mois, on calcule pour chaque categorie son CA total, sa part
moyenne du CA mensuel, la plage de cette part sur les 60 mois, son rang chaque mois et l'evolution de son CA entre
2001 et 2005. On descend ensuite au niveau produit (fait_ventes) pour mesurer la concentration du CA."""))

C.append(code("""kc = DF['gold/kpi_ventes_categorie_mois']
w_rang = Window.partitionBy('annee_mois').orderBy(F.desc('chiffre_affaires'))
kc_rang = kc.withColumn('rang_mois', F.rank().over(w_rang))
a1 = (kc_rang.groupBy('categorie')
          .agg(F.sum('chiffre_affaires').alias('ca'), F.avg('part_ca_mois_pct').alias('part_moyenne_pct'),
               F.min('part_ca_mois_pct').alias('part_min_pct'), F.max('part_ca_mois_pct').alias('part_max_pct'),
               F.min('rang_mois').alias('meilleur_rang'), F.max('rang_mois').alias('moins_bon_rang'),
               F.sum((F.col('rang_mois') == 1).cast('int')).alias('mois_au_1er_rang'),
               F.sum(F.when(F.col('annee') == 2001, F.col('chiffre_affaires'))).alias('ca_2001'),
               F.sum(F.when(F.col('annee') == 2005, F.col('chiffre_affaires'))).alias('ca_2005'))
          .toPandas())
for c in ['ca', 'part_moyenne_pct', 'part_min_pct', 'part_max_pct', 'ca_2001', 'ca_2005']:
    a1[c] = a1[c].astype(float)
a1['evolution_2005_vs_2001_pct'] = (100 * (a1['ca_2005'] - a1['ca_2001']) / a1['ca_2001']).round(2)
a1['part_ca_total_pct'] = (100 * a1['ca'] / a1['ca'].sum()).round(2)
a1 = a1.sort_values('ca', ascending=False).reset_index(drop=True)
a1.insert(0, 'rang', range(1, len(a1) + 1))
a1.round(2).to_csv(os.path.join(P.preuves(4), 'a1_categories.csv'), index=False, sep=';')
a1[['rang', 'categorie', 'ca', 'part_ca_total_pct', 'part_min_pct', 'part_max_pct', 'meilleur_rang', 'moins_bon_rang', 'mois_au_1er_rang', 'evolution_2005_vs_2001_pct']].round(2)"""))

C.append(code("""# Le classement des categories change-t-il d'un mois a l'autre ?
changements = (kc_rang.groupBy('annee_mois').agg(F.collect_list(F.struct('rang_mois', 'categorie')).alias('r'))
                      .toPandas())
ordre = changements['r'].apply(lambda r: tuple(x['categorie'] for x in sorted(r, key=lambda x: x['rang_mois'])))
print('Mois :', len(ordre), '| classements differents des 9 categories :', ordre.nunique())
print('Mois ou le podium (3 premieres) differe de Music, Books, Movies & TV :',
      sum(o[:3] != ('Music', 'Books', 'Movies & TV') for o in ordre))"""))

C.append(code("""# Niveau produit : concentration du CA
prod = (fait.groupBy('produit_sk').agg(F.sum('chiffre_affaires').alias('ca'), F.sum('quantite').alias('quantite'),
                                       F.countDistinct('ticket').alias('tickets'))
            .join(DF['gold/dim_produit'].select('produit_sk', 'produit_id', 'categorie'), 'produit_sk')
            .toPandas())
prod['ca'] = prod['ca'].astype(float)
prod = prod.sort_values('ca', ascending=False).reset_index(drop=True)
prod['part_pct'] = 100 * prod['ca'] / prod['ca'].sum()
prod['part_cumulee_pct'] = prod['part_pct'].cumsum()
n_vendus = len(prod)
concentration = pd.DataFrame({
    'produits vendus': [n_vendus],
    'part du CA des 10 premiers produits (%)': [prod['part_pct'].head(10).sum()],
    'part du CA des 1 % premiers produits (%)': [prod['part_pct'].head(round(n_vendus / 100)).sum()],
    'produits pour 50 % du CA': [int((prod['part_cumulee_pct'] < 50).sum() + 1)],
    'produits pour 80 % du CA': [int((prod['part_cumulee_pct'] < 80).sum() + 1)],
}).round(2)
concentration['part des produits pour 80 % du CA (%)'] = round(100 * concentration['produits pour 80 % du CA'] / n_vendus, 2)
prod.head(10).round(2).to_csv(os.path.join(P.preuves(4), 'a1_top10_produits.csv'), index=False, sep=';')
concentration.T.to_csv(os.path.join(P.preuves(4), 'a1_concentration_produits.csv'), sep=';')
display(concentration.T)
prod.head(10)[['produit_id', 'categorie', 'ca', 'quantite', 'tickets', 'part_pct']].round(3)"""))

C.append(code("""# Dans les 10 premiers produits, combien viennent de Music et Books ? Et sur 100 ?
print('Top 10  :', prod.head(10)['categorie'].value_counts().to_dict())
print('Top 100 :', prod.head(100)['categorie'].value_counts().to_dict())
cat_prod = prod.groupby('categorie').agg(produits=('produit_sk', 'count'), ca_moyen_par_produit=('ca', 'mean')).round(2)
cat_prod.to_csv(os.path.join(P.preuves(4), 'a1_produits_par_categorie.csv'), sep=';')
cat_prod.sort_values('ca_moyen_par_produit', ascending=False)"""))

C.append(md("""**Resultats et interpretation A1**

- Music est la premiere categorie **chaque mois** (60 mois sur 60) et le podium Music, Books, Movies & TV ne change
  jamais. Les parts mensuelles bougent peu (Music entre 43,5 % et 46,9 %).
- Le CA est concentre sur peu de produits : 1 % des produits vendus fait pres de 40 % du CA.
- La domination de Music vient du **nombre de produits** de la categorie (environ 46 % des produits vendus), pas
  d'un CA plus eleve par produit : un produit Music rapporte en moyenne moins qu'un produit Clothing & Accessories.

**Limites** : noms et marques de produits aleatoires (seule la categorie est interpretable) ; retours non deduits ;
la part d'une categorie depend de la taille de son catalogue, qui n'est pas une donnee de vente."""))

C.append(md("""### A2 - Position de chaque magasin par rapport aux autres

**Methode** : a partir des 720 lignes magasin x mois, on calcule pour chaque magasin son CA total, son ecart a la
moyenne des 12 magasins, son rang chaque mois (nombre de mois au premier et au dernier rang), son panier moyen et
son taux de remise. Pour ne pas conclure trop vite qu'un magasin « vend plus », on rapporte aussi le CA a la
surface et au nombre d'employes (dim_magasin)."""))

C.append(code("""km = DF['gold/kpi_ventes_magasin_mois']
w_m = Window.partitionBy('annee_mois').orderBy(F.desc('chiffre_affaires'))
km_rang = km.withColumn('rang_mois', F.rank().over(w_m))
a2 = (km_rang.groupBy('magasin_sk', 'magasin', 'ville', 'etat')
          .agg(F.sum('chiffre_affaires').alias('ca'), F.sum('nb_tickets').alias('tickets'),
               F.sum('remise').alias('remise'), F.sum('montant_catalogue').alias('catalogue'),
               F.avg('rang_mois').alias('rang_moyen'),
               F.sum((F.col('rang_mois') == 1).cast('int')).alias('mois_1er'),
               F.sum((F.col('rang_mois') == 12).cast('int')).alias('mois_dernier'),
               F.sum(F.when(F.col('annee') == 2001, F.col('chiffre_affaires'))).alias('ca_2001'),
               F.sum(F.when(F.col('annee') == 2005, F.col('chiffre_affaires'))).alias('ca_2005'))
          .join(DF['gold/dim_magasin'].select('magasin_sk', 'employes', 'surface'), 'magasin_sk')
          .toPandas())
for c in ['ca', 'remise', 'catalogue', 'ca_2001', 'ca_2005']:
    a2[c] = a2[c].astype(float)
a2['ecart_moyenne_pct'] = 100 * (a2['ca'] - a2['ca'].mean()) / a2['ca'].mean()
a2['panier_moyen'] = a2['ca'] / a2['tickets']
a2['taux_remise_pct'] = 100 * a2['remise'] / a2['catalogue']
a2['evolution_2005_vs_2001_pct'] = 100 * (a2['ca_2005'] - a2['ca_2001']) / a2['ca_2001']
a2['ca_par_m2'] = a2['ca'] / a2['surface']
a2['ca_par_employe'] = a2['ca'] / a2['employes']
a2 = a2.sort_values('ca', ascending=False).reset_index(drop=True)
a2.insert(0, 'rang', range(1, len(a2) + 1))
a2.round(2).to_csv(os.path.join(P.preuves(4), 'a2_magasins.csv'), index=False, sep=';')
a2[['rang', 'magasin', 'ville', 'ca', 'ecart_moyenne_pct', 'rang_moyen', 'mois_1er', 'mois_dernier', 'panier_moyen', 'taux_remise_pct', 'evolution_2005_vs_2001_pct']].round(2)"""))

C.append(code("""a2[['rang', 'magasin', 'surface', 'employes', 'ca_par_m2', 'ca_par_employe']].round(2)"""))

C.append(code("""# Le classement par CA tient-il quand on rapporte le CA a la taille du magasin ?
a2['rang_ca_par_m2'] = a2['ca_par_m2'].rank(ascending=False).astype(int)
a2['rang_ca_par_employe'] = a2['ca_par_employe'].rank(ascending=False).astype(int)
corr = a2[['ca', 'surface', 'employes', 'tickets']].astype(float).corr().round(3)
corr.to_csv(os.path.join(P.preuves(4), 'a2_correlations.csv'), sep=';')
print('Correlation CA / surface :', corr.loc['ca', 'surface'], '| CA / employes :', corr.loc['ca', 'employes'],
      '| CA / tickets :', corr.loc['ca', 'tickets'], '(12 magasins seulement)')
a2[['magasin', 'rang', 'rang_ca_par_m2', 'rang_ca_par_employe']]"""))

C.append(code("""# Le rang mensuel d'un magasin est-il stable ? Exemple du premier et du dernier magasin au total
rangs = km_rang.select('magasin', 'annee_mois', 'rang_mois').toPandas().pivot(index='annee_mois', columns='magasin', values='rang_mois')
rangs.to_csv(os.path.join(P.preuves(4), 'a2_rangs_mensuels.csv'), sep=';')
stab = rangs.agg(['min', 'max', 'mean', 'std']).T.round(2).sort_values('mean')
stab.to_csv(os.path.join(P.preuves(4), 'a2_stabilite_rangs.csv'), sep=';')
stab"""))

C.append(md("""**Resultats et interpretation A2**

- Les 12 magasins sont tres proches : tous sont a moins de 3,1 % de la moyenne sur cinq ans.
- Le classement mensuel est instable : **chaque magasin a ete au moins un mois premier et au moins un mois
  dernier**. Un mauvais mois ne suffit donc pas a juger un magasin ; il faut regarder plusieurs mois ou l'annee.
- Rapporte a la surface ou au nombre d'employes, le classement change completement (Buffalo, premier en CA, est
  8e en CA par unite de surface). Un CA plus eleve ne prouve pas qu'un magasin est plus performant.

**Limites** : unite de surface non indiquee (valeurs de 5,8 a 8,9 millions) ; seulement 12 magasins, les
correlations sont fragiles ; pas de donnees sur la zone de chalandise, les clients ou les prix locaux."""))


# ---------------------------------------------------------------------------------------------
# Etape 5 : visualisations
# ---------------------------------------------------------------------------------------------
C.append(md("""## Etape 5 - Produire des visualisations

Trois graphiques complementaires, chacun dessine a partir d'un petit tableau deja agrege par Spark :

| Graphique | Question | Type | Pourquoi ce type |
|---|---|---|---|
| G1 | Q1 : quelles categories font le CA ? | Barres horizontales | Comparer 9 categories, noms longs lisibles |
| G2 | Q3 : comment se situe chaque magasin ? | Barres + ligne de reference (moyenne) | Comparer 12 magasins a la moyenne |
| G3 | Comment evoluent les ventes dans le temps ? | Courbe | Evolution sur 60 mois |

Regles appliquees : titre explicite, axes nommes avec leur unite, une seule couleur par graphique (une seule
serie : pas de legende necessaire), source indiquee sous le graphique, grille discrete, pas d'effet 3D.
Les montants sont en **millions d'unites monetaires** (unite non indiquee dans la source, dollar probable)."""))

C.append(code("""import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
BLEU, ENCRE, ENCRE2, GRILLE, FOND = '#2a78d6', '#0b0b0b', '#52514e', '#e4e3df', '#ffffff'
plt.rcParams.update({'font.size': 10, 'axes.edgecolor': ENCRE2, 'axes.labelcolor': ENCRE, 'xtick.color': ENCRE2,
                     'ytick.color': ENCRE2, 'axes.spines.top': False, 'axes.spines.right': False,
                     'figure.facecolor': FOND, 'axes.facecolor': FOND, 'axes.titleweight': 'bold'})
millions = mtick.FuncFormatter(lambda x, _: f'{x / 1e6:,.0f}'.replace(',', ' '))

from matplotlib.transforms import blended_transform_factory

def source(ax, texte, y=-0.12):
    \"\"\"Source sous l'axe des abscisses (y en fraction de la hauteur du graphique).\"\"\"
    ax.text(0, y, 'Source : ' + texte + ' - TPCx-BB, donnees synthetiques ; calcul PySpark, equipe MaxMag',
            transform=ax.transAxes, fontsize=8, color=ENCRE2, ha='left', va='top')

def reference(ax, valeur, texte):
    \"\"\"Ligne de reference horizontale, etiquetee a droite du graphique, hors de la zone des donnees.\"\"\"
    ax.axhline(valeur, color=ENCRE2, linewidth=1.2, linestyle='--')
    ax.text(1.01, valeur, texte, transform=blended_transform_factory(ax.transAxes, ax.transData),
            ha='left', va='center', fontsize=9, color=ENCRE2)

def pct(v, signe=False):
    if abs(v) < 0.05:
        return '0,0 %'
    return (f'{v:+.1f} %' if signe else f'{v:.1f} %').replace('.', ',')

def enregistrer(fig, nom):
    chemin = os.path.join(P.preuves(5), nom)
    fig.savefig(chemin, dpi=150, bbox_inches='tight')
    print('graphique enregistre :', os.path.relpath(chemin, P.REPO_J4))"""))

C.append(md("""### G1 - Chiffre d'affaires par categorie de produits (2001-2005)"""))

C.append(code("""g1 = a1.sort_values('ca')           # tableau de l'analyse A1 : 9 lignes
fig, ax = plt.subplots(figsize=(10, 5))
ax.barh(g1['categorie'], g1['ca'], color=BLEU, height=0.7)
for y, (ca, part) in enumerate(zip(g1['ca'], g1['part_ca_total_pct'])):
    ax.text(ca + 4e6, y, pct(part), va='center', fontsize=9, color=ENCRE)
ax.xaxis.set_major_formatter(millions)
ax.set_xlabel(\"Chiffre d'affaires cumule 2001-2005 (millions d'unites monetaires)\")
ax.set_title(\"Music et Books font 72 % du chiffre d'affaires des magasins\", loc='left')
ax.grid(axis='x', color=GRILLE, linewidth=0.8); ax.set_axisbelow(True); ax.tick_params(axis='y', length=0)
ax.set_xlim(0, g1['ca'].max() * 1.12)
source(ax, 'gold/kpi_ventes_categorie_mois (540 lignes) ; % = part du CA total', y=-0.14)
enregistrer(fig, 'g1_ca_par_categorie.png'); plt.show()"""))

C.append(md("""**Lecture G1** : Music (45,2 %) et Books (26,7 %) dominent ; les sept autres categories pesent chacune moins de
7 %. Ce classement est le meme chaque mois (analyse A1). Limite : la part d'une categorie suit la taille de son
catalogue (Music compte environ 46 % des produits vendus) ; ce n'est pas une mesure de l'attractivite d'un produit."""))

C.append(md("""### G2 - Chiffre d'affaires par magasin compare a la moyenne (2001-2005)"""))

C.append(code("""g2 = a2.sort_values('ca', ascending=False)
moyenne = g2['ca'].mean()
fig, ax = plt.subplots(figsize=(11, 5))
ax.bar(g2['magasin'], g2['ca'], color=BLEU, width=0.7)
reference(ax, moyenne, f'moyenne\\n{moyenne / 1e6:.1f} M'.replace('.', ','))
for x, (ca, e) in enumerate(zip(g2['ca'], g2['ecart_moyenne_pct'])):
    ax.text(x, ca + 0.15e6, pct(e, signe=True), ha='center', fontsize=8, color=ENCRE)
ax.set_ylim(100e6, g2['ca'].max() * 1.02)     # axe tronque, signale dans le titre de l'axe
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x / 1e6:.0f}'))
ax.set_ylabel(\"CA cumule 2001-2005 (millions)\\naxe commencant a 100 M\")
ax.set_xlabel('Magasin (du plus fort au plus faible CA)')
ax.set_title('Les 12 magasins restent a moins de 3,1 % de la moyenne', loc='left')
ax.grid(axis='y', color=GRILLE, linewidth=0.8); ax.set_axisbelow(True)
plt.setp(ax.get_xticklabels(), rotation=30, ha='right')
source(ax, 'gold/kpi_ventes_magasin_mois (720 lignes) ; % = ecart a la moyenne des 12 magasins', y=-0.32)
enregistrer(fig, 'g2_ca_par_magasin.png'); plt.show()"""))

C.append(md("""**Lecture G2** : l'axe vertical commence a 100 millions pour rendre visibles des ecarts faibles : les barres
ne sont donc pas proportionnelles au CA, seuls les pourcentages au-dessus des barres doivent etre lus. Buffalo
depasse la moyenne de 3,1 %, Sherwood est 2,0 % en dessous. Limite : CA brut, non rapporte a la taille du magasin
(le classement change par unite de surface, analyse A2)."""))

C.append(md("""### G3 - Evolution mensuelle du chiffre d'affaires (janvier 2001 a decembre 2005)

Les mois n'ont pas tous le meme nombre de jours : la courbe montre le **CA moyen par jour de vente** de chaque
mois, pour qu'un mois de fevrier ne paraisse pas faible seulement parce qu'il est court."""))

C.append(code("""g3 = par_mois.copy()          # tableau de l'etape 3 : 60 lignes
g3['date'] = pd.to_datetime(g3['annee_mois'] + '-01')
moy_j = g3['ca_par_jour'].mean()
fig, ax = plt.subplots(figsize=(11, 4.8))
ax.plot(g3['date'], g3['ca_par_jour'], color=BLEU, linewidth=2, marker='o', markersize=3.5)
reference(ax, moy_j, f'moyenne\\n{moy_j / 1e3:.0f} k / jour')
for i in (g3['ca_par_jour'].idxmin(), g3['ca_par_jour'].idxmax()):
    r = g3.loc[i]
    ax.annotate(f\"{r['annee_mois']} : {r['ca_par_jour'] / 1e3:.0f} k\", (r['date'], r['ca_par_jour']),
                xytext=(0, 10 if i == g3['ca_par_jour'].idxmax() else -16), textcoords='offset points',
                ha='center', fontsize=8, color=ENCRE)
ax.set_ylim(moy_j * 0.9, moy_j * 1.1)
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x / 1e3:.0f}'))
ax.set_ylabel('CA moyen par jour de vente\\n(milliers, axe de -10 % a +10 % de la moyenne)')
ax.set_xlabel('Mois')
ax.set_title(\"Le chiffre d'affaires par jour reste entre -3 % et +5 % de sa moyenne sur cinq ans\", loc='left')
ax.grid(axis='y', color=GRILLE, linewidth=0.8); ax.set_axisbelow(True)
source(ax, 'silver/ventes_detaillees (674 211 lignes), agregation par mois', y=-0.14)
enregistrer(fig, 'g3_evolution_ca_mensuel.png'); plt.show()
print(f\"Ecart du mois le plus faible et du plus fort a la moyenne : {100 * (g3['ca_par_jour'].min() / moy_j - 1):.1f} % / {100 * (g3['ca_par_jour'].max() / moy_j - 1):+.1f} %\")"""))

C.append(md("""**Lecture G3** : aucune tendance ni saisonnalite nette : le CA par jour oscille autour de sa moyenne sans pic
de fin d'annee. Limite : jeu synthetique ; une vraie enseigne montrerait en general un pic en decembre."""))

# ---------------------------------------------------------------------------------------------
# Etape 6 : constats
# ---------------------------------------------------------------------------------------------
C.append(md("""## Etape 6 - Formuler des constats

Chaque constat distingue le **resultat observe** (mesure dans les etapes 2 a 5), son **interpretation possible**,
la **limite** de l'analyse et une **recommandation**. Une correlation ne demontre pas une causalite, et les donnees
TPCx-BB sont synthetiques : ces constats valident une demarche, ils ne decrivent pas une enseigne reelle."""))

C.append(code("""# Rappel des chiffres sur lesquels s'appuient les constats (recalcules, pas recopies)
appui = {
    'part CA Music + Books (%)': round(a1.loc[a1['categorie'].isin(['Music', 'Books']), 'part_ca_total_pct'].sum(), 2),
    'mois ou Music est 1re (sur 60)': int(a1.loc[a1['categorie'] == 'Music', 'mois_au_1er_rang'].iloc[0]),
    'part du CA des 1 % premiers produits (%)': round(float(concentration['part du CA des 1 % premiers produits (%)'].iloc[0]), 2),
    'ecart max d un magasin a la moyenne (%)': round(a2['ecart_moyenne_pct'].abs().max(), 2),
    'magasins passes au 1er ET au dernier rang (sur 12)': int(((stab['min'] == 1) & (stab['max'] == 12)).sum()),
    'panier moyen / panier median': f\"{stats_ticket.loc['montant_ticket', 'moyenne']:.2f} / {stats_ticket.loc['montant_ticket', 'mediane']:.2f}\",
    'indice de saisonnalite min et max': f\"{saison['indice_vs_moyenne'].min()} et {saison['indice_vs_moyenne'].max()}\",
}
pd.Series(appui, name='valeur').to_frame()"""))

C.append(code("""CONSTATS = pd.DataFrame([
    ('C1', 'Concentration sur deux categories',
     f\"Music et Books font {appui['part CA Music + Books (%)']} % du CA ; Music est premiere {appui['mois ou Music est 1re (sur 60)']} mois sur 60 ; 1 % des produits fait {appui['part du CA des 1 % premiers produits (%)']} % du CA.\",
     \"Le poids de Music vient surtout de la taille de son catalogue (46 % des produits vendus) : un produit Music rapporte en moyenne moins qu'un produit Clothing & Accessories.\",
     'Pas de marge fiable (coûts non qualifies) : on ne sait pas si ces categories sont rentables ; noms de produits aleatoires.',
     \"Suivre le CA moyen par produit et par categorie en plus du CA total ; analyser la rentabilite par categorie quand les couts seront qualifies.\"),
    ('C2', 'Magasins homogenes, classement mensuel instable',
     f\"Tous les magasins sont a moins de {appui['ecart max d un magasin a la moyenne (%)']} % de la moyenne sur cinq ans ; {appui['magasins passes au 1er ET au dernier rang (sur 12)']} magasins sur 12 ont ete au moins un mois premier et au moins un mois dernier.\",
     \"Les ecarts d'un mois a l'autre tiennent surtout de la variation aleatoire, pas d'une difference durable entre magasins.\",
     \"12 magasins seulement ; CA brut non rapporte a la taille : par unite de surface, le classement change completement.\",
     \"Comparer les magasins sur 12 mois glissants et rapporter le CA a la surface ou aux effectifs avant de fixer des objectifs ou d'alerter la direction.\"),
    ('C3', 'Ventes regulieres, paniers asymetriques',
     f\"Indice de saisonnalite entre {appui['indice de saisonnalite min et max']} ; CA par jour entre -3 % et +5 % de sa moyenne ; panier moyen / median : {appui['panier moyen / panier median']}.\",
     \"Aucun effet calendrier exploitable ; la moyenne du panier est tiree vers le haut par les gros tickets.\",
     \"Regularite tres probablement due a la generation synthetique des donnees ; pas de donnees clients pour expliquer les gros paniers.\",
     \"Presenter le panier median a cote du panier moyen ; comparer les mois en CA par jour ou au meme mois N-1, jamais deux mois bruts.\"),
], columns=['code', 'constat', 'resultat_observe', 'interpretation_possible', 'limite', 'recommandation'])
CONSTATS.to_csv(os.path.join(P.preuves(6), 'synthese_constats.csv'), index=False, sep=';')
CONSTATS[['code', 'constat', 'resultat_observe']]"""))

C.append(md("""**Synthese** : les KPI sont definis, calcules a partir de Gold et controles (0 ecart avec les jeux d'indicateurs).
Les donnees repondent bien a Q1 (categories) et a Q3 (magasins), avec deux reserves fortes : la marge n'est pas
exploitable et les retours ne sont pas encore prepares. Ces deux points sont les prochaines evolutions du Data Lake
(Silver pour store_returns et qualification des couts)."""))


def selection(cellules, derniere):
    """Garde les cellules jusqu'a l'etape `derniere` incluse et ecrit le plan du notebook."""
    gardees, etape = [], 0
    for cel in cellules:
        m = re.match(r'## Etape (\d+) - (.*)', cel.source) if cel.cell_type == 'markdown' else None
        if m:
            etape = int(m.group(1))
        if etape <= derniere:
            gardees.append(cel)
    titres = [re.match(r'## Etape (\d+) - (.*)', c.source).groups() for c in gardees
              if c.cell_type == 'markdown' and c.source.startswith('## Etape')]
    plan = '| Partie | Contenu |\n|---|---|\n| 0 | Import des bibliotheques et session Spark |\n'
    plan += '\n'.join(f'| {n} | Etape {n} : {t.splitlines()[0][0].lower() + t.splitlines()[0][1:]} |' for n, t in titres)
    gardees[0].source = gardees[0].source.replace('{PLAN}', plan)
    return gardees


if __name__ == "__main__":
    nb = nbf.v4.new_notebook()
    nb.cells = selection(C, int(sys.argv[2]) if len(sys.argv) > 2 else 99)
    nb.metadata["kernelspec"] = {"name": "retail-tp4", "display_name": "Python - Retail TP4", "language": "python"}
    nbf.write(nb, sys.argv[1])
    print("notebook construit :", sys.argv[1], "|", len(nb.cells), "cellules")
