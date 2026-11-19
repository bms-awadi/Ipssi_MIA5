"""Construit UC06_notebooks/01_exploration.ipynb (UC-06 etape 3)."""
import nbformat as nbf, sys
md, code = nbf.v4.new_markdown_cell, nbf.v4.new_code_cell
C = []
C.append(md("""# UC-06 Etape 3 - Exploration des donnees avec PySpark
Team **MaxMag** : Maxime DANINO - Awadi BEDJA - Gurvan Godin

Sources explorees (retenues a l'etape 2) : **store_sales**, **item**, **store**, **date_dim**, lues dans la zone Bronze de HDFS.
Pour chaque source : chargement, schema, echantillon, comptage, valeurs manquantes, doublons, valeurs atypiques.

Methode : chaque fichier est lu **tout en texte** avec les noms de colonnes du schema TPCx-BB (`schemas.py`), puis
converti vers les types cibles. Les valeurs non convertibles sont comptees : c'est le controle des types."""))
C.append(code("""import os, sys
sys.path.insert(0, os.path.expanduser('~/retail-intelligence/tp3/scripts'))
from pyspark.sql import functions as F
import config as C, schemas as SC
spark = C.get_spark('UC06-etape3-exploration')
print('Spark', spark.version, '|', spark.sparkContext.master)"""))
C.append(code("""def lire(table, lot='initial'):
    \"\"\"Lecture brute (texte) d'une table Bronze, avec les noms de colonnes du schema.\"\"\"
    return spark.read.option('sep', '|').schema(SC.schema_texte(table)).csv(C.bronze(table, lot))

def typer(df, table):
    \"\"\"Conversion vers les types cibles (sans rien supprimer).\"\"\"
    return df.select([F.col(n).cast(t).alias(n) for n, t in SC.SOURCES[table]['colonnes']])

def controle_types(brut, table):
    \"\"\"Nombre de valeurs non vides que la conversion transforme en null (= type incorrect).\"\"\"
    exprs = [F.sum((F.col(n).isNotNull() & F.col(n).cast(t).isNull()).cast('int')).alias(n)
             for n, t in SC.SOURCES[table]['colonnes']]
    r = brut.agg(*exprs).collect()[0].asDict()
    return {k: v for k, v in r.items() if v}

def valeurs_manquantes(df):
    r = df.select([F.sum(F.col(c).isNull().cast('int')).alias(c) for c in df.columns]).collect()[0].asDict()
    return {k: v for k, v in r.items() if v}

def profil(table, lot='initial'):
    brut = lire(table, lot)
    n = brut.count()
    print(f'{table} / {lot} : {n} lignes, {len(brut.columns)} colonnes')
    print('valeurs manquantes (null) :', valeurs_manquantes(brut) or 'aucune')
    print('valeurs non convertibles vers le type cible :', controle_types(brut, table) or 'aucune')
    print('lignes entierement identiques en double :', n - brut.distinct().count())
    return typer(brut, table)"""))

# store_sales
C.append(md("## 1. store_sales (ventes en magasin)"))
C.append(code("""ventes = profil('store_sales')
ventes.printSchema()
ventes.select('ss_sold_date_sk','ss_store_sk','ss_item_sk','ss_ticket_number','ss_quantity','ss_sales_price','ss_ext_sales_price','ss_net_paid','ss_net_profit').show(5)"""))
C.append(code("""# Doublons de la cle candidate ticket + produit : sont-ce de vraies copies ?
cle = ['ss_ticket_number', 'ss_item_sk']
dup = ventes.groupBy(*cle).count().filter('count > 1')
print('couples ticket+produit repetes :', dup.count(), '| lignes concernees :', dup.agg(F.sum('count')).collect()[0][0])
exemple = dup.orderBy('ss_ticket_number').first()
ventes.filter((F.col('ss_ticket_number') == exemple.ss_ticket_number) & (F.col('ss_item_sk') == exemple.ss_item_sk)) \\
      .select(*cle, 'ss_sold_time_sk', 'ss_promo_sk', 'ss_quantity', 'ss_sales_price', 'ss_net_paid').show()"""))
C.append(code("""# Valeurs atypiques : statistiques des mesures
ventes.select('ss_quantity','ss_sales_price','ss_ext_sales_price','ss_net_paid','ss_net_profit').summary('min','25%','50%','75%','max','mean').show()
ventes.agg(F.sum((F.col('ss_quantity') <= 0).cast('int')).alias('quantite_<=0'),
           F.sum((F.col('ss_sales_price') <= 0).cast('int')).alias('prix_vente_<=0'),
           F.sum((F.col('ss_net_paid') <= 0).cast('int')).alias('net_paye_<=0'),
           F.sum((F.col('ss_net_profit') < 0).cast('int')).alias('marge_<0'),
           F.sum((F.col('ss_sales_price') > F.col('ss_list_price')).cast('int')).alias('prix_vente_>_prix_catalogue')).show()"""))
C.append(code("""# Coherence des montants : regles de calcul TPC-DS verifiees ligne a ligne (tolerance 1 centime)
ecart = lambda a, b: (F.abs(a - b) > 0.01).cast('int')
ventes.agg(
  F.sum(ecart(F.col('ss_ext_sales_price'), F.col('ss_sales_price') * F.col('ss_quantity'))).alias('ext_sales != prix x qte'),
  F.sum(ecart(F.col('ss_ext_list_price'), F.col('ss_list_price') * F.col('ss_quantity'))).alias('ext_list != catalogue x qte'),
  F.sum(ecart(F.col('ss_net_paid'), F.col('ss_ext_sales_price') - F.col('ss_coupon_amt'))).alias('net_paid != ext_sales - coupon'),
  F.sum(ecart(F.col('ss_net_paid_inc_tax'), F.col('ss_net_paid') + F.col('ss_ext_tax'))).alias('net_paid_inc_tax != net_paid + taxe'),
  F.sum(ecart(F.col('ss_net_profit'), F.col('ss_net_paid') - F.col('ss_ext_wholesale_cost'))).alias('profit != net_paid - cout')).show(vertical=True)"""))

# item
C.append(md("## 2. item (produits)"))
C.append(code("""produits = profil('item')
produits.select('i_item_sk','i_item_id','i_category','i_class','i_brand','i_current_price','i_product_name').show(5, truncate=False)
produits.agg(F.countDistinct('i_item_sk').alias('i_item_sk distincts'), F.countDistinct('i_item_id').alias('i_item_id distincts'),
             F.min('i_current_price').alias('prix_min'), F.max('i_current_price').alias('prix_max')).show()
produits.groupBy('i_category').count().orderBy(F.desc('count')).show(truncate=False)"""))
C.append(code("""# Produits sans marque ou sans description
produits.filter(F.col('i_brand').isNull() | F.col('i_item_desc').isNull()).select('i_item_sk','i_category','i_brand','i_brand_id','i_item_desc').show(truncate=40)"""))

# store
C.append(md("## 3. store (magasins)"))
C.append(code("""magasins = profil('store')
magasins.select('s_store_sk','s_store_id','s_store_name','s_city','s_state','s_number_employees','s_floor_space','s_closed_date_sk').show(truncate=False)
magasins.agg(F.countDistinct('s_store_id').alias('s_store_id distincts')).show()"""))

# date_dim
C.append(md("## 4. date_dim (calendrier)"))
C.append(code("""dates = profil('date_dim')
dates.select('d_date_sk','d_date','d_year','d_moy','d_qoy','d_day_name','d_weekend','d_holiday').show(5)
dates.agg(F.min('d_date'), F.max('d_date'), F.min('d_date_sk'), F.max('d_date_sk')).show()
# d_date_sk = nombre de jours depuis le 01/01/1900 ? (dans TPC-DS d'origine c'est un jour julien, 2415021 pour cette date)
dates.agg((F.max('d_date_sk') - F.min('d_date_sk') + 1).alias('plage_sk'), F.count('*').alias('jours'),
          F.sum((F.datediff('d_date', F.lit('1900-01-01')) != F.col('d_date_sk')).cast('int')).alias('sk_differents_jours_depuis_1900')).show()
dates.groupBy('d_holiday').count().show()"""))
C.append(code("""# Date de fermeture des magasins : s_closed_date_sk renseigne pour les 12 magasins, que represente-t-il ?
magasins.join(dates, magasins.s_closed_date_sk == dates.d_date_sk, 'left') \\
        .select('s_store_sk', 's_store_name', 's_rec_start_date', 's_closed_date_sk', F.col('d_date').alias('date_fermeture')).orderBy('s_store_sk').show()"""))

# liens
C.append(md("## 5. Liens entre les sources (preparation des jointures)"))
C.append(code("""def orphelins(fait, cle_fait, dim, cle_dim):
    return fait.join(dim.select(F.col(cle_dim).alias(cle_fait)), cle_fait, 'left_anti').count()
print('ventes dont le produit est absent de item (lot initial) :', orphelins(ventes, 'ss_item_sk', produits, 'i_item_sk'))
print('ventes dont le magasin est absent de store             :', orphelins(ventes, 'ss_store_sk', magasins, 's_store_sk'))
print('ventes dont la date est absente de date_dim            :', orphelins(ventes, 'ss_sold_date_sk', dates, 'd_date_sk'))
periode = ventes.join(dates, ventes.ss_sold_date_sk == dates.d_date_sk).agg(F.min('d_date').alias('premiere_vente'), F.max('d_date').alias('derniere_vente'))
periode.show()"""))
C.append(code("""# Lots refresh : nouvelles lignes ou recouvrement ?
ventes_r = typer(lire('store_sales', 'refresh'), 'store_sales')
produits_r = typer(lire('item', 'refresh'), 'item')
print('tickets du refresh deja presents dans le lot initial :', ventes_r.select('ss_ticket_number').distinct().join(ventes.select('ss_ticket_number').distinct(), 'ss_ticket_number').count())
print('produits du refresh deja presents dans le lot initial :', produits_r.join(produits.select('i_item_sk'), 'i_item_sk').count(), 'sur', produits_r.count())
tous_produits = produits.unionByName(produits_r)
print('ventes du refresh dont le produit est absent de item (initial + refresh) :', orphelins(ventes_r, 'ss_item_sk', tous_produits, 'i_item_sk'))
ventes_r.join(dates, ventes_r.ss_sold_date_sk == dates.d_date_sk).agg(F.min('d_date'), F.max('d_date'), F.count('*')).show()"""))
C.append(code("spark.stop()"))

nb = nbf.v4.new_notebook()
nb.metadata["kernelspec"] = {"name": "tp3-retail", "display_name": "Python (TP3 Retail)", "language": "python"}
nb.cells = C
nbf.write(nb, sys.argv[1])
print("ok", sys.argv[1])
