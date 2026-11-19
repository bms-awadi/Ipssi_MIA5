"""Regles de nettoyage de l'UC-06 (etape 4), appliquees par les traitements Silver.

Principe : aucune ligne n'est supprimee en silence. Chaque regle de rejet ajoute un motif ;
une ligne avec au moins un motif part en rejet (quality/rejets) avec ses motifs, les autres
sont conservees. Les valeurs atypiques mais plausibles sont conservees.
Le catalogue REGLES documente chaque regle (code, anomalie, regle, action, justification).
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config as C
import schemas as SC

DEBUT_VENTES, FIN_VENTES = "2001-01-01", "2005-12-31"   # periode couverte par les deux lots (etape 3)
TOL = 0.01                                               # tolerance de calcul : 1 centime

# --- Catalogue des regles --------------------------------------------------------------------
REGLES = [
    # code, table, anomalie, regle, action, justification
    ("N01", "toutes", "Type incorrect", "Chaque colonne est convertie vers son type cible (schemas.py) ; une valeur non vide non convertible est une erreur",
     "Rejet de la ligne, motif type_invalide:<colonne>", "Un type faux fausserait les calculs ; la valeur d'origine reste lisible dans Bronze"),
    ("N02", "toutes", "Espaces parasites", "Suppression des espaces en debut et fin de texte ; un texte vide devient null",
     "Correction de la valeur", "Evite qu'un meme libelle apparaisse sous deux formes"),
    ("N03", "store_sales", "Valeur manquante obligatoire", "Date, magasin, produit, ticket, quantite, prix et montants ne doivent pas etre vides",
     "Rejet, motif obligatoire_vide:<colonne>", "Sans ces champs, la vente ne peut etre ni datee, ni rattachee, ni valorisee"),
    ("N04", "store_sales", "Valeur manquante secondaire", "Client, demographie, menage, adresse et promotion peuvent etre vides",
     "Conservation, valeur laissee a null (aucune imputation)", "Vide = information inconnue ; inventer une valeur fausserait les analyses clients et promotions"),
    ("N05", "store_sales", "Doublon", "Ligne strictement identique a une autre (toutes colonnes) : une seule est gardee. Le couple ticket + produit repete n'est PAS un doublon",
     "Suppression des copies exactes (0 constatee), conservation des ventes repetees", "Etape 3 : le meme produit sur un ticket avec promotion, quantite et prix differents est une vente distincte"),
    ("N06", "store_sales", "Absence de cle de ligne", "Creation de ss_ligne_id = empreinte SHA-256 de toutes les colonnes de la ligne et du lot",
     "Ajout d'une colonne", "Donne un identifiant stable a chaque ligne de vente (meme valeur a chaque execution)"),
    ("N07", "store_sales", "Identifiant invalide", "Produit, magasin et date doivent exister dans item, store et date_dim ; identifiants negatifs interdits",
     "Rejet, motif produit_inconnu, magasin_inconnu, date_inconnue ou identifiant_negatif", "Une vente orpheline disparaitrait des jointures et des indicateurs"),
    ("N08", "store_sales", "Date incoherente", f"La date de vente doit etre comprise entre le {DEBUT_VENTES} et le {FIN_VENTES}",
     "Rejet, motif date_hors_periode", "Periode reelle des deux lots ; une date en dehors signale une cle de date erronee"),
    ("N09", "store_sales", "Quantite invalide", "La quantite doit etre superieure ou egale a 1",
     "Rejet, motif quantite_invalide", "Les retours sont dans une autre table : une vente a toujours au moins un article"),
    ("N10", "store_sales", "Montant negatif", "Prix, montants, coupon et taxe ne doivent pas etre negatifs (la marge peut l'etre)",
     "Rejet, motif montant_negatif", "Un prix ou un montant paye negatif n'a pas de sens pour une vente"),
    ("N11", "store_sales", "Montants incoherents", "Montant = prix x quantite ; net paye = montant - coupon ; TTC = net paye + taxe ; marge = net paye - cout (a 1 centime pres)",
     "Rejet, motif calcul_incoherent", "Garantit que les indicateurs calcules a partir de colonnes differentes donnent le meme resultat"),
    ("N12", "store_sales", "Valeur atypique", "Prix de vente a 0, net paye a 0, marge negative",
     "Conservation sans modification, comptees dans le rapport qualite", "Plausibles (gratuite, coupon, vente a perte) ; les supprimer fausserait le chiffre d'affaires et la marge"),
    ("N13", "item", "Valeur manquante obligatoire / doublon", "i_item_sk, i_item_id et i_category obligatoires ; i_item_sk unique sur les deux lots",
     "Rejet, motif obligatoire_vide ou cle_dupliquee", "Ce sont les colonnes de jointure et d'analyse par categorie"),
    ("N14", "item", "Valeur manquante secondaire", "Marque, identifiant de marque, description peuvent etre vides",
     "Conservation, null (libelle Inconnu seulement en Gold)", "Pas d'information fiable pour les remplacer"),
    ("N15", "store", "Colonne incoherente", "s_closed_date_sk renvoie a des dates de 1917 a 2141 pour des magasins actifs de 2001 a 2005",
     "Colonne retiree de Silver", "Laisser la colonne inviterait a l'interpreter comme une fermeture"),
    ("N16", "date_dim", "Colonne incoherente", "d_holiday vaut Y pour tous les jours",
     "Colonne retiree de Silver", "Elle ne permet pas de distinguer un jour ferie"),
    ("N17", "toutes", "Provenance", "Ajout du lot (initial ou refresh), du fichier source et de la date de traitement ; les lots sont reunis",
     "Ajout de 3 colonnes", "Regle de tracabilite du TP ; les ventes du refresh ont besoin des produits du refresh"),
]

COLONNES_RETIREES = {"store": ["s_closed_date_sk"], "date_dim": ["d_holiday"]}
OBLIGATOIRES = {
    "store_sales": ["ss_sold_date_sk", "ss_store_sk", "ss_item_sk", "ss_ticket_number", "ss_quantity", "ss_wholesale_cost",
                    "ss_list_price", "ss_sales_price", "ss_ext_sales_price", "ss_net_paid", "ss_net_paid_inc_tax", "ss_net_profit"],
    "item": ["i_item_sk", "i_item_id", "i_category"],
    "store": ["s_store_sk", "s_store_name"],
    "date_dim": ["d_date_sk", "d_date", "d_year", "d_moy"],
}


# --- Lecture et typage -----------------------------------------------------------------------
def lire_bronze(spark, table, lots=("initial", "refresh")):
    """Reunit les lots presents d'une table Bronze, en texte, avec les colonnes de provenance (N17)."""
    jvm, sc = spark.sparkContext._jvm, spark.sparkContext
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(C.HDFS), sc._jsc.hadoopConfiguration())
    parts = []
    for lot in lots:
        chemin = C.bronze(table, lot)
        if fs.exists(jvm.org.apache.hadoop.fs.Path(chemin)):
            parts.append(spark.read.option("sep", "|").schema(SC.schema_texte(table)).csv(chemin)
                         .withColumn("lot", F.lit(lot)).withColumn("fichier_source", F.input_file_name()))
    df = parts[0]
    for p in parts[1:]:
        df = df.unionByName(p)
    return df.withColumn("date_traitement", F.current_timestamp())


def typer(brut, table):
    """N02 (espaces, vides) puis N01 (conversion) ; renvoie le DataFrame type et la colonne motifs."""
    cols, motifs = [], []
    for n, t in SC.SOURCES[table]["colonnes"]:
        v = F.nullif(F.trim(F.col(n)), F.lit(""))
        # try_cast : Spark 4 est en mode ANSI, un cast() impossible arreterait tout le traitement
        cols.append(v.try_cast(t).alias(n))
        motifs.append(F.when(v.isNotNull() & v.try_cast(t).isNull(), F.lit(f"type_invalide:{n}")))
    prov = [F.col("lot"), F.col("fichier_source"), F.col("date_traitement")]
    return brut.select(*cols, *prov, F.array(*motifs).alias("motifs"))


def ajouter_motifs(df, conditions):
    """conditions : liste (motif, expression booleenne vraie quand la ligne est invalide)."""
    nouv = [F.when(cond, F.lit(m)) for m, cond in conditions]
    return df.withColumn("motifs", F.concat(F.col("motifs"), F.array(*nouv)))


def separer(df):
    """Retire les motifs vides ; renvoie (lignes conservees, lignes rejetees avec leurs motifs)."""
    df = df.withColumn("motifs", F.filter("motifs", lambda m: m.isNotNull()))
    return df.filter(F.size("motifs") == 0).drop("motifs"), df.filter(F.size("motifs") > 0)


def obligatoires(table):
    return [(f"obligatoire_vide:{c}", F.col(c).isNull()) for c in OBLIGATOIRES[table]]


# --- Regles par table ------------------------------------------------------------------------
def nettoyer_item(spark):
    df = typer(lire_bronze(spark, "item"), "item")
    n_par_cle = F.count("*").over(Window.partitionBy("i_item_sk"))
    df = df.withColumn("_n", n_par_cle)
    df = ajouter_motifs(df, obligatoires("item") + [("cle_dupliquee:i_item_sk", F.col("_n") > 1)]).drop("_n")
    return separer(df)


def nettoyer_store(spark):
    df = typer(lire_bronze(spark, "store"), "store")
    df = ajouter_motifs(df, obligatoires("store"))
    ok, rej = separer(df)
    return ok.drop(*COLONNES_RETIREES["store"]), rej


def nettoyer_date_dim(spark):
    df = typer(lire_bronze(spark, "date_dim"), "date_dim")
    df = ajouter_motifs(df, obligatoires("date_dim"))
    ok, rej = separer(df)
    return ok.drop(*COLONNES_RETIREES["date_dim"]), rej


def nettoyer_store_sales(spark, item_ok, store_ok, date_ok, brut=None):
    """brut : DataFrame texte deja lu (utilise par les tests) ; par defaut, lecture de Bronze."""
    noms = SC.noms("store_sales")
    df = typer(brut if brut is not None else lire_bronze(spark, "store_sales"), "store_sales")
    # N05 : copies exactes (toutes les colonnes metier) ; la premiere est gardee
    w = Window.partitionBy(*noms).orderBy("lot")
    df = df.withColumn("_rang", F.row_number().over(w))
    # N07 : liens vers les dimensions (identifiants connus)
    df = (df.join(item_ok.select(F.col("i_item_sk").alias("ss_item_sk"), F.lit(1).alias("_p")), "ss_item_sk", "left")
            .join(store_ok.select(F.col("s_store_sk").alias("ss_store_sk"), F.lit(1).alias("_m")), "ss_store_sk", "left")
            .join(date_ok.select(F.col("d_date_sk").alias("ss_sold_date_sk"), F.col("d_date").alias("_d")), "ss_sold_date_sk", "left"))
    montants = ["ss_wholesale_cost", "ss_list_price", "ss_sales_price", "ss_ext_discount_amt", "ss_ext_sales_price",
                "ss_ext_wholesale_cost", "ss_ext_list_price", "ss_ext_tax", "ss_coupon_amt", "ss_net_paid", "ss_net_paid_inc_tax"]
    ecart = lambda a, b: F.abs(a - b) > TOL
    c = F.col
    conditions = obligatoires("store_sales") + [
        ("doublon_exact", c("_rang") > 1),
        ("identifiant_negatif", (c("ss_item_sk") < 0) | (c("ss_store_sk") < 0) | (c("ss_sold_date_sk") < 0) | (c("ss_ticket_number") < 0)),
        ("produit_inconnu", c("ss_item_sk").isNotNull() & c("_p").isNull()),
        ("magasin_inconnu", c("ss_store_sk").isNotNull() & c("_m").isNull()),
        ("date_inconnue", c("ss_sold_date_sk").isNotNull() & c("_d").isNull()),
        ("date_hors_periode", c("_d").isNotNull() & ((c("_d") < F.lit(DEBUT_VENTES)) | (c("_d") > F.lit(FIN_VENTES)))),
        ("quantite_invalide", c("ss_quantity") < 1),
        ("montant_negatif", F.greatest(*[(c(m) < 0).cast("int") for m in montants]) == 1),
        ("calcul_incoherent", ecart(c("ss_ext_sales_price"), c("ss_sales_price") * c("ss_quantity"))
                              | ecart(c("ss_ext_list_price"), c("ss_list_price") * c("ss_quantity"))
                              | ecart(c("ss_net_paid"), c("ss_ext_sales_price") - c("ss_coupon_amt"))
                              | ecart(c("ss_net_paid_inc_tax"), c("ss_net_paid") + c("ss_ext_tax"))
                              | ecart(c("ss_net_profit"), c("ss_net_paid") - c("ss_ext_wholesale_cost"))),
    ]
    df = ajouter_motifs(df, conditions)
    # N06 : cle de ligne stable
    df = df.withColumn("ss_ligne_id", F.sha2(F.concat_ws("|", c("lot"), *[F.coalesce(c(n).cast("string"), F.lit("")) for n in noms], c("_rang")), 256))
    df = df.drop("_rang", "_p", "_m", "_d")
    ok, rej = separer(df)
    return ok.select("ss_ligne_id", *noms, "lot", "fichier_source", "date_traitement"), rej
