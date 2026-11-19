"""Description des colonnes des tables Silver (dictionnaire de donnees de l'etape 6).

Les colonnes non listees gardent leur nom TPC-DS ; leur sens se deduit du schema de reference.
"""
DESCRIPTIONS = {
    "store_sales": {
        "ss_ligne_id": "Cle de ligne creee par le nettoyage (N06) : empreinte SHA-256 du lot et des valeurs de la ligne",
        "ss_sold_date_sk": "Cle du jour de vente (vers date_dim.d_date_sk)", "ss_sold_time_sk": "Cle de l'heure de vente (seconde de la journee)",
        "ss_item_sk": "Cle du produit (vers item.i_item_sk)", "ss_customer_sk": "Cle du client",
        "ss_cdemo_sk": "Cle du profil demographique client (peut etre vide)", "ss_hdemo_sk": "Cle du profil du menage (peut etre vide)",
        "ss_addr_sk": "Cle de l'adresse client (peut etre vide)", "ss_store_sk": "Cle du magasin (vers store.s_store_sk)",
        "ss_promo_sk": "Cle de la promotion (vide = sans promotion connue)", "ss_ticket_number": "Numero de ticket de caisse (plusieurs lignes par ticket)",
        "ss_quantity": "Quantite vendue (1 a 100)", "ss_wholesale_cost": "Cout unitaire d'achat", "ss_list_price": "Prix catalogue unitaire",
        "ss_sales_price": "Prix de vente unitaire apres remise", "ss_ext_discount_amt": "Remise totale de la ligne = catalogue - vente",
        "ss_ext_sales_price": "Montant de vente de la ligne = prix de vente x quantite (chiffre d'affaires)",
        "ss_ext_wholesale_cost": "Cout d'achat de la ligne", "ss_ext_list_price": "Montant catalogue de la ligne = prix catalogue x quantite",
        "ss_ext_tax": "Taxe de la ligne", "ss_coupon_amt": "Montant du coupon", "ss_net_paid": "Net paye = montant de vente - coupon",
        "ss_net_paid_inc_tax": "Net paye toutes taxes = net paye + taxe", "ss_net_profit": "Marge = net paye - cout d'achat (peut etre negative)",
    },
    "item": {"i_item_sk": "Cle du produit", "i_item_id": "Identifiant metier du produit", "i_category": "Categorie (9 valeurs)",
             "i_class": "Classe de produit", "i_brand": "Marque (texte genere, peut etre vide)", "i_current_price": "Prix courant",
             "i_product_name": "Nom du produit (texte genere)", "i_rec_start_date": "Debut de validite de la fiche",
             "i_rec_end_date": "Fin de validite (toujours vide : version courante)"},
    "store": {"s_store_sk": "Cle du magasin", "s_store_id": "Identifiant metier du magasin", "s_store_name": "Nom du magasin",
              "s_city": "Ville", "s_state": "Etat (code a 2 lettres)", "s_zip": "Code postal (texte, conserve les zeros)",
              "s_number_employees": "Nombre d'employes", "s_floor_space": "Surface de vente"},
    "date_dim": {"d_date_sk": "Cle du jour = nombre de jours depuis le 01/01/1900", "d_date": "Date du jour", "d_year": "Annee",
                 "d_moy": "Mois (1 a 12)", "d_qoy": "Trimestre (1 a 4)", "d_dow": "Jour de la semaine (0 = dimanche)",
                 "d_day_name": "Nom du jour (anglais)", "d_weekend": "Y si samedi ou dimanche"},
    "ventes_detaillees": {
        "ligne_id": "Cle de ligne (= ss_ligne_id de silver/store_sales, qui garde la provenance complete)",
        "ticket": "Numero de ticket", "date_sk": "Cle du jour", "heure_sk": "Cle de l'heure", "magasin_sk": "Cle du magasin",
        "produit_sk": "Cle du produit", "client_sk": "Cle du client", "promotion_sk": "Cle de la promotion",
        "quantite": "Quantite vendue", "prix_catalogue_unitaire": "Prix catalogue unitaire", "prix_vente_unitaire": "Prix de vente unitaire",
        "cout_unitaire": "Cout unitaire d'achat", "montant_catalogue": "Prix catalogue x quantite", "remise": "Montant catalogue - chiffre d'affaires",
        "chiffre_affaires": "Montant apres remise, hors taxe (definition du CA du Jour 1)", "coupon": "Montant du coupon",
        "net_paye": "Chiffre d'affaires - coupon", "taxe": "Taxe", "net_paye_ttc": "Net paye + taxe", "cout": "Cout d'achat de la ligne",
        "marge": "Net paye - cout (peut etre negative)", "lot": "Lot d'origine : initial ou refresh",
        "avec_promotion": "Vrai si une promotion est renseignee", "vente_gratuite": "Vrai si le chiffre d'affaires est nul",
        "taux_remise_ligne": "Remise / montant catalogue x 100", "date_vente": "Date de la vente", "annee": "Annee (partition)",
        "mois": "Mois", "trimestre": "Trimestre", "annee_mois": "Mois au format AAAA-MM", "jour_semaine": "Nom du jour",
        "week_end": "Vrai le samedi et le dimanche", "produit_id": "Identifiant metier du produit", "categorie": "Categorie du produit",
        "classe": "Classe du produit", "marque": "Marque (Inconnu si vide)", "prix_courant": "Prix courant du produit",
        "magasin_id": "Identifiant metier du magasin", "magasin": "Nom du magasin", "ville": "Ville du magasin", "etat": "Etat du magasin",
        "employes": "Nombre d'employes du magasin",
    },
}
PROVENANCE = {"lot": "Lot d'origine : initial ou refresh (N17)", "fichier_source": "Fichier Bronze lu (N17)",
              "date_traitement": "Date et heure du traitement qui a produit la ligne (N17)"}
