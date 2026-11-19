"""Schemas des sources retenues pour l'UC-06.

Les fichiers .dat de TPCx-BB n'ont pas de ligne d'en-tete : les noms de colonnes sont ceux du
schema TPCx-BB (derive de TPC-DS), deja releves et verifies au Jour 1 (UC01_inventaire_sources.xlsx,
feuille Colonnes). Les types cibles sont ceux du schema de reference ; ils sont controles a l'etape 3
avant d'etre appliques.
"""
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, DateType, DecimalType

I, S, D = IntegerType(), StringType(), DateType()
M = DecimalType(7, 2)     # montants et prix (2 decimales dans les fichiers)
P = DecimalType(5, 2)     # pourcentages (taux de taxe)


def _schema(cols):
    return StructType([StructField(n, t, True) for n, t in cols])


SOURCES = {
    # Fait : une ligne = un article d'un ticket de caisse
    "store_sales": {
        "granularite": "un article d'un ticket de caisse (ticket + produit, parfois repete)",
        "cle": ["ss_ticket_number", "ss_item_sk"],
        "colonnes": [
            ("ss_sold_date_sk", I), ("ss_sold_time_sk", I), ("ss_item_sk", I), ("ss_customer_sk", I),
            ("ss_cdemo_sk", I), ("ss_hdemo_sk", I), ("ss_addr_sk", I), ("ss_store_sk", I), ("ss_promo_sk", I),
            ("ss_ticket_number", I), ("ss_quantity", I), ("ss_wholesale_cost", M), ("ss_list_price", M),
            ("ss_sales_price", M), ("ss_ext_discount_amt", M), ("ss_ext_sales_price", M),
            ("ss_ext_wholesale_cost", M), ("ss_ext_list_price", M), ("ss_ext_tax", M), ("ss_coupon_amt", M),
            ("ss_net_paid", M), ("ss_net_paid_inc_tax", M), ("ss_net_profit", M),
        ],
    },
    # Dimension produit : une ligne = un produit
    "item": {
        "granularite": "un produit",
        "cle": ["i_item_sk"],
        "colonnes": [
            ("i_item_sk", I), ("i_item_id", S), ("i_rec_start_date", D), ("i_rec_end_date", D), ("i_item_desc", S),
            ("i_current_price", M), ("i_wholesale_cost", M), ("i_brand_id", I), ("i_brand", S), ("i_class_id", I),
            ("i_class", S), ("i_category_id", I), ("i_category", S), ("i_manufact_id", I), ("i_manufact", S),
            ("i_size", S), ("i_formulation", S), ("i_color", S), ("i_units", S), ("i_container", S),
            ("i_manager_id", I), ("i_product_name", S),
        ],
    },
    # Dimension magasin : une ligne = un magasin
    "store": {
        "granularite": "un magasin",
        "cle": ["s_store_sk"],
        "colonnes": [
            ("s_store_sk", I), ("s_store_id", S), ("s_rec_start_date", D), ("s_rec_end_date", D),
            ("s_closed_date_sk", I), ("s_store_name", S), ("s_number_employees", I), ("s_floor_space", I),
            ("s_hours", S), ("s_manager", S), ("s_market_id", I), ("s_geography_class", S), ("s_market_desc", S),
            ("s_market_manager", S), ("s_division_id", I), ("s_division_name", S), ("s_company_id", I),
            ("s_company_name", S), ("s_street_number", S), ("s_street_name", S), ("s_street_type", S),
            ("s_suite_number", S), ("s_city", S), ("s_county", S), ("s_state", S), ("s_zip", S),
            ("s_country", S), ("s_gmt_offset", P), ("s_tax_precentage", P),
        ],
    },
    # Dimension calendrier : une ligne = un jour
    "date_dim": {
        "granularite": "un jour calendaire",
        "cle": ["d_date_sk"],
        "colonnes": [
            ("d_date_sk", I), ("d_date_id", S), ("d_date", D), ("d_month_seq", I), ("d_week_seq", I),
            ("d_quarter_seq", I), ("d_year", I), ("d_dow", I), ("d_moy", I), ("d_dom", I), ("d_qoy", I),
            ("d_fy_year", I), ("d_fy_quarter_seq", I), ("d_fy_week_seq", I), ("d_day_name", S),
            ("d_quarter_name", S), ("d_holiday", S), ("d_weekend", S), ("d_following_holiday", S),
            ("d_first_dom", I), ("d_last_dom", I), ("d_same_day_ly", I), ("d_same_day_lq", I),
            ("d_current_day", S), ("d_current_week", S), ("d_current_month", S), ("d_current_quarter", S),
            ("d_current_year", S),
        ],
    },
}


def noms(table):
    return [n for n, _ in SOURCES[table]["colonnes"]]


def schema_cible(table):
    return _schema(SOURCES[table]["colonnes"])


def schema_texte(table):
    """Toutes les colonnes en texte : lecture brute, sans perte, pour l'exploration."""
    return _schema([(n, S) for n in noms(table)])
