# TP Jour 3 : Traitement et transformation des données avec PySpark

> **Équipe MaxMag :** Maxime DANINO - Awadi BEDJA - Gurvan Godin
> **Mission :** explorer, nettoyer et transformer les données TPCx-BB déposées au Jour 2, produire les zones Silver et Gold en Parquet dans HDFS et contrôler leur qualité.
> **Socle obligatoire :** UC-06 (traitement et transformation des données avec Python/PySpark) · **Compétences :** C02 · C03 · C04 · C05 · C10

## Organisation du travail

Le TP a été mené avec mes camarades :
- les **11 PDF** ont été rédigés ensemble à partir de l'exécution faite sur le poste de **Maxime** (18/11/2026) ;
- **toutes les preuves et les notebooks de ce dossier** proviennent de l'exécution complète des mêmes scripts sur mon poste (19/11/2026), pour que chaque membre ait des preuves issues de sa propre installation.

Les PDF décrivent donc l'exécution du poste de Maxime (dates, heures, versions, chemins). Les sections ci-dessous indiquent, document par document, **quelle valeur lire à la place** dans les preuves de ce dossier, et **où les deux exécutions diffèrent**. Tous les résultats de traitement (volumes, schémas, règles, jointures, indicateurs, contrôles qualité) sont **identiques au caractère près** sur les deux postes, sauf les cas listés.

## Livrables

| Étape | Livrable | Preuves (mon poste) |
|---|---|---|
| 1 - Environnement | [UC06_etape1_fiche_environnement.pdf](UC06_etape1_fiche_environnement.pdf) | [UC06_preuves/etape1/](UC06_preuves/etape1/), [00_test_environnement.ipynb](UC06_notebooks/00_test_environnement.ipynb) |
| 2 - Inventaire des sources | [UC06_etape2_inventaire_sources.pdf](UC06_etape2_inventaire_sources.pdf) | [UC06_preuves/etape2/](UC06_preuves/etape2/) |
| 3 - Exploration | [UC06_etape3_exploration.pdf](UC06_etape3_exploration.pdf) | [01_exploration.ipynb](UC06_notebooks/01_exploration.ipynb), copie HTML dans [UC06_preuves/etape3/](UC06_preuves/etape3/) |
| 4 - Règles de nettoyage | [UC06_etape4_regles_nettoyage.pdf](UC06_etape4_regles_nettoyage.pdf) | [UC06_preuves/etape4/](UC06_preuves/etape4/) |
| 5 - Transformations | [UC06_etape5_transformations.pdf](UC06_etape5_transformations.pdf) | [UC06_preuves/etape5/](UC06_preuves/etape5/) |
| 6 - Données Silver | [UC06_etape6_donnees_silver.pdf](UC06_etape6_donnees_silver.pdf) | [UC06_preuves/etape6/](UC06_preuves/etape6/) |
| 7 - Données Gold | [UC06_etape7_donnees_gold.pdf](UC06_etape7_donnees_gold.pdf) | [UC06_preuves/etape7/](UC06_preuves/etape7/) |
| 8 - Format Parquet | [UC06_etape8_format_parquet.pdf](UC06_etape8_format_parquet.pdf) | [UC06_preuves/etape8/](UC06_preuves/etape8/) |
| 9 - Contrôles qualité | [UC06_etape9_rapport_qualite.pdf](UC06_etape9_rapport_qualite.pdf) | [UC06_preuves/etape9/](UC06_preuves/etape9/) |
| 10 - Organisation des traitements | [UC06_etape10_organisation_traitements.pdf](UC06_etape10_organisation_traitements.pdf) | [UC06_preuves/etape10/](UC06_preuves/etape10/), ce README |
| Questions de recul | [UC06_questions_de_recul.pdf](UC06_questions_de_recul.pdf) | |
| Scripts | | [UC06_traitements/](UC06_traitements/) |

## Ce qui est identique sur les deux postes

- Spark en mode `local[*]`, 16 cœurs, 4 Go pour le driver, Java 21.0.13 pour Spark, client Hadoop 3.5.0, HDFS du Jour 2 (réplication 1) ;
- zone Bronze lue : 36 fichiers, 1 033 993 587 octets, mêmes tailles par source (`store_sales.dat` 92 764 700 + 922 171 octets...) ;
- inventaire, exploration, 17 règles de nettoyage, test des rejets (11 cas sur 11), 3 jointures sans perte, agrégations : fichiers identiques ;
- Silver : 5 tables, 10 fichiers Parquet, 177,8 Mo, **mêmes tailles en octets**, mêmes schémas, 0 bloc sous-répliqué ;
- Gold : 6 jeux (1 826 / 18 000 / 12 / 674 211 / 720 / 540 lignes), chiffre d'affaires 1 290 066 342,21 à chaque niveau ;
- Parquet : mêmes tailles (CSV 87,83 Mo, JSON 336,75 Mo, Parquet 39,14 Mo) et mêmes chiffres d'affaires relus ;
- qualité : 49 contrôles, 47 OK, 2 ALERTE, 0 KO ; vérification de la chaîne : aucun problème.

## Valeurs à lire dans les preuves de ce dossier

### Valables dans tous les documents

| Dans les documents (poste de Maxime) | Dans les preuves (mon poste) |
|---|---|
| machine `LAPTOP-5IITUUUR` | `PICOXIA-03` |
| compte Linux `maxim`, `/home/maxim/...` | `abedja`, `/home/abedja/...` |
| Ubuntu 24.04.5, noyau 6.18.33.2 | Ubuntu 22.04.5, noyau 6.6.87.2 |
| Python 3.12.3 | Python 3.10.12 |
| PySpark / Spark 4.3.0 | PySpark / Spark 4.2.1 |
| exécution du 18/11/2026 | exécution du 19/11/2026 |
| preuves écrites dans le dépôt (`J3/UC06_preuves`) | preuves écrites dans `~/retail-intelligence/tp3/UC06_preuves`, puis copiées ici |

### Par document

| PDF | Poste de Maxime | Dans les preuves (mon poste) |
|---|---|---|
| Étape 1 | vérification du 18/11/2026 à 12:26:38, session Spark créée en 4,6 s | 19/11/2026 à 11:05:12, en 4,7 s (`journal_verif_environnement.txt`) |
| Étape 1 | PySpark 4.3.0, 92 paquets | PySpark 4.2.1, 95 paquets (`requirements_observes.txt`) |
| Étape 1 | `scripts/` et `notebooks/` sont des liens vers le dépôt | copies des scripts du dépôt (voir Différences) |
| Étape 3 | notebook exécuté à 14:33 avec Spark 4.3.0 | 11:23 avec Spark 4.2.1 ; toutes les autres sorties identiques |
| Étape 6 | exécution 15:31:39 - 15:32:57, journal `silver_20261118_153139.json` | 11:29:05 - 11:30:32, `silver_20261119_112905.json` |
| Étape 6 | `fsck_apres_correction_replication.txt` | `fsck_silver.txt` (même résultat : HEALTHY, 0 bloc sous-répliqué) |
| Étape 7 | exécution 16:15:54 - 16:16:18, journal `gold_20261118_161554.json` | 11:33:57 - 11:34:16, `gold_20261119_113357.json` |
| Étape 8 | exécution 16:47:18 | 11:34:26 |
| Étape 8 | écriture CSV / JSON / Parquet : 1,6 / 1,4 / 1,6 s | 4,2 / 1,3 / 1,8 s |
| Étape 8 | lecture complète : 1,9 / 1,6 / 0,5 s ; somme de 2 colonnes : 1,0 / 1,0 / 0,2 s | 1,5 / 1,3 / 0,5 s ; 0,9 / 0,9 / 0,2 s |
| Étape 9 | exécution 17:16:14, rapport `controles_20261118_171614` | 11:36:12, `controles_20261119_113612` |
| Étape 10 | vérification 17:34:27, `logs/traitements` : 12 fichiers (6 journaux) | 11:36:40, 4 fichiers (2 journaux : 1 Silver, 1 Gold) |

## Différences entre les deux exécutions

| Étape | Poste de Maxime | Mon poste | Explication |
|---|---|---|---|
| 1 | PySpark 4.3.0 | PySpark 4.2.1 | `pip install pyspark` prend la plus récente version compatible : la 4.3 n'est pas proposée pour Python 3.10 (`pip index versions pyspark`). Les fonctions utilisées (`try_cast`, mode ANSI) existent depuis Spark 4.0 : aucun résultat ne change. |
| 1 | 92 paquets | 95 paquets | dépendances propres à Python 3.10 (`decorator`, `exceptiongroup`, `overrides`, `tomli` en plus, `ipython_pygments_lexers` en moins) |
| 1 | - | `sudo apt install python3.10-venv` avant l'installation | module `venv` absent par défaut sous Ubuntu 22.04 |
| 1 | - | preuve en plus : `journal_test_spark.txt` | sortie du premier test (`00_test_spark.py`) conservée |
| 1 | scripts et notebooks liés au dépôt par `lier_dossiers_depot.sh` | scripts copiés dans `~/retail-intelligence/tp3/scripts`, notebooks recréés | pour ne pas écraser les preuves de Maxime ; le script de liaison contient un chemin de son poste et n'est pas utilisé |
| 2 | `colonnes_sources.csv` : type cible de `s_gmt_offset` = `decimal(7,2)` | `decimal(5,2)` | la preuve de Maxime a été produite avant une modification de `schemas.py` ; sa zone Silver est elle aussi en `decimal(5,2)` (`schema_store.txt`). Ma preuve correspond aux scripts livrés. |
| 6 | 1er essai en 1 000 petits fichiers, Silver à 533 Mo avec 100 % de blocs sous-répliqués, puis correction | aucun incident : 10 fichiers, 177,8 Mo, 0 bloc sous-répliqué dès la première exécution | les deux corrections (regroupement avant écriture, `spark.hadoop.dfs.replication = 1`) étaient déjà dans les scripts. Les preuves de ces incidents sont conservées dans [UC06_preuves/etape6/historique_maxime/](UC06_preuves/etape6/historique_maxime/). |
| 8 | écriture CSV en 1,6 s | 4,2 s | le CSV est écrit en premier et supporte le démarrage des exécuteurs ; les conclusions (Parquet 2,2 fois plus petit, 5 fois plus rapide sur 2 colonnes, seul à relire un CA exact) ne changent pas |

## Corrections à apporter aux documents

Relevées à la relecture, elles s'appliquent aux deux postes.

| Document | Texte des documents | Texte correct |
|---|---|---|
| Étape 1, espace de travail | `scripts/ -> J3/UC06_traitements/`, `notebooks/ -> J3/UC06_notebooks/` | dossier du dépôt `TPs/Jour3/` ; les scripts peuvent aussi être copiés (voir Reproduction) |
| Étape 2, tableau d'inventaire | clé candidate : « 4 136 doublons (initial) et 46 (refresh) » | « 4 136 lignes en trop (initial) et 46 (refresh) » : ce sont des lignes, pas des couples |
| Étape 2, question 4 | « le couple ticket + produit se répète (4 182 cas au total) » | 4 182 lignes en trop au total, soit 4 082 couples répétés (chiffre de l'étape 9) ; 4 038 couples sur le seul lot initial (étape 3) |
| Étape 2, preuve | `s_gmt_offset` en `decimal(7,2)` dans `colonnes_sources.csv` | `decimal(5,2)`, comme dans `schemas.py` et en Silver |

## Reproduction

### Prérequis et versions observées

| Élément | Version |
|---|---|
| Système | Ubuntu 22.04.5 sous WSL 2 (Maxime : 24.04.5) |
| Python | 3.10.12 dans un environnement virtuel `~/retail-intelligence/tp3/.venv` (Maxime : 3.12.3) |
| PySpark / Spark | 4.2.1 (Maxime : 4.3.0), installé par `pip`, sans installation séparée de Spark |
| Java pour Spark | OpenJDK 21 (Java par défaut) ; Hadoop reste sur Java 17 |
| JupyterLab | 4.6.6, noyau « Python (TP3 Retail) » |
| HDFS | Hadoop 3.5.0 du Jour 2, démarré, zone Bronze déposée |

Liste complète des paquets : [requirements_observes.txt](UC06_preuves/etape1/requirements_observes.txt).

### Emplacement des données

| Zone | Chemin | Rôle |
|---|---|---|
| Bronze | `hdfs://localhost:9000/retail-lake/bronze/tpcxbb/<table>/lot=initial\|refresh/` | entrée, en lecture seule |
| Silver | `/retail-lake/silver/` | `store_sales`, `item`, `store`, `date_dim`, `ventes_detaillees` |
| Gold | `/retail-lake/gold/` | `fait_ventes`, `dim_date`, `dim_produit`, `dim_magasin`, `kpi_ventes_magasin_mois`, `kpi_ventes_categorie_mois` |
| Qualité | `/retail-lake/quality/rejets/`, `/retail-lake/quality/controles/` | lignes rejetées, rapports de contrôle |
| Journaux | `/retail-lake/logs/traitements/` | un journal JSON par exécution Silver et Gold |
| Local | `~/retail-intelligence/tp3/data/output/` | résultats de test |
| Repli | `~/kit_tpcxbb` | kit local, si HDFS est indisponible |

Les données ne sont jamais versionnées.

### Paramètres

Tous les chemins et réglages sont dans [config.py](UC06_traitements/config.py) : racine HDFS, zones, mémoire du driver (4 Go), fuseau horaire, réplication 1. `UC06_ESSAI=1` envoie les sorties dans un espace d'essai (`/tmp/uc06_essai`) pendant la mise au point.

### Ordre d'exécution

```bash
# 1. Installation (une fois)
sudo apt install -y python3.10-venv
REPO=/mnt/c/.../TPs/Jour3          # ce dossier, vu depuis WSL
TP=~/retail-intelligence/tp3
mkdir -p $TP/notebooks $TP/UC06_preuves
cp -r $REPO/UC06_traitements $TP/scripts
bash $TP/scripts/installation/installer_environnement.sh

# 2. Environnement (étape 1)
cd $TP
bash scripts/installation/demarrer_hdfs.sh
./scripts/run.sh 00_test_spark.py $TP/UC06_preuves/etape1/journal_test_spark.txt
bash scripts/installation/creer_notebook_test.sh
./scripts/run_pipeline.sh 1 2                          # environnement, inventaire

# 3. Exploration (étape 3, notebook)
./scripts/run_notebook.sh 01_exploration.ipynb scripts/outils/construire_notebook_exploration.py
source .venv/bin/activate
python scripts/outils/ajouter_constats_exploration.py notebooks/01_exploration.ipynb
jupyter nbconvert --to html notebooks/01_exploration.ipynb --output-dir UC06_preuves/etape3
deactivate

# 4. Traitements (étapes 4 à 10)
./scripts/run_pipeline.sh 4 44 5 6                     # 44 = test des règles de rejet
bash scripts/outils/ls_silver.sh   > UC06_preuves/etape6/arborescence_silver.txt
bash scripts/outils/fsck_silver.sh > UC06_preuves/etape6/fsck_silver.txt
./scripts/run_pipeline.sh 7 8 9 10
```

| Étape | Lit | Écrit |
|---|---|---|
| 1 à 5 | Bronze | preuves uniquement (test HDFS dans `/tmp`, supprimé) |
| 6 | Bronze | `silver/`, `quality/rejets/`, `logs/traitements/` |
| 7 | `silver/` | `gold/`, `logs/traitements/` |
| 8 | `silver/`, `gold/` | dossier temporaire HDFS, supprimé |
| 9 | `silver/`, `gold/`, `quality/rejets/` | `quality/controles/` |
| 10 | toutes les zones | preuves uniquement |

Chaque étape est relançable : les sorties sont remplacées dans leur propre dossier (mode `overwrite`), Bronze n'est jamais modifié. Ma chaîne complète a pris environ 30 minutes, notebook compris.

### Sorties produites

Dans HDFS : les zones ci-dessus. Dans [UC06_preuves/](UC06_preuves/) : un dossier par étape avec son journal, ses CSV de résultats et de contrôles, ses schémas, et les deux notebooks exécutés dans [UC06_notebooks/](UC06_notebooks/).

### Contrôles disponibles

- relecture après écriture de chaque table Silver et Gold (lignes, schéma, clé unique, `_SUCCESS`) ;
- `test_nettoyage.py` : 11 lignes synthétiques pour vérifier chaque règle de rejet ;
- `09_controles_qualite.py` : 49 contrôles (complétude, unicité, validité, cohérence, volumes, intégrité référentielle) ;
- `10_verifier_chaine.py` : présence des modules, des sorties, des journaux et relance du test des rejets.

### Limites connues

- Spark en mode local sur un seul PC : les temps ne sont pas ceux d'un cluster ;
- HDFS s'arrête avec WSL : `run.sh` le redémarre et attend la fin du mode sans échec ;
- données synthétiques (marges négatives à 74 %, panier moyen d'environ 14 300) : les chiffres valident la chaîne, pas un commerce réel ;
- seules 4 sources sont traitées (ventes en magasin) ; retours, web et stocks ne sont pas contrôlés ;
- Parquet n'est pas transactionnel : pas d'atomicité ni d'historique (Delta Lake, Iceberg ou Hudi, partie avancée).
