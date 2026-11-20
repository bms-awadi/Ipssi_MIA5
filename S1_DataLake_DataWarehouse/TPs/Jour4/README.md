# TP Jour 4 : Analyse, visualisation et transposition Azure

> **Équipe MaxMag :** Maxime DANINO - Awadi BEDJA - Gurvan Godin
> **Mission :** exploiter les zones Silver et Gold du Jour 3 pour calculer des KPI, des statistiques et des analyses métier avec PySpark, les illustrer par des graphiques, puis étudier la transposition de la plateforme sur Azure.
> **Socle obligatoire :** UC-08 (analyse et visualisation) et UC-07 (transposition Azure, **parcours B : local / open source**) · **Livrables :** J4-01 à J4-11

## Organisation du travail

Le TP a été mené avec mes camarades :
- les **documents** ont été rédigés ensemble à partir de l'exécution faite sur le poste de **Maxime** (19/11/2026) ;
- **toutes les preuves et le notebook de ce dossier** proviennent de l'exécution des mêmes scripts sur mon poste (20/11/2026), sur ma propre zone Silver et Gold produite au Jour 3.

Les documents décrivent donc l'exécution du poste de Maxime (dates, heures, versions, compte Linux). Les sections ci-dessous indiquent **quelle valeur lire à la place** dans les preuves de ce dossier et **où les deux exécutions diffèrent**. Tous les résultats d'analyse (KPI, statistiques, analyses, constats) sont **identiques au caractère près** sur les deux postes.

UC-07 est traité en parcours B : aucune ressource Azure n'a été créée. La cible (ADLS Gen2, espace de noms hiérarchique, RBAC, ACL, Data Factory, Synapse) est documentée ; les opérations de stockage sont démontrées sur notre HDFS, qui joue le rôle d'ADLS Gen2.

## Livrables

| Code | Livrable | Documents | Preuves (mon poste) |
|---|---|---|---|
| J4-01 | Inventaire analytique | [UC08_etape1_selection_donnees.pdf](UC08_etape1_selection_donnees.pdf) | [UC08_preuves/etape1/](UC08_preuves/etape1/) |
| J4-02 | Dictionnaire des KPI | [UC08_etape2_verification_kpi.pdf](UC08_etape2_verification_kpi.pdf) | [UC08_preuves/etape2/](UC08_preuves/etape2/) |
| J4-03 | Analyses statistiques | [UC08_etape3_statistiques.pdf](UC08_etape3_statistiques.pdf) | [UC08_preuves/etape3/](UC08_preuves/etape3/) |
| J4-04 | Analyses métier | [UC08_etape4_analyses_metier.pdf](UC08_etape4_analyses_metier.pdf) | [UC08_preuves/etape4/](UC08_preuves/etape4/) |
| J4-05 | Visualisations | [UC08_etape5_visualisations.pdf](UC08_etape5_visualisations.pdf) | [UC08_preuves/etape5/](UC08_preuves/etape5/) (3 graphiques PNG) |
| J4-06 | Synthèse décisionnelle | [UC08_etape6_constats.pdf](UC08_etape6_constats.pdf) | [UC08_preuves/etape6/](UC08_preuves/etape6/) |
| J4-07 | Architecture Azure | [UC07_etape1_composants_azure.pdf](UC07_etape1_composants_azure.pdf), [UC07_etape6_adf_synapse.pdf](UC07_etape6_adf_synapse.pdf), [UC07_etape7_schema_architecture.pdf](UC07_etape7_schema_architecture.pdf) | [schema_architecture_azure.png](UC07_preuves/etape7/schema_architecture_azure.png) |
| J4-08 | Organisation ADLS Gen2 | [UC07_etape2_organisation_adls.pdf](UC07_etape2_organisation_adls.pdf), [UC07_etape3_hns.pdf](UC07_etape3_hns.pdf) | |
| J4-09 | Sécurité | [UC07_etape5_securite.pdf](UC07_etape5_securite.pdf) | |
| J4-10 | Preuves Azure ou simulation | [UC07_etape4_mise_en_oeuvre.pdf](UC07_etape4_mise_en_oeuvre.pdf) | [journal_demo_stockage_local.txt](UC07_preuves/etape4/journal_demo_stockage_local.txt), [UC07_simulation/](UC07_simulation/) |
| J4-11 | README J4 | ce fichier | |
| | Parcours local et correspondance des services | [UC07_etape8_parcours_local.pdf](UC07_etape8_parcours_local.pdf), [UC07_etape9_correspondance_services.pdf](UC07_etape9_correspondance_services.pdf) | |
| | Questions de recul | [J4_questions_de_recul.pdf](J4_questions_de_recul.pdf) | |
| | Notebook exécuté | | [TP4_Retail_Intelligence.ipynb](UC08_notebooks/TP4_Retail_Intelligence.ipynb), copie HTML [TP4_Retail_Intelligence.html](UC08_preuves/TP4_Retail_Intelligence.html) |
| | Scripts | | [UC08_traitements/](UC08_traitements/), [UC07_traitements/](UC07_traitements/) |

## Ce qui est identique sur les deux postes

- mêmes jeux lus dans HDFS : 1 jeu Silver et 6 jeux Gold, mêmes schémas, mêmes volumes (674 211 lignes de ventes, 720 lignes magasin x mois, 540 lignes catégorie x mois) ;
- mêmes journaux du Jour 3 relus : 49 contrôles qualité, 47 OK, 2 ALERTE ;
- **23 des 30 fichiers de `UC08_preuves` identiques octet pour octet** : tous les CSV d'analyse des étapes 2 à 6, `schemas.txt` et `coherence_jeux.csv` ;
- KPI : CA 1 290 066 342,21, 90 000 tickets, panier moyen 14 334,07 (médiane 13 082,84), taux de remise 49,98 %, 0 ligne en écart entre le recalcul et les jeux Gold ;
- analyses : Music 45,21 % du CA et 1re catégorie les 60 mois, top 1 % des produits = 39,27 % du CA, écart de 5,0 % entre le magasin le plus fort et le plus faible, corrélations CA / tickets 0,826, CA / surface 0,483, CA / employés -0,156 ;
- graphiques : même contenu (mêmes valeurs, titres, annotations, axe de -2,7 % / +4,9 % sur G3) ;
- démonstration UC-07 : même fichier déposé (97 746 octets, 721 lignes), **même empreinte MD5** `f9fff659e0dbe2de1eafa9a01d4ec172` en local et dans HDFS, mêmes ACL posées.

## Valeurs à lire dans les preuves de ce dossier

### Valables dans tous les documents

| Dans les documents (poste de Maxime) | Dans les preuves (mon poste) |
|---|---|
| compte Linux `maxim`, `/home/maxim/...` | `abedja`, `/home/abedja/...` |
| Ubuntu 24.04 | Ubuntu 22.04.5 |
| Python 3.12.3 | Python 3.10.12 |
| PySpark / Spark 4.3.0 | PySpark / Spark 4.2.1 |
| Matplotlib 3.11.2, pyarrow 26.0.1, numpy 2.5.5, ipykernel 7.4.0 | Matplotlib 3.10.9, pyarrow 25.0.1, numpy 2.2.6, ipykernel 7.3.0 |
| OpenJDK 21.0.13.0.3 | OpenJDK 21.0.13.1 |
| exécution du 19/11/2026 | exécution du 20/11/2026 |
| dépôt organisé en `J3/` et `J4/` | dépôt organisé en `TPs/Jour3/` et `TPs/Jour4/` |

### Par document

| Document | Poste de Maxime | Dans les preuves (mon poste) |
|---|---|---|
| UC-08 étape 1 | notebook exécuté le 19/11/2026 vers 16:44 | 20/11/2026 vers 13:51, en 1 min 10 (`TP4_Retail_Intelligence.html`) |
| UC-08 étape 1 | journaux Jour 3 `silver_20261118_153139.json`, `gold_20261118_161554.json`, rapport `controles_20261118_171614` | `silver_20261119_112905.json`, `gold_20261119_113357.json`, `controles_20261119_113612` |
| UC-08 étape 1 | `inventaire_jeux.csv` : Silver écrit le 2026-11-18 15:32, Gold le 2026-11-18 16:16 | Silver le 2026-11-19 11:30, Gold le 2026-11-19 11:34 (seules colonnes de date différentes) |
| UC-08 étape 1 | `journal_preparation_tp4.txt` : liens `scripts` et `notebooks` vers le dépôt | copies dans `~/retail-intelligence/tp4` et lien `J3/UC06_traitements` (voir Différences) |
| UC-07 étape 4 | démonstration du 19/11/2026 16:58:05 - 16:58:39 | 20/11/2026 13:52:34 - 13:53:09 |
| UC-07 étape 4 | dossiers et ACL au nom de `maxim` | au nom de `abedja` |
| UC-07 étape 4 | `/retail-lake/logs` : 9 dossiers, 13 fichiers, 22,8 K | 5 dossiers, 5 fichiers, 15,5 K (Maxime a relancé plus de traitements au Jour 3) |
| UC-07 étapes 5 et 8 | compte `maxim` propriétaire et superutilisateur HDFS | compte `abedja` |

## Différences entre les deux exécutions

| Sujet | Poste de Maxime | Mon poste | Explication |
|---|---|---|---|
| Versions | Python 3.12, PySpark 4.3.0, Matplotlib 3.11.2, pyarrow 26.0.1 | Python 3.10, PySpark 4.2.1, Matplotlib 3.10.9, pyarrow 25.0.1 | `pip` installe la version la plus récente compatible avec Python 3.10. Les fonctions utilisées (`percentile`, fenêtres, `toPandas`) existent dans les deux versions : aucun résultat ne change. |
| Graphiques PNG | Matplotlib 3.11.2 | Matplotlib 3.10.9 | fichiers différents en octets (rendu et métadonnées), contenu identique |
| Installation | `preparer_tp4.sh` crée des liens vers son dépôt | scripts copiés dans `~/retail-intelligence/tp4/scripts`, notebooks générés dans `~/retail-intelligence/tp4/notebooks` | `preparer_tp4.sh` contient le chemin de son poste ; la copie évite d'écraser ses preuves dans le dépôt |
| Paramètres | `parametres.py` importe `config.py` depuis `J4/../J3/UC06_traitements` | lien `~/retail-intelligence/J3/UC06_traitements -> ~/retail-intelligence/tp3/scripts` | même `config.py` (chemins HDFS du Jour 3), sans modifier le script |
| Démonstration UC-07 | `04_demo_stockage_local.sh` avec le chemin de son dépôt | copie du script avec deux lignes adaptées : `REPO` (mon dépôt vu de WSL) et `FICHIER` (`$REPO/Jour3/UC06_preuves/etape7/...`) | même fichier déposé, même MD5 ; le reste du script est inchangé |

## Corrections à apporter aux documents

Relevées à la relecture, elles s'appliquent aux deux postes.

| Document | Texte des documents | Texte correct |
|---|---|---|
| UC-08 étape 2 | « Écart maximal observé : 0,01 sur le panier moyen par catégorie » | 0,005 sur le panier moyen (K4), dans les deux jeux d'indicateurs (`controle_kpi_gold.csv`) ; reste sous la tolérance de 0,01 |
| UC-08 étape 3, distribution des montants | une seule tranche « 30 000 à 45 000 » (6 169 tickets) | `distribution_montant_ticket.csv` en compte trois : 30-35 k (3 751), 35-40 k (1 711), 40-45 k (707) ; le total est correct |
| README de Maxime, chemins | `J3/...`, `J4/...` | `TPs/Jour3/...`, `TPs/Jour4/...` dans ce dépôt |

## Reproduction

### Prérequis et versions observées

| Élément | Version |
|---|---|
| Système | Ubuntu 22.04.5 sous WSL 2 (Maxime : 24.04) |
| Python | 3.10.12, environnement virtuel du TP 3 `~/retail-intelligence/tp3/.venv` réutilisé |
| PySpark / Spark | 4.2.1, mode `local[*]`, 16 cœurs (Maxime : 4.3.0) |
| pandas / Matplotlib / pyarrow | 2.3.3 / 3.10.9 / 25.0.1 |
| JupyterLab | 4.6.6, noyau « Python - Retail TP4 » (`retail-tp4`) |
| Java / Hadoop | OpenJDK 21 / Hadoop 3.5.0, HDFS `hdfs://localhost:9000`, ACL activées |
| Données | zones Silver et Gold du Jour 3 dans `/retail-lake` |

### Données utilisées

Lues dans HDFS sans aucune modification (chemins dans [config.py](../Jour3/UC06_traitements/config.py), repris par [parametres.py](UC08_traitements/parametres.py)) :

| Jeu | Zone | Granularité |
|---|---|---|
| `/retail-lake/silver/ventes_detaillees` | Silver | un article d'un ticket |
| `/retail-lake/gold/fait_ventes`, `dim_date`, `dim_produit`, `dim_magasin` | Gold | article de ticket ; jour ; produit ; magasin |
| `/retail-lake/gold/kpi_ventes_magasin_mois` | Gold | magasin x mois |
| `/retail-lake/gold/kpi_ventes_categorie_mois` | Gold | catégorie x mois |

### Ordre d'exécution

```bash
# 1. Installation (une fois, sans réinstaller Spark)
REPO=/mnt/c/.../TPs               # dossier TPs du dépôt, vu depuis WSL
TP=~/retail-intelligence/tp4
source ~/retail-intelligence/tp3/.venv/bin/activate
pip install pandas matplotlib pyarrow nbformat nbconvert ipykernel
python -m ipykernel install --user --name retail-tp4 --display-name "Python - Retail TP4"
deactivate
mkdir -p $TP/notebooks $TP/UC08_preuves $TP/uc07 ~/retail-intelligence/J3
cp -r $REPO/Jour4/UC08_traitements $TP/scripts
ln -s ~/retail-intelligence/tp3/scripts ~/retail-intelligence/J3/UC06_traitements
cp $REPO/Jour4/UC07_traitements/04_demo_stockage_local.sh $TP/uc07/
sed -i "s|^REPO=.*|REPO=\"$REPO\"|; s|\$REPO/J3/|\$REPO/Jour3/|" $TP/uc07/04_demo_stockage_local.sh

# 2. Analyses UC-08 (étapes 1 à 6), environ 1 minute
cd $TP/scripts
./run_notebook.sh                 # reconstruit puis exécute le notebook complet
./run_notebook.sh 4               # seulement les étapes 1 à 4
UC08_ESSAI=1 ./run_notebook.sh    # mise au point : sorties dans ~/retail-intelligence/tp4/essai

# 3. Démonstration de stockage UC-07 (étape 4)
mkdir -p $TP/UC07_preuves/etape4
bash $TP/uc07/04_demo_stockage_local.sh 2>&1 | tee $TP/UC07_preuves/etape4/journal_demo_stockage_local.txt
```

`construire_notebook.py` génère `TP4_Retail_Intelligence.ipynb` (70 cellules) ; `run_notebook.sh` démarre HDFS s'il est arrêté, exécute le notebook avec le noyau `retail-tp4` et l'exporte en HTML. Chaque cellule repart de HDFS, rien n'est saisi à la main. Les preuves sont ensuite copiées de `~/retail-intelligence/tp4` vers ce dossier.

La démonstration crée `/retail-lake/_demo_uc07`, y dépose un CSV d'indicateurs (aucune donnée personnelle), vérifie son MD5, renomme un dossier entier, pose des ACL pour un groupe `analystes`, affiche l'équivalent `abfss://` puis **supprime le dossier** : le lac n'est pas modifié.

[commandes_azure_non_executees.sh](UC07_simulation/commandes_azure_non_executees.sh) décrit les commandes Azure CLI du parcours A. Il **n'a pas été exécuté** et s'arrête de lui-même s'il est lancé.

### Sorties produites

| Dossier | Contenu |
|---|---|
| [UC08_preuves/etape1/](UC08_preuves/etape1/) à [etape6/](UC08_preuves/etape6/) | 23 CSV, schémas, versions et 3 graphiques PNG produits par le notebook |
| [UC08_notebooks/](UC08_notebooks/) | notebook exécuté, sans erreur |
| [UC07_preuves/etape4/](UC07_preuves/etape4/) | journal de la démonstration de stockage |
| [UC07_preuves/etape7/](UC07_preuves/etape7/) | schéma de l'architecture Azure proposée |

### Contrôles disponibles

- cohérence des jeux : lignes relues = lignes attendues dans les journaux du Jour 3, CA identique à Silver dans chaque jeu Gold ([coherence_jeux.csv](UC08_preuves/etape1/coherence_jeux.csv)) ;
- recalcul indépendant des KPI depuis `fait_ventes`, comparé aux jeux Gold avec une tolérance de 0,01 ([controle_kpi_gold.csv](UC08_preuves/etape2/controle_kpi_gold.csv)) ;
- démonstration UC-07 : empreinte MD5 et nombre de lignes comparés entre le fichier local et le fichier relu dans HDFS.

### Limites connues

- données TPCx-BB synthétiques : les résultats valident la démarche, ils ne décrivent pas une enseigne réelle ;
- taux de retour, niveau de stock et répartition par canal non calculables : sources non préparées au Jour 3 ;
- marge non retenue comme KPI : coûts non qualifiés (74,3 % de marges négatives) ;
- 12 magasins seulement : les corrélations sont indicatives ;
- aucune ressource Azure créée : l'architecture cloud est une proposition documentée ; seules les opérations de stockage sont démontrées, sur HDFS, avec un seul compte Linux ;
- graphiques statiques (PNG) : pas de tableau de bord interactif.
