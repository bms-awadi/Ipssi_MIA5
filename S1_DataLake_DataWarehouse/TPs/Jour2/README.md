# TP Jour 2 : Data Lake HDFS et ingestion NiFi

> **Équipe MaxMag :** Maxime DANINO - Awadi BEDJA - Gurvan Godin
> **Mission :** créer le Data Lake de Retail Intelligence sur HDFS, y déposer les données TPCx-BB, puis automatiser l'ingestion avec Apache NiFi.
> **Socle obligatoire :** UC-03 (Hadoop, HDFS et création du Data Lake) · UC-04 (ingestion et orchestration avec NiFi) · **Compétences :** C02 · C03 · C04 · C05 · C10

## Organisation du travail

Le TP a été mené avec mes camarades :
- les **10 PDF** ont été rédigés ensemble à partir de l'exécution faite sur le poste de **Maxime** (17/11/2026) ;
- **toutes les preuves de ce dossier** (journaux, captures, CSV, JSON, inventaire Excel) proviennent d'exécution complète des mêmes scripts sur mon poste (18/11/2026), pour que chaque membre ait des preuves issues de sa propre installation.

Les PDF citent donc des valeurs du poste de Maxime (dates, heures, identifiants, chemins). Les sections ci-dessous indiquent, document par document, **quelle valeur lire à la place** dans les preuves de ce dossier, et **où les deux exécutions diffèrent**. Tout ce qui ne figure pas dans ces tableaux est **identique** sur les deux postes.

## Livrables

### UC-03 : Hadoop, HDFS et création du Data Lake

| Étape | Livrable | Preuves (poste d'Awadi) |
|---|---|---|
| 1 - Environnement Hadoop | [UC03_etape1_environnement_hadoop.pdf](UC03_etape1_environnement_hadoop.pdf) | [UC03_preuves/etape1/](UC03_preuves/etape1/), [annexe_installation/](annexe_installation/) |
| 2 - Commandes HDFS | [UC03_etape2_commandes_hdfs.pdf](UC03_etape2_commandes_hdfs.pdf) | [UC03_preuves/etape2/](UC03_preuves/etape2/) |
| 3 - Arborescence du Data Lake | [UC03_etape3_arborescence_datalake.pdf](UC03_etape3_arborescence_datalake.pdf) | [UC03_preuves/etape3/](UC03_preuves/etape3/) |
| 4 - Dépôt des données | [UC03_etape4_depot_donnees.pdf](UC03_etape4_depot_donnees.pdf) | [UC03_preuves/etape4/](UC03_preuves/etape4/), [UC03_inventaire_depot.xlsx](UC03_inventaire_depot.xlsx) |
| 5 - Vérification du stockage | [UC03_etape5_verification_stockage.pdf](UC03_etape5_verification_stockage.pdf) | [UC03_preuves/etape5/](UC03_preuves/etape5/) |
| Scripts | | [UC03_preuves/scripts/](UC03_preuves/scripts/) |

### UC-04 : Ingestion et orchestration avec Apache NiFi

| Étape | Livrable | Preuves (poste d'Awadi) |
|---|---|---|
| 6 - Découverte de NiFi | [UC04_etape6_decouverte_nifi.pdf](UC04_etape6_decouverte_nifi.pdf) | [UC04_preuves/etape6/](UC04_preuves/etape6/) |
| 7 à 9 - Flux GetFile vers PutHDFS | [UC04_etapes7-9_flux_getfile_puthdfs.pdf](UC04_etapes7-9_flux_getfile_puthdfs.pdf) | [UC04_preuves/etape8/](UC04_preuves/etape8/), [UC04_preuves/etape9/](UC04_preuves/etape9/) |
| 10 - Erreurs contrôlées | [UC04_etape10_erreur_controlee.pdf](UC04_etape10_erreur_controlee.pdf) | [UC04_preuves/etape10/](UC04_preuves/etape10/) |
| 11 - Provenance | [UC04_etape11_provenance.pdf](UC04_etape11_provenance.pdf) | [UC04_preuves/etape11/](UC04_preuves/etape11/) |
| 12 - Flux enrichi | [UC04_etape12_flux_enrichi.pdf](UC04_etape12_flux_enrichi.pdf) | [UC04_preuves/etape12/](UC04_preuves/etape12/) |
| Scripts | | [UC04_preuves/scripts/](UC04_preuves/scripts/) |

## Ce qui est identique sur les deux postes

- Hadoop 3.5.0 (Java 17), NiFi 2.12.0 (Java 21), HDFS mono-nœud, réplication 1 ;
- 36 fichiers déposés, 1 033 993 587 octets, **toutes les empreintes SHA-256 identiques** au kit et entre les deux postes ;
- 105 répertoires vérifiés, `inventory.dat` en 4 blocs, fichiers en fin de ligne CRLF ;
- NiFi : 119 + 2 modules NAR, 304 processeurs, 125 services, 1 connecteur ;
- `store.dat` : 3 386 octets, SHA-256 `a936bc38...6729d1808ef` ;
- configuration des deux flux NiFi (propriétés, relations, 3 puis 8 connexions) : identique ;
- comportement des tests : rejet de configuration (test 1), routage vers `failure` sans écrasement (test 2), `.csv` en quarantaine, 0 ERROR / WARN au flux enrichi.

## Valeurs à lire dans les preuves de ce dossier

### Valables dans tous les PDF

| Dans les PDF (poste de Maxime) | Dans les preuves (poste d'Awadi) |
|---|---|
| machine `LAPTOP-5IITUUUR` | `PICOXIA-03` |
| compte Linux `maxim`, `/home/maxim/...` | `abedja`, `/home/abedja/...` |
| Ubuntu 24.04.5, noyau 6.18.33.2 | Ubuntu 22.04.5, noyau 6.6.87.2 |
| Python 3.12.3 | Python 3.10.12 |
| exécution du 17/11/2026 | exécution du 18/11/2026 |

### UC-03

| PDF | Dans le PDF | Dans les preuves |
|---|---|---|
| Étape 1 | PID NameNode / DataNode / SecondaryNameNode : 11919 / 12065 / 12327 | 607 / 751 / 1028 (`02_jps.txt`) |
| Étape 1 | espace libre 950 Go, DFS Remaining 949,04 Go, Non DFS 6,60 Go | 945 Go, 944,36 Go, 11,28 Go (`03_dfsadmin_report.txt`, `07_installation.txt`) |
| Étape 1 | relevé à 12:47 | relevé à 10:30 |
| Étape 2 | exécution 12:53:19 - 12:53:43 | 10:40:23 - 10:40:53 |
| Étape 3 | exécution 14:16:40 - 14:16:54 | 10:42:47 - 10:43:00 |
| Étape 4 | source du kit : chemin Windows avec espaces (`/mnt/c/.../retail_project`) | `/home/abedja/tp2/kit` (kit copié dans Linux) |
| Étape 4 | dépôt 14:39:46 - 14:42:34, vérification 14:43:03 - 14:43:21 | 10:44:52 - 10:47:07, 10:47:19 - 10:47:40 |
| Étape 4 | `manifeste_depot_20261117_143841.csv` | `manifeste_depot_20261118_104441.csv` |
| Étape 5 | exécution 14:50:49 - 14:53:16 | 10:50:14 - 10:52:37 |

### UC-04

| PDF | Dans le PDF | Dans les preuves |
|---|---|---|
| Étape 6 | NiFi démarré à 12:27:31 en 11,257 s | 10:23:51 en 11,996 s |
| Étape 6 | PID NiFi / Bootstrap : 13609 / 13578 | 2445 / 2417 |
| Étape 6 | certificat valable du 17/11/2026 12:19 au 16/01/2027 12:19 (heure de Paris) | du 18/11/2026 10:14 au 17/01/2027 10:14 |
| Étapes 7-9 | préparation à 15:23:04 | 11:27:23 |
| Étapes 7-9 | `store.dat` écrit dans HDFS à 16:05:08 | 14:09:33 |
| Étapes 7-9 | GetFile (Run Schedule 0 s) : 7 099 737 exécutions | 10 852 983 exécutions en 1 min 48 s (49,6 s de CPU) |
| Étapes 7-9 | dépôt UC-03 de `store.dat` daté de 14:40 | 10:45 |
| Étape 10 | début des tests 16:13:54 | 14:20:11 |
| Étape 10 | avertissement PutHDFS 16:27:53, LogAttribute 16:28:25 | 14:27:33, 14:33:42 |
| Étape 10 | UUID du FlowFile en échec `535341de-...` | `42a5afe1-9532-47b5-bbad-dc4f5f77e02e` |
| Étape 11 | SEND à 16:05:08.263, durée 0,744 s, lignée 0,875 s | 14:09:33.956, 0,395 s, 0,506 s |
| Étape 11 | UUID du FlowFile `262b38e1-...` | `afdd82af-1e9f-4edd-9d7e-fc4f516213ab` |
| Étape 12 | copie des 3 fichiers à 17:02, passage dans le flux à 17:03:42 | 15:31, 15:33:04 |
| Étape 12 | UUID reason / ship_mode / csv : `f7f48ba0` / `96de9879` / `909a3fd4` | `38d146c3` / `2b078b18` / `b7f50f82` |

## Différences entre les deux exécutions

| Étape | Poste de Maxime | Poste d'Awadi | Explication |
|---|---|---|---|
| 4 | 1er essai de dépôt en échec (`erreur_1er_essai_depot.txt`), puis contournement par un lien `~/kit_tpcxbb` | aucun incident, fichier absent de ce dossier | le kit de Maxime était dans un chemin Windows contenant des espaces ; celui d'Awadi est dans Linux, sans espace |
| 4 | - | preuve en plus : `fins_de_ligne.txt` (36 / 36 fichiers en CRLF) | contrôle séparé dans un fichier dédié |
| 1 | une capture du NameNode | deux captures (`capture_namenode_9870.png` et `_stockage.png`) | ajout de l'onglet stockage |
| 1 | installation non tracée | [annexe_installation/](annexe_installation/) : environnement, Hadoop, démarrage HDFS, NiFi | installation pas à pas sur le poste d'Awadi |
| 6 | identifiants NiFi générés | identifiants définis avec `nifi.sh set-single-user-credentials` (mot de passe non publié) | un premier essai de connexion a échoué, d'où la redéfinition |
| 9 | 7 099 737 exécutions de GetFile | 10 852 983 | même constat (exécution à vide avec un Run Schedule à 0 s) ; l'écart vient de la durée de fonctionnement et de la machine |
| 9 | - | avertissement Java 21 `java.lang.foreign.Linker` à la console au démarrage de PutHDFS | sans incidence sur le dépôt |
| 11, 12 | une capture par vue | captures `_suite` en plus | la fenêtre de provenance ne tenait pas sur un seul écran |
| 12 | canvas horizontal | canvas vertical | même flux : 6 processeurs, 8 connexions, propriétés identiques |
| 12 | - | GetFile à 5 s : 11 exécutions | chiffre de la correction, contre 10,8 millions à l'étape 9 |

## Corrections à apporter aux PDF

Relevées à la relecture des PDF, elles s'appliquent aux deux postes.

| PDF | Texte des PDF | Texte correct |
|---|---|---|
| Étape 1, tableau des ports | 9867 : « échanges internes avec le NameNode » | 9867 : appels d'administration vers le DataNode (`dfsadmin`, récupération de blocs). Le DataNode contacte le NameNode sur le port 9000. |
| Étape 1, contraintes réseau | « ils ne sont joignables que depuis le PC » | Le RPC du NameNode (9000) n'écoute que sur 127.0.0.1. Les autres ports écoutent sur 0.0.0.0, mais WSL 2 (mode NAT) ne les expose qu'à Windows via localhost. |
| Étape 1, tableau des rôles | numéros de processus | ajouter « au moment du relevé » (ils changent à chaque démarrage) |
| Étape 4, provenance du kit | « version du kit non indiquée » | TPCx-BB Tools 1.7.0 (Cours 2, slide 18), version non indiquée dans le kit lui-même |
| Étape 4, encodage | « ASCII » | ASCII (sous-ensemble de l'UTF-8), UTF-8 pour `customer.dat` |
| Étape 4, lecture seule | « sur toute la zone Raw » | sur `/retail-lake/raw/tpcxbb` ; `raw/nifi` reste en écriture pour l'UC-04 |
| Étape 5, permissions | « Dossiers de Raw » | « Dossiers de `raw/tpcxbb` » |
| Étape 5, lecture de `customer.dat` | « les 18 champs s'affichent » | les 7 premiers champs (identifiants) sont affichés, sans données personnelles |
| Étape 6, composants | 304 processeurs, 125 services... | ajouter 1 connecteur (`KafkaToS3`), présent dans `composants_nifi.csv` |

## Production

Les scripts sont ceux des PDF, avec trois adaptations pour être indépendants du poste :
- `maxim` remplacé par `$USER` dans `etape6_decouverte_nifi.sh` et `etape8_preparation_flux.sh` ;
- version du kit renseignée dans `etape4_inventaire_excel.py` ;
- affichage de `customer.dat` limité aux 7 premiers champs dans `etape5_verification_stockage.sh`.

```bash
# HDFS et NiFi démarrés, kit TPCx-BB dans ~/tp2/kit (jamais versionné), P = dossier de preuves
bash UC03_preuves/scripts/etape1_verification_hadoop.sh $P/etape1
bash UC03_preuves/scripts/etape2_commandes_hdfs.sh $P/etape2
bash UC03_preuves/scripts/etape3_arborescence.sh $P/etape3
bash UC03_preuves/scripts/etape4_depot.sh ~/tp2/kit $P/etape4        # crée aussi le lien ~/kit_tpcxbb
bash UC03_preuves/scripts/etape4_verification.sh ~/tp2/kit $P/etape4
bash UC03_preuves/scripts/etape5_verification_stockage.sh $P/etape4 $P/etape5
bash UC04_preuves/scripts/etape6_decouverte_nifi.sh $P/etape6
bash UC04_preuves/scripts/etape8_preparation_flux.sh $P/etape8         # puis flux construit dans l'interface
bash UC04_preuves/scripts/etape9_test_flux.sh avant|apres $P/etape9    # de même etape10_erreurs.sh et etape12_flux_enrichi.sh
```
