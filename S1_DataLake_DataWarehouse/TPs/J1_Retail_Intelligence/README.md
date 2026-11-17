# TP Jour 1 : Retail Intelligence Platform

> **Équipe MaxMag :** Maxime DANINO - Awadi BEDJA - Gurvan Godin
> **Mission :** consultant Data pour Retail Intelligence. Partir des besoins métier, examiner les données **réellement disponibles** (kit TPCx-BB) et proposer une première architecture **justifiée**.
> **Socle obligatoire :** UC-01 (besoin métier et exploration des sources) · UC-02 (architecture Data) · **Compétences :** C01 · C02 · C03 · C10

## Livrables

### UC-01 : Besoin métier et exploration des sources

| Étape | Livrable |
|---|---|
| A - Comprendre les utilisateurs | [UC01_cadrage_metier.pdf](UC01_cadrage_metier.pdf) |
| B - Questions décisionnelles, décisions, priorités | [UC01_questions_decisionnelles.pdf](UC01_questions_decisionnelles.pdf) |
| B - Dictionnaire des KPI (K1 à K8) | [UC01_dictionnaire_KPI.pdf](UC01_dictionnaire_KPI.pdf) |
| C - Inventaire des sources (fiche kit, inventaire, 522 colonnes, domaines, anomalies) | [UC01_inventaire_sources.xlsx](UC01_inventaire_sources.xlsx) |
| C et D - Qualification des données et relations | [UC01_qualification_donnees.pdf](UC01_qualification_donnees.pdf) |
| E - Limites, hypothèses, besoins non couverts | [UC01_limites_et_hypotheses.pdf](UC01_limites_et_hypotheses.pdf) |
| Preuves | [UC01_preuves/](UC01_preuves/) |

### UC-02 : Architecture Data

| Étape | Livrable |
|---|---|
| A et B - Contraintes, comparaison Data Warehouse / Data Lake / Lakehouse | [UC02_comparaison_architectures.pdf](UC02_comparaison_architectures.pdf) |
| C - Architecture logique, zones Bronze / Silver / Gold, modèle en étoile | [UC02_architecture_cible.pdf](UC02_architecture_cible.pdf) |
| D - Positionnement des composants | [UC02_positionnement_composants.pdf](UC02_positionnement_composants.pdf) |
| E - Flux de données F1 à F6 | [UC02_schema_flux.pdf](UC02_schema_flux.pdf) |
| F - Gouvernance | [UC02_gouvernance.pdf](UC02_gouvernance.pdf) |
| G - Architecture locale et cible Azure | [UC02_local_vers_azure.pdf](UC02_local_vers_azure.pdf) |
| Schémas | [UC02_preuves/](UC02_preuves/) |

## Synthèse

**Choix d'architecture :** Data Lake en zones Bronze / Silver / Gold (+ zone de rejets) sur HDFS, ingestion NiFi, traitements PySpark, format Parquet ; zone Gold modélisée en étoile (faits ventes, retours, stock) pour les KPI K1 à K8. Delta Lake reste une évolution facultative. Cible cloud : ADLS Gen2 (conception uniquement, déploiement au Jour 4).

**Faits clés vérifiés :**
- 36 fichiers : 34 `.dat` (23 tables dans `data`, 11 dans `data_refresh`) + 2 rapports d'audit CSV ; 978 Mo + 9,2 Mo ; nombre de colonnes conforme au schéma TPCx-BB pour les 34 fichiers.
- 46 relations testées : 0 orphelin, à condition de charger aussi les référentiels de `data_refresh` (sinon 232 380 lignes de stock pointent vers 180 produits absents de `data/item`).
- `data_refresh` complète `data` sans aucun doublon : ventes du 13 au 31/12/2005, 180 produits et 1 000 clients nouveaux, photo de stock du 12/12/2005 répartie entre les deux lots (35 100 + 54 900 lignes).
- Retours de `data` : 100 % rattachés à une vente ; retours de `data_refresh` : 778 sur 826 sans vente d'origine (mis en rejets).
- Pas de clé de ligne de vente (ticket + produit non unique) ; motifs de retour illisibles ; stock connu par photo hebdomadaire et par entrepôt uniquement.

## Preuves et reproduction

| Fichier | Rôle |
|---|---|
| [UC01_preuves/scripts/inventaire.py](UC01_preuves/scripts/inventaire.py) | Étape C : inventaire des fichiers et profil des colonnes (types, vides, distincts, min / max, unicité des clés) |
| [UC01_preuves/scripts/relations.py](UC01_preuves/scripts/relations.py) | Étape D : 46 relations (vides, orphelins, cardinalités) et rattachement retours → ventes |
| [UC01_preuves/scripts/controles_complementaires.py](UC01_preuves/scripts/controles_complementaires.py) | Cohérence de `data` seul, nature de `wcs_sales_sk` / `pr_order_sk`, retours du refresh, recouvrement `data` / `data_refresh` |
| [UC01_preuves/inventaire_fichiers.csv](UC01_preuves/inventaire_fichiers.csv), [profil_colonnes.csv](UC01_preuves/profil_colonnes.csv), [relations.csv](UC01_preuves/relations.csv) | Résultats des scripts |
| [UC01_preuves/arborescence_data.png](UC01_preuves/arborescence_data.png), [arborescence_data_refresh.png](UC01_preuves/arborescence_data_refresh.png) | Arborescence du kit |

```bash
# kit TPCx-BB décompressé dans supports_cours/retail_project/ (jamais versionné)
python -I UC01_preuves/scripts/inventaire.py <racine retail_project> . inventaire.json
python -I UC01_preuves/scripts/relations.py <racine retail_project> relations.json
python -I UC01_preuves/scripts/controles_complementaires.py <racine retail_project>
```

## Statuts de certitude

**Vérifié** (observé dans les fichiers) · **Déduit** (obtenu par analyse, justifié) · **Hypothèse** (à confirmer) · **Non disponible** (absent des données) · **À valider** (décision du formateur).
