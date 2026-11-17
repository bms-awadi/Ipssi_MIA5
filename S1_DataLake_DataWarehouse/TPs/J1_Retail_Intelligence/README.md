# TP Jour 1 — Retail Intelligence Platform

> **Mission :** consultant Data pour Retail Intelligence. Partir des besoins métier, examiner les données **réellement disponibles** et proposer une première architecture **justifiée**.
> **Socle obligatoire :** UC-01 (besoin métier & exploration des sources) · UC-02 (architecture Data)
> **Compétences :** C01 · C02 · C03 · C10

## Arborescence

```
J1_Retail_Intelligence/
├── README.md
├── 01_UC01_besoin_exploration/
│   ├── cadrage_metier.md
│   ├── questions_decisionnelles.md
│   ├── dictionnaire_KPI.md
│   ├── inventaire_sources.(csv|xlsx)
│   ├── qualification_donnees.md
│   ├── limites_hypotheses.md
│   └── preuves/
├── 02_UC02_architecture/
│   ├── comparaison_architectures.md
│   ├── architecture_cible.md
│   ├── schema_flux.md
│   ├── positionnement_composants.md
│   ├── gouvernance.md
│   ├── local_vers_azure.md
│   └── preuves/
└── scripts/            ← scripts Python d'exploration (preuves reproductibles)
```

## Statuts de certitude utilisés

| Statut | Signification |
|---|---|
| **Vérifié** | Observé directement dans un fichier ou une documentation |
| **Déduit** | Obtenu par analyse, avec justification |
| **Hypothèse** | À confirmer par une vérification complémentaire |
| **Non disponible** | Absent des données examinées |
| **À valider** | Nécessite une décision ou une vérification du formateur |

## Avancement

### UC-01 — Besoin métier et exploration
- [ ] Utilisateurs métier identifiés (≥ 3 profils)
- [ ] Questions décisionnelles formulées (≥ 2 par profil) et décisions associées
- [ ] KPI définis (≥ 5) avec formules et périmètres
- [ ] Sources TPCx-BB inventoriées (`data/` et `data_refresh/`)
- [ ] Fichiers et colonnes vérifiés
- [ ] Relations possibles documentées
- [ ] Limites et hypothèses explicites (≥ 5)
- [ ] Preuves conservées

### UC-02 — Architecture
- [ ] Contraintes du projet identifiées
- [ ] Comparaison Data Warehouse / Data Lake / Lakehouse
- [ ] Architecture logique cible
- [ ] Flux de données représentés
- [ ] Zones Bronze / Silver / Gold décrites
- [ ] Composants positionnés selon leur statut
- [ ] Gouvernance prise en compte
- [ ] Cible Azure présentée (conception, pas déploiement)
- [ ] Choix justifiés

## Reproduire l'exploration

```bash
# depuis la racine du dépôt, avec le kit TPCx-BB placé dans supports_cours/retail_project/
python S1_DataLake_DataWarehouse/TPs/J1_Retail_Intelligence/scripts/<script>.py
```
