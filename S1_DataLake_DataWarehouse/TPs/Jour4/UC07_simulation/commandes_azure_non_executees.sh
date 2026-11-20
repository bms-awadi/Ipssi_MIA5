#!/usr/bin/env bash
# =====================================================================================================
# UC-07 etape 4 - SIMULATION DOCUMENTAIRE : commandes Azure CLI prevues pour le parcours A.
# CE SCRIPT N'A PAS ETE EXECUTE. Aucune ressource Azure n'a ete creee (parcours B retenu).
# Il decrit les operations qui seraient faites si un abonnement autorise etait fourni par le formateur.
# Pre-requis : az login avec un compte autorise, budget et alertes crees dans Cost Management.
# Aucune cle, aucun mot de passe : authentification Entra ID (--auth-mode login) uniquement.
# =====================================================================================================
set -euo pipefail
echo "Simulation : ce script ne doit etre lance qu'avec l'accord du formateur." ; exit 1   # garde-fou

RG=rg-retail-intelligence
COMPTE=stretailmaxmag
REGION=francecentral

# 1. Groupe de ressources du projet (une seule suppression nettoie tout a la fin)
az group create --name $RG --location $REGION

# 2. Compte de stockage ADLS Gen2 : HNS active, redondance LRS, acces par cle partagee desactive,
#    pas d'acces public anonyme, TLS 1.2 minimum
az storage account create --name $COMPTE --resource-group $RG --location $REGION \
  --sku Standard_LRS --kind StorageV2 --hns true \
  --allow-shared-key-access false --allow-blob-public-access false --min-tls-version TLS1_2

# 3. Verifier les caracteristiques attendues pour ADLS Gen2 (doit afficher true)
az storage account show --name $COMPTE --resource-group $RG --query isHnsEnabled

# 4. Un conteneur (systeme de fichiers) par zone
for z in raw bronze silver rejets gold gouvernance; do
  az storage fs create --name $z --account-name $COMPTE --auth-mode login
done

# 5. Organisation des repertoires du conteneur gold
for d in modele indicateurs analyses; do
  az storage fs directory create --name $d --file-system gold --account-name $COMPTE --auth-mode login
done

# 6. Depot d'un fichier de demonstration autorise (indicateurs agreges, aucune donnee personnelle)
az storage fs file upload --source kpi_ventes_magasin_mois.csv --path indicateurs/kpi_ventes_magasin_mois.csv \
  --file-system gold --account-name $COMPTE --auth-mode login

# 7. Verifier que le fichier est accessible
az storage fs file list --file-system gold --path indicateurs --account-name $COMPTE --auth-mode login -o table

# 8. Droits : RBAC par conteneur, ACL par dossier (identifiants des groupes Entra ID a renseigner)
ID_CONTENEUR_GOLD=$(az storage account show --name $COMPTE --resource-group $RG --query id -o tsv)/blobServices/default/containers/gold
az role assignment create --role "Storage Blob Data Reader" --assignee "<objectId du groupe metiers>" --scope "$ID_CONTENEUR_GOLD"
az storage fs access set --acl "group:<objectId du groupe analystes>:r-x" --path indicateurs \
  --file-system gold --account-name $COMPTE --auth-mode login

# 9. Nettoyage apres le TP : suppression du groupe de ressources et de tout ce qu'il contient
az group delete --name $RG --yes
