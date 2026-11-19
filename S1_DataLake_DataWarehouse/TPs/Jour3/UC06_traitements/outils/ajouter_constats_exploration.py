"""Ajoute les cellules de constats (Markdown) au notebook d'exploration deja execute, sans le reexecuter."""
import nbformat as nbf, sys
p = sys.argv[1]
nb = nbf.read(p, as_version=4)
md = nbf.v4.new_markdown_cell
titre = lambda c: c.cell_type == "markdown" and c.source.startswith("## ")
idx = {c.source.split("\n")[0]: i for i, c in enumerate(nb.cells) if titre(c)}
nb.cells = [c for c in nb.cells if not (c.cell_type == "markdown" and c.source.startswith("**Constats"))]
idx = {c.source.split("\n")[0]: i for i, c in enumerate(nb.cells) if titre(c)}

constats = {
 "## 2. item (produits)": """**Constats store_sales** (lot initial) :
- 667 579 lignes, 23 colonnes ; toutes les valeurs se convertissent vers les types cibles (entiers, decimal(7,2)).
- Valeurs manquantes seulement sur 4 cles secondaires : client demographique 238, menage 295, adresse 448, promotion 158 (moins de 0,07 %). Les cles date, magasin, produit et ticket ne sont jamais vides.
- Aucune ligne copiee a l'identique. Mais 4 038 couples ticket + produit apparaissent plusieurs fois (8 174 lignes) : meme ticket, meme produit, meme heure, mais promotion, quantite et prix differents. Ce sont des lignes de vente distinctes, a conserver.
- Quantites entre 1 et 100, aucune nulle ou negative ; prix de vente jamais superieur au prix catalogue.
- Atypiques : 3 324 lignes a prix de vente 0 et 3 954 a net paye 0 ou moins (coupon couvrant le montant) ; 74 % des lignes (495 971) ont une marge negative, marge moyenne -827,70. Plausibles dans un jeu synthetique, a conserver et a signaler.
- Les 5 regles de calcul des montants (montant = prix x quantite, net paye = montant - coupon, etc.) sont verifiees sur 100 % des lignes.""",
 "## 3. store (magasins)": """**Constats item** :
- 17 820 produits, cle i_item_sk unique, i_item_id aussi unique : une seule version par produit (i_rec_end_date toujours vide).
- 9 categories exploitables (Music 8 143, Books 5 335...). Les marques, descriptions et noms de produits sont des textes aleatoires : utilisables comme libelles, pas pour des analyses par marque.
- 6 marques et 8 identifiants de marque vides, 4 descriptions vides ; la marque peut etre vide alors que son identifiant existe.
- Prix courant entre 0,10 et 99,98.""",
 "## 4. date_dim (calendrier)": """**Constats store** :
- 12 magasins, cles s_store_sk et s_store_id uniques, aucune valeur non convertible.
- s_rec_end_date toujours vide.
- s_closed_date_sk est renseigne pour les 12 magasins, mais correspond a des dates sans sens (de 1917 a 2141), alors que tous les magasins vendent de 2001 a 2005 : colonne inutilisable, a ne pas interpreter comme une fermeture.""",
 "## 5. Liens entre les sources (preparation des jointures)": """**Constats date_dim** :
- 109 573 jours, du 01/01/1900 au 31/12/2199, sans trou ; aucune valeur manquante.
- d_date_sk = nombre de jours depuis le 01/01/1900 (0 pour cette date), verifie sur toutes les lignes. Dans TPC-DS d'origine c'est un jour julien : le kit utilise donc une numerotation propre.
- d_holiday vaut Y pour tous les jours : colonne inutilisable pour distinguer les jours feries.""",
}
for t in sorted(constats, key=lambda k: idx[k], reverse=True):
    nb.cells.insert(idx[t], md(constats[t]))
nb.cells.insert(len(nb.cells) - 1, md("""**Constats sur les liens** :
- Aucune vente orpheline : tous les produits, magasins et dates des ventes existent dans item, store et date_dim. Les jointures ventes-produits, ventes-magasins et ventes-dates sont donc possibles sans perte.
- Les ventes du lot initial vont du 01/01/2001 au 13/12/2005, celles du lot refresh du 13/12/2005 au 31/12/2005.
- Les lots ne se recouvrent pas : aucun ticket ni aucun produit du refresh n'existe dans le lot initial. Les ventes du refresh ne trouvent leur produit qu'en reunissant les deux lots de item : il faudra charger item initial + refresh."""))
nbf.write(nb, p)
print("constats ajoutes :", sum(1 for c in nb.cells if c.cell_type == "markdown" and c.source.startswith("**Constats")))
