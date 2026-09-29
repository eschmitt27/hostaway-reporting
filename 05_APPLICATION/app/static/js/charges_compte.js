/* Saisie d'une charge — bloc « Compte comptable » et écart de montant (Mission 36).
 *
 * À la sélection de la catégorie :
 *   · un seul compte valide  → il est présélectionné ;
 *   · plusieurs autorisés    → l'utilisateur choisit (rien n'est imposé, ex. impôts) ;
 *   · aucun                  → « Compte comptable à définir » (tranché au rapprochement).
 * Un compte hors de ceux proposés est une IMPUTATION LIBRE : la justification apparaît et devient
 * obligatoire. Le serveur rejoue toutes ces règles ; ce script ne fait qu'aider à les suivre.
 */
(function () {
  "use strict";
  var source = document.getElementById("donnees_comptes");
  if (!source) { return; }
  var donnees = JSON.parse(source.textContent || "{}");
  var categorie = document.getElementById("categorie_charge_id");
  var compte = document.getElementById("compte_comptable");
  var info = document.getElementById("compte_proposition");
  var libre = document.getElementById("group_imputation_libre");
  var impact = document.getElementById("code_impact");
  var bloc = document.getElementById("fs_compte");
  var montant = document.getElementById("montant");
  var restant = document.getElementById("mouvement_restant");
  var ecart = document.getElementById("group_ecart_montant");
  if (!categorie || !compte) { return; }

  var MARQUE = " (proposé)";

  function proposition() {
    return (donnees.propositions || {})[categorie.value] || null;
  }

  function comptesProposes() {
    var p = proposition();
    return p ? p.comptes.map(function (c) { return c.compte; }) : [];
  }

  function majLibre() {
    var parmi = comptesProposes();
    libre.hidden = !compte.value || parmi.indexOf(compte.value) >= 0;
  }

  function majCategorie(auto) {
    var p = proposition();
    var parmi = comptesProposes();
    Array.prototype.forEach.call(compte.options, function (o) {
      var texte = o.textContent.replace(MARQUE, "");
      o.textContent = parmi.indexOf(o.value) >= 0 ? texte + MARQUE : texte;
    });
    if (!p) {
      info.textContent = "Choisissez une catégorie : le compte est proposé automatiquement.";
    } else if (p.statut === "UNIQUE") {
      info.textContent = "Compte proposé pour cette catégorie : " + p.comptes[0].compte + " — "
        + p.comptes[0].libelle + ".";
      if (auto) { compte.value = p.compte_defaut; }
    } else if (p.statut === "CHOIX") {
      info.textContent = "Plusieurs comptes sont autorisés pour cette catégorie ("
        + p.comptes.map(function (c) { return c.compte + " " + c.libelle; }).join(", ")
        + ") : choisissez celui qui correspond.";
      if (auto) { compte.value = p.compte_defaut || ""; }
    } else {
      info.textContent = "Compte comptable à définir : aucun compte n'est proposé pour cette "
        + "catégorie. Laissez-le à définir (il sera choisi au rapprochement) ou faites une "
        + "imputation libre justifiée.";
      if (auto) { compte.value = ""; }
    }
    majLibre();
  }

  function majImpact() {
    if (bloc && impact) { bloc.hidden = impact.value === "HC"; }
  }

  function nombre(texte) {
    return parseFloat(String(texte || "").replace(/\s/g, "").replace(",", "."));
  }

  function majEcart() {
    if (!ecart || !restant || !montant) { return; }
    var m = nombre(montant.value);
    var r = nombre(restant.value);
    ecart.hidden = isNaN(m) || isNaN(r) || Math.abs(m - r) < 0.005;
  }

  categorie.addEventListener("change", function () { majCategorie(true); });
  compte.addEventListener("change", majLibre);
  if (impact) { impact.addEventListener("change", majImpact); }
  if (montant) { montant.addEventListener("input", majEcart); }
  majCategorie(!compte.value);
  majImpact();
  majEcart();
})();
