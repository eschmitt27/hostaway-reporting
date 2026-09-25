/* Nouvelle saisie charge — comportement de l'écran `/fournisseurs/nouvelle`.
 *
 * Présentation uniquement. Aucune règle métier n'est calculée ici : le serveur reste seul juge
 * (validation, périmètre propriétaire → logements actifs, répartition, refacturation).
 *
 * 1. Multi-sélection avec recherche. Les cases à cocher natives rendues par le serveur RESTENT
 *    la donnée du formulaire : le composant ne fait que les cocher / décocher. Le POST envoyé est
 *    donc exactement celui d'avant (mêmes noms, mêmes valeurs, même ordre).
 * 2. Affichage conditionnel des sections et récapitulatif « Effet de la saisie » — logique reprise
 *    telle quelle de l'ancien script en ligne.
 */
(function () {
  "use strict";

  /* ═════════════ 1. Multi-sélection avec recherche ═════════════ */

  var SVG_CHEVRON = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" ' +
    'stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">' +
    '<polyline points="6 9 12 15 18 9"/></svg>';
  var SVG_CROIX = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" ' +
    'stroke-width="2.5" stroke-linecap="round" aria-hidden="true" focusable="false">' +
    '<line x1="6" y1="6" x2="18" y2="18"/><line x1="18" y1="6" x2="6" y2="18"/></svg>';
  var SVG_COCHE = '<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" ' +
    'stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false">' +
    '<polyline points="20 6 9 17 4 12"/></svg>';

  var compteur = 0;

  // Recherche insensible à la casse et aux accents : « helene » trouve « Hélène ».
  function normaliser(texte) {
    return String(texte || "").normalize("NFD").replace(/[̀-ͯ]/g, "")
      .toLowerCase().replace(/\s+/g, " ").trim();
  }

  function creer(tag, classe, attributs) {
    var e = document.createElement(tag);
    if (classe) e.className = classe;
    if (attributs) {
      Object.keys(attributs).forEach(function (k) { e.setAttribute(k, attributs[k]); });
    }
    return e;
  }

  function initMultiselect(racine) {
    var natif = racine.querySelector(".cs-ms__natif");
    if (!natif) return;
    var cases = Array.prototype.slice.call(natif.querySelectorAll('input[type="checkbox"]'));
    if (!cases.length) return;

    var uid = "cs-ms-" + (++compteur);
    var idLibelle = racine.getAttribute("data-label-id") || "";
    var elLibelle = idLibelle ? document.getElementById(idLibelle) : null;
    var nomChamp = elLibelle ? elLibelle.textContent.replace(/\s+/g, " ").trim() : "";
    var placeholder = racine.getAttribute("data-placeholder") || "Rechercher…";

    var libelles = cases.map(function (c) {
      var lab = c.closest("label");
      return (lab ? lab.textContent : c.value).replace(/\s+/g, " ").trim();
    });
    var cles = libelles.map(normaliser);

    // ── Structure ──
    var controle = creer("div", "cs-ms__control");
    var valeurs = creer("div", "cs-ms__values");
    var puces = creer("ul", "cs-ms__chips", { "aria-label": "Sélection — " + nomChamp });
    var saisie = creer("input", "cs-ms__search", {
      type: "text", role: "combobox", autocomplete: "off", spellcheck: "false",
      "aria-autocomplete": "list", "aria-expanded": "false", "aria-controls": uid + "-liste",
      placeholder: placeholder
    });
    if (idLibelle) saisie.setAttribute("aria-labelledby", idLibelle);
    var effacer = creer("button", "cs-ms__icon-btn cs-ms__clear", {
      type: "button", title: "Tout retirer", "aria-label": "Tout retirer — " + nomChamp
    });
    effacer.innerHTML = SVG_CROIX;
    var separateur = creer("span", "cs-ms__sep", { "aria-hidden": "true" });
    var chevron = creer("button", "cs-ms__icon-btn cs-ms__chevron", {
      type: "button", tabindex: "-1", "aria-hidden": "true"
    });
    chevron.innerHTML = SVG_CHEVRON;
    valeurs.appendChild(puces);
    valeurs.appendChild(saisie);
    controle.appendChild(valeurs);
    controle.appendChild(effacer);
    controle.appendChild(separateur);
    controle.appendChild(chevron);

    var panneau = creer("div", "cs-ms__panel");
    panneau.hidden = true;
    var liste = creer("ul", "cs-ms__listbox", {
      id: uid + "-liste", role: "listbox", "aria-multiselectable": "true"
    });
    if (idLibelle) liste.setAttribute("aria-labelledby", idLibelle);
    var vide = creer("p", "cs-ms__empty");
    vide.textContent = racine.getAttribute("data-empty") || "Aucun résultat.";
    vide.hidden = true;
    var pied = creer("div", "cs-ms__footer");
    var compte = creer("span", "cs-ms__compte");
    var fermerBtn = creer("button", "cs-link-btn", { type: "button", tabindex: "-1" });
    fermerBtn.textContent = "Fermer";
    pied.appendChild(compte);
    pied.appendChild(fermerBtn);

    var options = cases.map(function (c, i) {
      var li = creer("li", "cs-ms__option", {
        id: uid + "-opt-" + i, role: "option", "aria-selected": "false"
      });
      var coche = creer("span", "cs-ms__coche", { "aria-hidden": "true" });
      coche.innerHTML = SVG_COCHE;
      var texte = creer("span", "cs-ms__option-texte");
      texte.textContent = libelles[i];
      li.appendChild(coche);
      li.appendChild(texte);
      liste.appendChild(li);
      return li;
    });
    panneau.appendChild(liste);
    panneau.appendChild(vide);
    panneau.appendChild(pied);

    var annonce = creer("span", "cs-sr-only", { "aria-live": "polite" });

    racine.appendChild(controle);
    racine.appendChild(panneau);
    racine.appendChild(annonce);
    racine.classList.add("is-enhanced");

    // ── État ──
    var actif = -1;

    function estOuvert() { return !panneau.hidden; }
    function annoncer(message) { annonce.textContent = message; }

    function indicesVisibles() {
      var r = [];
      options.forEach(function (o, i) { if (!o.hidden) r.push(i); });
      return r;
    }
    function indicesCoches() {
      var r = [];
      cases.forEach(function (c, i) { if (c.checked) r.push(i); });
      return r;
    }

    function definirActif(i) {
      if (actif >= 0 && options[actif]) options[actif].classList.remove("is-active");
      actif = i;
      if (i >= 0) {
        options[i].classList.add("is-active");
        saisie.setAttribute("aria-activedescendant", options[i].id);
        options[i].scrollIntoView({ block: "nearest" });
      } else {
        saisie.removeAttribute("aria-activedescendant");
      }
    }

    function majCompte(n) {
      compte.textContent = n + " sur " + cases.length + (n > 1 ? " sélectionnés" : " sélectionné");
    }

    function rendre() {
      puces.innerHTML = "";
      var n = 0;
      cases.forEach(function (c, i) {
        options[i].setAttribute("aria-selected", c.checked ? "true" : "false");
        if (!c.checked) return;
        n++;
        var li = creer("li", "cs-chip");
        var t = creer("span", "cs-chip__texte", { title: libelles[i] });
        t.textContent = libelles[i];
        var b = creer("button", "cs-chip__retirer", { type: "button", "aria-label": "Retirer " + libelles[i] });
        b.innerHTML = SVG_CROIX;
        b.addEventListener("click", function (e) {
          e.stopPropagation();
          retirerDepuisPuce(i);
        });
        li.appendChild(t);
        li.appendChild(b);
        puces.appendChild(li);
      });
      puces.hidden = n === 0;
      effacer.hidden = n === 0;
      separateur.hidden = n === 0;
      saisie.placeholder = n ? "Ajouter…" : placeholder;
      majCompte(n);
    }

    // Coche / décoche la VRAIE case, puis prévient le reste de la page (récapitulatif) par
    // l'événement `change` natif — celui qu'émettrait un clic sur la case elle-même.
    function basculer(i, valeur) {
      var c = cases[i];
      var cible = typeof valeur === "boolean" ? valeur : !c.checked;
      if (c.checked === cible) return;
      c.checked = cible;
      c.dispatchEvent(new Event("change", { bubbles: true }));
      rendre();
      annoncer(libelles[i] + (cible ? " : sélectionné." : " : retiré."));
    }

    // Choix depuis la liste (clic ou Entrée). Après un AJOUT, la recherche se vide et la liste
    // reste ouverte : on enchaîne « taper, Entrée, taper, Entrée » sans rien effacer à la main.
    function choisir(i) {
      basculer(i);
      if (cases[i].checked && saisie.value) {
        saisie.value = "";
        filtrer();
        definirActif(i);
      }
    }

    function retirerDepuisPuce(i) {
      var position = indicesCoches().indexOf(i);
      basculer(i, false);
      var boutons = puces.querySelectorAll(".cs-chip__retirer");
      if (boutons.length) boutons[Math.min(position, boutons.length - 1)].focus();
      else saisie.focus();
    }

    function toutRetirer() {
      indicesCoches().forEach(function (i) {
        cases[i].checked = false;
        cases[i].dispatchEvent(new Event("change", { bubbles: true }));
      });
      rendre();
      annoncer("Sélection vidée.");
    }

    function filtrer() {
      var q = normaliser(saisie.value);
      var n = 0;
      options.forEach(function (o, i) {
        var garde = !q || cles[i].indexOf(q) !== -1;
        o.hidden = !garde;
        if (garde) n++;
      });
      vide.hidden = n > 0;
      liste.hidden = n === 0;
      var visibles = indicesVisibles();
      if (q) definirActif(visibles.length ? visibles[0] : -1);
      else if (visibles.indexOf(actif) === -1) definirActif(-1);
      if (q) annoncer(n === 0 ? "Aucun résultat." : n + (n > 1 ? " résultats." : " résultat."));
    }

    function ouvrir() {
      if (estOuvert()) return;
      panneau.hidden = false;
      racine.classList.add("is-open");
      saisie.setAttribute("aria-expanded", "true");
    }

    function fermer() {
      if (!estOuvert()) return;
      panneau.hidden = true;
      racine.classList.remove("is-open");
      saisie.setAttribute("aria-expanded", "false");
      definirActif(-1);
      if (saisie.value) {
        saisie.value = "";
        filtrer();
      }
    }

    function deplacer(sens) {
      var visibles = indicesVisibles();
      if (!visibles.length) return;
      var p = visibles.indexOf(actif);
      if (sens > 0) definirActif(visibles[p < 0 ? 0 : Math.min(p + 1, visibles.length - 1)]);
      else definirActif(visibles[p < 0 ? visibles.length - 1 : Math.max(p - 1, 0)]);
    }

    // ── Souris / toucher ──
    controle.addEventListener("mousedown", function (e) {
      if (e.button !== 0) return;
      if (e.target.closest(".cs-chip__retirer, .cs-ms__clear")) return;
      var surChevron = !!e.target.closest(".cs-ms__chevron");
      if (e.target !== saisie) e.preventDefault();   // le focus reste dans la recherche
      saisie.focus();
      if (surChevron && estOuvert()) fermer();
      else ouvrir();
    });
    effacer.addEventListener("click", function () {
      toutRetirer();
      saisie.focus();
    });
    // Un clic dans la liste ne doit pas retirer le focus du champ de recherche.
    panneau.addEventListener("mousedown", function (e) { e.preventDefault(); });
    liste.addEventListener("click", function (e) {
      var o = e.target.closest(".cs-ms__option");
      if (!o) return;
      var i = options.indexOf(o);
      definirActif(i);
      choisir(i);
    });
    fermerBtn.addEventListener("click", function () {
      fermer();
      saisie.focus();
    });
    document.addEventListener("mousedown", function (e) {
      if (!racine.contains(e.target)) fermer();
    });
    racine.addEventListener("focusout", function (e) {
      if (!e.relatedTarget || !racine.contains(e.relatedTarget)) fermer();
    });

    // Le libellé n'est pas un <label for> (il n'y a pas UN champ natif) : on lui en rend l'usage.
    if (elLibelle) elLibelle.addEventListener("click", function () { saisie.focus(); });

    // ── Clavier ──
    saisie.addEventListener("input", function () {
      ouvrir();
      filtrer();
    });
    saisie.addEventListener("keydown", function (e) {
      switch (e.key) {
        case "ArrowDown":
          e.preventDefault();
          ouvrir();
          if (!e.altKey) deplacer(1);
          break;
        case "ArrowUp":
          e.preventDefault();
          if (e.altKey) { fermer(); break; }
          ouvrir();
          deplacer(-1);
          break;
        case "Enter":
          // Jamais d'envoi du formulaire depuis la recherche : Entrée choisit l'option active.
          e.preventDefault();
          if (!estOuvert()) ouvrir();
          else if (actif >= 0) choisir(actif);
          break;
        case "Escape":
          if (estOuvert()) { e.preventDefault(); fermer(); }
          else if (saisie.value) { e.preventDefault(); saisie.value = ""; filtrer(); }
          break;
        case "Backspace":
          if (!saisie.value) {
            var coches = indicesCoches();
            if (coches.length) {
              e.preventDefault();
              basculer(coches[coches.length - 1], false);
            }
          }
          break;
        case "Tab":
          fermer();
          break;
      }
    });

    rendre();
    filtrer();
  }

  Array.prototype.forEach.call(document.querySelectorAll(".cs-saisie [data-multiselect]"), initMultiselect);


  /* ═════════════ 2. Affichage conditionnel du formulaire ═════════════ */

  var MODES_ASSOCIE = ["PAY_003", "PAY_004"];
  var MODE_CARTE = "PAY_003";
  var LIBELLES_MODE_MENAGE = { INTERVENANT: "par intervenant", LOGEMENT: "par logement" };

  function el(id) { return document.getElementById(id); }
  function show(id) { var e = el(id); if (e) e.hidden = false; }
  function hide(id) { var e = el(id); if (e) e.hidden = true; }
  function catOpt() { var s = el("categorie_charge_id"); return s.options[s.selectedIndex]; }
  function menageComport() { var o = catOpt(); return o ? (o.getAttribute("data-menage") || "") : ""; }
  function avantagePossible() { var o = catOpt(); return o ? (o.getAttribute("data-avantage") === "OUI") : false; }

  function impactMenageEffectif() {
    var c = menageComport();
    if (c === "FORCE") return true;
    if (c === "INTERDIT") return false;
    return el("impact_menage").value === "OUI";
  }
  function selectedCount(name) { return document.querySelectorAll('input[name="' + name + '"]:checked').length; }

  function updatePaiement() {
    var mode = el("mode_paiement_id").value;
    if (MODES_ASSOCIE.indexOf(mode) >= 0) show("group_associe_id");
    else { hide("group_associe_id"); el("associe_id").value = ""; }
    if (mode === MODE_CARTE) show("group_carte_id");
    else { hide("group_carte_id"); el("carte_id").value = ""; }
  }

  function updateMenageMode() {
    var m = el("menage_mode").value;
    if (m === "INTERVENANT") { show("group_menage_intervenants"); hide("group_menage_logements"); }
    else if (m === "LOGEMENT") { hide("group_menage_intervenants"); show("group_menage_logements"); }
    else { hide("group_menage_intervenants"); hide("group_menage_logements"); }
  }

  function refreshVisibility() {
    var c = menageComport();
    var val = el("categorie_charge_id").value;
    // Impact ménage : visible seulement si CHOIX ; forcé affiché si FORCE
    if (c === "CHOIX") { show("fs_impact_menage"); show("group_impact_menage_choix"); hide("menage_force_note"); }
    else if (c === "FORCE") { show("fs_impact_menage"); hide("group_impact_menage_choix"); show("menage_force_note"); }
    else { hide("fs_impact_menage"); }

    var men = impactMenageEffectif();
    if (men) {
      show("fs_menage"); hide("fs_affectation");
      hide("group_refacturable");           // ménage jamais refacturable
      el("refacturable").value = "NON";
    } else {
      hide("fs_menage"); show("fs_affectation"); show("group_refacturable");
    }
    // Avantage associé
    if (avantagePossible() && !men) show("fs_avantage"); else { hide("fs_avantage"); el("avantage_associe").value = "NON"; }
    // Libellé personnalisé
    if (val === "CHG_024") { show("group_libelle_perso"); } else { hide("group_libelle_perso"); var lp = el("libelle_categorie_personnalise"); if (lp) lp.value = ""; }
    updateMenageMode();
    updateAvantage();
    updateEffet();
  }

  function updateAvantage() {
    if (el("avantage_associe").value === "OUI") show("group_avantage_associe_id");
    else { hide("group_avantage_associe_id"); el("avantage_associe_id").value = ""; }
  }

  function updateEffet() {
    var impact = el("code_impact").value;
    el("ef_compta").textContent = impact === "IC" ? "Oui" : (impact === "HC" ? "Non" : "—");
    var men = impactMenageEffectif();
    el("ef_menage").textContent = men ? "Oui" : "Non";
    if (men) {
      var mode = el("menage_mode").value;
      var nb = mode === "INTERVENANT" ? selectedCount("menage_intervenants")
             : (mode === "LOGEMENT" ? (selectedCount("menage_logements") + selectedCount("menage_proprietaires")) : 0);
      el("ef_perimetre").textContent = (LIBELLES_MODE_MENAGE[mode] || "—") + " (" + nb + " sélection" + (nb > 1 ? "s" : "") + ")";
      el("ef_refac").textContent = "non applicable (ménage)";
    } else {
      var np = selectedCount("proprietaires"), nl = selectedCount("logements");
      el("ef_perimetre").textContent = (np === 0 && nl === 0) ? "global conciergerie" : (np + " propriétaire(s), " + nl + " logement(s) sélectionné(s)");
      el("ef_refac").textContent = el("refacturable").value === "OUI" ? "mise en réserve de facturation" : "non refacturable";
    }
    el("ef_avantage").textContent = (el("avantage_associe").value === "OUI") ? "associé bénéficiaire" : "non";
  }

  el("categorie_charge_id").addEventListener("change", refreshVisibility);
  el("code_impact").addEventListener("change", updateEffet);
  el("mode_paiement_id").addEventListener("change", updatePaiement);
  el("impact_menage").addEventListener("change", refreshVisibility);
  el("menage_mode").addEventListener("change", function () { updateMenageMode(); updateEffet(); });
  el("avantage_associe").addEventListener("change", function () { updateAvantage(); updateEffet(); });
  el("refacturable").addEventListener("change", updateEffet);
  document.addEventListener("change", function (e) {
    if (e.target && e.target.type === "checkbox") updateEffet();
  });

  // Raccourci de date : aujourd'hui, en date locale (en UTC, juste après minuit, ce serait la veille).
  var btnAujourdhui = el("btn_date_aujourdhui");
  if (btnAujourdhui) {
    btnAujourdhui.addEventListener("click", function () {
      var d = new Date();
      var champ = el("date_charge");
      champ.value = d.getFullYear() + "-" + String(d.getMonth() + 1).padStart(2, "0") + "-" +
        String(d.getDate()).padStart(2, "0");
      champ.dispatchEvent(new Event("change", { bubbles: true }));
      champ.focus();
    });
  }

  updatePaiement();
  refreshVisibility();
})();
