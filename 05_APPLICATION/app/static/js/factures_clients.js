/* Factures clients — comportements de confort, strictement présentationnels.
   Ne calcule aucun montant, ne modifie aucune valeur envoyée au serveur, ne désactive aucun
   bouton : tout ce qui part au serveur est exactement ce que l'utilisateur a saisi. Sans
   JavaScript, chaque formulaire de la fiche fonctionne à l'identique. */
(function () {
  "use strict";

  function toutes(sel, racine) {
    return Array.prototype.slice.call((racine || document).querySelectorAll(sel));
  }

  // Erreur d'action : le message reçoit le focus, pour être lu aussitôt par un lecteur d'écran et
  // vu sans chercher. Le défilement suit.
  function focaliserErreur() {
    var e = document.querySelector(".ecran.fc [data-fc-erreur]");
    if (!e) return;
    e.focus({ preventScroll: true });
    e.scrollIntoView({ block: "start", behavior: "auto" });
  }

  // Actions irréversibles ou lourdes : confirmation explicite, message porté par le gabarit.
  function confirmations() {
    // Suppression d'une facture non émise : dialogue « Annuler / Supprimer » (repli : confirm()).
    var dlg = document.getElementById("fc-dialogue-supprimer");
    var enAttente = null;
    if (dlg && typeof dlg.showModal === "function") {
      dlg.querySelector("[data-fc-dialogue-annuler]").addEventListener("click", function () {
        enAttente = null; dlg.close();
      });
      dlg.querySelector("[data-fc-dialogue-confirmer]").addEventListener("click", function () {
        var f = enAttente; enAttente = null; dlg.close();
        if (f) { f.setAttribute("data-fc-confirme", "1"); f.submit(); }
      });
    }
    var retour = document.getElementById("fc-dialogue-brouillon");
    var formulaireRetour = null;
    var declencheurRetour = null;
    if (retour && typeof retour.showModal === "function") {
      retour.querySelector("[data-fc-brouillon-annuler]").addEventListener("click", function () { retour.close(); });
      retour.addEventListener("close", function () {
        formulaireRetour = null;
        if (declencheurRetour) declencheurRetour.focus();
      });
      retour.querySelector("[data-fc-brouillon-confirmer]").addEventListener("click", function () {
        var f = formulaireRetour; formulaireRetour = null; retour.close();
        if (f) { f.submit(); }
      });
    }
    toutes(".ecran.fc form[data-fc-confirmer]").forEach(function (form) {
      form.addEventListener("submit", function (e) {
        if (form.hasAttribute("data-fc-brouillon") && retour && typeof retour.showModal === "function") {
          e.preventDefault(); formulaireRetour = form; declencheurRetour = form.closest(".fc-menu") ? form.closest(".fc-menu").querySelector("summary") : document.activeElement;
          var emise = form.getAttribute("data-fc-brouillon") === "emise";
          retour.querySelector("[data-fc-brouillon-titre]").textContent = emise ?
            "Retirer l'émission et remettre cette facture en brouillon ?" : "Remettre cette facture en brouillon ?";
          retour.querySelector("#fc-brouillon-texte").textContent = emise ?
            "La facture redeviendra modifiable. Son émission actuelle sera annulée dans l'application et son ancien numéro restera tracé comme ayant été utilisé. Une nouvelle émission recevra un nouveau numéro." :
            "Elle redeviendra modifiable et pourra être recalculée avant une nouvelle validation.";
          retour.querySelector("#fc-brouillon-info").hidden = !emise;
          retour.showModal(); return;
        }
        if (form.hasAttribute("data-fc-supprimer") && dlg && typeof dlg.showModal === "function") {
          e.preventDefault(); enAttente = form; dlg.showModal(); return;
        }
        if (!window.confirm(form.getAttribute("data-fc-confirmer"))) e.preventDefault();
      });
    });
  }

  // Champs qui n'ont de sens que pour un choix donné (ex. motif « hors compta », bloc « charge »).
  // Le champ masqué reste dans le formulaire : rien n'est retiré de ce qui est envoyé.
  function dependances() {
    toutes(".ecran.fc [data-fc-si]").forEach(function (bloc) {
      var regle = bloc.getAttribute("data-fc-si").split("=");
      var nom = regle[0], attendu = regle[1];
      var form = bloc.closest("form");
      if (!form) return;
      function maj() {
        var ctl = form.querySelector('[name="' + nom + '"]:checked') ||
                  form.querySelector('select[name="' + nom + '"]');
        bloc.hidden = !(ctl && ctl.value === attendu);
      }
      form.addEventListener("change", maj);
      maj();
    });
  }

  // Effet d'une charge saisie depuis la fiche : impact résultat / comptabilité du mode choisi.
  // Mêmes libellés et même mécanique que la saisie de charge fournisseur.
  function impactCharge() {
    var donnees = document.getElementById("fc-modes-charge");
    var select = document.getElementById("code_impact");
    if (!donnees || !select) return;
    var modes;
    try { modes = JSON.parse(donnees.textContent || "[]"); } catch (err) { return; }
    function maj() {
      var mode = modes.filter(function (m) { return m.code_impact === select.value; })[0];
      var reel = document.getElementById("ef_reel"), compta = document.getElementById("ef_compta");
      if (reel) reel.textContent = mode ? (mode.impact_resultat_reel === "OUI" ? "Oui" : "Non") : "—";
      if (compta) compta.textContent = mode ? (mode.impact_resultat_comptable === "OUI" ? "Oui" : "Non") : "—";
    }
    select.addEventListener("change", maj);
    maj();
  }

  // Séjours : une ligne modifiée et pas encore enregistrée est signalée, et son bouton passe en
  // action principale. Le formulaire reste celui de la ligne (attribut `form`).
  function sejoursModifies() {
    toutes(".ecran.fc tr[data-fc-sejour]").forEach(function (tr) {
      var champs = toutes("input[data-fc-initial]", tr);
      var bouton = tr.querySelector("button[type=submit]");
      function maj() {
        var change = champs.some(function (c) { return c.value.trim() !== c.getAttribute("data-fc-initial"); });
        tr.classList.toggle("is-modifie", change);
        if (bouton) {
          bouton.classList.toggle("btn-primary", change);
          bouton.classList.toggle("btn-secondary", !change);
        }
        var etat = tr.querySelector("[data-fc-etat]");
        if (etat) etat.textContent = change ? "Modification non enregistrée" : "";
      }
      champs.forEach(function (c) { c.addEventListener("input", maj); });
    });
  }

  // Sommaire : la section visible est signalée (aria-current), sans modifier le défilement.
  function sommaireActif() {
    var liens = toutes(".ecran.fc .fc-sommaire a[href^='#']");
    if (!liens.length || !("IntersectionObserver" in window)) return;
    var parId = {};
    liens.forEach(function (a) { parId[a.getAttribute("href").slice(1)] = a; });
    var obs = new IntersectionObserver(function (entrees) {
      entrees.forEach(function (en) {
        if (!en.isIntersecting) return;
        liens.forEach(function (a) { a.removeAttribute("aria-current"); });
        var a = parId[en.target.id];
        if (a) a.setAttribute("aria-current", "true");
      });
    }, { rootMargin: "-20% 0px -70% 0px" });
    Object.keys(parId).forEach(function (id) {
      var cible = document.getElementById(id);
      if (cible) obs.observe(cible);
    });
  }

  function init() {
    focaliserErreur();
    confirmations();
    dependances();
    impactCharge();
    sejoursModifies();
    sommaireActif();
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
