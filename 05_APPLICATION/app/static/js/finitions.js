/* Finitions — comportements de confort, strictement visuels.
   Chargé écran par écran à côté de finitions.css. Ne lit ni ne modifie aucune valeur envoyée au
   serveur : pas de name, pas de value, pas de désactivation de bouton (un bouton désactivé au
   moment de l'envoi disparaîtrait des données du formulaire). */
(function () {
  "use strict";

  // Filtres : un champ qui s'écarte de sa valeur par défaut est signalé, et le nombre de filtres
  // posés s'affiche sur « Réinitialiser ».
  function marquerFiltres(form) {
    var actifs = 0;
    form.querySelectorAll(".ec-champ").forEach(function (champ) {
      var ctl = champ.querySelector("select, input:not([type=checkbox]):not([type=file])");
      var actif = !!(ctl && ctl.value && ctl.value.trim() !== "");
      champ.classList.toggle("is-actif", actif);
      if (actif) actifs++;
    });
    form.querySelectorAll(".ec-case").forEach(function (cas) {
      var cb = cas.querySelector("input[type=checkbox]");
      var actif = !!(cb && cb.checked);
      cas.classList.toggle("is-actif", actif);
      if (actif) actifs++;
    });
    form.setAttribute("data-fi-actifs", String(actifs));
    var nb = form.querySelector(".fi-nb-filtres");
    if (nb) {
      nb.textContent = String(actifs);
      nb.hidden = actifs === 0;
    }
  }

  function init() {
    document.querySelectorAll(".ecran .ec-filtres[data-fi-filtres]").forEach(function (form) {
      marquerFiltres(form);
      form.addEventListener("change", function () { marquerFiltres(form); });
      form.addEventListener("input", function () { marquerFiltres(form); });
    });

    // Menus d'actions : un seul ouvert à la fois ; clic ailleurs ou Échap pour refermer.
    var menus = Array.prototype.slice.call(document.querySelectorAll(".ecran details.fi-menu"));
    menus.forEach(function (m) {
      m.addEventListener("toggle", function () {
        if (!m.open) return;
        menus.forEach(function (autre) { if (autre !== m) autre.open = false; });
        var premier = m.querySelector(".fi-menu__panneau a, .fi-menu__panneau input, .fi-menu__panneau button");
        if (premier && m.matches(":focus-within")) premier.focus({ preventScroll: true });
      });
    });
    if (menus.length) {
      document.addEventListener("click", function (e) {
        menus.forEach(function (m) { if (m.open && !m.contains(e.target)) m.open = false; });
      });
      document.addEventListener("keydown", function (e) {
        if (e.key !== "Escape") return;
        menus.forEach(function (m) {
          if (m.open) { m.open = false; var s = m.querySelector("summary"); if (s) s.focus(); }
        });
      });
    }

    // Retour visuel après envoi : le bouton cliqué indique que la demande est partie.
    document.querySelectorAll(".ecran form[data-fi-occupe]").forEach(function (form) {
      form.addEventListener("submit", function (e) {
        if (e.defaultPrevented) return;
        // Un second clic pendant l'envoi ne relance pas le traitement.
        if (form.getAttribute("data-fi-envoye") === "1") { e.preventDefault(); return; }
        form.setAttribute("data-fi-envoye", "1");
        var b = e.submitter || form.querySelector("button[type=submit], button:not([type])");
        if (!b) return;
        b.classList.add("is-occupe");
        b.setAttribute("aria-busy", "true");
        var libelle = form.getAttribute("data-fi-occupe");
        if (libelle) { b.setAttribute("data-fi-libelle", b.textContent); b.textContent = libelle; }
      });
    });
  }

  // Revenir sur la page par « Précédent » ne doit pas laisser un bouton figé « en cours ».
  window.addEventListener("pageshow", function (e) {
    if (!e.persisted) return;
    document.querySelectorAll(".ecran .btn.is-occupe").forEach(function (b) {
      b.classList.remove("is-occupe");
      b.removeAttribute("aria-busy");
      var l = b.getAttribute("data-fi-libelle");
      if (l !== null) { b.textContent = l; b.removeAttribute("data-fi-libelle"); }
    });
    document.querySelectorAll(".ecran form[data-fi-envoye]").forEach(function (f) {
      f.removeAttribute("data-fi-envoye");
    });
  });

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
