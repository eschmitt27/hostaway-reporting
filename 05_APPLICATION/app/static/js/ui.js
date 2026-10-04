/* UI — comportements de présentation communs (2026-10-04). STRICTEMENT VISUELS.
   Ne lit ni ne modifie aucune valeur envoyée au serveur, n'empêche aucune soumission, ne
   désactive aucun bouton. Chargé par base.html sur tous les écrans. */
(function () {
  "use strict";

  // Barre latérale escamotable (tablette / téléphone).
  function navigation() {
    var racine = document.documentElement;
    var bouton = document.querySelector("[data-ui-burger]");
    var voile = document.querySelector("[data-ui-voile]");
    if (!bouton) return;
    function basculer(ouvrir) {
      racine.classList.toggle("ui-nav-ouverte", ouvrir);
      bouton.setAttribute("aria-expanded", ouvrir ? "true" : "false");
    }
    bouton.addEventListener("click", function () {
      basculer(!racine.classList.contains("ui-nav-ouverte"));
    });
    if (voile) voile.addEventListener("click", function () { basculer(false); });
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape" && racine.classList.contains("ui-nav-ouverte")) { basculer(false); bouton.focus(); }
    });
  }

  // Menus « ⋯ » des factures clients (details.fc-menu) : un seul ouvert, clic ailleurs ou Échap.
  function menus() {
    var liste = Array.prototype.slice.call(document.querySelectorAll("details.fc-menu"));
    if (!liste.length) return;
    liste.forEach(function (m) {
      var s = m.querySelector(":scope > summary");
      if (s) s.setAttribute("aria-haspopup", "menu");
      m.addEventListener("toggle", function () {
        if (s) s.setAttribute("aria-expanded", m.open ? "true" : "false");
        if (!m.open) return;
        liste.forEach(function (autre) { if (autre !== m) autre.open = false; });
      });
    });
    document.addEventListener("click", function (e) {
      liste.forEach(function (m) { if (m.open && !m.contains(e.target)) m.open = false; });
    });
    document.addEventListener("keydown", function (e) {
      if (e.key !== "Escape") return;
      liste.forEach(function (m) {
        if (!m.open) return;
        m.open = false;
        var s = m.querySelector(":scope > summary");
        if (s) s.focus();
      });
    });
  }

  // Tableaux nus trop larges : enveloppés dans un conteneur à défilement horizontal propre.
  function tableaux() {
    document.querySelectorAll(".page-content table").forEach(function (t) {
      var p = t.parentElement;
      if (!p || p.classList.contains("ui-table-scroll")) return;
      var st = window.getComputedStyle(p);
      if (st.overflowX === "auto" || st.overflowX === "scroll") return;
      if (t.scrollWidth <= p.clientWidth + 1) return;
      var w = document.createElement("div");
      w.className = "ui-table-scroll";
      p.insertBefore(w, t);
      w.appendChild(t);
    });
  }

  // Indicateur d'envoi sur les formulaires POST qui n'en ont pas déjà (data-fi-occupe) : purement
  // visuel, posé seulement si l'envoi n'a pas été annulé (confirmation refusée).
  function envois() {
    document.addEventListener("submit", function (e) {
      var f = e.target;
      if (!(f instanceof HTMLFormElement)) return;
      if ((f.getAttribute("method") || "get").toLowerCase() !== "post") return;
      if (f.hasAttribute("data-fi-occupe") || f.hasAttribute("download")) return;
      var b = e.submitter;
      setTimeout(function () {
        if (e.defaultPrevented || !b || !b.classList || !b.classList.contains("btn")) return;
        b.classList.add("is-occupe");
      }, 0);
    });
    window.addEventListener("pageshow", function (ev) {
      if (!ev.persisted) return;
      document.querySelectorAll(".btn.is-occupe").forEach(function (b) { b.classList.remove("is-occupe"); });
    });
  }

  // Ligne visée par l'ancre (#…) : brève surbrillance pour retrouver l'élément après une action.
  function cible() {
    if (!location.hash || location.hash.length < 2) return;
    var el;
    try { el = document.querySelector(location.hash); } catch (e) { return; }
    if (!el) return;
    var ligne = el.closest("tr") || el;
    ligne.classList.add("ui-ligne-cible");
  }

  function init() {
    navigation();
    menus();
    tableaux();
    envois();
    cible();
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
