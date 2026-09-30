/* Résultats — comportements des écrans /resultats (synthèse) et /resultats/pilotage (analyse).
   Chargé à côté de finitions.js. Présentation seulement : rien n'est envoyé au serveur, aucune
   valeur n'est recalculée — le graphique dessine les montants reçus tels quels (bloc JSON posé par
   le gabarit), le tableau « Voir les données » les donne en chiffres.

   1. Filtres : choisir un propriétaire restreint la liste des logements aux siens (sans recharger).
   2. Graphique « Évolution mensuelle » : courbes sur un canvas, légende qui affiche / masque
      chaque courbe, infobulle au survol, au toucher et au clavier (flèches). */
(function () {
  "use strict";

  var MOIS = ["janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août",
              "septembre", "octobre", "novembre", "décembre"];
  var MOIS_ABR = ["Janv.", "Févr.", "Mars", "Avr.", "Mai", "Juin", "Juil.", "Août",
                  "Sept.", "Oct.", "Nov.", "Déc."];
  var SERIES = [
    { cle: "ca_conciergerie", libelle: "CA conciergerie", couleur: "--fi-serie-2", defaut: "#eb6834" },
    { cle: "commission", libelle: "Commission", couleur: "--fi-serie-3", defaut: "#1baf7a" },
    { cle: "total_payout", libelle: "Total payout", couleur: "--fi-serie-1", defaut: "#2a78d6" }
  ];

  var nf2 = new Intl.NumberFormat("fr-FR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  var nf0 = new Intl.NumberFormat("fr-FR", { maximumFractionDigits: 0 });
  var nf1 = new Intl.NumberFormat("fr-FR", { maximumFractionDigits: 1 });
  // Même rendu que le filtre serveur `euros` : espace insécable, vrai signe moins.
  function espaces(t) { return t.replace(/[ \s]/g, " "); }
  function euros(v) {
    var signe = v < 0 && Math.abs(v) >= 0.005 ? "−" : "";
    return signe + espaces(nf2.format(Math.abs(v))) + " €";
  }
  function eurosAxe(v) {
    var signe = v < 0 ? "−" : "";
    var a = Math.abs(v);
    if (a >= 10000) return signe + espaces(nf1.format(a / 1000)) + " k€";
    return signe + espaces(nf0.format(a)) + " €";
  }
  function estNombre(v) { return typeof v === "number" && isFinite(v); }
  function moisLong(m) {
    var p = String(m || "").split("-");
    return p.length === 2 ? MOIS[+p[1] - 1].charAt(0).toUpperCase() + MOIS[+p[1] - 1].slice(1) + " " + p[0] : String(m);
  }

  // ── 1. Filtres : logements du propriétaire choisi ─────────────────────────────────────────
  function lierFiltres(form) {
    var selP = form.querySelector("[data-rs-proprietaire]");
    var selL = form.querySelector("[data-rs-logement]");
    if (!selP || !selL) return;
    function restreindre(auChangement) {
      var pid = selP.value;
      Array.prototype.forEach.call(selL.options, function (o) {
        if (!o.value) return;
        var ok = !pid || (o.getAttribute("data-proprietaires") || "").split(" ").indexOf(pid) >= 0;
        // Au chargement, le logement déjà appliqué reste visible même s'il ne correspond pas :
        // la liste ne doit jamais contredire le filtre réellement appliqué à l'écran.
        var garde = !auChangement && o.selected;
        o.hidden = !ok && !garde;
        o.disabled = !ok && !garde;
      });
      if (auChangement && selL.selectedIndex > 0 && selL.options[selL.selectedIndex].disabled) {
        selL.value = "";
        selL.dispatchEvent(new Event("change", { bubbles: true }));
      }
    }
    selP.addEventListener("change", function () { restreindre(true); });
    restreindre(false);
  }

  // ── 2. Graphique « Évolution mensuelle » ──────────────────────────────────────────────────
  function pasRond(brut) {
    var p = Math.pow(10, Math.floor(Math.log10(brut)));
    var n = brut / p;
    return (n <= 1 ? 1 : n <= 2 ? 2 : n <= 2.5 ? 2.5 : n <= 5 ? 5 : 10) * p;
  }

  function Graphique(section) {
    var donnees;
    try {
      donnees = JSON.parse(section.querySelector("[data-rs-donnees]").textContent);
    } catch (e) { return; }
    var points = (donnees && donnees.points) || [];
    var canvas = section.querySelector("[data-rs-canvas]");
    var bulle = section.querySelector("[data-rs-infobulle]");
    var message = section.querySelector("[data-rs-message]");
    if (!canvas || !points.length || !canvas.getContext) return;
    var ctx = canvas.getContext("2d");
    var css = getComputedStyle(section);
    function lire(nom, defaut) { return (css.getPropertyValue(nom) || "").trim() || defaut; }
    var encre = lire("--fi-encre-secondaire", "#5b5f61");
    var encreForte = lire("--color-on-surface", "#101b30");
    var grille = lire("--fi-grille", "#e8edff");
    var axeZero = lire("--color-outline-variant", "#c0c8cb");
    var surface = "#ffffff";
    var series = SERIES.map(function (s) {
      return { cle: s.cle, libelle: s.libelle, couleur: lire(s.couleur, s.defaut) };
    });
    var actives = {};
    series.forEach(function (s) { actives[s.cle] = true; });
    var surligne = points.map(function (p) { return p.mois; }).indexOf(donnees.surligne);
    var geo = null, survol = -1;

    // Accès clavier : le graphique se parcourt aux flèches ; l'infobulle suit, et l'annonce vocale
    // reprend la même lecture (le tableau reste l'équivalent complet).
    canvas.tabIndex = 0;
    var aide = document.createElement("p");
    aide.className = "sr-only";
    aide.id = "rs-aide-" + Math.random().toString(36).slice(2, 8);
    aide.textContent = "Flèches gauche et droite : lire chaque mois.";
    var annonce = document.createElement("div");
    annonce.className = "sr-only";
    annonce.setAttribute("aria-live", "polite");
    section.appendChild(aide);
    section.appendChild(annonce);
    canvas.setAttribute("aria-describedby", aide.id);

    function visibles() { return series.filter(function (s) { return actives[s.cle]; }); }

    function redimensionner() {
      var r = canvas.getBoundingClientRect();
      if (!r.width) return;
      var dpr = window.devicePixelRatio || 1;
      canvas.width = Math.round(r.width * dpr);
      canvas.height = Math.round(r.height * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      dessiner();
    }

    function dessiner() {
      var w = canvas.clientWidth, h = canvas.clientHeight;
      ctx.clearRect(0, 0, w, h);
      var vis = visibles();
      var valeurs = [];
      points.forEach(function (p) { vis.forEach(function (s) { if (estNombre(p[s.cle])) valeurs.push(p[s.cle]); }); });
      if (!vis.length || !valeurs.length) {
        geo = null;
        message.textContent = !vis.length
          ? "Aucune courbe affichée : sélectionnez au moins un indicateur dans la légende."
          : "Aucune valeur pour les courbes sélectionnées sur cette période.";
        message.hidden = false;
        return;
      }
      message.hidden = true;

      // Échelle unique en euros, bornes rondes, zéro toujours inclus.
      var min = Math.min(0, Math.min.apply(null, valeurs));
      var max = Math.max(0, Math.max.apply(null, valeurs));
      if (max === min) max = min + 1;
      var pas = pasRond((max - min) / 4);
      var bas = Math.floor(min / pas) * pas, haut = Math.ceil(max / pas) * pas;
      var graduations = [];
      for (var v = bas; v <= haut + pas / 2; v += pas) graduations.push(Math.abs(v) < pas / 1e6 ? 0 : v);

      ctx.font = '12px "Segoe UI", Arial, sans-serif';
      var largeurAxe = 0;
      graduations.forEach(function (g) { largeurAxe = Math.max(largeurAxe, ctx.measureText(eurosAxe(g)).width); });
      var padL = Math.ceil(largeurAxe) + 14, padR = 14, padT = 12, padB = 40;
      var plotW = Math.max(10, w - padL - padR), plotH = Math.max(10, h - padT - padB);
      var n = points.length;
      var stepX = n > 1 ? plotW / (n - 1) : 0;
      function xDe(i) { return padL + (n > 1 ? stepX * i : plotW / 2); }
      function yDe(val) { return padT + plotH - ((val - bas) / (haut - bas)) * plotH; }

      // Mois sélectionné (fenêtre de contexte) : un aplat discret derrière sa colonne.
      if (surligne >= 0) {
        var lb = Math.max(18, Math.min(56, stepX || 56));
        var g0 = Math.max(padL - 6, xDe(surligne) - lb / 2);
        var g1 = Math.min(padL + plotW + 6, xDe(surligne) + lb / 2);
        ctx.fillStyle = "rgba(0, 52, 65, 0.07)";
        ctx.fillRect(g0, padT, g1 - g0, plotH);
      }

      // Grille horizontale recessive, libellés à gauche.
      ctx.textAlign = "right";
      ctx.textBaseline = "middle";
      graduations.forEach(function (g) {
        var y = Math.round(yDe(g)) + 0.5;
        ctx.strokeStyle = g === 0 ? axeZero : grille;
        ctx.lineWidth = 1;
        ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(padL + plotW, y); ctx.stroke();
        ctx.fillStyle = encre;
        ctx.fillText(eurosAxe(g), padL - 8, y);
      });

      // Mois : abréviation, année dessous au premier libellé et à chaque changement d'année.
      // Espacés pour ne jamais se chevaucher, ancrés sur le DERNIER mois (le plus récent est
      // toujours nommé).
      ctx.textAlign = "center";
      ctx.textBaseline = "alphabetic";
      var largeurMois = ctx.measureText("Sept.").width + 14;
      var pasLib = Math.max(1, Math.ceil(largeurMois / Math.max(1, stepX || plotW)));
      var anneePrec = "";
      points.forEach(function (p, i) {
        if ((n - 1 - i) % pasLib !== 0) return;
        var morceaux = String(p.mois).split("-");
        var fort = i === survol || i === surligne;
        ctx.font = (fort ? "600 " : "") + '12px "Segoe UI", Arial, sans-serif';
        ctx.fillStyle = fort ? encreForte : encre;
        ctx.fillText(MOIS_ABR[+morceaux[1] - 1] || p.mois, xDe(i), padT + plotH + 18);
        if (morceaux[0] !== anneePrec) {
          ctx.font = '11px "Segoe UI", Arial, sans-serif';
          ctx.fillStyle = encre;
          ctx.fillText(morceaux[0], xDe(i), padT + plotH + 33);
          anneePrec = morceaux[0];
        }
      });
      ctx.font = '12px "Segoe UI", Arial, sans-serif';

      // Repère vertical du mois lu.
      if (survol >= 0) {
        var xs = Math.round(xDe(survol)) + 0.5;
        ctx.strokeStyle = lire("--color-outline", "#70787c");
        ctx.lineWidth = 1;
        ctx.beginPath(); ctx.moveTo(xs, padT); ctx.lineTo(xs, padT + plotH); ctx.stroke();
      }

      // Courbes : traits de 2 px ; points de 8 px cerclés de la couleur du fond (lisibles aux
      // croisements) quand la série est courte, toujours sur le mois lu.
      vis.forEach(function (s) {
        ctx.strokeStyle = s.couleur;
        ctx.lineWidth = 2;
        ctx.lineJoin = "round";
        ctx.lineCap = "round";
        ctx.beginPath();
        var ouvert = false;
        points.forEach(function (p, i) {
          if (!estNombre(p[s.cle])) { ouvert = false; return; }
          if (!ouvert) { ctx.moveTo(xDe(i), yDe(p[s.cle])); ouvert = true; }
          else ctx.lineTo(xDe(i), yDe(p[s.cle]));
        });
        ctx.stroke();
        points.forEach(function (p, i) {
          if (!estNombre(p[s.cle])) return;
          var rayon = i === survol ? 5 : (n <= 24 || n === 1 ? 4 : 0);
          if (!rayon) return;
          ctx.beginPath(); ctx.arc(xDe(i), yDe(p[s.cle]), rayon + 2, 0, Math.PI * 2);
          ctx.fillStyle = surface; ctx.fill();
          ctx.beginPath(); ctx.arc(xDe(i), yDe(p[s.cle]), rayon, 0, Math.PI * 2);
          ctx.fillStyle = s.couleur; ctx.fill();
        });
      });
      geo = { padL: padL, stepX: stepX, xDe: xDe, n: n };
    }

    function lecture(i) {
      var p = points[i];
      return visibles().filter(function (s) { return estNombre(p[s.cle]); })
        .map(function (s) { return { serie: s, valeur: p[s.cle] }; });
    }

    // Infobulle construite en DOM (textContent) : la valeur d'abord, en gras, puis la série.
    function montrer(i, annoncer) {
      if (i < 0 || !geo) { bulle.hidden = true; return; }
      while (bulle.firstChild) bulle.removeChild(bulle.firstChild);
      var titre = document.createElement("div");
      titre.className = "fi-infobulle__titre";
      titre.textContent = moisLong(points[i].mois);
      bulle.appendChild(titre);
      var lignes = lecture(i);
      lignes.forEach(function (l) {
        var ligne = document.createElement("div");
        ligne.className = "fi-infobulle__ligne rs-infobulle__ligne";
        var cle = document.createElement("span");
        cle.className = "rs-infobulle__cle";
        cle.style.background = l.serie.couleur;
        var val = document.createElement("strong");
        val.textContent = euros(l.valeur);
        var lib = document.createElement("span");
        lib.textContent = l.serie.libelle;
        ligne.appendChild(cle); ligne.appendChild(val); ligne.appendChild(lib);
        bulle.appendChild(ligne);
      });
      if (!lignes.length) {
        var vide = document.createElement("div");
        vide.textContent = "Aucune valeur affichée";
        bulle.appendChild(vide);
      }
      bulle.hidden = false;
      var largeur = canvas.clientWidth;
      var x = geo.xDe(i);
      var gauche = x + 14 + bulle.offsetWidth > largeur ? x - 14 - bulle.offsetWidth : x + 14;
      bulle.style.left = Math.max(0, gauche) + "px";
      if (annoncer) {
        annonce.textContent = moisLong(points[i].mois) + " : " + (lignes.length
          ? lignes.map(function (l) { return l.serie.libelle + " " + euros(l.valeur); }).join(", ")
          : "aucune valeur affichée") + ".";
      }
    }

    function lireIndex(i, annoncer) {
      i = Math.min(points.length - 1, Math.max(0, i));
      if (i !== survol) { survol = i; dessiner(); }
      montrer(i, annoncer);
    }

    function indexDepuis(clientX) {
      if (!geo) return -1;
      var x = clientX - canvas.getBoundingClientRect().left;
      return geo.n > 1 ? Math.round((x - geo.padL) / geo.stepX) : 0;
    }

    canvas.addEventListener("pointermove", function (e) { if (geo) lireIndex(indexDepuis(e.clientX), false); });
    canvas.addEventListener("pointerdown", function (e) { if (geo) lireIndex(indexDepuis(e.clientX), false); });
    canvas.addEventListener("pointerleave", function (e) {
      if (e.pointerType === "mouse" && document.activeElement !== canvas) { survol = -1; dessiner(); montrer(-1); }
    });
    canvas.addEventListener("focus", function () {
      if (geo) lireIndex(survol >= 0 ? survol : (surligne >= 0 ? surligne : points.length - 1), true);
    });
    canvas.addEventListener("blur", function () { survol = -1; dessiner(); montrer(-1); });
    canvas.addEventListener("keydown", function (e) {
      if (!geo) return;
      var cible = { ArrowLeft: survol - 1, ArrowRight: survol + 1, Home: 0, End: points.length - 1 }[e.key];
      if (e.key === "Escape") { canvas.blur(); return; }
      if (cible === undefined) return;
      e.preventDefault();
      lireIndex(cible, true);
    });

    // Légende : chaque bouton affiche / masque sa courbe ; l'échelle se réajuste aux courbes
    // visibles (comparer deux indicateurs sans la troisième qui écrase l'axe).
    section.querySelectorAll(".rs-legende__item[data-serie]").forEach(function (b) {
      b.addEventListener("click", function () {
        var cle = b.getAttribute("data-serie");
        actives[cle] = !actives[cle];
        b.setAttribute("aria-pressed", actives[cle] ? "true" : "false");
        dessiner();
        if (survol >= 0) montrer(survol, false);
      });
    });

    if (window.ResizeObserver) new ResizeObserver(redimensionner).observe(canvas);
    else window.addEventListener("resize", redimensionner);
    redimensionner();
  }

  function init() {
    document.querySelectorAll(".ecran form[data-rs-filtres]").forEach(lierFiltres);
    document.querySelectorAll(".ecran [data-rs-graphique]").forEach(function (s) {
      try { Graphique(s); } catch (e) { /* le tableau « Voir les données » reste disponible */ }
    });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
