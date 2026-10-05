/* L'écran reste visible pendant le POST ; le moteur et ses états restent côté serveur. */
(function () {
  "use strict";
  const forms = Array.from(document.querySelectorAll("form[data-clo-actualiser]"));
  let enCours = false;
  function focusModule(id) {
    const carte = document.getElementById(id);
    if (!carte) return;
    carte.querySelector("h3").focus({preventScroll: true});
    carte.scrollIntoView({block: "start"});
  }
  // Après le GET, l'ancre et le focus retrouvent la carte, même si son bouton a disparu.
  window.addEventListener("pageshow", function () {
    if (new URLSearchParams(location.search).has("module_actualise")) {
      focusModule(location.hash.slice(1));
    }
  });
  forms.forEach(function (form) {
    form.addEventListener("submit", async function (event) {
      event.preventDefault();
      if (enCours) return;
      enCours = true;
      const carte = form.closest("[data-module]");
      const button = form.querySelector("button");
      const original = button.innerHTML;
      const originalLabel = button.getAttribute("aria-label");
      const feedback = carte.querySelector("[data-clo-feedback]");
      forms.forEach(f => { f.querySelector("button").disabled = true; });
      button.classList.add("is-occupe");
      button.setAttribute("aria-busy", "true");
      carte.setAttribute("aria-busy", "true");
      button.textContent = "Actualisation…";
      button.setAttribute("aria-label", "Actualisation en cours…");
      feedback.hidden = false;
      feedback.setAttribute("role", "status");
      feedback.textContent = "Actualisation en cours…";
      try {
        const response = await fetch(form.action, {
          method: "POST", headers: {"Accept": "application/json"},
          credentials: "same-origin"
        });
        if (!response.ok) throw new Error("actualisation indisponible");
        const resultat = await response.json();
        // Retour fourni par le serveur : même clôture, état relu, ancre du module.
        if (new URL(resultat.retour, location.href).href === location.href) {
          location.reload();
        } else {
          location.replace(resultat.retour);
        }
      } catch (_) {
        feedback.setAttribute("role", "alert");
        feedback.textContent = "L'actualisation n'a pas pu aboutir ou sa réponse n'a pas été reçue. Rechargez cette page pour vérifier les calculs.";
        forms.forEach(f => { f.querySelector("button").disabled = false; });
        button.innerHTML = original;
        if (originalLabel !== null) button.setAttribute("aria-label", originalLabel);
        else button.removeAttribute("aria-label");
        button.classList.remove("is-occupe");
        button.removeAttribute("aria-busy");
        carte.removeAttribute("aria-busy");
        enCours = false;
        button.focus({preventScroll: true});
      }
    });
  });
})();
