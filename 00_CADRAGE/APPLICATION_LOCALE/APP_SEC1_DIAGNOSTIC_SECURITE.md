# APP-SEC-1 — Sécurisation diagnostics, chemins locaux, exposition réseau

État : implémenté, corrigé, testé (36 + 50 tests dédiés + non-régression), prêt pour intégration.
Aucun commit, aucune écriture réelle, flags tous False.

## 0. Correctifs de cette mission (post-audit initial)
- `DOCS_ENABLED` bascule par défaut `False` (était `True`) : `/docs`, `/redoc`, `/openapi.json` → 404
  sans activation explicite.
- `print()` technique du handler d'erreur remplacé par un logger centralisé
  (`app/services/logging_config.py`), jamais l'objet `Request` complet journalisé.
- Sanitizer renforcé : filet générique (chemins UNC, préfixe étendu Windows, guillemets, variables
  d'environnement littérales, tout chemin Windows de racine inconnue) + masquage des identifiants
  bancaires reconnus (compte `CM_xxxxx_xxxxxx`, mouvement `MVT-CM_...`, IBAN) partout où le texte
  passe par le sanitizer — défense en profondeur au-delà des chemins.
- Revue manuelle du diff : aucun import dupliqué, aucune fonction dupliquée, aucun code mort.

## 1. Ce qui a changé

### `/health` — contrat public minimal
Avant : 5 chemins absolus exposés (project_root, master_run_log, exports_powerbi, ref_setup,
snapshots_dir, sqlite.path), y compris le nom du profil Windows via le chemin OneDrive.
Après :
```json
{"status": "ok", "application": "Pilotage Conciergerie", "database": "ok", "sources": "ok",
 "writers_enabled": false}
```
`Cache-Control: no-store`. Codes : 200 (ok) / 503 (degraded).

### `/health/diagnostic` — détail technique local, désactivé par défaut
`app/config.py: DIAGNOSTIC_DETAILS_ENABLED = False` (env `DIAGNOSTIC_DETAILS_ENABLED=true` pour
activer). Retourne 404 si désactivé, 403 si client non local (`127.0.0.1`/`::1` uniquement). Jamais
lié dans le menu. Tout chemin retourné passe par `sanitize_path()` (`<PROJECT_ROOT>`, `<APP_DATA_DIR>`
au lieu du chemin réel) ; état des fichiers en présent/absent/lecture/écriture plutôt qu'en chemin brut.

### Sanitisation centralisée — `app/services/path_sanitizer.py`
`sanitize_text/sanitize_path/sanitize_exception/sanitize_command`. Remplace PROJECT_ROOT,
APP_DATA_DIR, racine du worktree (APP_ROOT.parent), répertoire utilisateur (`Path.home()`), dossier
temporaire (`tempfile.gettempdir()`) — du plus spécifique au plus général, insensible casse Windows,
gère `\` et `/`, chemins multiples dans un même texte, tracebacks, arguments de commande.

### Runner — logs/motifs sanitisés
`controles_runner_service.py` : tous les `stderr` de Lot8c/Lot11 tronqués et sanitisés avant d'être
inclus dans une exception ; le message final (`motif`) affiché à l'utilisateur ET journalisé en base
(`controles_runs.erreur_resume`) passe par `sanitize_text` — plus aucun chemin Temp/username en cas
d'échec moteur.

### Gestion des erreurs — `app/main.py`
Handler d'exception global : message public générique + référence courte (`ERR-XXXXXXXX`), détail
sanitisé écrit dans les logs serveur locaux (jamais renvoyé au client). Aucune stack trace, aucune
requête SQL, aucun chemin dans la réponse HTTP.

### Headers HTTP
Middleware ajouté : `X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`,
`Referrer-Policy: no-referrer` sur toutes les réponses ; `Cache-Control: no-store` sur les écrans
contenant des données métier (banques, contrôles, propriétaires, réservations, logements, ménages,
fournisseurs, health).

### Documentation API
`DOCS_ENABLED` (env, défaut `false`) contrôle `/docs`, `/redoc`, `/openapi.json` — désactivés par
défaut, activation explicite requise (`DOCS_ENABLED=true`).

### Garde de démarrage — `run_app.py`
Hôte forcé `127.0.0.1`. `_refuse_si_reseau()` refuse tout host non local sauf
`cfg.ALLOW_NETWORK_BIND=True` explicite (jamais par défaut). Détection d'instance déjà active sur le
port avant démarrage (refus). Affichage de l'état des writers sans jamais afficher de chemin.

## 2. Ce qui n'a pas changé (déjà sûr)
- Pas de middleware CORS (donc pas d'origine `*`).
- Identifiants bancaires/contrôles déjà opaques (CPT-/MVT-/CTRL-, APP-4B/5B).
- Exports CSV et écrans HTML déjà sans chemin absolu (audités intégration).
- Aucun secret, token, clé API, mot de passe trouvé dans la base de code.

## 3. Limites
Voir POLITIQUE_EXPOSITION_LOCALE.md : pas d'authentification complète (hors périmètre volontaire —
application locale mono-utilisateur), pas de TLS (HTTP local). Le masquage des identifiants bancaires
couvre les FORMES CONNUES (compte, mouvement, IBAN) ; une chaîne arbitraire non structurée (ex. un
« token » inventé sans motif reconnaissable) ne peut pas être détectée génériquement — non un risque
réel ici : l'application ne détient aucun secret/token/clé API par conception (audité).
