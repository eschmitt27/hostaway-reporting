# Audit application locale existante

Date: 2026-06-30
Perimetre audite: `Conciergerie app/`
Mode: lecture seule sur l'application; creation de ce rapport uniquement.

## 1. Structure constatee

```text
Conciergerie app/
├── Bienvenue.md
├── Sans titre/
└── .obsidian/
    ├── app.json
    ├── appearance.json
    ├── community-plugins.json
    ├── core-plugins.json
    ├── graph.json
    ├── workspace.json
    └── plugins/
        ├── claude-code-mcp/
        │   ├── main.js
        │   ├── manifest.json
        │   └── styles.css
        └── obsidian42-brat/
            ├── brat-migrations.json
            ├── data.json
            ├── main.js
            ├── manifest.json
            └── styles.css
```

Le dossier `Sans titre/` est vide. Le seul contenu metier visible hors configuration Obsidian est `Bienvenue.md`, qui correspond a la note d'accueil standard d'un nouveau coffre Obsidian.

## 2. Langage et framework

Aucune application web, desktop ou backend dediee n'est presente.

Element detecte: coffre Obsidian.

Langages presents uniquement via plugins Obsidian installes:

- JavaScript: bundles de plugins communautaires dans `.obsidian/plugins/*/main.js`.
- CSS: styles des plugins.
- JSON: configuration Obsidian et manifests de plugins.
- Markdown: note d'accueil `Bienvenue.md`.

Aucun framework applicatif detecte: pas de React, Vue, Svelte, Next.js, Vite, Electron applicatif propre, Flask, FastAPI, Django, Streamlit, etc.

## 3. Fichiers de configuration

Configuration Obsidian detectee:

- `.obsidian/app.json`: vide (`{}`).
- `.obsidian/appearance.json`: vide (`{}`).
- `.obsidian/community-plugins.json`: plugins communautaires actives: `obsidian42-brat`, `claude-code-mcp`.
- `.obsidian/core-plugins.json`: plugins coeur Obsidian configures.
- `.obsidian/graph.json`: configuration de la vue graphe.
- `.obsidian/workspace.json`: etat de l'interface Obsidian, derniere vue ouverte: graphe.

Plugins detectes:

- `claude-code-mcp`, version `1.1.8`, plugin desktop reliant Obsidian a Claude Code / MCP.
- `obsidian42-brat`, version `2.0.8`, plugin d'installation de plugins beta.

Aucun fichier de configuration applicatif standard detecte:

- pas de `package.json`;
- pas de lockfile npm/pnpm/yarn;
- pas de `pyproject.toml`, `requirements.txt`, `Pipfile` ou `poetry.lock`;
- pas de `Dockerfile` ou `docker-compose.yml`;
- pas de `.env` ou `.env.example`;
- pas de `tsconfig.json`, `vite.config.*`, `next.config.*`.

## 4. Point d'entree probable

Point d'entree reel actuellement: ouverture du dossier comme coffre Obsidian.

Aucun point d'entree applicatif autonome n'est present:

- pas de `main.py`, `app.py`, `server.js`, `index.html`;
- pas de dossier `src/`, `app/`, `pages/`, `components/`, `api/`, `server/`, `backend/` ou `frontend/`;
- pas de script de lancement.

## 5. Dependances

Aucune dependance applicative declaree.

Les seules dependances observables sont embarquees dans les plugins Obsidian installes sous `.obsidian/plugins/`. Elles ne constituent pas une base applicative metier pour la conciergerie.

## 6. Base de donnees eventuelle

Aucune base de donnees detectee dans `Conciergerie app/`:

- pas de fichier `.db`, `.sqlite`, `.sqlite3`;
- pas de schema SQL;
- pas de dossier `database/`, `db/` ou `migrations/`.

## 7. Docker eventuel

Aucun Docker detecte:

- pas de `Dockerfile`;
- pas de `docker-compose.yml` ou `docker-compose.yaml`.

## 8. Pages, ecrans et interface existants

Ecrans applicatifs metier: aucun.

Elements d'interface presents uniquement via Obsidian:

- explorateur de fichiers;
- recherche;
- graphe;
- retroliens;
- liens sortants;
- mots-cles;
- proprietes;
- plan.

Le workspace Obsidian ouvre actuellement la vue graphe. Aucun ecran pour Clients, Reservations hors Hostaway, Menages, AirCover / To-Do, Rapprochement bancaire, Prefactures / factures proprietaires, Referentiels ou Parametres n'existe dans le dossier audite.

## 9. API ou backend existant

Aucun backend applicatif detecte.

Le plugin `claude-code-mcp` contient du code JavaScript lie au protocole MCP / WebSocket, mais il s'agit d'un plugin Obsidian tiers, pas d'une API conciergerie ni d'un backend metier du projet.

## 10. Composants d'interface

Aucun composant d'interface propre au projet detecte.

Les seuls fichiers CSS/JS sont ceux des plugins Obsidian tiers:

- `.obsidian/plugins/claude-code-mcp/main.js`
- `.obsidian/plugins/claude-code-mcp/styles.css`
- `.obsidian/plugins/obsidian42-brat/main.js`
- `.obsidian/plugins/obsidian42-brat/styles.css`

## 11. Variables d'environnement attendues

Aucune variable d'environnement applicative n'est documentee ou declaree dans `Conciergerie app/`.

Le fichier BRAT contient des champs de configuration internes (`globalTokenName`, `personalAccessToken`), vides, propres au plugin Obsidian. Cela ne constitue pas une configuration de l'application locale cible.

## 12. Fichiers manquants ou incoherents

Pour pouvoir lancer une application locale, il manque au minimum:

- une decision de stack technique;
- un manifeste de dependances;
- un point d'entree;
- une structure source minimale;
- un mode de lancement explicite;
- une strategie de lecture/ecriture des donnees sans dupliquer le moteur existant;
- une convention claire pour declencher les traitements existants uniquement sur action utilisateur;
- une convention pour afficher les controles et exports deja produits par les lots.

Incoherence principale: le dossier s'appelle `Conciergerie app/`, mais son contenu correspond actuellement a un coffre Obsidian quasi vide, pas a une application locale executable.

## 13. Verification des duplications du moteur metier

Recherche effectuee sur les mots-cles suivants: commissions, net proprietaire, resolution proprietaire, taux de commission, menages, reservations, flux, factures, controles, AirCover, To-Do, clients, banque/bancaire.

Resultat hors plugins Obsidian tiers: aucune occurrence metier detectee.

Resultat dans plugins Obsidian tiers: occurrences generiques du mot `client` dans le code du plugin `claude-code-mcp`, liees au protocole WebSocket/MCP, sans rapport avec les clients de la conciergerie.

Conclusion: aucun calcul de commission, net proprietaire, resolution proprietaire, taux de commission, menages, reservations, flux, factures ou controles n'est recode dans `Conciergerie app/` a ce stade.

## 14. Architecture actuelle

Architecture actuelle effective:

```text
Obsidian vault vide
├── configuration locale Obsidian
├── plugins Obsidian tiers
└── note d'accueil standard
```

Architecture applicative cible non encore presente.

## 15. Elements deja exploitables

Exploitables immediatement:

- le dossier `Conciergerie app/` comme emplacement de travail applicatif;
- le fait qu'aucune duplication du moteur comptable n'est presente;
- la possibilite de repartir d'une structure minimale propre;
- le contexte fonctionnel fourni par le projet principal et les dossiers existants `01_SOURCES_BRUTES/`, `02_TRAVAIL/`, `03_EXPORTS_PBI/`.

Peu ou pas exploitable pour l'application cible:

- la configuration Obsidian;
- les plugins Obsidian tiers;
- la note `Bienvenue.md`.

## 16. Risques de duplication avec le moteur comptable

Risque actuel: faible, car aucune logique metier applicative n'existe encore.

Risque de conception pour la suite: eleve si l'application commence a recalculer elle-meme:

- les commissions;
- le net proprietaire;
- la resolution proprietaire applicable a date;
- les taux de commission applicables a date;
- les traitements de reservations, menages, flux, factures et controles deja portes par les lots.

Regle de construction recommandee: l'application doit orchestrer et afficher, pas recalculer. Elle doit guider les saisies, declencher explicitement les traitements existants, lire les resultats/controles produits et signaler les blocages.

## 17. Premiere etape de construction proposee

Premiere etape proposee, sans execution: definir un squelette d'application locale minimal centre sur l'orchestration, avec:

- un ecran d'accueil operationnel listant les modules cibles;
- un module `Referentiels et parametres` en lecture/controle, branche sur les fichiers sources existants;
- un module de lancement manuel des traitements existants, sans execution automatique a l'import;
- une zone de restitution lisant les exports et controles deja produits;
- aucune implementation de calcul comptable dans l'application.

La decision de stack doit preceder tout code. Pour un outil local Windows simple, une option pragmatique serait une application Python locale avec interface web legere ou desktop, a condition de reutiliser les scripts existants comme traitements externes explicites et non comme bibliotheque importee declenchant du travail.
