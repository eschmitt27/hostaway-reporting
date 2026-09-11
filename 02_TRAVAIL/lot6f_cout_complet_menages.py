"""
lot6f_cout_complet_menages.py — Coût complet ménage AVANCÉ (vue analytique)
================================================================================
Vue ANALYTIQUE pure (D105). N'écrit RIEN dans Flux / Resultats / Commissions /
NetProprietaire / Factures. Ne modifie ni la base hors `menages_cout_complet`,
ni M04 / REF_Setup / banque / VRBO. Ne relance pas lot9-12.

SOURCE UNIQUE : SQLite. Ce calcul ne lit plus AUCUN classeur et n'ouvre plus AUCUNE connexion
réseau. Il lisait auparavant REF_Setup.xlsm (référentiels), MASTER_FACT_MEN_MenagesExternes.xlsx
(ménages externes), la Google Sheet M04 (déclarations internes + lavage) et
SAISIE_Charges_Flux.xlsx (pools courses/consommables) ; ces quatre lectures sont remplacées par
`ref_*`, `facture_lignes_menage`, `menages_declarations_internes` (alimentée par lot6b depuis
cette même feuille) et `charges`. Le chemin Excel a été SUPPRIMÉ, pas désactivé : conserver deux
moteurs de lecture, c'est accepter qu'ils divergent sans que personne ne s'en aperçoive.
Équivalence prouvée avant suppression sur 2026-06, 2026-07 et 2026-08, à photographie de données
identique : tous les indicateurs monétaires et de volume à l'écart 0. Le seul écart assumé est un
ENRICHISSEMENT — `proprietaire_id`, colonne de sortie qu'aucun calcul n'utilise, restait vide via
REF_Logements (qui ne la porte pas) et vaut désormais le propriétaire de la période.
Le classeur de SORTIE reste écrit : lot11, lot13/PowerBI et `menages_reader` le lisent encore.

cout_complet_total = cout_direct_total + Σ quote_parts (local + lavage + courses + conso + autres)
ecart_vs_standard_total = cout_standard_total - cout_complet_total   ( >0 GAIN | <0 PERTE | =0 EQUILIBRE )

Périmètre ACCEPTÉ (§6) :
  - direct externe = montant facture prestataire (lot6c, via `facture_lignes_menage`)
  - direct interne <=2026-05 = heures × taux PARAM_004 ; >=2026-06 = forfait ref_couts_menage_interne
  - LOCAL_CAVE = REC_002 (date-aware) ventilé sur tous les ménages du mois
  - LAVAGE interne = `menages_declarations_internes` (attribuable par appart + non-attribuable
    par intervenant), que lot6b résout depuis la Google Sheet M04
  - COURSES/CONSO/ACHATS = `charges` (affectable_menage=OUI, statut ACTIVE)
HORS périmètre (étape ultérieure) : heures cave, heures courses, consommables non saisis.
Pas de logique 'fournitures_incluses'. L'affectation dépend de affectable_menage + intervenant_concerne.

Clé de ventilation (D103) : poids = nb_menages × cout_standard_unitaire.
"""

import argparse, sys, os, hashlib, datetime, collections, warnings
warnings.filterwarnings("ignore")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import openpyxl
from openpyxl.styles import Font, PatternFill
from lib_menage_costs import resolve_internal_cleaning_cost
import lot3_generateur_charges as lot3   # mois dérivé de date_charge (jamais le cache formule)
import lib_db_moteur as dbm
import lib_repartition as rp   # répartition monétaire canonique : aucun centime perdu

_ap = argparse.ArgumentParser()
# SQLITE est desormais le SEUL moteur de lecture (cf. en-tete « SOURCE UNIQUE »). Le drapeau est
# conserve parce que les appelants existants (orchestrateur_moteur, menages_chaine_service,
# run_menages_pipeline) le passent explicitement ; `choices` reduit a une seule valeur fait
# echouer BRUYAMMENT un `--source EXCEL` residuel plutot que de le laisser retomber en silence
# sur un chemin qui n'existe plus.
_ap.add_argument("--source", choices=("SQLITE",), default="SQLITE",
                 help="SQLITE (seule valeur admise) : declarations internes et lavage lus dans "
                      "menages_declarations_internes, charges menage dans `charges`. Le chemin "
                      "EXCEL a ete supprime — equivalence prouvee sur 2026-06/07/08.")
_ap.add_argument("--db", default=None)
_ap.add_argument("--mois", default=None,
                 help="AAAA-MM. Absent = dernier mois present dans menages_taches_enrichies.")
# Porte sur la SORTIE, pas sur les sources : plus aucun classeur n'est LU. Le classeur produit
# garde des consommateurs (lot11, lot13/PowerBI, `menages_reader`) ; ce drapeau permet de ne pas
# l'ecrire quand seul le dataset SQLite `menages_cout_complet` est attendu.
_ap.add_argument("--sans-excel", action="store_true",
                 help="N'ecrit pas le classeur de sortie (le dataset SQLite reste ecrit).")
_ap.add_argument("--sans-sqlite", action="store_true")
_ap.add_argument("--run-id", default="")
args = _ap.parse_args()
chemin_base = dbm.chemin_db(args.db)

# AUD-005 — mono-mois volontaire : l'extension multi-mois de l'écart analytique ménage est
# différée jusqu'à la mise en place d'un vrai processus de clôture mensuelle métier/comptable.
# Ne pas utiliser les clôtures techniques réservations/VRBO (REF_Cloture_Mensuelle) comme
# déclencheur de ce calcul. Statut registre : DIFFERE / BYPASS_PROVISOIRE.
PIVOT = "2026-06"
if chemin_base is None:
    sys.exit("[lot6f] ERREUR : une base est exigee (--db / PILOTAGE_DB_PATH / APP_DATA_DIR).")
_conn0 = dbm.ouvrir(chemin_base)
if args.mois:
    MONTH = args.mois
else:
    r = _conn0.execute(
        "SELECT MAX(mois) FROM menages_taches_enrichies WHERE mois IS NOT NULL").fetchone()
    MONTH = r[0] if r and r[0] else datetime.date.today().strftime("%Y-%m")
_conn0.close()
DREF = datetime.date.fromisoformat(MONTH + "-01")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTD = os.path.join(ROOT, "02_TRAVAIL", "Lot6f_CoutComplet_Menages")
OUT  = os.path.join(OUTD, "MASTER_CALC_CoutComplet_Menages.xlsx")

def mois_charge(d):
    """mois d'une charge, dérivé de date_charge — jamais d'une colonne `mois` pré-calculée.

    Règle héritée du classeur, où `mois` était une formule dont openpyxl ne lisait que le cache :
    vide après une écriture applicative, la charge disparaissait des pools sans un mot. La source
    est maintenant SQLite, mais la règle reste juste — `date_charge` est la donnée saisie, `mois`
    une dérivée — et elle garde la parité de résultat avec l'ancien chemin.
    Retourne "" si la date est inexploitable (l'appelant émet alors un contrôle explicite).
    """
    return lot3.mois_de(d.get("date_charge"))

def rh(*v): return hashlib.sha256("|".join("" if x is None else str(x) for x in v).encode()).hexdigest()[:16]
def to_d(v):
    if isinstance(v, datetime.datetime): return v.date()
    if isinstance(v, datetime.date): return v
    try: return datetime.date.fromisoformat(str(v)[:10])
    except (ValueError, TypeError): return None
def f(x):
    try: return float(x)
    except (TypeError, ValueError): return None

# ── Référentiels ─────────────────────────────────────────────────────────────
def _num(v):
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None

_conn = dbm.ouvrir(chemin_base)
from lib_ref_history import resolve_management_period
log_info_raw = {r["logement_id"]: r for r in dbm.lignes(
    _conn, "ref_logements", ("logement_id", "type_logement_id", "nom_logement_officiel"),
    ordre="logement_id")}
gest_rows = dbm.lignes(_conn, "ref_gestion_logements_hist",
    ("gestion_id", "logement_id", "proprietaire_id", "date_debut", "date_fin",
     "statut_gestion", "source", "commentaire"), ordre="gestion_id") \
    if dbm.table_presente(_conn, "ref_gestion_logements_hist") else []
# `proprietaire_id` est une colonne de SORTIE (feuille DETAIL) : aucun calcul de lot6f ne s'en
# sert. L'ancien chemin Excel la laissait vide — `REF_Logements` ne porte pas la colonne — alors
# que la base resout le proprietaire de la PERIODE via `ref_gestion_logements_hist`. C'est le seul
# ecart assume de la bascule : un enrichissement, jamais un changement de montant.
log_info = {}
for lid, d in log_info_raw.items():
    prop_id = None
    if gest_rows:
        res = resolve_management_period(gest_rows, logement_id=lid, date_arrivee=MONTH + "-15")
        prop_id = res.value if res.status == "OK" else None
    log_info[lid] = {**d, "proprietaire_id": prop_id}
typ_lib = {r["type_logement_id"]: r["type_logement"] for r in dbm.lignes(
    _conn, "ref_types_logements", ("type_logement_id", "type_logement"),
    ordre="type_logement_id")}
int_info = {r["intervenant_id"]: r for r in dbm.lignes(
    _conn, "ref_intervenants", ("intervenant_id", "nom_intervenant"), ordre="intervenant_id")}
std_ref = [{**r, "cout_standard_menage": _num(r.get("cout_standard_menage"))} for r in dbm.lignes(
    _conn, "ref_couts_standards_menage",
    ("type_logement_id", "cout_standard_menage", "actif", "date_debut_validite",
     "date_fin_validite"), ordre="cout_standard_id")]
int_ref = [{**r, "montant_interne_standard": _num(r.get("montant_interne_standard"))}
          for r in dbm.lignes(_conn, "ref_couts_menage_interne",
                              ("type_logement_id", "montant_interne_standard", "actif",
                               "date_debut_validite", "date_fin_validite"),
                              ordre="cout_menage_interne_id")] \
    if dbm.table_presente(_conn, "ref_couts_menage_interne") else []
hourly_ref = [{**r, "taux_horaire": _num(r.get("taux_horaire"))} for r in dbm.lignes(
    _conn, "ref_taux_heures_menage",
    ("intervenant_id", "taux_horaire", "actif", "date_debut", "date_fin"),
    ordre="taux_horaire_id")] if dbm.table_presente(_conn, "ref_taux_heures_menage") else []
rec_ref = [{**r, "montant_ttc": _num(r.get("montant_ttc"))} for r in dbm.lignes(
    _conn, "ref_charges_recurrentes",
    ("charge_recurrente_id", "montant_ttc", "actif", "date_debut_validite",
     "date_fin_validite"), ordre="charge_recurrente_id")]

def date_aware(rows, type_id, montant_field, type_field="type_logement_id"):
    best = None
    for d in rows:
        if d.get(type_field) != type_id or str(d.get("actif")) != "OUI": continue
        deb, fin = to_d(d.get("date_debut_validite")), to_d(d.get("date_fin_validite"))
        if deb and DREF < deb: continue
        if fin and DREF > fin: continue
        best = f(d.get(montant_field))
    return best
def std_unit(type_id): return date_aware(std_ref, type_id, "cout_standard_menage")
def int_unit(type_id): return date_aware(int_ref, type_id, "montant_interne_standard")

# REC_002 local cave date-aware
def rec_montant(rec_id):
    best = None
    for d in rec_ref:
        if d.get("charge_recurrente_id") != rec_id or str(d.get("actif")) != "OUI": continue
        deb, fin = to_d(d.get("date_debut_validite")), to_d(d.get("date_fin_validite"))
        if deb and DREF < deb: continue
        if fin and DREF > fin: continue
        best = f(d.get("montant_ttc"))
    return best
local_cave_montant = rec_montant("REC_002") or 0.0

controls = []

# ── Direct EXTERNE — facture_lignes_menage (bridge 6c) ────────────────────────────────────────
ext = collections.defaultdict(lambda: [0, 0.0])
if dbm.table_presente(_conn, "facture_lignes_menage"):
    cur = _conn.execute(
        "SELECT l.logement_id, f.fournisseur_id_opaque, l.montant_ttc, d.quantite, "
        "f.date_facture, l.facture_id_opaque "
        "FROM facture_lignes_menage l "
        "JOIN factures f ON f.facture_id_opaque = l.facture_id_opaque "
        "LEFT JOIN facture_lignes_menage_detail d ON d.ligne_id_opaque = l.ligne_id_opaque "
        # Seules les factures VALIDEES entrent dans le cout complet : une facture A_CONTROLER
        # est un document recu, pas une charge acceptee. Sans ce filtre, son montant remontait
        # jusqu'a menages_cout_complet puis TYPE_FLUX_018/019 dans lot9.
        "WHERE l.type_ligne = 'MENAGE_EXTERNE' "
        f"AND {dbm.filtre_sql_factures_comptables('f')}")
    for lg, pid, mttc, qte, dfac, fid in cur.fetchall():
        if str(dfac or "")[:7] != MONTH: continue
        q = qte if qte is not None else 1
        if q == 0 and (mttc or 0) == 0:
            controls.append(("EXCLU_VOLUME", "INFO", f"facture {fid} 0€/q0")); continue
        e = ext[(lg, pid)]; e[0] += q; e[1] += (mttc or 0)

# ── Interne + LAVAGE — menages_declarations_internes (Lot6b, deja resolu) ─────────────────────
# lot6b lit la Google Sheet M04, resout `appartement_source` -> logement_id et `Prénom` ->
# intervenant_id, puis ecrit ici AVEC SA PROVENANCE. lot6f n'a donc plus ni appel reseau, ni
# mapping de libelles, ni table de prenoms en dur : une seule etape parle a la feuille.
interne = collections.defaultdict(lambda: [0, 0.0, 0.0])
lav_na_by_int = collections.Counter()
for d in dbm.lignes(_conn, "menages_declarations_internes",
                    ("mois", "logement_id", "intervenant_id", "nb_menages", "nb_heures",
                     "cout_lavage_attribue", "lavage_non_attribuable_mois"), ordre="id"):
    if str(d.get("mois"))[:7] != MONTH: continue
    lg, iid = d.get("logement_id"), d.get("intervenant_id")
    e = interne[(lg, iid)]
    e[0] += d.get("nb_menages") or 0
    e[1] += d.get("nb_heures") or 0
    e[2] += d.get("cout_lavage_attribue") or 0
    lav_na = d.get("lavage_non_attribuable_mois")
    if lav_na:
        lav_na_by_int[iid] += lav_na
_conn.close()

# ── Construction lignes de base (direct + standard) ──────────────────────────
lines = []   # dict par (mois,lg,iid)
def base_line(lg, iid, typ_interv, nb, heures, methode, direct, lav_attr=0.0,
              cout_ref=None, cout_priority=None, cout_status="OK", cout_message=""):
    ti = log_info.get(lg) or {}; type_id = ti.get("type_logement_id")
    su = std_unit(type_id)
    poids = (nb * su) if (su is not None) else 0
    lines.append({"mois": MONTH, "logement_id": lg, "nom_appartement": ti.get("nom_logement_officiel"),
        "proprietaire_id": ti.get("proprietaire_id"),
        "type_logement_id": type_id, "type_logement_libelle": typ_lib.get(type_id),
        "intervenant_id": iid, "nom_intervenant": (int_info.get(iid) or {}).get("nom_intervenant"),
        "type_intervenant": typ_interv, "nb_menages": nb, "nb_heures": heures,
        "cout_standard_unitaire": su, "cout_standard_total": (su*nb if su is not None else None),
        "methode": methode, "cout_direct_total": direct, "cout_interne_ref_id": cout_ref,
        "cout_interne_priorite": cout_priority,
        "controle_cout_interne": "OK" if cout_status == "OK" else cout_message,
        "poids": poids,
        "lavage_attribuable": lav_attr})

for (lg, iid), (nb, mont) in ext.items():
    base_line(lg, iid, "EXTERNE", nb, None, "EXTERNE_FACTURE", round(mont, 2))
for (lg, iid), (nb, heures, lav) in interne.items():
    ti = (log_info.get(lg) or {}).get("type_logement_id")
    cost = resolve_internal_cleaning_cost(
        ref_date=DREF,
        intervenant_id=iid,
        logement_id=lg,
        type_logement_id=ti,
        nb_menages=nb,
        nb_heures=heures,
        hourly_rows=hourly_ref,
        fixed_rows=int_ref,
    )
    if cost.status != "OK":
        controls.append((f"COUT_INTERNE_{cost.status}", "BLOQUANT", f"{lg}/{iid}/{MONTH}: {cost.message}"))
    base_line(
        lg, iid, "INTERNE", nb, heures, cost.method, cost.total,
        lav_attr=round(lav, 2), cout_ref=cost.ref_id,
        cout_priority=cost.priority, cout_status=cost.status,
        cout_message=cost.message,
    )

# ── POOLS de charges communes ────────────────────────────────────────────────
sum_poids_all = sum(l["poids"] for l in lines) or 1
# La cave/local ne sert QU'AUX ménages internes -> ventilée sur le poids interne uniquement.
sum_poids_interne = sum(l["poids"] for l in lines if l["type_intervenant"] == "INTERNE") or 1
sum_poids_int = collections.Counter()
for l in lines:
    if l["type_intervenant"] == "INTERNE": sum_poids_int[l["intervenant_id"]] += l["poids"]

# ── Pool courses / consommables / achats — charges ménage affectables ────────────────────────
# SOURCE : la table `charges` de la base. C'est le dernier classeur que ce calcul lisait encore ;
# le drapeau `affectable_menage` y est persisté depuis la migration 0076. Avant elle, une charge
# ménage saisie dans l'application ne pouvait rejoindre aucun de ces pools.
#
# Deux filtres n'avaient pas d'équivalent dans le classeur, et c'est délibéré :
#   · `statut = 'ACTIVE'`  — une charge ANNULÉE n'est pas un coût. Le classeur ne portait pas de
#     charge annulée ; la base, si.
#   · `statut_controle = VALIDE` — une charge NON VALIDEE n'alimente aucun calcul. Le classeur
#     n'en posait aucun, et cette parité a été maintenue un temps ; c'est désormais tranché
#     (mission « arbitrages du banc », §8). L'asymétrie précédente était difficile à défendre :
#     une FACTURE externe non validée était déjà exclue du coût complet, alors qu'une CHARGE non
#     contrôlée y entrait. Une colonne vide vaut A_CONTROLER, donc exclue : l'absence d'avis ne
#     vaut pas accord.
_c = dbm.ouvrir(chemin_base)
try:
    # `dbm.lignes` est l'accès canonique du moteur (il renvoie des dictionnaires) : le
    # réutiliser évite d'écrire un second idiome d'accès qui finirait par diverger.
    saisie_rows = dbm.lignes(
        _c, "charges",
        ("charge_id", "date_charge", "mois", "montant", "categorie_charge_id",
         "affectable_menage", "statut", "statut_controle"),
        ou="statut = 'ACTIVE'", ordre="id") if dbm.table_presente(_c, "charges") else []
finally:
    _c.close()
pool_courses = pool_conso = pool_autres = 0.0
nb_affectables = nb_date_invalide = 0
# Charges ménage écartées faute de validation : comptées pour être SIGNALÉES. Une charge exclue
# d'un calcul ne doit jamais disparaître sans laisser de trace — c'est exactement ce qui rendrait
# l'exclusion dangereuse (§9).
non_validees = []
for d in saisie_rows:
    if str(d.get("affectable_menage")) != "OUI": continue
    if not dbm.charge_entre_dans_les_calculs(d):
        if mois_charge(d) == MONTH:
            non_validees.append(d)
        continue
    nb_affectables += 1
    m_charge = mois_charge(d)
    if not m_charge:
        nb_date_invalide += 1
        continue
    if m_charge != MONTH: continue
    cat = str(d.get("categorie_charge_id")); m = f(d.get("montant")) or 0
    if cat == "CHG_004": pool_conso += m
    elif cat in ("CHG_018",): pool_autres += m
    else: pool_courses += m
if non_validees:
    _ids = ", ".join(str(d.get("charge_id")) for d in non_validees[:5])
    _montant = round(sum(f(d.get("montant")) or 0 for d in non_validees), 2)
    controls.append(("CHARGE_MENAGE_NON_VALIDEE", "A_CONTROLER",
                     f"{len(non_validees)} charge(s) ménage de {MONTH} exclue(s) du coût complet "
                     f"faute de validation ({_montant}€) : {_ids}. Valider puis relancer le calcul."))
if nb_date_invalide:
    controls.append(("CHARGE_MENAGE_DATE_INVALIDE", "A_CONTROLER",
                     f"{nb_date_invalide} charge(s) ménage affectable(s) sans date_charge exploitable — mois non dérivable, exclues des pools"))
if pool_courses == 0 and pool_conso == 0 and pool_autres == 0:
    if nb_affectables:
        controls.append(("POOL_VIDE_HORS_MOIS", "INFO",
                         f"{nb_affectables} charge(s) ménage affectable(s) en base, aucune sur {MONTH} (pools courses/conso=0)"))
    else:
        controls.append(("POOL_VIDE_NON_SAISI", "INFO",
                         "Aucune charge ménage affectable dans la base (charges) (pools courses/conso=0)"))

# contrôle double source lavage
lav_saisie = [d for d in saisie_rows
              if str(d.get("categorie_charge_id")) == "CHG_003"
              and str(d.get("affectable_menage")) == "OUI"
              and dbm.charge_entre_dans_les_calculs(d)
              and mois_charge(d) == MONTH]
if lav_saisie and sum(l["lavage_attribuable"] for l in lines) > 0:
    controls.append(("DOUBLE_SOURCE_LAVAGE_A_CONTROLER", "A_CONTROLER", f"{len(lav_saisie)} charge(s) lavage affectable=OUI en base + lavage Google Sheet présent"))

POOLS = {"LOCAL_CAVE": local_cave_montant, "COURSES": pool_courses, "CONSOMMABLES": pool_conso, "AUTRES": pool_autres}
# Règle figée : cave/local REC_002 ventilée UNIQUEMENT sur les ménages internes (jamais les externes).
controls.append(("REC_002_LOCAL_CAVE_INTERNE_ONLY", "INFO",
    f"Cave {local_cave_montant}€ ventilée sur poids internes ({round(sum_poids_interne,2)}) — externes exclus"))

# ── Quote-parts + coût complet par ligne ─────────────────────────────────────
#
# AUCUN CENTIME NE DISPARAIT. Chaque pool est reparti EN UNE FOIS sur l'ensemble de ses lignes
# eligibles, par `lib_repartition` — la regle canonique du depot : centimes entiers, part entiere,
# puis residu aux parts que l'arrondi a le plus lesees, departage par cle triee.
#
# Ce que cela remplace : `round(pool * poids_ligne / total_poids, 2)`, calcule ligne par ligne et
# sans rattrapage. Chaque arrondi etait juste isolement, mais la SOMME ne valait plus le pool :
# 100,00 EUR sur trois poids egaux ventilaient 99,99 EUR. Un centime n'allait a personne, et rien
# ne le signalait.
#
# La cle de repartition est le couple (logement, intervenant) : c'est l'identite d'une ligne
# DETAIL. Elle est triee par `lib_repartition`, donc l'attribution du residu ne depend jamais de
# l'ordre de lecture SQL.
def _cle_ligne(ligne):
    return f'{ligne["logement_id"]}|{ligne["intervenant_id"]}'

_poids_tous = {_cle_ligne(l): l["poids"] for l in lines}
_poids_internes = {_cle_ligne(l): l["poids"] for l in lines if l["type_intervenant"] == "INTERNE"}

_part_local = rp.repartir(local_cave_montant, _poids_internes) if local_cave_montant else {}
_part_courses = rp.repartir(pool_courses, _poids_tous) if pool_courses else {}
_part_conso = rp.repartir(pool_conso, _poids_tous) if pool_conso else {}
_part_autres = rp.repartir(pool_autres, _poids_tous) if pool_autres else {}

# Lavage non attribuable : un pool PAR INTERVENANT, reparti sur les seules lignes internes de cet
# intervenant. Ce sont des pools distincts, jamais un seul : melanger les intervenants deplacerait
# le cout de lavage de l'un vers l'autre.
_part_lavage_na = {}
for _iid, _montant_na in lav_na_by_int.items():
    if not _montant_na:
        continue
    _poids_iid = {_cle_ligne(l): l["poids"] for l in lines
                  if l["type_intervenant"] == "INTERNE" and l["intervenant_id"] == _iid}
    _part_lavage_na.update(rp.repartir(_montant_na, _poids_iid))

ventil = []
for l in lines:
    w = l["poids"]
    _cle = _cle_ligne(l)
    # cave = ménages internes uniquement
    qp_local = round(_part_local.get(_cle, 0.0), 2)
    # lavage = attribuable (déclaré par ligne) + non-attribuable ventilé par intervenant
    qp_lav_na = round(_part_lavage_na.get(_cle, 0.0), 2)
    qp_lavage = round(l["lavage_attribuable"] + qp_lav_na, 2)
    qp_courses = round(_part_courses.get(_cle, 0.0), 2)
    qp_conso = round(_part_conso.get(_cle, 0.0), 2)
    qp_autres = round(_part_autres.get(_cle, 0.0), 2)
    cc = None if l["cout_direct_total"] is None else round(l["cout_direct_total"] + qp_local + qp_lavage + qp_courses + qp_conso + qp_autres, 2)
    st = l["cout_standard_total"]
    ecart = round(st - cc, 2) if (st is not None and cc is not None) else None
    statut_e = "NON_CALCULABLE" if ecart is None else ("GAIN" if ecart > 0 else "PERTE" if ecart < 0 else "EQUILIBRE")
    if l.get("controle_cout_interne") not in (None, "OK"):
        statut_c, code = "A_CONTROLER", "COUT_INTERNE_A_CONTROLER"
    else:
        statut_c, code = ("A_CONTROLER", "COUT_STANDARD_ABSENT") if st is None else ("VALIDE", "")
    l.update({"quote_part_local": qp_local, "quote_part_courses": qp_courses, "quote_part_lavage": qp_lavage,
        "quote_part_consommables": qp_conso, "quote_part_autres_charges_menage": qp_autres,
        "cout_complet_total": cc, "cout_complet_unitaire": round(cc/l["nb_menages"], 2) if (cc is not None and l["nb_menages"]) else None,
        "ecart_vs_standard_total": ecart,
        "ecart_unitaire": (round(ecart/l["nb_menages"], 2) if (ecart is not None and l["nb_menages"]) else None),
        "statut_ecart": statut_e, "statut_controle": statut_c,
        "code_controle": code, "commentaire": "", "ROW_HASH": rh(MONTH, l["logement_id"], l["intervenant_id"], cc)})
    for nm, val in [("LOCAL_CAVE", qp_local), ("LAVAGE", qp_lavage), ("COURSES", qp_courses), ("CONSOMMABLES", qp_conso), ("AUTRES", qp_autres)]:
        if val: ventil.append({"mois": MONTH, "pool": nm, "logement_id": l["logement_id"], "intervenant_id": l["intervenant_id"], "poids": round(w,2), "quote_part": val})

# ── Écriture ──────────────────────────────────────────────────────────────────
os.makedirs(OUTD, exist_ok=True)
wb = openpyxl.Workbook()
def wsheet(title, cols, rows, first=False):
    ws = wb.active if first else wb.create_sheet(title)
    if first: ws.title = title
    ws.append(cols)
    for c in ws[1]: c.font = Font(bold=True); c.fill = PatternFill("solid", fgColor="DDDDDD")
    for r in rows: ws.append([r.get(c) for c in cols])
DET = ["mois","logement_id","nom_appartement","proprietaire_id","type_logement_id","intervenant_id","nom_intervenant","type_intervenant",
    "nb_menages","cout_standard_total","cout_direct_total","quote_part_local","quote_part_courses","quote_part_lavage",
    "quote_part_consommables","quote_part_autres_charges_menage","cout_complet_total","cout_complet_unitaire",
    "ecart_vs_standard_total","ecart_unitaire","methode","cout_interne_ref_id","cout_interne_priorite",
    "controle_cout_interne","statut_ecart","statut_controle","code_controle","commentaire"]

# ── SQLite : menages_cout_complet (0038) — remplacement integral par mois ───────────────────────
if args.sans_sqlite:
    print("[lot6f] --sans-sqlite : menages_cout_complet non ecrit.")
elif chemin_base is None:
    print("[lot6f] Aucune base designee : menages_cout_complet non ecrit.")
else:
    _conn = dbm.ouvrir(chemin_base)
    try:
        _conn.execute("DELETE FROM menages_cout_complet WHERE mois = ?", (MONTH,))
        if lines:
            _trous = ", ".join(["?"] * (len(DET) + 1))
            _conn.executemany(
                f"INSERT INTO menages_cout_complet ({', '.join(DET)}, run_id) VALUES ({_trous})",
                [tuple(l.get(c) for c in DET) + (args.run_id or None,) for l in lines])
        _conn.commit()
    finally:
        _conn.close()
    print(f"[lot6f] SQLite : menages_cout_complet — {len(lines)} lignes (mois={MONTH})")

# Les CONTROLES sont un resultat metier, pas un artefact de classeur : ils doivent etre restitues
# quel que soit le mode de sortie. Ils etaient construits APRES le court-circuit `--sans-excel`, si
# bien qu'un run sans classeur n'en disait rien — une charge ecartee du calcul disparaissait alors
# en silence, exactement ce que le controle existe pour empecher (§9).
cc_ctrl = collections.Counter((c[0], c[1]) for c in controls)
# Rappel permanent : le menage externe (TYPE_FLUX_014) est deja compte dans Flux ; le cout complet
# est analytique et n'y est jamais reinjecte.
cc_ctrl[("CHARGE_EXTERNE_DEJA_EN_FLUX_NON_REINJECTEE", "INFO")] += 1


def _afficher_controles():
    for (code, niveau), n in cc_ctrl.most_common():
        exemple = next((c[2] for c in controls if (c[0], c[1]) == (code, niveau)), "")
        print(f"  [{niveau}] {code} x{n}" + (f" — {exemple}" if exemple else ""))


if args.sans_excel:
    print(f"[lot6f] --sans-excel : classeur legacy non ecrit. mois={MONTH} lignes={len(lines)}")
    print("  CONTROLES:")
    _afficher_controles()
    sys.exit(0)

wsheet("DETAIL_COUT_COMPLET", DET, lines, first=True)
wsheet("POOLS_CHARGES_MENAGE", ["mois","pool","montant_total","source","cle_repartition"],
    [{"mois":MONTH,"pool":k,"montant_total":round(v,2),"source":("REC_002 (date-aware)" if k=="LOCAL_CAVE" else "menages_declarations_internes" if k=="LAVAGE" else "charges (affectable_menage=OUI)"),"cle_repartition":"poids=nb×cout_standard"} for k,v in {**POOLS,"LAVAGE":sum(l['lavage_attribuable'] for l in lines)+sum(lav_na_by_int.values())}.items()])
wsheet("VENTILATION_CHARGES", ["mois","pool","logement_id","intervenant_id","poids","quote_part"], ventil)
def resume(keyf):
    agg = collections.defaultdict(lambda: [0,0.0,0.0,0.0])
    for l in lines:
        k = keyf(l); a = agg[k]; a[0]+=l["nb_menages"]; a[1]+=l["cout_standard_total"] or 0; a[2]+=l["cout_complet_total"] or 0; a[3]+=l["ecart_vs_standard_total"] or 0
    return agg
wsheet("RESUME_LOGEMENT", ["mois","logement_id","nom_appartement","nb_menages","cout_standard_total","cout_complet_total","ecart_total","ecart_unitaire_moyen"],
    [{"mois":MONTH,"logement_id":k[0],"nom_appartement":k[1],"nb_menages":v[0],"cout_standard_total":round(v[1],2),"cout_complet_total":round(v[2],2),"ecart_total":round(v[3],2),"ecart_unitaire_moyen":(round(v[3]/v[0],2) if v[0] else None)} for k,v in sorted(resume(lambda l:(l["logement_id"],l["nom_appartement"])).items())])
wsheet("RESUME_INTERVENANT", ["mois","intervenant_id","nom_intervenant","type_intervenant","nb_menages","cout_standard_total","cout_complet_total","ecart_total"],
    [{"mois":MONTH,"intervenant_id":k[0],"nom_intervenant":k[1],"type_intervenant":k[2],"nb_menages":v[0],"cout_standard_total":round(v[1],2),"cout_complet_total":round(v[2],2),"ecart_total":round(v[3],2)} for k,v in sorted(resume(lambda l:(l["intervenant_id"],l["nom_intervenant"],l["type_intervenant"])).items())])
wsheet("RESUME_PRESTATAIRE", ["mois","intervenant_id","nom_intervenant","nb_menages","cout_complet_total","ecart_total"],
    [{"mois":MONTH,"intervenant_id":k[0],"nom_intervenant":k[1],"nb_menages":v[0],"cout_complet_total":round(v[2],2),"ecart_total":round(v[3],2)} for k,v in sorted(resume(lambda l:(l["intervenant_id"],l["nom_intervenant"],l["type_intervenant"])).items()) if k[2]=="EXTERNE"])
wsheet("CONTROLES_DOUBLE_COMPTAGE", ["code_controle","niveau","nb","exemple"],
    [{"code_controle":k[0],"niveau":k[1],"nb":n,"exemple":next((c[2] for c in controls if (c[0],c[1])==k),"ménage externe (TYPE_FLUX_014) déjà compté dans Flux ; coût complet = analytique, non réinjecté")} for k,n in cc_ctrl.most_common()])
# Aucune transaction de provenance DEF-1 ici : elle traçait l'appel de lot6f à la Google Sheet
# M04, et cet appel n'existe plus. Les déclarations internes viennent de
# `menages_declarations_internes`, que lot6b alimente depuis cette même feuille EN ENREGISTRANT SA
# PROPRE PROVENANCE. En poser une seconde ici décrirait une lecture qui n'a pas lieu.
try:
    wb.save(OUT)
except PermissionError:
    OUT = OUT.replace(".xlsx","_MAJ.xlsx"); wb.save(OUT); print(f"[lot6f] original verrouillé -> {os.path.basename(OUT)}")

# ── Rapport ──────────────────────────────────────────────────────────────────
ts = sum(l["cout_standard_total"] or 0 for l in lines); tc = sum(l["cout_complet_total"] or 0 for l in lines)
print(f"[lot6f] DRY-RUN mois={MONTH} | local_cave={local_cave_montant} -> {OUT}")
print(f"  lignes DETAIL : {len(lines)}")
print(f"  POOLS : LOCAL_CAVE={round(local_cave_montant,2)} LAVAGE={round(sum(l['lavage_attribuable'] for l in lines)+sum(lav_na_by_int.values()),2)} COURSES={pool_courses} CONSO={pool_conso}")
print(f"  COUT GLOBAL : standard={round(ts,2)} complet={round(tc,2)} ecart={round(ts-tc,2)} ({'GAIN' if ts-tc>0 else 'PERTE' if ts-tc<0 else 'EQ'})")
print("\n  par intervenant:")
for k,v in sorted(resume(lambda l:(l["intervenant_id"],l["nom_intervenant"],l["type_intervenant"])).items()):
    print(f"    {str(k[0]):10} {str(k[1])[:9]:9} {str(k[2]):8} nb={v[0]:3} std={round(v[1],2):8} complet={round(v[2],2):8} ecart={round(v[3],2):8}")
print("\n  CONTROLES:", dict(cc_ctrl))
