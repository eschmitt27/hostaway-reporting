-- 0087 — « Pricing dynamique » : une question, deux réponses mélangées dans une colonne (§19).
--
-- CE QUI ÉTAIT CONFONDU
-- `ref_logements.dynamic_pricing` portait « hostdynamic » (14 logements) ou « non » (5). Une même
-- colonne répondait donc à deux questions distinctes :
--     le pricing dynamique est-il activé ?        → oui / non
--     quel moteur s'en charge ?                   → Hostdynamic / autre / aucun
-- Tant qu'il n'existe qu'un fournisseur, la confusion ne se voit pas. Elle apparaît le jour où on
-- en change : « non » cesserait de vouloir dire « désactivé » pour vouloir dire « pas Hostdynamic »,
-- et aucune requête écrite avant ce jour-là ne saurait la différence.
--
-- L'utilisateur demandait à l'écran « Pricing dynamique : Oui / Non ». Réduire la colonne à un
-- booléen aurait répondu à la demande en PERDANT le nom du moteur — une information que personne
-- ne nous a autorisés à jeter. Les deux questions sont donc séparées.
--
-- CE QUE CETTE MIGRATION ÉTABLIT
--     dynamic_pricing_enabled   OUI | NON      — la question de l'utilisateur
--     dynamic_pricing_provider  hostdynamic | … | NULL   — le moteur, quand il y en a un
--
-- UNE SEULE SOURCE DE VÉRITÉ, MALGRÉ TROIS COLONNES
-- `dynamic_pricing` reste la valeur BRUTE telle que la source l'écrit (import du classeur Setup,
-- et `lot10` qui la recopie dans son instantané des logements). Les deux colonnes normalisées en
-- DÉRIVENT, par déclencheur : quel que soit le chemin d'écriture — import, administration,
-- migration future — elles se recalculent. Il est donc impossible qu'elles divergent de la valeur
-- brute, ce qu'une synchronisation confiée au code applicatif n'aurait jamais garanti.
--
-- L'écran, lui, présente et modifie la paire ; le service recompose la valeur brute. La
-- correspondance est bijective : « non » ↔ (NON, NULL), tout autre libellé ↔ (OUI, ce libellé).

ALTER TABLE ref_logements ADD COLUMN dynamic_pricing_enabled TEXT;
ALTER TABLE ref_logements ADD COLUMN dynamic_pricing_provider TEXT;

-- Reprise de l'existant. « non » et la valeur vide signifient tous deux « désactivé » ; toute
-- autre valeur est le nom du moteur, donc « activé ».
UPDATE ref_logements
   SET dynamic_pricing_enabled =
        CASE WHEN LOWER(TRIM(COALESCE(dynamic_pricing, ''))) IN ('', 'non', 'no', 'aucun', 'false', '0')
             THEN 'NON' ELSE 'OUI' END,
       dynamic_pricing_provider =
        CASE WHEN LOWER(TRIM(COALESCE(dynamic_pricing, ''))) IN ('', 'non', 'no', 'aucun', 'false', '0')
             THEN NULL ELSE TRIM(dynamic_pricing) END;

DROP TRIGGER IF EXISTS trg_ref_logements_dynamic_pricing_insert;
CREATE TRIGGER trg_ref_logements_dynamic_pricing_insert
AFTER INSERT ON ref_logements
BEGIN
    UPDATE ref_logements
       SET dynamic_pricing_enabled =
            CASE WHEN LOWER(TRIM(COALESCE(NEW.dynamic_pricing, ''))) IN ('', 'non', 'no', 'aucun', 'false', '0')
                 THEN 'NON' ELSE 'OUI' END,
           dynamic_pricing_provider =
            CASE WHEN LOWER(TRIM(COALESCE(NEW.dynamic_pricing, ''))) IN ('', 'non', 'no', 'aucun', 'false', '0')
                 THEN NULL ELSE TRIM(NEW.dynamic_pricing) END
     WHERE logement_id = NEW.logement_id;
END;

DROP TRIGGER IF EXISTS trg_ref_logements_dynamic_pricing_update;
CREATE TRIGGER trg_ref_logements_dynamic_pricing_update
AFTER UPDATE OF dynamic_pricing ON ref_logements
BEGIN
    UPDATE ref_logements
       SET dynamic_pricing_enabled =
            CASE WHEN LOWER(TRIM(COALESCE(NEW.dynamic_pricing, ''))) IN ('', 'non', 'no', 'aucun', 'false', '0')
                 THEN 'NON' ELSE 'OUI' END,
           dynamic_pricing_provider =
            CASE WHEN LOWER(TRIM(COALESCE(NEW.dynamic_pricing, ''))) IN ('', 'non', 'no', 'aucun', 'false', '0')
                 THEN NULL ELSE TRIM(NEW.dynamic_pricing) END
     WHERE logement_id = NEW.logement_id;
END;
