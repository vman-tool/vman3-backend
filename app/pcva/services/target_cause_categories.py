"""Maps a CCVA-predicted cause of death to the broad group (Communicable /
Non-Communicable / Injuries) and major cause category (Infectious and
parasitic diseases, Neoplasms, ...) it belongs to in the WHO target cause
list configured under Settings > PCVA Configuration (see
app/pcva/services/target_cause_seed.py for that list's own three-level
shape: icd10_category_type -> icd10_category -> icd10).

InterVA5's own 61-cause vocabulary
(app/ccva/utilits/interva/data/causetext.py's CAUSETEXTV5) and the target
cause list's `icd10.code` both derive from the same WHO 2016 VA instrument
cause-grouping numbers - InterVA5's "b_GGSS" codes and the target list's
"VAs-GG.SS" codes share the same "GG" group number - so causes are matched
by that shared group number rather than by name. The two lists' wording
differs in several places (abbreviations, British/American spelling, word
order - e.g. "Diarrhoeal diseases" vs "Diarrheal diseases", "Accid fall" vs
"Accidental fall"), which would make a name-based match miss regularly.

VManML10 predicts at a coarser "cluster" level already close in wording to
the target list's own category names (e.g. "Renal Disorders" for
"Renal disorders") - those, and anything else not in the InterVA5 list
above (including InterVA5's own "Undeterminant"/"Undetermined", which isn't
one of its 61 numbered causes), are matched with a case-insensitive keyword
fallback instead.
"""
from typing import Dict, List, Optional, Tuple

from arango.database import StandardDatabase
from fastapi.concurrency import run_in_threadpool

from app.shared.configs.constants import db_collections

# Cause name (as causetext.py spells it) -> WHO group code. Generated from
# CAUSETEXTV5's b_-prefixed entries (b_GGSS -> group "GG").
_INTERVA5_CAUSE_TO_GROUP: Dict[str, str] = {
    "Sepsis (non-obstetric)": "01",
    "Acute resp infect incl pneumonia": "01",
    "HIV/AIDS related death": "01",
    "Diarrhoeal diseases": "01",
    "Malaria": "01",
    "Measles": "01",
    "Meningitis and encephalitis": "01",
    "Tetanus": "01",
    "Pulmonary tuberculosis": "01",
    "Pertussis": "01",
    "Haemorrhagic fever (non-dengue)": "01",
    "Dengue fever": "01",
    "Other and unspecified infect dis": "01",
    "Oral neoplasms": "02",
    "Digestive neoplasms": "02",
    "Respiratory neoplasms": "02",
    "Breast neoplasms": "02",
    "Reproductive neoplasms MF": "02",
    "Other and unspecified neoplasms": "02",
    "Severe anaemia": "03",
    "Severe malnutrition": "03",
    "Diabetes mellitus": "03",
    "Acute cardiac disease": "04",
    "Stroke": "04",
    "Sickle cell with crisis": "04",
    "Other and unspecified cardiac dis": "04",
    "Chronic obstructive pulmonary dis": "05",
    "Asthma": "05",
    "Acute abdomen": "06",
    "Liver cirrhosis": "06",
    "Renal failure": "07",
    "Epilepsy": "08",
    "Ectopic pregnancy": "09",
    "Abortion-related death": "09",
    "Pregnancy-induced hypertension": "09",
    "Obstetric haemorrhage": "09",
    "Obstructed labour": "09",
    "Pregnancy-related sepsis": "09",
    "Anaemia of pregnancy": "09",
    "Ruptured uterus": "09",
    "Other and unspecified maternal CoD": "09",
    "Prematurity": "10",
    "Birth asphyxia": "10",
    "Neonatal pneumonia": "10",
    "Neonatal sepsis": "10",
    "Congenital malformation": "10",
    "Other and unspecified neonatal CoD": "10",
    "Fresh stillbirth": "11",
    "Macerated stillbirth": "11",
    "Road traffic accident": "12",
    "Other transport accident": "12",
    "Accid fall": "12",
    "Accid drowning and submersion": "12",
    "Accid expos to smoke fire & flame": "12",
    "Contact with venomous plant/animal": "12",
    "Accid poisoning & noxious subs": "12",
    "Intentional self-harm": "12",
    "Assault": "12",
    "Exposure to force of nature": "12",
    "Other and unspecified external CoD": "12",
    "Other and unspecified NCD": "98",
    "Undeterminant": "99",
    "Undetermined": "99",
}

CategoryLookup = Tuple[Dict[str, Tuple[Optional[str], Optional[str]]], List[Tuple[str, Optional[str]]]]


async def get_cause_category_lookup(db: StandardDatabase) -> CategoryLookup:
    """(group_to_category, category_names).

    group_to_category maps a WHO group code ("01".."12", "98", "99") to
    (major_cause_name, broad_group_name). category_names is every
    icd10_category's own (name, broad_group_name), for the keyword fallback
    used on causes outside the InterVA5 list above.

    Reads from the DB rather than the seed resource file, since a
    deployment may have edited or extended its target cause list under
    Settings > PCVA Configuration (see target_cause_seed.py).
    """
    def execute():
        icd10 = list(db.collection(db_collections.ICD10).all())
        categories = {c["uuid"]: c for c in db.collection(db_collections.ICD10_CATEGORY).all()}
        types = {t["uuid"]: t["name"] for t in db.collection(db_collections.ICD10_CATEGORY_TYPE).all()}
        return icd10, categories, types

    icd10, categories, types = await run_in_threadpool(execute)

    group_to_category: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    for entry in icd10:
        code = (entry.get("code") or "").strip()
        if not code.startswith("VAs-"):
            continue
        group = code[4:6]
        if group in group_to_category:
            continue
        category = categories.get(entry.get("category"))
        if not category:
            continue
        group_to_category[group] = (category.get("name"), types.get(category.get("type")))

    category_names = [
        (category.get("name"), types.get(category.get("type")))
        for category in categories.values()
        if category.get("name")
    ]
    return group_to_category, category_names


def _keyword_match(cause_name: str, category_names: List[Tuple[str, Optional[str]]]) -> Optional[Tuple[Optional[str], Optional[str]]]:
    needle = cause_name.strip().lower().replace("&", "and")
    if not needle:
        return None
    for name, broad in category_names:
        haystack = (name or "").strip().lower()
        if haystack and (haystack in needle or needle in haystack):
            return name, broad
    return None


def classify_cause(cause_name: Optional[str], lookup: CategoryLookup) -> Tuple[Optional[str], Optional[str]]:
    """(major_cause, broad_group) for one cause name, or (None, None) when
    nothing in the target cause list matches it."""
    if not cause_name:
        return None, None
    group_to_category, category_names = lookup
    group = _INTERVA5_CAUSE_TO_GROUP.get(cause_name)
    if group and group in group_to_category:
        return group_to_category[group]
    return _keyword_match(cause_name, category_names) or (None, None)
