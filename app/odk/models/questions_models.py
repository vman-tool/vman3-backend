
from typing import Dict, List
from arango import Optional
from pydantic import BaseModel
from app.shared.configs.models import VManBaseModel
from app.shared.configs.constants import db_collections


class Option(BaseModel):
    path: str
    value: str
    label: str
    # Per-language choice text, keyed the same way as VA_Question.labels.
    # ODK sync supplies one language only; an xForm upload adds the rest.
    labels: Optional[Dict[str, str]] = None

class VA_Question(VManBaseModel):
    path: str
    name: str
    type: str
    binary: Optional[bool] = None
    selectMultiple: Optional[bool] = None
    label: str
    # Per-language labels keyed by language name, e.g.
    # {"English": "...", "Swahili": "...", "Siswati": "..."}.
    # ODK sync only ever supplies one language, so `label` remains the
    # primary/display value; uploading an xForm adds the others additively.
    labels: Optional[Dict[str, str]] = None
    options: Optional[List[Option]] = None

    @classmethod
    def get_collection_name(cls) -> str:
        return db_collections.VA_QUESTIONS