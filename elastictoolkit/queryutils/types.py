from typing import Any, Dict, List
from pydantic import BaseModel


class NestedField(BaseModel):
    field_name: str
    nested_path: str


class FieldValue(BaseModel):
    fields: List[str] = []
    values_map: Dict[str, Any] = {}
    values_list: List[Any] = []
