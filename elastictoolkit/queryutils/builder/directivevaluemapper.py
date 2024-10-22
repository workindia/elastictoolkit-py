import typing as t
from elastictoolkit.queryutils.types import FieldValue


class DirectiveValueMapper:
    @classmethod
    def get_field_value(cls, key: str) -> t.Optional[FieldValue]:
        return getattr(cls, key, None)
