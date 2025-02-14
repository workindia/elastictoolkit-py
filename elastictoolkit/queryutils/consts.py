from enum import Enum


class MatchMode(str, Enum):
    INCLUDE = "include"
    EXCLUDE = "exclude"
    INCLUDE_IF_EXIST_ANY = "include_if_exist_any"


class AndQueryOp(str, Enum):
    """Configure the AND query operation whether to generate a `must` or `filter` clause."""

    MUST = "must"
    FILTER = "filter"


class FieldMatchType(str, Enum):
    ANY = "any"
    ALL = "all"


class WaterFallMatchOp(str, Enum):
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
