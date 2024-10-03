from enum import Enum


class MatchMode(str, Enum):
    INCLUDE = "include"
    EXCLUDE = "exclude"
    INCLUDE_IF_EXIST = "include_if_exist"


class FieldMatchType(str, Enum):
    ANY = "any"
    ALL = "all"


class WaterFallMatchOp(str, Enum):
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
