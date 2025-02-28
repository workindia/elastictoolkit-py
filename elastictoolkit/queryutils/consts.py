from enum import Enum


class MatchMode(str, Enum):
    INCLUDE = "include"
    EXCLUDE = "exclude"
    INCLUDE_IF_EXIST_ANY = "include_if_exist_any"


class BaseMatchOp(str, Enum):
    """The base match operation for the query"""

    AND = "and"
    OR = "or"


class AndQueryOp(str, Enum):
    """Configure the AND query operation whether to generate a `must` or `filter` clause"""

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


class ScoreNullFilterAction(str, Enum):
    """What to do when a `ScoreFunctionDirective` has a filter directive but it is resolved to None"""

    RAISE_EXC = "raise_exc"
    ALLOW = "allow"
    DISABLE_FUNCTION = "disable_function"
