import typing as t
from elasticquerydsl.base import DSLQuery
from elasticquerydsl.utils import BooleanDSLBuilder

from elastictoolkit.queryutils.types import FieldValue
from elastictoolkit.queryutils.builder.matchdirective import MatchDirective
from elastictoolkit.queryutils.builder.directivevaluemapper import (
    DirectiveValueMapper,
)


class DirectiveEngine:
    class Config:
        value_mapper: DirectiveValueMapper = None

    def __init__(self) -> None:
        self._match_params = None

    def set_match_params(self, match_params: t.Dict[str, t.Any] = None):
        self._match_params = match_params
        return self

    @property
    def match_params(self):
        return self._match_params

    def to_dsl(self) -> DSLQuery:
        bool_builder = BooleanDSLBuilder()
        for attr_key in dir(self):
            directive = getattr(self, attr_key)
            if not isinstance(directive, MatchDirective):
                continue

            directive = directive.copy()
            fields, values_list, values_map = [], [], {}
            value_mapper = self.Config.value_mapper
            attr_field_mapping: FieldValue = value_mapper.get_field_value(
                attr_key
            )

            if attr_field_mapping:
                fields, values_list, values_map = (
                    attr_field_mapping.fields,
                    attr_field_mapping.values_list,
                    attr_field_mapping.values_map,
                )
            directive.set_match_params(self.match_params)
            directive.set_values(*values_list, **values_map)
            if fields:
                directive.set_field(*fields)
            directive.execute(bool_builder)
        return bool_builder.build()
