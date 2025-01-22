import typing as t
from typing_extensions import Self

from elastictoolkit.queryutils.builder.custommatchdirective import (
    CustomMatchDirective,
)
from elastictoolkit.queryutils.builder.matchdirective import MatchDirective
from elastictoolkit.queryutils.builder.directivevaluemapper import (
    DirectiveValueMapper,
)


class MatchDirectiveBuilder:
    def __init__(self, directive: MatchDirective) -> None:
        self._directive = directive
        self._value_map_key: str = (
            None  # Key linking DirectiveEngine and DirectiveValueMapper
        )
        self._value_mapper: DirectiveValueMapper = None
        self._match_params: t.Dict[str, t.Any] = None
        self._parent_engine_name: str = None
        self._match_directive_config: t.Dict[str, t.Any] = {}

    def set_value_mapper(
        self, mapping_key: str, value_mapper: DirectiveValueMapper
    ) -> Self:
        self._value_map_key = mapping_key
        self._value_mapper = value_mapper
        return self

    def set_match_params(self, match_params: t.Dict[str, t.Any]) -> Self:
        self._match_params = match_params
        return self

    def set_parent_engine_name(self, engine_name: str) -> Self:
        self._parent_engine_name = engine_name
        return self

    def set_match_directive_config(self, **config: t.Any) -> Self:
        self._match_directive_config = config
        return self

    def build(self) -> MatchDirective:
        fields, values_list, values_map = self._get_field_value_mapping()
        directive = self._build_directive(fields, values_list, values_map)
        self._configure_directive(directive, fields, values_list, values_map)
        return directive

    def _build_directive(
        self, fields: list, values_list: list, values_map: dict
    ) -> MatchDirective:
        # If no field/value mapping exists then user might have set the params while declaring directive
        # In that case copy the params
        copy_fields = not fields
        copy_values = not (values_list or values_map)
        directive = self._directive.copy(
            fields=copy_fields, values=copy_values
        )

        if isinstance(directive, CustomMatchDirective):
            directive.validate_directive_engine(self._parent_engine_name)
            directive.set_directive_value_mapper(self._value_mapper)

        return directive

    def _configure_directive(
        self,
        directive: MatchDirective,
        fields: list,
        values_list: list,
        values_map: dict,
    ) -> None:
        directive.configure(**self._match_directive_config)

        if isinstance(self._match_params, dict):  # Empty dict is allowed
            directive.set_match_params(self._match_params)
        if values_list or values_map:
            directive.set_values(*values_list, **values_map)
        if fields:
            directive.set_field(*fields)

    def _get_field_value_mapping(self) -> t.Tuple[list, list, dict]:
        if not self._value_map_key:
            return [], [], {}

        attr_field_mapping = self._value_mapper.get_field_value(
            self._value_map_key
        )
        if not attr_field_mapping:
            return [], [], {}

        return (
            attr_field_mapping.fields,
            attr_field_mapping.values_list,
            attr_field_mapping.values_map,
        )
