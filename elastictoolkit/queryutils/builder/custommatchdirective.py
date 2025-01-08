import typing as t
from typing_extensions import Self
from elasticquerydsl.base import DSLQuery, BoolQuery
from elasticquerydsl.utils import BooleanDSLBuilder

from elastictoolkit.queryutils.builder.matchdirective import MatchDirective
from elastictoolkit.queryutils.consts import AndQueryOp, MatchMode
from elastictoolkit.queryutils.builder.directivevaluemapper import (
    DirectiveValueMapper,
)


class CustomMatchDirective(MatchDirective):
    allowed_engine_cls_name = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Ensure that each subclass defines `allowed_engine_cls` explicitly
        if "allowed_engine_cls_name" not in cls.__dict__:
            raise NotImplementedError(
                f"Class {cls.__name__} must define its own `allowed_engine_cls_name`"
            )

    def __init__(
        self,
        mode=MatchMode.INCLUDE,
        nullable_value: bool = False,
    ) -> None:
        super().__init__(mode, nullable_value)
        self.directive_value_mapper = None
        self.directive_engine = None

    def copy(
        self,
        fields: bool = False,
        values: bool = False,
        match_params: bool = False,
    ) -> Self:
        self_copy = self.__class__(
            self.mode,
            self.nullable_value,
        ).set_directive_value_mapper(self.directive_value_mapper)
        self_copy._match_params = self._match_params if match_params else None
        self_copy.directive_engine = self.directive_engine
        return self_copy

    def validate_directive_engine(self, directive_engine: t.Any):
        self._validate_directive_engine(directive_engine)
        self.directive_engine = directive_engine
        return self

    def _validate_directive_engine(self, directive_engine: t.Any):
        if type(directive_engine).__name__ != self.allowed_engine_cls_name:
            raise TypeError(
                f"Engine instance must be an instance of {self.allowed_engine_cls_name}"
            )

    def set_directive_value_mapper(
        self, directive_value_mapper: DirectiveValueMapper
    ):
        self.directive_value_mapper = directive_value_mapper
        return self

    def get_directive(self) -> t.Optional["BoolDirective"]:
        raise NotImplementedError(
            f"`get_directives` method is not implemented in {self.__class__.__name__}"
        )

    def _get_custom_directive_query(self) -> t.Optional[BoolQuery]:
        bool_directive = self.get_directive()
        if not bool_directive:
            return None
        bool_directive.set_match_params(self._match_params)
        bool_directive.configure(
            self.directive_value_mapper, self.and_query_op
        )
        return bool_directive.to_dsl()

    def _get_bool_and_queries(self) -> t.List[DSLQuery]:
        if self.mode != MatchMode.INCLUDE:
            return []
        query = self._get_custom_directive_query()
        return [query] if query else []

    def _get_bool_must_not_queries(self) -> t.List[DSLQuery]:
        if self.mode != MatchMode.EXCLUDE:
            return []
        query = self._get_custom_directive_query()
        return [query] if query else []


class BoolDirective:
    directive_value_mapper: DirectiveValueMapper = None
    and_query_op = AndQueryOp.MUST

    def __init__(
        self,
        *bool_directives: "BoolDirective",
        **directives_map: MatchDirective,
    ):
        self.bool_directives = bool_directives
        self.directives_map = directives_map
        self.directive_value_mapper = None
        self._match_params = None

    def configure(
        self,
        directive_value_mapper: DirectiveValueMapper,
        and_query_op: AndQueryOp,
    ):
        self.directive_value_mapper = directive_value_mapper
        self.and_query_op = and_query_op
        return self

    def copy(self):
        self_copy = self.__class__(
            *self.bool_directives,
            **self.directives_map,
        )
        return self_copy

    def set_match_params(self, match_params: t.Dict[str, t.Any] = None):
        self._match_params = match_params
        return self

    @property
    def match_params(self):
        return self._match_params

    def to_dsl(self) -> BoolQuery:
        raise NotImplementedError(
            f"Method `to_dsl` is not implemented in {self.__class__.__name__}"
        )

    def _collect_match_queries(self):
        match_queries = []
        match_queries.extend(self._collect_bool_directive_queries())
        match_queries.extend(self._collect_directive_map_queries())
        return match_queries

    def _collect_bool_directive_queries(self):
        bool_queries = []
        for directive in self.bool_directives:
            directive = directive.copy()
            directive.configure(self.directive_value_mapper, self.and_query_op)
            directive.set_match_params(self.match_params)
            bool_queries.append(directive.to_dsl())
        return bool_queries

    def _collect_directive_map_queries(self):
        match_queries = []
        for attr_key, directive in self.directives_map.items():
            if isinstance(directive, CustomMatchDirective):
                raise TypeError(
                    f"A {type(self).__name__} cannot include a `{CustomMatchDirective.__name__}`"
                )
            attr_field_mapping = self.directive_value_mapper.get_field_value(
                attr_key
            )
            fields, values_list, values_map = [], [], {}
            if attr_field_mapping:
                fields, values_list, values_map = (
                    attr_field_mapping.fields,
                    attr_field_mapping.values_list,
                    attr_field_mapping.values_map,
                )
            # If no field/value mapping exists then user might have set the params while declaring directive
            # In that case copy the params
            copy_fields = bool(not fields)
            copy_values = bool(not values_list and not values_map)
            directive = directive.copy(fields=copy_fields, values=copy_values)
            directive.set_match_params(self._match_params)
            if values_list or values_map:
                directive.set_values(*values_list, **values_map)
            if fields:
                directive.set_field(*fields)
            match_queries.append(directive.to_dsl())
        return match_queries


class OrDirective(BoolDirective):
    def to_dsl(self) -> BoolQuery:
        match_queries = self._collect_match_queries()
        bool_builder = BooleanDSLBuilder()
        bool_builder.add_should_query(*match_queries)
        return bool_builder.build()


class AndDirective(BoolDirective):
    def to_dsl(self) -> BoolQuery:
        match_queries = self._collect_match_queries()

        bool_builder = BooleanDSLBuilder()
        if self.and_query_op == AndQueryOp.MUST:
            bool_builder.add_must_query(*match_queries)
        else:
            bool_builder.add_filter_query(*match_queries)
        return bool_builder.build()
