import typing as t
from typing_extensions import Self
from elasticquerydsl.base import BoolQuery
from elasticquerydsl.utils import BooleanDSLBuilder

from elastictoolkit.queryutils.builder.base import BaseDirective
from elastictoolkit.queryutils.builder.custommatchdirective import (
    CustomMatchDirective,
)
from elastictoolkit.queryutils.builder.matchdirective import MatchDirective
from elastictoolkit.queryutils.builder.matchdirectivebuilder import (
    MatchDirectiveBuilder,
)
from elastictoolkit.queryutils.consts import AndQueryOp
from elastictoolkit.queryutils.builder.directivevaluemapper import (
    DirectiveValueMapper,
)


class BoolDirective(BaseDirective):
    def __init__(
        self,
        *bool_directives: "BoolDirective",
        **directives_map: MatchDirective,
    ):
        super().__init__()
        self.bool_directives = bool_directives
        self.directives_map = directives_map
        self.directive_value_mapper = None
        self._match_params = None
        self._name = None

    def copy(self, match_params: bool = False, **kwargs):
        self_copy = (
            self.__class__(
                *self.bool_directives,
                **self.directives_map,
            )
            .configure(**self.config_kwargs)
            .set_directive_value_mapper(self.directive_value_mapper)
        )
        self_copy._match_params = self._match_params if match_params else None
        return self_copy

    def add_directive(
        self,
        *bool_directives: "BoolDirective",
        **directives_map: MatchDirective,
    ) -> Self:
        self.bool_directives = (*self.bool_directives, *bool_directives)
        self.directives_map.update(directives_map)
        return self

    def set_match_params(
        self, match_params: t.Dict[str, t.Any] = None
    ) -> Self:
        self._match_params = match_params
        return self

    def set_name(self, name: str) -> Self:
        self._name = name
        return self

    def set_directive_value_mapper(
        self, directive_value_mapper: DirectiveValueMapper
    ) -> Self:
        self.directive_value_mapper = directive_value_mapper
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
            directive = (
                directive.copy()
                .configure(**self.config_kwargs)
                .set_directive_value_mapper(self.directive_value_mapper)
                .set_match_params(self.match_params)
            )
            bool_queries.append(directive.to_dsl())
        return bool_queries

    def _collect_directive_map_queries(self):
        match_queries = []
        for attr_key, directive in self.directives_map.items():
            if isinstance(directive, CustomMatchDirective):
                raise TypeError(
                    f"A {type(self).__name__} cannot include a `{CustomMatchDirective.__name__}`"
                )
            directive = (
                MatchDirectiveBuilder(directive)
                .set_value_mapper(attr_key, self.directive_value_mapper)
                .set_match_params(self.match_params)
                .set_parent_engine_name(self.__class__.__name__)
                .set_match_directive_config(
                    value_parser_config=self._value_parser_config,
                    and_query_op=self._and_query_op,
                )
                .build()
            )
            directive_dsl = directive.to_dsl(nullable=True)
            if directive_dsl:
                match_queries.append(directive_dsl)
        return match_queries


class OrDirective(BoolDirective):
    def to_dsl(self) -> BoolQuery:
        match_queries = self._collect_match_queries()
        bool_builder = BooleanDSLBuilder()
        bool_builder.add_should_query(*match_queries)
        bool_builder.set_name(self._name)
        return bool_builder.build()


class AndDirective(BoolDirective):
    def to_dsl(self) -> BoolQuery:
        match_queries = self._collect_match_queries()

        bool_builder = BooleanDSLBuilder()
        if self._and_query_op == AndQueryOp.MUST:
            bool_builder.add_must_query(*match_queries)
        else:
            bool_builder.add_filter_query(*match_queries)
        bool_builder.set_name(self._name)
        return bool_builder.build()
