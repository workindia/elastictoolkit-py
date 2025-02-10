import typing as t
from typing_extensions import Self

from elastictoolkit.queryutils.builder.matchdirectivebuilder import (
    MatchDirectiveBuilder,
)
from elastictoolkit.queryutils.builder.customscorefunctiondirective import (
    CustomScoreFunctionDirective,
)
from elastictoolkit.queryutils.builder.directivevaluemapper import (
    DirectiveValueMapper,
)
from elastictoolkit.queryutils.builder.matchdirective import MatchDirective
from elastictoolkit.queryutils.builder.scorefunctiondirective import (
    ScoreFunctionDirective,
)


class ScoreFunctionDirectiveBuilder:
    def __init__(self, directive: ScoreFunctionDirective) -> None:
        self._directive = directive
        self._match_params: t.Dict[str, t.Any] = None

    def set_value_mapper(
        self, mapping_key: str, value_mapper: DirectiveValueMapper
    ) -> Self:
        self._value_map_key = mapping_key
        self._value_mapper = value_mapper
        return self

    def set_match_params(self, match_params: t.Dict[str, t.Any]) -> Self:
        self._match_params = match_params
        return self

    def set_match_directive_config(self, config: t.Dict[str, t.Any]) -> Self:
        self._match_directive_config = config
        return self

    def set_parent_engine_name(self, engine_name: str) -> Self:
        self._parent_engine_name = engine_name
        return self

    @property
    def match_params(self) -> t.Dict[str, t.Any]:
        return self._match_params

    def build(self) -> ScoreFunctionDirective:
        score_directive = self._directive.copy().set_match_params(
            self._match_params
        )
        if isinstance(score_directive, CustomScoreFunctionDirective):
            score_directive.validate_score_engine(self._parent_engine_name)

        filter_directive = score_directive.filter_directive

        if score_directive.filter_directive is not None:
            filter_directive = self._build_filter_directive(
                score_directive.filter_directive,
                self._match_directive_config,
            )
            filter_dsl = filter_directive.to_dsl(nullable=True)
            score_directive.set_filter_dsl(filter_dsl)

        return score_directive

    def _build_filter_directive(
        self,
        filter_directive: MatchDirective,
        match_directive_config: dict,
    ) -> MatchDirective:
        return (
            MatchDirectiveBuilder(filter_directive)
            .set_value_mapper(self._value_map_key, self._value_mapper)
            .set_match_params(self.match_params)
            .set_parent_engine_name(self._parent_engine_name)
            .set_match_directive_config(**match_directive_config)
            .build()
        )
