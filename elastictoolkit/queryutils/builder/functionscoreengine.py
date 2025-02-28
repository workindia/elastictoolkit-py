import typing as t
from typing_extensions import Self
from elasticquerydsl.base import DSLQuery
from elasticquerydsl.score import FunctionScoreQuery
from elasticquerydsl.score import ScoreFunction


from elastictoolkit.queryutils.builder.base import BaseQueryEngine
from elastictoolkit.queryutils.builder.scorefunctiondirective import (
    ScoreFunctionDirective,
)
from elastictoolkit.queryutils.builder.scorefunctiondirectivebuilder import (
    ScoreFunctionDirectiveBuilder,
)


class FunctionScoreEngine(BaseQueryEngine):
    class _DefaultConfig(BaseQueryEngine._DefaultConfig):
        score_mode: t.Optional[str] = None
        boost_mode: t.Optional[str] = None
        max_boost: t.Optional[float] = None
        min_score: t.Optional[float] = None
        __all__ = ["score_mode", "boost_mode", "max_boost", "min_score"]

    class Config(BaseQueryEngine.Config):
        """`Config` class should be overriden in Sub-Class"""

    def __init__(self) -> None:
        super().__init__()
        self._match_dsl_query = None

    def set_match_dsl(self, dsl_query: DSLQuery) -> Self:
        if not isinstance(dsl_query, DSLQuery):
            raise ValueError(
                "`set_match_dsl`: param `dsl_query` must be a `DSLQuery`"
            )

        self._match_dsl_query = dsl_query
        return self

    @property
    def match_dsl(self):
        return self._match_dsl_query

    def to_dsl(self) -> DSLQuery:
        score_functions = self._build_score_functions()
        func_score_query_params = self._build_function_score_params(
            score_functions
        )
        return FunctionScoreQuery(**func_score_query_params)

    def _build_score_functions(self) -> t.List[ScoreFunction]:
        engine_attributes = self.get_engine_attr(ScoreFunctionDirective)
        match_directive_config = (
            getattr(self.Config, "match_directive_config", None)
            or self._DefaultConfig.match_directive_config
        )
        score_functions = []

        for attr, score_directive in engine_attributes.items():
            score_directive = self._build_score_directive(
                score_directive, attr, match_directive_config
            )
            score_func = score_directive.generate_score_function()
            if score_func is not None:
                score_functions.append(score_func)

        return score_functions

    def _build_score_directive(
        self,
        score_directive: ScoreFunctionDirective,
        attr: str,
        match_directive_config: dict,
    ) -> ScoreFunctionDirective:
        return (
            ScoreFunctionDirectiveBuilder(score_directive)
            .set_value_mapper(attr, self.Config.value_mapper)
            .set_match_params(self.match_params)
            .set_parent_engine_name(self.__class__.__name__)
            .set_match_directive_config(match_directive_config)
            .build()
        )

    def _build_function_score_params(
        self, score_functions: t.List[ScoreFunction]
    ) -> t.Dict[str, t.Any]:
        params = {
            "query": self.match_dsl,
            "functions": score_functions,
        }

        for param in self._DefaultConfig.__all__:
            value = getattr(
                self.Config, param, getattr(self._DefaultConfig, param)
            )
            if value is not None:
                params[param] = value

        return params
