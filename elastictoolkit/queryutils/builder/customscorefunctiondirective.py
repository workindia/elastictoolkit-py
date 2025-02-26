import typing as t
from typing_extensions import Self
from elasticquerydsl.score import ScoreFunction

from elastictoolkit.queryutils.builder.matchdirective import MatchDirective
from elastictoolkit.queryutils.builder.scorefunctiondirective import (
    ScoreFunctionDirective,
)
from elastictoolkit.queryutils.consts import ScoreNullFilterAction


class CustomScoreFunctionDirective(ScoreFunctionDirective):
    allowed_engine_cls_name: str = None

    def __init_subclass__(cls, **kwargs):
        """Ensures that each subclass defines `allowed_engine_cls` explicitly"""
        super().__init_subclass__(**kwargs)
        if "allowed_engine_cls_name" not in cls.__dict__:
            raise NotImplementedError(
                f"Class {cls.__name__} must define its own `allowed_engine_cls_name`"
            )

    def __init__(
        self,
        weight: t.Optional[float] = None,
        null_filter_action: ScoreNullFilterAction = ScoreNullFilterAction.ALLOW,
        nullable_value: bool = False,
    ) -> None:
        """
        Initialize a CustomScoreFunctionDirective.

        Args:
            weight (float, optional): Weight to apply to the score function | Not used directly. Can be used
                when defining a ScoreFunctionDirective in get_score_directive. [default: None]
            null_filter_action (ScoreNullFilterAction): Action when filter resolves to None. [default: ALLOW]
            nullable_value (bool): Whether to allow null values in the filter | Not used directly. Can be used
                when defining a ScoreFunctionDirective in get_score_directive. [default: False]
        """
        super().__init__(
            filter=None,
            weight=weight,
            null_filter_action=null_filter_action,
            nullable_value=nullable_value,
        )
        self._score_directive_resolved = ...

    def copy(self, **kwargs) -> Self:
        self_copy = self.__class__(
            self._weight,
            self._null_filter_action,
            self._nullable_value,
        )
        return self_copy

    @property
    def filter_directive(self) -> t.Optional[MatchDirective]:
        score_directive = self._get_score_directive()
        if score_directive is None:
            return None
        return score_directive.filter_directive

    def set_match_params(self, match_params: t.Dict[str, t.Any]) -> Self:
        self._score_directive_resolved = ...
        return super().set_match_params(match_params)

    def validate_score_engine(self, engine_name: str) -> None:
        if engine_name != self.allowed_engine_cls_name:
            raise ValueError(
                f"FunctionScore Engine: {engine_name} is not allowed for {self.__class__.__name__}"
            )

    def get_score_directive(self) -> t.Optional[ScoreFunctionDirective]:
        raise NotImplementedError(
            f"`get_score_directive` method is not implemented in {self.__class__.__name__}"
        )

    def _get_score_directive(self) -> t.Optional[ScoreFunctionDirective]:
        if self._score_directive_resolved is ...:
            self._score_directive_resolved = self.get_score_directive()
        return self._score_directive_resolved

    def generate_score_function(self) -> t.Optional[ScoreFunction]:
        score_directive = self._get_score_directive()
        if score_directive is None:
            return None
        if not self._validate_score_function():
            return None
        score_directive = score_directive.copy().set_match_params(
            self.match_params
        )
        return score_directive.set_filter_dsl(
            self.filter_dsl
        ).generate_score_function()
