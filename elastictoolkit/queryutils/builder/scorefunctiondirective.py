import copy
from inspect import signature
import logging
import typing as t
from typing_extensions import Self
from elasticquerydsl.base import DSLQuery
from elasticquerydsl.score import (
    ScoreFunction,
    ScriptScoreFunction,
    RandomScoreFunction,
    FieldValueFactorFunction,
    DecayFunction,
    WeightFunction,
)

from elastictoolkit.queryutils.builder.helpers.valueparser import (
    RuntimeValueParser,
    ValueParser,
)
from elastictoolkit.queryutils.builder.matchdirective import MatchDirective

logger = logging.getLogger(__name__)


class ScoreFunctionDirective:
    class _DefaultConfig:
        """Default configuration class for BaseDirective."""

        value_parser_config: t.Dict[str, t.Any] = {
            "parser_cls": RuntimeValueParser,
            "prefix": "match_params",
        }

    def __init__(
        self,
        filter: t.Optional[MatchDirective] = None,
        weight: t.Optional[float] = None,
        nullable_value: bool = False,
    ) -> None:
        self._filter_directive = filter
        self._weight = weight
        self._nullable_value = nullable_value
        self._score_func_kwargs = {}
        self._match_params = {}
        self._filter_dsl: t.Optional[DSLQuery] = ...
        self._value_parser_config = self._DefaultConfig.value_parser_config

    def copy(self, **kwargs) -> Self:
        logger.warning(
            f"Copy method not implemented for {type(self).__name__} and will generate a deepcopy which may not be efficient."
        )
        self_copy = copy.deepcopy(self)
        return self_copy

    def set_score_func_extra_args(self, **kwargs):
        # When implementing in subclass, use Ellipsis (...) as default for all parameters:
        #   def set_score_func_extra_args(self, param1=..., param2=..., **kwargs):
        #       ...
        # This distinguishes between parameters not provided (...) vs explicitly set to None
        update_args = {k: v for k, v in kwargs.items() if v is not ...}
        self._score_func_kwargs.update(update_args)
        return self

    def configure(self, value_parser_config: t.Dict[str, t.Any] = ...):
        if value_parser_config is not ...:
            self._value_parser_config = (
                value_parser_config or self._value_parser_config
            )

    def set_filter_dsl(self, dsl_query: t.Optional[DSLQuery]):
        self._filter_dsl = dsl_query
        return self

    @property
    def filter_dsl(self) -> t.Optional[DSLQuery]:
        if not self._filter_directive:
            return None

        if self._filter_dsl is ...:
            raise ValueError(
                "`filter`: `MatchDirective` must be resolved externally and set using `set_filter_dsl` method"
            )
        return self._filter_dsl

    def set_match_params(self, match_params: t.Dict[str, t.Any]) -> Self:
        self._match_params = match_params
        return self

    @property
    def match_params(self) -> t.Dict[str, t.Any]:
        return self._match_params

    def get_filter_directive(self) -> t.Optional[MatchDirective]:
        return self._filter_directive

    def generate_score_function(self) -> t.Optional[ScoreFunction]:
        raise NotImplementedError(
            f"Method `generate_score_function` is not implemented in subclass: {type(self).__name__}"
        )

    def get_value_parser(self) -> ValueParser:
        parser_cls = self._value_parser_config.get("parser_cls")
        if not parser_cls:
            raise ValueError(
                f"Value parser class not set for {type(self).__name__}"
            )
        parser_kwargs = {"data": self.match_params}
        for k, v in self._value_parser_config.items():
            if k != "parser_cls":
                parser_kwargs[k] = v
        parser = parser_cls(**parser_kwargs)
        return parser

    def _generate_score_function(
        self, score_cls: t.Type[ScoreFunction], **kwargs
    ):
        """
        Generate a ScoreFunction instance by filtering out unsupported parameters.

        Args:
            score_cls: The Score function class to instantiate
            **kwargs: Parameters to pass to the DSL query constructor

        Returns:
            An instance of the specified ScoreFunction class
        """
        # Get the constructor parameters for the DSL class
        valid_params = signature(score_cls.__init__).parameters.keys()

        filtered_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k in valid_params and v is not None
        }

        return score_cls(**filtered_kwargs)


class ScriptScoreDirective(ScoreFunctionDirective):
    def __init__(
        self,
        script: str,
        filter: t.Optional[MatchDirective] = None,
        weight: t.Optional[float] = None,
        lang: str = None,
        mandatory_params_keys: t.List[str] = [],
        nullable_value: bool = False,
    ) -> None:
        super().__init__(filter, weight, nullable_value)
        self._script = script
        self._lang = lang
        self._script_params = {}
        self._mandatory_params_keys = mandatory_params_keys

    def copy(self, **kwargs) -> Self:
        self_copy = self.__class__(
            self._script,
            self._filter_directive,
            self._weight,
            self._lang,
            self._mandatory_params_keys,
            self._nullable_value,
        ).set_score_func_extra_args(**self._score_func_kwargs)
        self_copy._script_params = self._script_params
        return self_copy

    def set_script_params(self, **script_params: t.Any) -> Self:
        self._script_params = script_params
        return self

    @property
    def script_params(self) -> t.Dict[str, t.Any]:
        if not hasattr(self, "_script_params_parsed"):
            value_parser = self.get_value_parser()
            self._script_params_parsed = value_parser.parse(
                self._script_params
            )
        return self._script_params_parsed

    def generate_score_function(self) -> t.Optional[ScriptScoreFunction]:
        if not self._validate_script_parameters():
            return None
        score_func = ScriptScoreFunction(
            script=self._script,
            query=self.filter_dsl,
            params=self.script_params,
            lang=self._lang,
            weight=self._weight,
        )
        return score_func

    def _validate_script_parameters(self):
        missing_params = [
            key
            for key in self._mandatory_params_keys
            if self.script_params.get(key) is None
        ]
        if missing_params and not self._nullable_value:
            raise ValueError(
                f"Missing mandatory script parameters for {type(self).__name__}: {missing_params}"
                "| Mandatory params must be present and be non-null"
            )
        if missing_params:
            return False
        return True


class RandomScoreDirective(ScoreFunctionDirective):
    def __init__(
        self,
        seed: t.Optional[int] = None,
        field: t.Optional[str] = None,
        filter: t.Optional[MatchDirective] = None,
        weight: t.Optional[float] = None,
    ) -> None:
        super().__init__(filter, weight, nullable_value=False)
        self._seed = seed
        self._field = field

    def copy(self, **kwargs) -> Self:
        self_copy = self.__class__(
            self._seed,
            self._field,
            self._filter_directive,
            self._weight,
        ).set_score_func_extra_args(**self._score_func_kwargs)
        return self_copy

    def generate_score_function(self) -> RandomScoreFunction:
        score_func = RandomScoreFunction(
            seed=self._seed,
            field=self._field,
            query=self.filter_dsl,
            weight=self._weight,
        )
        return score_func


class FieldValueFactorDirective(ScoreFunctionDirective):
    def __init__(
        self,
        field: str,
        factor: t.Optional[float] = None,
        modifier: t.Optional[str] = None,
        missing: t.Optional[float] = None,
        filter: t.Optional[MatchDirective] = None,
        weight: t.Optional[float] = None,
    ) -> None:
        super().__init__(filter, weight, nullable_value=False)
        self._field = field
        self._factor = factor
        self._modifier = modifier
        self._missing = missing

    def copy(self, **kwargs) -> Self:
        self_copy = self.__class__(
            self._field,
            self._factor,
            self._modifier,
            self._missing,
            self._filter_directive,
            self._weight,
        ).set_score_func_extra_args(**self._score_func_kwargs)
        return self_copy

    def generate_score_function(self) -> FieldValueFactorFunction:
        score_func = FieldValueFactorFunction(
            field=self._field,
            factor=self._factor,
            modifier=self._modifier,
            missing=self._missing,
            query=self.filter_dsl,
            weight=self._weight,
        )
        return score_func


class DecayFunctionDirective(ScoreFunctionDirective):
    def __init__(
        self,
        field: str,
        origin: t.Any,
        scale: t.Any,
        decay_type: str = "gauss",
        offset: t.Optional[t.Any] = None,
        decay: t.Optional[float] = None,
        filter: t.Optional[MatchDirective] = None,
        weight: t.Optional[float] = None,
    ) -> None:
        super().__init__(filter, weight, nullable_value=False)
        self._field = field
        self._origin = origin
        self._scale = scale
        self._offset = offset
        self._decay = decay
        self._decay_type = decay_type

    def copy(self, **kwargs) -> Self:
        self_copy = self.__class__(
            self._field,
            self._origin,
            self._scale,
            self._decay_type,
            self._offset,
            self._decay,
            self._filter_directive,
            self._weight,
        ).set_score_func_extra_args(**self._score_func_kwargs)
        return self_copy

    def generate_score_function(self) -> DecayFunction:
        score_func = DecayFunction(
            field=self._field,
            origin=self._origin,
            scale=self._scale,
            offset=self._offset,
            decay=self._decay,
            decay_type=self._decay_type,
            query=self.filter_dsl,
            weight=self._weight,
        )
        return score_func


class WeightDirective(ScoreFunctionDirective):
    def __init__(
        self,
        weight: float,
        filter: t.Optional[MatchDirective] = None,
    ) -> None:
        super().__init__(filter, weight, nullable_value=False)

    def copy(self, **kwargs) -> Self:
        self_copy = self.__class__(
            self._weight,
            self._filter_directive,
        ).set_score_func_extra_args(**self._score_func_kwargs)
        return self_copy

    def generate_score_function(self) -> WeightFunction:
        score_func = WeightFunction(
            weight=self._weight,
            query=self.filter_dsl,
        )
        return score_func
