import copy
import logging
import typing as t
from typing_extensions import Self
from elasticquerydsl.base import DSLQuery

from elastictoolkit.queryutils.builder.helpers.valueparser import (
    RuntimeValueParser,
)
from elastictoolkit.queryutils.consts import AndQueryOp, BaseMatchOp
from elastictoolkit.queryutils.builder.directivevaluemapper import (
    DirectiveValueMapper,
)


logger = logging.getLogger(__name__)


class BaseDirective:
    class _DefaultConfig:
        """Default configuration class for BaseDirective."""

        value_parser_config: t.Dict[str, t.Any] = {
            "parser_cls": RuntimeValueParser,
            "prefix": "match_params",
        }
        and_query_op: AndQueryOp = AndQueryOp.FILTER
        base_match_op: BaseMatchOp = BaseMatchOp.AND

    def __init__(self):
        self._value_parser_config = self._DefaultConfig.value_parser_config
        self._and_query_op = self._DefaultConfig.and_query_op
        self._base_match_op = self._DefaultConfig.base_match_op

    def configure(
        self,
        value_parser_config: t.Dict[str, t.Any] = ...,
        and_query_op: AndQueryOp = ...,
        base_match_op: BaseMatchOp = ...,
    ):
        """Configure the directive with the given parameters.

        Args:
            value_parser_config: The configuration for the value parser
            and_query_op: The operation to use for the AND query [default: FILTER]
            base_match_op: The base match operation for the query [default: AND]
        """
        # NOTE: When adding a new parameter, make sure to update the `get_config_kwargs` method
        if value_parser_config is not ...:
            self._value_parser_config = (
                value_parser_config or self._value_parser_config
            )
        if and_query_op is not ...:
            self._and_query_op = and_query_op or self._and_query_op
        if base_match_op is not ...:
            self._base_match_op = base_match_op or self._base_match_op
        return self

    @property
    def config_kwargs(self) -> t.Dict[str, t.Any]:
        return {
            "value_parser_config": self._value_parser_config,
            "and_query_op": self._and_query_op,
            "base_match_op": self._base_match_op,
        }

    def copy(self, **kwargs) -> Self:
        logger.warning(
            f"Copy method not implemented for {type(self).__name__} and will generate a deepcopy which may not be efficient."
        )
        self_copy = copy.deepcopy(self)
        return self_copy


class BaseQueryEngine:
    class _DefaultConfig:
        match_directive_config: t.Dict[str, t.Any] = {}

    class Config:
        """`Config` class should be overriden in Sub-Class"""

        value_mapper: DirectiveValueMapper = ...
        match_directive_config: t.Dict[str, t.Any] = {}

    def __init__(self) -> None:
        self._match_params = None
        self._validate_engine()

    def _validate_engine(self):
        if getattr(self.Config, "value_mapper", ...) is ...:
            raise AttributeError(
                f"{type(self).__name__}: `Config.value_mapper` is not set"
            )

    def set_match_params(self, match_params: t.Dict[str, t.Any]) -> Self:
        self._match_params = match_params
        return self

    @property
    def match_params(self):
        return self._match_params

    def get_engine_attr(self, *supported_types: t.Type) -> t.Dict[str, t.Any]:
        """
        Get all attributes of the engine that are instances of the given types.

        Args:
            *supported_types: Variable number of types to filter attributes by

        Returns:
            Dict mapping attribute names to their values for attributes that match
            the supported types
        """
        return {
            attr_key: getattr(self, attr_key)
            for attr_key in dir(self)
            if isinstance(getattr(self, attr_key), supported_types)
        }

    def to_dsl(self) -> DSLQuery:
        raise NotImplementedError("Subclasses must implement to_dsl()")
