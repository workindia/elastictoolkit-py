import typing as t
from typing_extensions import Self, TYPE_CHECKING
from elasticquerydsl.base import DSLQuery, BoolQuery


from elastictoolkit.queryutils.builder.matchdirective import MatchDirective
from elastictoolkit.queryutils.consts import BaseMatchOp, MatchMode
from elastictoolkit.queryutils.builder.directivevaluemapper import (
    DirectiveValueMapper,
)

if TYPE_CHECKING:
    from elastictoolkit.queryutils.builder.booldirective import BoolDirective


class CustomMatchDirective(MatchDirective):
    allowed_engine_cls_name: str = None
    name: str = None

    def __init_subclass__(cls, **kwargs):
        """Ensures that each subclass defines `allowed_engine_cls` explicitly"""
        super().__init_subclass__(**kwargs)
        if "allowed_engine_cls_name" not in cls.__dict__:
            raise NotImplementedError(
                f"Class {cls.__name__} must define its own `allowed_engine_cls_name`"
            )

    def __init__(
        self,
        mode=MatchMode.INCLUDE,
        nullable_value: bool = False,
        name: t.Optional[str] = None,
    ) -> None:
        super().__init__(mode, nullable_value)
        self._directive_value_mapper = None
        self._parent_engine_name = None
        self._name = name or self.name

    def copy(
        self,
        fields: bool = False,
        values: bool = False,
        match_params: bool = False,
    ) -> Self:
        self_copy = self.__class__(
            self.mode,
            self.nullable_value,
        ).set_directive_value_mapper(self._directive_value_mapper)
        self_copy._parent_engine_name = self._parent_engine_name
        self_copy._fields = self._fields if fields else None
        self_copy._values_list = self._values_list if values else None
        self_copy._match_params = self._match_params if match_params else None
        self_copy._name = self._name
        return self_copy

    def validate_directive_engine(self, parent_engine_name: str) -> Self:
        self._validate_directive_engine(parent_engine_name)
        self._parent_engine_name = parent_engine_name
        return self

    def _validate_directive_engine(self, parent_engine_name: str):
        if parent_engine_name != self.allowed_engine_cls_name:
            raise TypeError(
                f"CustomDirective: {type(self).__name__} can only be used in instance of DirectiveEngine: {self.allowed_engine_cls_name}"
            )

    def set_directive_value_mapper(
        self, directive_value_mapper: DirectiveValueMapper
    ):
        self._directive_value_mapper = directive_value_mapper
        return self

    def get_name(self) -> t.Optional[str]:
        return self._name

    def get_directive(self) -> t.Optional["BoolDirective"]:
        raise NotImplementedError(
            f"`get_directives` method is not implemented in {self.__class__.__name__}"
        )

    def _get_custom_directive_query(self) -> t.Optional[BoolQuery]:
        bool_directive = self.get_directive()
        if not bool_directive:
            return None
        bool_directive = bool_directive.copy()
        bool_directive.set_match_params(
            self._match_params
        ).set_directive_value_mapper(self._directive_value_mapper)
        bool_directive.configure(**self.config_kwargs)
        bool_directive.set_name(self.get_name())
        return bool_directive.to_dsl()

    def _get_bool_and_queries(self) -> t.List[DSLQuery]:
        if (
            self.mode != MatchMode.INCLUDE
            or self._base_match_op != BaseMatchOp.AND
        ):
            return []
        query = self._get_custom_directive_query()
        return [query] if query else []

    def _get_bool_should_queries(self) -> t.List[DSLQuery]:
        if (
            self.mode != MatchMode.INCLUDE
            or self._base_match_op != BaseMatchOp.OR
        ):
            return []
        query = self._get_custom_directive_query()
        return [query] if query else []

    def _get_bool_must_not_queries(self) -> t.List[DSLQuery]:
        if self.mode != MatchMode.EXCLUDE:
            return []
        query = self._get_custom_directive_query()
        return [query] if query else []
