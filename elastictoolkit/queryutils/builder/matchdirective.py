import copy
from inspect import signature
import logging
import typing as t
from typing_extensions import Self
from elasticquerydsl.base import DSLQuery, BoolQuery
from elasticquerydsl.filter import (
    MatchQuery,
    MultiMatchQuery,
    NestedQuery,
    RangeQuery,
    ScriptQuery,
    TermQuery,
    TermsQuery,
    ExistsQuery,
    QueryStringQuery,
)
from elasticquerydsl.utils import BooleanDSLBuilder

from elastictoolkit.queryutils.builder.base import BaseDirective
from elastictoolkit.queryutils.types import NestedField
from elastictoolkit.queryutils.consts import (
    AndQueryOp,
    BaseMatchOp,
    MatchMode,
    FieldMatchType,
    WaterFallMatchOp,
)
from elastictoolkit.queryutils.builder.helpers.valueparser import ValueParser

logger = logging.getLogger(__name__)


class MatchDirective(BaseDirective):
    def __init__(
        self, mode=MatchMode.INCLUDE, nullable_value: bool = False
    ) -> None:
        super().__init__()
        self.mode = mode
        self.nullable_value = nullable_value
        self.es_query_params = {}

        self._match_params = None
        self._fields = None
        self._values_list = None
        self._values_map = None
        self._match_query_kwargs = {}

    def copy(
        self,
        fields: bool = False,
        values: bool = False,
        match_params: bool = False,
    ) -> Self:
        logger.warning(
            f"Copy method not implemented for {type(self).__name__} and will generate a deepcopy which may not be efficient."
        )
        self_copy = copy.deepcopy(self)
        self_copy._fields = self._fields if fields else None
        self_copy._values_list = self._values_list if values else None
        self_copy._match_params = self._match_params if match_params else None
        return self_copy

    def set_match_query_extra_args(self, **kwargs):
        # When implementing in subclass, use Ellipsis (...) as default for all parameters:
        #   def set_match_query_args(self, param1=..., param2=..., **kwargs):
        #       ...
        # This distinguishes between parameters not provided (...) vs explicitly set to None
        update_args = {k: v for k, v in kwargs.items() if v is not ...}
        self._match_query_kwargs.update(update_args)
        return self

    def set_field(self, *fields: t.Union[str, NestedField]):
        self._fields = fields
        return self

    @property
    def fields(self):
        if self._fields is None:
            raise ValueError(
                f"{type(self).__name__} directive requires: `field`. This must be set using `set_field` method."
            )
        fields = [f for f in self._fields if isinstance(f, str)]
        nested_fields = [f for f in self._fields if isinstance(f, NestedField)]
        return fields, nested_fields

    def set_match_params(self, match_params: t.Dict[str, t.Any] = None):
        self._match_params = match_params
        return self

    @property
    def match_params(self):
        if self._match_params is None:
            raise ValueError(
                f"{type(self).__name__} directive requires: `match_params`. This must be set using `set_match_params` method."
            )
        return self._match_params

    def set_values(self, *values_list, **values_map):
        self._values_list = values_list
        self._values_map = values_map
        return self

    @property
    def values_list(self):
        if self._values_list is None:
            raise ValueError(
                f"{type(self).__name__} directive requires: `values_list`. This must be set using `set_values` method."
            )
        if not hasattr(self, "_values_list_parsed"):
            value_parser = self.get_value_parser()
            self._values_list_parsed = value_parser.parse(self._values_list)
        return self._values_list_parsed

    @property
    def values_map(self):
        if self._values_map is None:
            raise ValueError(
                f"{type(self).__name__} directive requires: `values_map`. This must be set using `set_values` method."
            )
        if not hasattr(self, "_values_map_parsed"):
            value_parser = self.get_value_parser()
            self._values_map_parsed = value_parser.parse(self._values_map)
        return self._values_map_parsed

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

    def execute(self, bool_builder: BooleanDSLBuilder):
        bool_builder.add_should_query(*self._get_bool_should_queries())
        bool_builder.add_must_query(*self._get_bool_must_queries())
        bool_builder.add_must_not_query(*self._get_bool_must_not_queries())
        bool_builder.add_filter_query(*self._get_bool_filter_queries())

    def to_dsl(self, nullable: bool = False) -> t.Optional[BoolQuery]:
        """
        Generates Boolean DSL query for the MatchDirective

        Args:
            nullable (bool, optional): Whether an empty query can be expected. Defaults to False.
            Exception will be raised if trying to generate an empty boolean query if this is not set to `True`

        Returns:
            BoolQuery: The constructed boolean query combining all the directive's clauses.
        """
        builder = BooleanDSLBuilder()
        self.execute(builder)
        if nullable and not any(
            (builder.should, builder.filter, builder.must, builder.must_not)
        ):
            return None
        return builder.build()

    def _get_bool_should_queries(self) -> t.List[DSLQuery]:
        return []

    def _get_bool_must_queries(self) -> t.List[DSLQuery]:
        if self._and_query_op != AndQueryOp.MUST:
            return []
        return self._get_bool_and_queries()

    def _get_bool_must_not_queries(self) -> t.List[DSLQuery]:
        return []

    def _get_bool_filter_queries(self) -> t.List[DSLQuery]:
        if self._and_query_op != AndQueryOp.FILTER:
            return []
        return self._get_bool_and_queries()

    def _get_bool_and_queries(self) -> t.List[DSLQuery]:
        """
        Generates AND operation queries. This is used for either `must` or
        `filter` operation based on `and_query_op`
        """
        return []

    def _generate_dsl_query(self, dsl_cls: t.Type[DSLQuery], **kwargs):
        """
        Generate a DSL query instance by filtering out unsupported parameters.

        Args:
            dsl_cls: The DSL query class to instantiate
            **kwargs: Parameters to pass to the DSL query constructor

        Returns:
            An instance of the specified DSL query class
        """
        # Get the constructor parameters for the DSL class
        valid_params = signature(dsl_cls.__init__).parameters.keys()

        filtered_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k in valid_params and v is not None
        }

        return dsl_cls(**filtered_kwargs)


class ConstMatchDirective(MatchDirective):
    def __init__(
        self,
        rule: FieldMatchType,
        mode=MatchMode.INCLUDE,
        nullable_value: bool = False,
        name: t.Optional[str] = None,
    ) -> None:
        self.rule = rule
        self._match_values = []
        self._name = name

        super().__init__(mode, nullable_value)

    def copy(
        self,
        fields: bool = False,
        values: bool = False,
        match_params: bool = False,
    ) -> Self:
        self_copy = (
            self.__class__(
                self.rule, self.mode, self.nullable_value, self._name
            )
            .configure(**self.config_kwargs)
            .set_match_query_extra_args(**self._match_query_kwargs)
        )
        self_copy._fields = self._fields if fields else None
        self_copy._values_list = self._values_list if values else None
        self_copy._match_params = self._match_params if match_params else None
        return self_copy

    def _get_bool_and_queries(self) -> t.List[DSLQuery]:
        if self._base_match_op != BaseMatchOp.AND:
            return []
        and_queries = self._get_bool_queries(
            MatchMode.INCLUDE
        ) or self._get_bool_queries(MatchMode.INCLUDE_IF_EXIST_ANY)
        return and_queries

    def _get_bool_should_queries(self) -> t.List[DSLQuery]:
        if self._base_match_op != BaseMatchOp.OR:
            return []
        or_queries = self._get_bool_queries(
            MatchMode.INCLUDE
        ) or self._get_bool_queries(MatchMode.INCLUDE_IF_EXIST_ANY)
        return or_queries

    def _get_bool_must_not_queries(self) -> t.List[DSLQuery]:
        return self._get_bool_queries(MatchMode.EXCLUDE)

    def _get_bool_queries(self, match_mode: MatchMode) -> t.List[DSLQuery]:
        if self.mode != match_mode:
            return []
        match_dsl_query = self._make_match_dsl_query()
        return [match_dsl_query] if match_dsl_query else []

    def _make_match_dsl_query(self) -> t.Optional[DSLQuery]:
        self._validate_match_parameters()

        match_queries = self._collect_match_queries()
        if not match_queries:
            return None

        not_exists_query = self._collect_not_exists_query()

        if len(match_queries) == 1 and not not_exists_query:
            return match_queries[0]  # No need to build a Bool Query

        return self._build_bool_query(match_queries, not_exists_query)

    def _validate_match_parameters(self) -> None:
        if not self.values_list and not self.nullable_value:
            raise ValueError(f"No match value set for {type(self).__name__}")

        fields, nested_fields = self.fields
        if not fields and not nested_fields:
            raise ValueError(f"No match field set for {type(self).__name__}")

    def _collect_match_queries(self) -> t.List[DSLQuery]:
        if not self.values_list:
            return []
        match_queries = []
        match_queries.extend(self._get_fields_queries())
        match_queries.extend(self._get_nested_fields_queries())
        return match_queries

    def _collect_not_exists_query(self) -> t.Optional[DSLQuery]:
        if self.mode != MatchMode.INCLUDE_IF_EXIST_ANY:
            return None
        # Return a query that checks if none of the fields exists
        fields, nested_fields = self.fields
        exists_queries = [ExistsQuery(f) for f in fields]
        exists_queries.extend(
            [
                NestedQuery(
                    path=f.nested_path, query=ExistsQuery(f.field_name)
                )
                for f in nested_fields
            ]
        )
        bool_builder = BooleanDSLBuilder()
        bool_builder.add_must_not_query(*exists_queries)
        return bool_builder.build()

    def _build_bool_query(
        self,
        match_queries: t.List[DSLQuery],
        not_exists_query: DSLQuery,
    ) -> DSLQuery:
        bool_builder = BooleanDSLBuilder()

        if self.rule == FieldMatchType.ANY:
            bool_builder.add_should_query(*match_queries)
        elif self.rule == FieldMatchType.ALL:
            bool_builder.add_filter_query(*match_queries)

        match_bool_query = bool_builder.build()

        if not_exists_query:
            # Insert not exists query if MatchMode is `INCLUDE_IF_EXISTS_ANY`
            bool_builder = BooleanDSLBuilder()
            bool_builder.add_should_query(not_exists_query, match_bool_query)
            match_bool_query = bool_builder.build()
        return match_bool_query

    def _get_fields_queries(self) -> t.List[DSLQuery]:
        fields, _ = self.fields
        if not fields:
            return []

        if len(fields) > 1:
            return self._get_multi_field_queries(fields)

        return self._get_single_field_queries(fields[0])

    def _get_multi_field_queries(
        self, fields: t.List[str]
    ) -> t.List[DSLQuery]:
        return [
            MultiMatchQuery(value, fields, _name=self._name)
            for value in self.values_list
        ]

    def _get_single_field_queries(self, field: str) -> t.List[DSLQuery]:
        if len(self.values_list) > 1:
            if self.rule == FieldMatchType.ANY:
                # Use terms query for multiple values
                return [TermsQuery(field, self.values_list, _name=self._name)]

            return [
                TermQuery(field, value, _name=self._name)
                for value in self.values_list
            ]

        return [TermQuery(field, self.values_list[0], _name=self._name)]

    def _get_nested_fields_queries(self) -> t.List[DSLQuery]:
        _, nested_fields = self.fields
        return [
            NestedQuery(
                path=field.nested_path,
                query=TermQuery(
                    field.field_name,
                    v,
                    _name=self._name,
                ),
            )
            for field in nested_fields
            for v in self.values_list
        ]


class WaterfallFieldMatchDirective(ConstMatchDirective):
    def __init__(
        self,
        rule: FieldMatchType,
        waterfall_order: t.List[t.Any],
        op: WaterFallMatchOp,
        mode=MatchMode.INCLUDE,
        nullable_value: bool = False,
        name: t.Optional[str] = None,
    ) -> None:
        self.waterfall_order = waterfall_order
        self.op = op
        super().__init__(rule, mode, nullable_value, name)

    def copy(
        self,
        fields: bool = False,
        values: bool = False,
        match_params: bool = False,
    ) -> Self:
        self_copy = (
            self.__class__(
                self.rule,
                self.waterfall_order,
                self.op,
                self.mode,
                self.nullable_value,
                self._name,
            )
            .configure(**self.config_kwargs)
            .set_match_query_extra_args(**self._match_query_kwargs)
        )
        self_copy._fields = self._fields if fields else None
        self_copy._values_list = self._values_list if values else None
        self_copy._match_params = self._match_params if match_params else None
        return self_copy

    @property
    def values_list(self):
        if self._values_list is None:
            raise ValueError(
                f"{type(self).__name__} directive requires: `values_list`. This must be set using `set_values` method."
            )
        if not hasattr(self, "_values_list_parsed"):
            if len(self._values_list) != 1:
                raise ValueError(
                    f"{type(self).__name__} directive requires exactly 1 value. Given: {len(self._values_list)}"
                )
            value_parser = self.get_value_parser()
            value_parsed = value_parser.parse(self._values_list[0])
            self._values_list_parsed = []
            if value_parsed is not None:
                self._values_list_parsed = self._get_waterfall_match_values(
                    value_parsed
                )

        return self._values_list_parsed

    def _get_waterfall_match_values(self, value: t.Any):
        idx = self.waterfall_order.index(value)
        start_idx, end_idx = 0, len(self.waterfall_order)
        if self.op == WaterFallMatchOp.GT:
            start_idx = idx + 1
        if self.op == WaterFallMatchOp.GTE:
            start_idx = idx
        if self.op == WaterFallMatchOp.LT:
            end_idx = idx
        if self.op == WaterFallMatchOp.LTE:
            end_idx = idx + 1
        return self.waterfall_order[start_idx:end_idx]


class TextMatchDirective(ConstMatchDirective):
    def set_match_query_extra_args(
        self,
        operator: t.Optional[str] = ...,
        minimum_should_match: t.Optional[t.Union[str, int]] = ...,
        fuzziness: t.Optional[t.Union[str, int]] = ...,
        prefix_length: t.Optional[int] = ...,
        max_expansions: t.Optional[int] = ...,
        analyzer: t.Optional[str] = ...,
        auto_generate_synonyms_phrase_query: t.Optional[bool] = ...,
        **kwargs,
    ):
        """
        Updates the ES Match/MultiMatch DSLQuery Args

        :param operator: Optional, operator to use for the query ('and' or 'or'). [default: 'or']
        :param minimum_should_match: Optional, minimum number of clauses that must match. [default: None]
        :param fuzziness: Optional, defines the fuzziness (e.g., 'AUTO', 1, 2). [default: None]
        :param prefix_length: Optional, length of the prefix to apply fuzziness. [default: 0]
        :param max_expansions: Optional, maximum number of terms to expand the query to. [default: 50]
        :param analyzer: Optional, analyzer to use for the query string. [default: None]
        :param auto_generate_synonyms_phrase_query: Optional, whether to automatically generate synonyms phrases. [default: True]
        """
        return super().set_match_query_extra_args(
            operator=operator,
            minimum_should_match=minimum_should_match,
            fuzziness=fuzziness,
            prefix_length=prefix_length,
            max_expansions=max_expansions,
            analyzer=analyzer,
            auto_generate_synonyms_phrase_query=auto_generate_synonyms_phrase_query,
            **kwargs,
        )

    def _get_multi_field_queries(
        self, fields: t.List[str]
    ) -> t.List[DSLQuery]:
        return [
            self._generate_dsl_query(
                MultiMatchQuery,
                query=v,
                fields=fields,
                _name=self._name,
                **self._match_query_kwargs,
            )
            for v in self.values_list
        ]

    def _get_single_field_queries(self, field: str) -> t.List[DSLQuery]:
        if not self.values_list:
            return []

        return [
            self._generate_dsl_query(
                MatchQuery,
                field=field,
                value=v,
                _name=self._name,
                **self._match_query_kwargs,
            )
            for v in self.values_list
        ]

    def _get_nested_fields_queries(self) -> t.List[DSLQuery]:
        _, nested_fields = self.fields
        return [
            NestedQuery(
                path=field.nested_path,
                query=self._generate_dsl_query(
                    MatchQuery,
                    field=field.field_name,
                    value=v,
                    **self._match_query_kwargs,
                ),
            )
            for field in nested_fields
            for v in self.values_list
        ]


class QueryStringMatchDirective(TextMatchDirective):
    def _get_multi_field_queries(
        self, fields: t.List[str]
    ) -> t.List[DSLQuery]:
        return [
            self._generate_dsl_query(
                QueryStringQuery,
                query=v,
                fields=fields,
                _name=self._name,
                **self._match_query_kwargs,
            )
            for v in self.values_list
        ]

    def _get_single_field_queries(self, field: str) -> t.List[DSLQuery]:
        if not self.values_list:
            return []

        return [
            self._generate_dsl_query(
                QueryStringQuery,
                fields=[field],
                query=v,
                _name=self._name,
                **self._match_query_kwargs,
            )
            for v in self.values_list
        ]

    def _get_nested_fields_queries(self) -> t.List[DSLQuery]:
        _, nested_fields = self.fields
        return [
            NestedQuery(
                path=field.nested_path,
                query=self._generate_dsl_query(
                    QueryStringQuery,
                    fields=[field.field_name],
                    query=v,
                    _name=self._name,
                    **self._match_query_kwargs,
                ),
            )
            for field in nested_fields
            for v in self.values_list
        ]


class RangeMatchDirective(MatchDirective):
    def __init__(
        self,
        mode=MatchMode.INCLUDE,
        nullable_value: bool = False,
        name: t.Optional[str] = None,
    ) -> None:
        self.name = name
        super().__init__(mode, nullable_value)

    def copy(
        self,
        fields: bool = False,
        values: bool = False,
        match_params: bool = False,
    ) -> Self:
        self_copy = (
            self.__class__(self.mode, self.nullable_value, self.name)
            .configure(**self.config_kwargs)
            .set_match_query_extra_args(**self._match_query_kwargs)
        )
        self_copy._fields = self._fields if fields else None
        self_copy._values_list = self._values_list if values else None
        self_copy._values_map = self._values_map if values else None
        self_copy._match_params = self._match_params if match_params else None
        return self_copy

    def _get_bool_and_queries(self) -> t.List[DSLQuery]:
        if self._base_match_op != BaseMatchOp.AND:
            return []
        return self._get_bool_queries(MatchMode.INCLUDE)

    def _get_bool_should_queries(self) -> t.List[DSLQuery]:
        if self._base_match_op != BaseMatchOp.OR:
            return []
        return self._get_bool_queries(MatchMode.INCLUDE)

    def _get_bool_must_not_queries(self) -> t.List[DSLQuery]:
        return self._get_bool_queries(MatchMode.EXCLUDE)

    def _get_bool_queries(self, match_mode: MatchMode) -> t.List[DSLQuery]:
        if self.mode != match_mode:
            return []
        match_dsl_query = self._make_match_dsl_query()
        return [match_dsl_query] if match_dsl_query else []

    def _make_match_dsl_query(self) -> t.Optional[DSLQuery]:
        if not self._validate_match_parameters():
            return None
        fields, nested_fields = self.fields
        field = fields[0] if fields else nested_fields[0]
        is_nested = isinstance(field, NestedField)
        field_name = field.field_name if is_nested else field
        query = RangeQuery(
            field_name,
            gte=self.values_map.get("gte"),
            gt=self.values_map.get("gt"),
            lte=self.values_map.get("lte"),
            lt=self.values_map.get("lt"),
            _name=self.name,
        )
        if is_nested:
            query = NestedQuery(
                path=field.nested_path, query=query, _name=self.name
            )
        return query

    def _validate_match_parameters(self) -> None:
        if (
            len(
                {
                    key
                    for key in ["gt", "gte", "lt", "lte"]
                    if self.values_map and self.values_map.get(key) is not None
                }
            )
            < 1
        ):
            if self.nullable_value:
                return False
            raise ValueError(
                f"No compare value provided for: {type(self).__name__}"
            )

        fields, nested_fields = self.fields
        field_lenght = len(fields + nested_fields)
        if len(fields + nested_fields) != 1:
            raise ValueError(
                f"Exactly 1 field needed for {type(self).__name__}. Give: {field_lenght}"
            )
        return True


class ScriptMatchDirective(MatchDirective):
    def __init__(
        self,
        script: str,
        mode=MatchMode.INCLUDE,
        mandatory_params_keys: t.List[str] = [],
        nullable_value: bool = False,
        name: t.Optional[str] = None,
    ) -> None:
        self.script = script
        self.mandatory_params_keys = mandatory_params_keys
        self.name = name
        super().__init__(mode, nullable_value=nullable_value)

    def copy(
        self,
        fields: bool = False,
        values: bool = False,
        match_params: bool = False,
    ) -> Self:
        self_copy = (
            self.__class__(
                self.script,
                self.mode,
                self.mandatory_params_keys,
                self.nullable_value,
                self.name,
            )
            .configure(**self.config_kwargs)
            .set_match_query_extra_args(**self._match_query_kwargs)
        )
        self_copy._fields = self._fields if fields else None
        self_copy._values_list = self._values_list if values else None
        self_copy._values_map = self._values_map if values else None
        self_copy._match_params = self._match_params if match_params else None
        return self_copy

    def _get_bool_and_queries(self) -> t.List[DSLQuery]:
        if self._base_match_op != BaseMatchOp.AND:
            return []
        return self._get_bool_queries(MatchMode.INCLUDE)

    def _get_bool_should_queries(self) -> t.List[DSLQuery]:
        if self._base_match_op != BaseMatchOp.OR:
            return []
        return self._get_bool_queries(MatchMode.INCLUDE)

    def _get_bool_must_not_queries(self) -> t.List[DSLQuery]:
        return self._get_bool_queries(MatchMode.EXCLUDE)

    def _get_bool_queries(self, match_mode: MatchMode) -> t.List[DSLQuery]:
        if self.mode != match_mode:
            return []
        match_dsl_query = self._make_match_dsl_query()
        return [match_dsl_query] if match_dsl_query else []

    def _make_match_dsl_query(self) -> ScriptQuery:
        if not self._validate_script_parameters():
            return None
        values_map = self.values_map if self._values_map is not None else {}
        return ScriptQuery(
            script=self.script, params=values_map or None, _name=self.name
        )

    def _validate_script_parameters(self):
        if self._values_map is None:
            missing_params = self.mandatory_params_keys
        else:
            missing_params = [
                key
                for key in self.mandatory_params_keys
                if self.values_map.get(key) is None
            ]
        if missing_params and not self.nullable_value:
            raise ValueError(
                f"Missing mandatory script parameters for {type(self).__name__}: {missing_params}"
                "| Mandatory params must be present and be non-null"
            )
        if missing_params:
            return False
        return True


class FieldExistsDirective(MatchDirective):
    def __init__(
        self,
        rule: FieldMatchType = FieldMatchType.ANY,
        mode: MatchMode = MatchMode.INCLUDE,
        name: t.Optional[str] = None,
    ):
        self.rule = rule
        self.name = name
        super().__init__(mode, nullable_value=False)

    def copy(self, **kwargs) -> Self:
        self_copy = self.__class__(self.rule, self.mode, self.name)
        return self_copy

    def _get_bool_and_queries(self) -> t.List[DSLQuery]:
        if self._base_match_op != BaseMatchOp.AND:
            return []
        return self._get_bool_queries(MatchMode.INCLUDE)

    def _get_bool_should_queries(self) -> t.List[DSLQuery]:
        if self._base_match_op != BaseMatchOp.OR:
            return []
        return self._get_bool_queries(MatchMode.INCLUDE)

    def _get_bool_must_not_queries(self) -> t.List[DSLQuery]:
        return self._get_bool_queries(MatchMode.EXCLUDE)

    def _get_bool_queries(self, match_mode: MatchMode) -> t.List[DSLQuery]:
        if self.mode != match_mode:
            return []
        match_dsl_query = self._make_match_dsl_query()
        return [match_dsl_query] if match_dsl_query else []

    def _make_match_dsl_query(self) -> ExistsQuery:
        fields, nested_fields = self.fields

        if not fields and not nested_fields:
            if not self.nullable_value:
                raise ValueError(
                    f"No field provided for {type(self).__name__}"
                )
            return None

        exists_queries = self._get_fields_queries()
        exists_queries.extend(self._get_nested_fields_queries())
        if len(exists_queries) > 1:
            bool_query = self._build_bool_query(exists_queries)
            return bool_query
        return exists_queries[0] if exists_queries else None

    def _get_fields_queries(self) -> t.List[DSLQuery]:
        fields, _ = self.fields
        return [ExistsQuery(field, _name=self.name) for field in fields]

    def _get_nested_fields_queries(self) -> t.List[DSLQuery]:
        _, nested_fields = self.fields
        return [
            NestedQuery(
                path=field.nested_path,
                query=ExistsQuery(field.field_name, _name=self.name),
            )
            for field in nested_fields
        ]

    def _build_bool_query(self, match_queries: t.List[DSLQuery]) -> DSLQuery:
        bool_builder = BooleanDSLBuilder()

        if self.rule == FieldMatchType.ANY:
            bool_builder.add_should_query(*match_queries)
        elif self.rule == FieldMatchType.ALL:
            bool_builder.add_filter_query(*match_queries)

        return bool_builder.build()
