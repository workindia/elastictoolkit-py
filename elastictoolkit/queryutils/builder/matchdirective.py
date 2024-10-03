import copy
import typing as t
from typing_extensions import Self
from elasticquerydsl.base import DSLQuery, BoolQuery
from elasticquerydsl.filter import (
    MultiMatchQuery,
    NestedQuery,
    RangeQuery,
    ScriptQuery,
    TermQuery,
    TermsQuery,
)
from elasticquerydsl.utils import BooleanDSLBuilder

from elastictoolkit.queryutils.types import NestedField
from elastictoolkit.queryutils.consts import (
    MatchMode,
    FieldMatchType,
    WaterFallMatchOp,
)


class MatchDirective:
    def __init__(
        self, mode=MatchMode.INCLUDE, nullable_value: bool = False
    ) -> None:
        self.mode = mode
        self.nullable_value = nullable_value
        self.es_query_params = {}

        self._match_params = None
        self._fields = None
        self._values_list = None
        self._values_map = None

    def copy(
        self,
        fields: bool = False,
        values: bool = False,
        match_params: bool = False,
    ) -> Self:
        self_copy = copy.deepcopy(self)
        self_copy._fields = self._fields if fields else None
        self_copy._values_list = self._values_list if values else None
        self_copy._match_params = self._match_params if match_params else None
        return self_copy

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
                f"{type(self).__name__} directive requires: `match_params`. This must be set using `set_value_map` method."
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
            values_list_parsed = []
            for v in self._values_list:
                should_unpack = isinstance(v, str) and v.startswith("*")
                if should_unpack:
                    # TODO: Refactor this
                    v = v[1:]
                parsed_value = self._parse_runtime_value(v)
                if should_unpack and hasattr(parsed_value, "__iter__"):
                    values_list_parsed.extend(parsed_value)
                else:
                    values_list_parsed.append(parsed_value)
            self._values_list_parsed = values_list_parsed
        return self._values_list_parsed

    @property
    def values_map(self):
        if self._values_map is None:
            raise ValueError(
                f"{type(self).__name__} directive requires: `values_map`. This must be set using `set_values` method."
            )
        if not hasattr(self, "_values_map_parsed"):
            self._values_map_parsed = dict(
                (k, self._parse_runtime_value(v))
                for k, v in self._values_map.items()
            )
        return self._values_map_parsed

    def execute(self, bool_builder: BooleanDSLBuilder):
        bool_builder.add_should_query(*self._get_bool_should_queries())
        bool_builder.add_must_query(*self._get_bool_must_queries())
        bool_builder.add_must_not_query(*self._get_bool_must_not_queries())
        bool_builder.add_filter_query(*self._get_bool_filter_queries())

    def to_dsl(self) -> BoolQuery:
        builder = BooleanDSLBuilder()
        self.execute(builder)
        return builder.build()

    def _get_bool_should_queries(self) -> t.List[DSLQuery]:
        return []

    def _get_bool_must_queries(self) -> t.List[DSLQuery]:
        return []

    def _get_bool_must_not_queries(self) -> t.List[DSLQuery]:
        return []

    def _get_bool_filter_queries(self) -> t.List[DSLQuery]:
        return []

    def _parse_runtime_value(self, value) -> t.Any:
        """
        Parses the input `value` based on its type and content, dynamically resolving it if necessary.

        The function handles three cases:
        1. **String Starting with `match_params.`**:
        - If `value` is a string and starts with the prefix `match_params.`, the function treats it as a key path within the `self.match_params` dictionary.
        - It traverses `self.match_params` according to the key path, resolving the string to the corresponding value in the dictionary.
        - For example, if `self.match_params = {"foo": {"bar": 1}}` and `value = "match_params.foo.bar"`, the function will return `1`.
        - If any part of the key path does not exist within `self.match_params`, the function returns `None`.

        2. **Callable (Function)**:
        - If `value` is a callable (a function), the function executes it, passing `self.match_params` as the argument.
        - The return value of the function is returned by `_parse_runtime_value`.
        - This allows for dynamic computation of values based on `self.match_params`.

        3. **Other Types**:
        - If `value` is neither a string starting with `match_params.` nor a callable, the function simply returns `value` as it is.
        - This ensures that other types of values (e.g., integers, lists, etc.) are handled correctly without modification.

        Args:
            value (str | callable | Any): The value to be parsed. It can be:
                - A string that starts with `match_params.` indicating a key path within `self.match_params`.
                - A callable function that accepts `self.match_params` as its argument.
                - Any other value that should be returned as-is.

        Returns:
            Any: The resolved value, which could be:
                - The value from `self.match_params` corresponding to the key path in the string.
                - The result of the function if `value` is callable.
                - The original `value` if it is of any other type.
        """

        if isinstance(value, str):
            if value.startswith("match_params."):
                # Extract the key path after 'match_params.'
                key_path = value[len("match_params.") :]
                keys = key_path.split(".")

                # Traverse the match_params dictionary to get the value
                result = self.match_params
                for key in keys:
                    result = result.get(key)
                    if result is None:
                        break
                return result

        elif callable(value):
            # If the value is a function, execute and return the value
            return value(self.match_params)

        # If it's not a special case, return the value as it is
        return value


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
        self_copy = self.__class__(
            self.rule, self.mode, self.nullable_value, self._name
        )
        self_copy._fields = self._fields if fields else None
        self_copy._values_list = self._values_list if values else None
        self_copy._match_params = self._match_params if match_params else None
        return self_copy

    def _get_bool_filter_queries(self) -> t.List[DSLQuery]:
        return self._get_bool_queries(MatchMode.INCLUDE)

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

        if len(match_queries) == 1:
            return match_queries[0]  # No need to build a Bool Query

        return self._build_bool_query(match_queries)

    def _validate_match_parameters(self) -> None:
        if not self.values_list and not self.nullable_value:
            # TODO: remove print
            print("ERROR:", self._name)
            raise ValueError(f"No match value set for {type(self).__name__}")

        fields, nested_fields = self.fields
        if not fields and not nested_fields:
            raise ValueError(f"No match field set for {type(self).__name__}")

    def _collect_match_queries(self) -> t.List[DSLQuery]:
        match_queries = []
        match_queries.extend(self._get_fields_queries())
        match_queries.extend(self._get_nested_fields_queries())
        return match_queries

    def _build_bool_query(self, match_queries: t.List[DSLQuery]) -> DSLQuery:
        bool_builder = BooleanDSLBuilder()

        if self.rule == FieldMatchType.ANY:
            bool_builder.add_should_query(*match_queries)
        elif self.rule == FieldMatchType.ALL:
            bool_builder.add_filter_query(*match_queries)

        return bool_builder.build()

    def _get_fields_queries(self) -> t.List[DSLQuery]:
        fields, _ = self.fields

        if len(fields) > 1:
            if self.rule == FieldMatchType.ANY:
                return [
                    MultiMatchQuery(
                        " ".join(self.values_list), fields, _name=self._name
                    )
                ]
            else:
                return [
                    MultiMatchQuery(v, fields, _name=self._name)
                    for v in self.values_list
                ]
        elif len(fields) == 1:
            if len(self.values_list) > 1:
                if self.rule == FieldMatchType.ANY:
                    return [TermsQuery(fields[0], self.values_list)]
                else:
                    return [TermQuery(fields[0], v) for v in self.values_list]
            elif len(self.values_list) == 1:
                return [TermQuery(fields[0], self.values_list[0])]
        return []

    def _get_nested_fields_queries(self) -> t.List[DSLQuery]:
        _, nested_fields = self.fields
        return [
            NestedQuery(
                path=field.nested_path,
                query=TermQuery(f"{field.field_name}", v),
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
        self_copy = self.__class__(
            self.rule,
            self.waterfall_order,
            self.op,
            self.mode,
            self.nullable_value,
            self._name,
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
            value_parsed = self._parse_runtime_value(self._values_list[0])
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


class RangeMatchDirective(MatchDirective):
    def __init__(
        self, mode=MatchMode.INCLUDE, nullable_value: bool = False
    ) -> None:
        super().__init__(mode, nullable_value)

    def copy(
        self,
        fields: bool = False,
        values: bool = False,
        match_params: bool = False,
    ) -> Self:
        self_copy = self.__class__(self.mode, self.nullable_value)
        self_copy._fields = self._fields if fields else None
        self_copy._values_list = self._values_list if values else None
        self_copy._values_map = self._values_map if values else None
        self_copy._match_params = self._match_params if match_params else None
        return self_copy

    def _get_bool_filter_queries(self) -> t.List[DSLQuery]:
        return self._get_bool_queries(MatchMode.INCLUDE)

    def _get_bool_must_not_queries(self) -> t.List[DSLQuery]:
        return self._get_bool_queries(MatchMode.EXCLUDE)

    def _get_bool_queries(self, match_mode: MatchMode) -> t.List[DSLQuery]:
        if self.mode != match_mode:
            return []
        match_dsl_query = self._make_match_dsl_query()
        return [match_dsl_query] if match_dsl_query else []

    def _make_match_dsl_query(self) -> ScriptQuery:
        self._validate_match_parameters()
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
        )
        if is_nested:
            query = NestedQuery(path=field.nested_path, query=query)
        return query

    def _validate_match_parameters(self) -> None:
        if (
            not self.values_map
            or len(
                set(["gt", "gte", "lt", "lte"]).intersection(
                    self.values_map.keys()
                )
            )
            < 1
        ):
            raise ValueError(
                f"No compare value provided for: {type(self).__name__}"
            )

        fields, nested_fields = self.fields
        field_lenght = len(fields + nested_fields)
        if len(fields + nested_fields) != 1:
            raise ValueError(
                f"Exactly 1 field needed for {type(self).__name__}. Give: {field_lenght}"
            )


class ScriptMatchDirective(MatchDirective):
    def __init__(self, script: str, mode=MatchMode.INCLUDE) -> None:
        self.script = script
        super().__init__(mode, nullable_value=False)

    def copy(
        self,
        fields: bool = False,
        values: bool = False,
        match_params: bool = False,
    ) -> Self:
        self_copy = self.__class__(self.script, self.mode)
        self_copy._fields = self._fields if fields else None
        self_copy._values_list = self._values_list if values else None
        self_copy._values_map = self._values_map if values else None
        self_copy._match_params = self._match_params if match_params else None
        return self_copy

    def _get_bool_filter_queries(self) -> t.List[DSLQuery]:
        return self._get_bool_queries(MatchMode.INCLUDE)

    def _get_bool_must_not_queries(self) -> t.List[DSLQuery]:
        return self._get_bool_queries(MatchMode.EXCLUDE)

    def _get_bool_queries(self, match_mode: MatchMode) -> t.List[DSLQuery]:
        if self.mode != match_mode:
            return []
        match_dsl_query = self._make_match_dsl_query()
        return [match_dsl_query] if match_dsl_query else []

    def _make_match_dsl_query(self) -> ScriptQuery:
        return ScriptQuery(script=self.script, params=self.values_map or None)
