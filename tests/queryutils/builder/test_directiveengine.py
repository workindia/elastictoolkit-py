from elastictoolkit.queryutils.builder.directiveengine import DirectiveEngine
from elastictoolkit.queryutils.builder.matchdirective import (
    ConstMatchDirective,
    TextMatchDirective,
    RangeMatchDirective,
    WaterfallFieldMatchDirective,
)
from elastictoolkit.queryutils.builder.custommatchdirective import (
    CustomMatchDirective,
)
from elastictoolkit.queryutils.builder.directivevaluemapper import (
    DirectiveValueMapper,
)
from elastictoolkit.queryutils.builder.booldirective import (
    AndDirective,
    OrDirective,
)
from elastictoolkit.queryutils.consts import (
    BaseMatchOp,
    FieldMatchType,
    WaterFallMatchOp,
)
from elastictoolkit.queryutils.types import FieldValue
from tests.queryutils.builder.basetestcase import DirectiveEngineBaseTest


class TestValueMapper(DirectiveValueMapper):
    field1_match = FieldValue(
        fields=["field1"], values_list=["value1", "value2"]
    )
    field2_search = FieldValue(
        fields=["field2"], values_list=["*match_params.dynamic_value"]
    )
    nested_field_range = FieldValue(
        fields=["nested.field"],
        values_map={
            "gte": "match_params.range_min",
            "lte": "match_params.range_max",
        },
    )
    field1_waterfall = FieldValue(
        fields=["field1"],
        values_list=["match_params.waterfall_value"],
    )


class SimpleEngine(DirectiveEngine):
    field1_match = ConstMatchDirective(
        rule=FieldMatchType.ANY, name="simple_match"
    )

    class Config:
        value_mapper = TestValueMapper()


class TestSimpleDirectiveEngine(DirectiveEngineBaseTest):
    """Test case for simple DirectiveEngine with single ConstMatchDirective"""

    engine_cls = SimpleEngine
    match_params = {}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "terms": {
                        "field1": ["value1", "value2"],
                        "_name": "simple_match",
                    }
                }
            ]
        }
    }


class ComplexEngine(DirectiveEngine):
    field1_waterfall = WaterfallFieldMatchDirective(
        rule=FieldMatchType.ANY,
        waterfall_order=["low", "medium", "high"],
        op=WaterFallMatchOp.GT,
        name="waterfall_filter",
    )
    field2_search = TextMatchDirective(
        rule=FieldMatchType.ANY, name="text_search"
    ).set_match_query_extra_args(fuzziness="auto")

    nested_field_range = RangeMatchDirective(name="range_filter")

    class Config:
        value_mapper = TestValueMapper


class TestComplexDirectiveEngine(DirectiveEngineBaseTest):
    """Test case for DirectiveEngine with multiple directive types"""

    engine_cls = ComplexEngine
    match_params = {
        "dynamic_value": "test_value",
        "nested_value": "nested_test",
        "range_min": 100,
        "range_max": 200,
        "waterfall_value": "low",
    }
    expected_query = {
        "bool": {
            "filter": [
                {
                    "terms": {
                        "field1": ["medium", "high"],
                        "_name": "waterfall_filter",
                    }
                },
                {
                    "match": {
                        "field2": {
                            "query": "test_value",
                            "fuzziness": "auto",
                            "_name": "text_search",
                        }
                    }
                },
                {
                    "range": {
                        "nested.field": {
                            "gte": 100,
                            "lte": 200,
                            "_name": "range_filter",
                        }
                    }
                },
            ]
        }
    }


class BaseMatchOpShouldQueriesEngine(ComplexEngine):
    class Config:
        value_mapper = TestValueMapper
        match_directive_config = {
            "base_match_op": BaseMatchOp.OR,
        }


class TestBaseMatchOpShouldQueries(DirectiveEngineBaseTest):
    """Test case for BaseMatchOp.SHOULD queries"""

    engine_cls = BaseMatchOpShouldQueriesEngine
    match_params = {
        "dynamic_value": "test_value",
        "nested_value": "nested_test",
        "range_min": 100,
        "range_max": 200,
        "waterfall_value": "low",
    }
    expected_query = {
        "bool": {
            "should": [
                {
                    "terms": {
                        "field1": ["medium", "high"],
                        "_name": "waterfall_filter",
                    }
                },
                {
                    "match": {
                        "field2": {
                            "query": "test_value",
                            "fuzziness": "auto",
                            "_name": "text_search",
                        }
                    }
                },
                {
                    "range": {
                        "nested.field": {
                            "gte": 100,
                            "lte": 200,
                            "_name": "range_filter",
                        }
                    }
                },
            ]
        }
    }


class CustomDirective(CustomMatchDirective):
    allowed_engine_cls_name = "CustomEngine"

    def get_directive(self):
        return OrDirective(
            field1_match=ConstMatchDirective(FieldMatchType.ANY),
            field2_search=TextMatchDirective(FieldMatchType.ALL),
        )


class CustomEngine(DirectiveEngine):
    custom = CustomDirective()

    class Config:
        value_mapper = TestValueMapper


class TestCustomDirectiveEngine(DirectiveEngineBaseTest):
    """Test case for DirectiveEngine with CustomMatchDirective"""

    engine_cls = CustomEngine
    match_params = {"dynamic_value": "test_value"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {
                                "bool": {
                                    "filter": [
                                        {
                                            "terms": {
                                                "field1": ["value1", "value2"]
                                            }
                                        }
                                    ]
                                }
                            },
                            {
                                "bool": {
                                    "filter": [
                                        {
                                            "match": {
                                                "field2": {
                                                    "query": "test_value"
                                                }
                                            }
                                        }
                                    ]
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class InvalidCustomDirective(CustomMatchDirective):
    allowed_engine_cls_name = "WrongEngine"

    def get_directive(self):
        return AndDirective()


class InvalidEngine(DirectiveEngine):
    invalid = InvalidCustomDirective()

    class Config:
        value_mapper = TestValueMapper


class TestInvalidDirectiveEngine(DirectiveEngineBaseTest):
    """Test case for DirectiveEngine with invalid CustomMatchDirective"""

    engine_cls = InvalidEngine
    match_params = {}
    expected_exc_cls = TypeError
    exc_message = (
        "can only be used in instance of DirectiveEngine: WrongEngine"
    )
