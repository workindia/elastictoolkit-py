from elastictoolkit.queryutils.builder.matchdirective import (
    ConstMatchDirective,
    FieldExistsDirective,
    RangeMatchDirective,
    ScriptMatchDirective,
    TextMatchDirective,
    WaterfallFieldMatchDirective,
    QueryStringMatchDirective,
)
from elastictoolkit.queryutils.consts import (
    AndQueryOp,
    FieldMatchType,
    MatchMode,
    WaterFallMatchOp,
)
from elastictoolkit.queryutils.types import NestedField
from tests.queryutils.builder.basetestcase import MatchDirectiveBaseTest


# ConstMatchDirective


class TestConstMatchDirectiveSingleFieldAll(MatchDirectiveBaseTest):
    """Test cases for ConstMatchDirective with single field and ALL match type"""

    match_directive = ConstMatchDirective(
        rule=FieldMatchType.ALL, name="test_const"
    )
    fields = ["field1"]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "term": {
                                    "field1": {
                                        "value": "test1",
                                        "_name": "test_const",
                                    }
                                }
                            },
                            {
                                "term": {
                                    "field1": {
                                        "value": "test2",
                                        "_name": "test_const",
                                    }
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestConstMatchDirectiveMultiFieldAll(MatchDirectiveBaseTest):
    """Test cases for ConstMatchDirective with multi-field and ALL match type"""

    match_directive = ConstMatchDirective(
        rule=FieldMatchType.ALL, name="test_const"
    )
    fields = ["field1", "field2"]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "multi_match": {
                                    "query": "test1",
                                    "fields": ["field1", "field2"],
                                    "_name": "test_const",
                                }
                            },
                            {
                                "multi_match": {
                                    "query": "test2",
                                    "fields": ["field1", "field2"],
                                    "_name": "test_const",
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestConstMatchDirectiveSingleFieldAny(MatchDirectiveBaseTest):
    """Test cases for ConstMatchDirective with single field and ANY match type"""

    match_directive = ConstMatchDirective(
        rule=FieldMatchType.ANY, name="test_const"
    )
    fields = ["field1"]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "terms": {
                        "field1": ["test1", "test2"],
                        "_name": "test_const",
                    }
                }
            ]
        }
    }


class TestConstMatchDirectiveMultiFieldAny(MatchDirectiveBaseTest):
    """Test cases for ConstMatchDirective with multi-field and ANY match type"""

    match_directive = ConstMatchDirective(
        rule=FieldMatchType.ANY, name="test_const"
    )
    fields = ["field1", "field2"]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {
                                "multi_match": {
                                    "query": "test1",
                                    "fields": ["field1", "field2"],
                                    "_name": "test_const",
                                }
                            },
                            {
                                "multi_match": {
                                    "query": "test2",
                                    "fields": ["field1", "field2"],
                                    "_name": "test_const",
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestConstMatchDirectiveSingleNestedFieldAny(MatchDirectiveBaseTest):
    """Test cases for ConstMatchDirective with single nested field and ANY match type"""

    match_directive = ConstMatchDirective(
        rule=FieldMatchType.ANY, name="test_const"
    )
    fields = [
        NestedField(
            nested_path="nested_path", field_name="nested_path.nested_field"
        )
    ]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {
                                "nested": {
                                    "path": "nested_path",
                                    "query": {
                                        "term": {
                                            "nested_path.nested_field": {
                                                "value": "test1",
                                                "_name": "test_const",
                                            }
                                        }
                                    },
                                }
                            },
                            {
                                "nested": {
                                    "path": "nested_path",
                                    "query": {
                                        "term": {
                                            "nested_path.nested_field": {
                                                "value": "test2",
                                                "_name": "test_const",
                                            }
                                        }
                                    },
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestConstMatchDirectiveSingleNestedFieldAll(MatchDirectiveBaseTest):
    """Test cases for ConstMatchDirective with single nested field and ALL match type"""

    match_directive = ConstMatchDirective(
        rule=FieldMatchType.ALL, name="test_const"
    )
    fields = [
        NestedField(
            nested_path="nested_path", field_name="nested_path.nested_field"
        )
    ]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "nested": {
                                    "path": "nested_path",
                                    "query": {
                                        "term": {
                                            "nested_path.nested_field": {
                                                "value": "test1",
                                                "_name": "test_const",
                                            }
                                        }
                                    },
                                }
                            },
                            {
                                "nested": {
                                    "path": "nested_path",
                                    "query": {
                                        "term": {
                                            "nested_path.nested_field": {
                                                "value": "test2",
                                                "_name": "test_const",
                                            }
                                        }
                                    },
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestConstMatchDirectiveMixedFieldsAny(MatchDirectiveBaseTest):
    """Test cases for ConstMatchDirective with mixed fields (string and nested) and ANY match type"""

    match_directive = ConstMatchDirective(
        rule=FieldMatchType.ANY, name="test_const"
    )
    fields = [
        "normal_field",
        NestedField(nested_path="path1", field_name="path1.nested1"),
        NestedField(nested_path="path2", field_name="path2.nested2"),
    ]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {
                                "terms": {
                                    "normal_field": ["test1", "test2"],
                                    "_name": "test_const",
                                }
                            },
                            {
                                "nested": {
                                    "path": "path1",
                                    "query": {
                                        "term": {
                                            "path1.nested1": {
                                                "value": "test1",
                                                "_name": "test_const",
                                            }
                                        }
                                    },
                                }
                            },
                            {
                                "nested": {
                                    "path": "path1",
                                    "query": {
                                        "term": {
                                            "path1.nested1": {
                                                "value": "test2",
                                                "_name": "test_const",
                                            }
                                        }
                                    },
                                }
                            },
                            {
                                "nested": {
                                    "path": "path2",
                                    "query": {
                                        "term": {
                                            "path2.nested2": {
                                                "value": "test1",
                                                "_name": "test_const",
                                            }
                                        }
                                    },
                                }
                            },
                            {
                                "nested": {
                                    "path": "path2",
                                    "query": {
                                        "term": {
                                            "path2.nested2": {
                                                "value": "test2",
                                                "_name": "test_const",
                                            }
                                        }
                                    },
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestConstMatchDirectiveMixedFieldsAll(MatchDirectiveBaseTest):
    """Test cases for ConstMatchDirective with mixed fields (string and nested) and ALL match type"""

    match_directive = ConstMatchDirective(
        rule=FieldMatchType.ALL, name="test_const"
    )
    fields = [
        "normal_field",
        NestedField(nested_path="path1", field_name="path1.nested1"),
        NestedField(nested_path="path2", field_name="path2.nested2"),
    ]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "term": {
                                    "normal_field": {
                                        "value": "test1",
                                        "_name": "test_const",
                                    }
                                }
                            },
                            {
                                "term": {
                                    "normal_field": {
                                        "value": "test2",
                                        "_name": "test_const",
                                    }
                                }
                            },
                            {
                                "nested": {
                                    "path": "path1",
                                    "query": {
                                        "term": {
                                            "path1.nested1": {
                                                "value": "test1",
                                                "_name": "test_const",
                                            }
                                        }
                                    },
                                }
                            },
                            {
                                "nested": {
                                    "path": "path1",
                                    "query": {
                                        "term": {
                                            "path1.nested1": {
                                                "value": "test2",
                                                "_name": "test_const",
                                            }
                                        }
                                    },
                                }
                            },
                            {
                                "nested": {
                                    "path": "path2",
                                    "query": {
                                        "term": {
                                            "path2.nested2": {
                                                "value": "test1",
                                                "_name": "test_const",
                                            }
                                        }
                                    },
                                }
                            },
                            {
                                "nested": {
                                    "path": "path2",
                                    "query": {
                                        "term": {
                                            "path2.nested2": {
                                                "value": "test2",
                                                "_name": "test_const",
                                            }
                                        }
                                    },
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestConstMatchDirectiveAndOpMust(MatchDirectiveBaseTest):
    """Test cases for ConstMatchDirective with AND op and MUST match type"""

    match_directive = ConstMatchDirective(
        rule=FieldMatchType.ALL,
        name="test_const",
    ).configure(and_query_op=AndQueryOp.MUST)
    fields = ["field1", "field2"]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "must": [
                {"match": {"field1": "value1"}},
                {"match": {"field2": "value2"}},
            ]
        }
    }
    expected_query = {
        "bool": {
            "must": [
                {
                    "bool": {
                        "filter": [
                            {
                                "multi_match": {
                                    "query": "test1",
                                    "fields": ["field1", "field2"],
                                    "_name": "test_const",
                                }
                            },
                            {
                                "multi_match": {
                                    "query": "test2",
                                    "fields": ["field1", "field2"],
                                    "_name": "test_const",
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


# TextMatchDirective


class TestTextMatchDirectiveSingleFieldAll(MatchDirectiveBaseTest):
    """Test cases for TextMatchDirective with single field and ALL match type"""

    match_directive = TextMatchDirective(
        rule=FieldMatchType.ALL, name="test_text"
    ).set_match_query_extra_args(fuzziness="auto", operator="and")
    fields = ["field1"]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "match": {
                                    "field1": {
                                        "query": "test1",
                                        "fuzziness": "auto",
                                        "operator": "and",
                                        "_name": "test_text",
                                    }
                                }
                            },
                            {
                                "match": {
                                    "field1": {
                                        "query": "test2",
                                        "fuzziness": "auto",
                                        "operator": "and",
                                        "_name": "test_text",
                                    }
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestTextMatchDirectiveMultiFieldAll(MatchDirectiveBaseTest):
    """Test cases for TextMatchDirective with multi-field and ALL match type"""

    match_directive = TextMatchDirective(
        rule=FieldMatchType.ALL, name="test_text"
    ).set_match_query_extra_args(minimum_should_match="75%", prefix_length=2)
    fields = ["field1", "field2"]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "multi_match": {
                                    "query": "test1",
                                    "fields": ["field1", "field2"],
                                    "minimum_should_match": "75%",
                                    "prefix_length": 2,
                                    "_name": "test_text",
                                }
                            },
                            {
                                "multi_match": {
                                    "query": "test2",
                                    "fields": ["field1", "field2"],
                                    "minimum_should_match": "75%",
                                    "prefix_length": 2,
                                    "_name": "test_text",
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestTextMatchDirectiveSingleFieldAny(MatchDirectiveBaseTest):
    """Test cases for TextMatchDirective with single field and ANY match type"""

    match_directive = TextMatchDirective(
        rule=FieldMatchType.ANY, name="test_text"
    ).set_match_query_extra_args(fuzziness=2, max_expansions=10)
    fields = ["field1"]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {
                                "match": {
                                    "field1": {
                                        "query": "test1",
                                        "fuzziness": 2,
                                        "max_expansions": 10,
                                        "_name": "test_text",
                                    }
                                }
                            },
                            {
                                "match": {
                                    "field1": {
                                        "query": "test2",
                                        "fuzziness": 2,
                                        "max_expansions": 10,
                                        "_name": "test_text",
                                    }
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestTextMatchDirectiveMultiFieldAny(MatchDirectiveBaseTest):
    """Test cases for TextMatchDirective with multi-field and ANY match type"""

    match_directive = TextMatchDirective(
        rule=FieldMatchType.ANY, name="test_text"
    ).set_match_query_extra_args(
        analyzer="standard", auto_generate_synonyms_phrase_query=True
    )
    fields = ["field1", "field2"]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {
                                "multi_match": {
                                    "query": "test1",
                                    "fields": ["field1", "field2"],
                                    "analyzer": "standard",
                                    "auto_generate_synonyms_phrase_query": True,
                                    "_name": "test_text",
                                }
                            },
                            {
                                "multi_match": {
                                    "query": "test2",
                                    "fields": ["field1", "field2"],
                                    "analyzer": "standard",
                                    "auto_generate_synonyms_phrase_query": True,
                                    "_name": "test_text",
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestTextMatchDirectiveSingleNestedFieldAny(MatchDirectiveBaseTest):
    """Test cases for TextMatchDirective with single nested field and ANY match type"""

    match_directive = TextMatchDirective(
        rule=FieldMatchType.ANY, name="test_text"
    ).set_match_query_extra_args(fuzziness="auto", operator="or")
    fields = [
        NestedField(
            nested_path="nested_path", field_name="nested_path.nested_field"
        )
    ]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {
                                "nested": {
                                    "path": "nested_path",
                                    "query": {
                                        "match": {
                                            "nested_path.nested_field": {
                                                "query": "test1",
                                                "operator": "or",
                                                "fuzziness": "auto",
                                            }
                                        }
                                    },
                                }
                            },
                            {
                                "nested": {
                                    "path": "nested_path",
                                    "query": {
                                        "match": {
                                            "nested_path.nested_field": {
                                                "query": "test2",
                                                "operator": "or",
                                                "fuzziness": "auto",
                                            }
                                        }
                                    },
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestTextMatchDirectiveMixedFieldsAll(MatchDirectiveBaseTest):
    """Test cases for TextMatchDirective with mixed fields (string and nested) and ALL match type"""

    match_directive = TextMatchDirective(
        rule=FieldMatchType.ALL, name="test_text"
    ).set_match_query_extra_args(
        fuzziness="auto", minimum_should_match="2<70%"
    )
    fields = [
        "normal_field",
        NestedField(nested_path="path1", field_name="path1.nested1"),
        NestedField(nested_path="path2", field_name="path2.nested2"),
    ]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "match": {
                                    "normal_field": {
                                        "query": "test1",
                                        "fuzziness": "auto",
                                        "minimum_should_match": "2<70%",
                                        "_name": "test_text",
                                    }
                                }
                            },
                            {
                                "match": {
                                    "normal_field": {
                                        "query": "test2",
                                        "fuzziness": "auto",
                                        "minimum_should_match": "2<70%",
                                        "_name": "test_text",
                                    }
                                }
                            },
                            {
                                "nested": {
                                    "path": "path1",
                                    "query": {
                                        "match": {
                                            "path1.nested1": {
                                                "query": "test1",
                                                "fuzziness": "auto",
                                                "minimum_should_match": "2<70%",
                                            }
                                        }
                                    },
                                }
                            },
                            {
                                "nested": {
                                    "path": "path1",
                                    "query": {
                                        "match": {
                                            "path1.nested1": {
                                                "query": "test2",
                                                "fuzziness": "auto",
                                                "minimum_should_match": "2<70%",
                                            }
                                        }
                                    },
                                }
                            },
                            {
                                "nested": {
                                    "path": "path2",
                                    "query": {
                                        "match": {
                                            "path2.nested2": {
                                                "query": "test1",
                                                "fuzziness": "auto",
                                                "minimum_should_match": "2<70%",
                                            }
                                        }
                                    },
                                }
                            },
                            {
                                "nested": {
                                    "path": "path2",
                                    "query": {
                                        "match": {
                                            "path2.nested2": {
                                                "query": "test2",
                                                "fuzziness": "auto",
                                                "minimum_should_match": "2<70%",
                                            }
                                        }
                                    },
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


# QueryStringMatchDirective


class TestQueryStringMatchDirectiveSingleFieldAll(MatchDirectiveBaseTest):
    """Test cases for QueryStringMatchDirective with single field and ALL match type"""

    match_directive = QueryStringMatchDirective(
        rule=FieldMatchType.ALL, name="test_query_string"
    ).set_match_query_extra_args(default_operator="AND", fuzziness="AUTO")
    fields = ["field1"]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "query_string": {
                                    "query": "test1",
                                    "fields": ["field1"],
                                    "default_operator": "AND",
                                    "fuzziness": "AUTO",
                                    "_name": "test_query_string",
                                }
                            },
                            {
                                "query_string": {
                                    "query": "test2",
                                    "fields": ["field1"],
                                    "default_operator": "AND",
                                    "fuzziness": "AUTO",
                                    "_name": "test_query_string",
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestQueryStringMatchDirectiveMultiFieldAll(MatchDirectiveBaseTest):
    """Test cases for QueryStringMatchDirective with multi-field and ALL match type"""

    match_directive = QueryStringMatchDirective(
        rule=FieldMatchType.ALL, name="test_query_string"
    ).set_match_query_extra_args(minimum_should_match="75%", boost=2.0)
    fields = ["field1", "field2"]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "query_string": {
                                    "query": "test1",
                                    "fields": ["field1", "field2"],
                                    "minimum_should_match": "75%",
                                    "boost": 2.0,
                                    "_name": "test_query_string",
                                }
                            },
                            {
                                "query_string": {
                                    "query": "test2",
                                    "fields": ["field1", "field2"],
                                    "minimum_should_match": "75%",
                                    "boost": 2.0,
                                    "_name": "test_query_string",
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestQueryStringMatchDirectiveSingleFieldAny(MatchDirectiveBaseTest):
    """Test cases for QueryStringMatchDirective with single field and ANY match type"""

    match_directive = QueryStringMatchDirective(
        rule=FieldMatchType.ANY, name="test_query_string"
    ).set_match_query_extra_args(analyze_wildcard=True, lenient=True)
    fields = ["field1"]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1*", "value2": "test2*"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {
                                "query_string": {
                                    "query": "test1*",
                                    "fields": ["field1"],
                                    "analyze_wildcard": True,
                                    "lenient": True,
                                    "_name": "test_query_string",
                                }
                            },
                            {
                                "query_string": {
                                    "query": "test2*",
                                    "fields": ["field1"],
                                    "analyze_wildcard": True,
                                    "lenient": True,
                                    "_name": "test_query_string",
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestQueryStringMatchDirectiveSingleNestedFieldAny(
    MatchDirectiveBaseTest
):
    """Test cases for QueryStringMatchDirective with single nested field and ANY match type"""

    match_directive = QueryStringMatchDirective(
        rule=FieldMatchType.ANY, name="test_query_string"
    ).set_match_query_extra_args(
        default_operator="OR", allow_leading_wildcard=False
    )
    fields = [
        NestedField(
            nested_path="nested_path", field_name="nested_path.nested_field"
        )
    ]
    values_list = ["match_params.value1", "match_params.value2"]
    match_params = {"value1": "test1", "value2": "test2"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {
                                "nested": {
                                    "path": "nested_path",
                                    "query": {
                                        "query_string": {
                                            "query": "test1",
                                            "fields": [
                                                "nested_path.nested_field"
                                            ],
                                            "default_operator": "OR",
                                            "allow_leading_wildcard": False,
                                            "_name": "test_query_string",
                                        }
                                    },
                                }
                            },
                            {
                                "nested": {
                                    "path": "nested_path",
                                    "query": {
                                        "query_string": {
                                            "query": "test2",
                                            "fields": [
                                                "nested_path.nested_field"
                                            ],
                                            "default_operator": "OR",
                                            "allow_leading_wildcard": False,
                                            "_name": "test_query_string",
                                        }
                                    },
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


# RangeMatchDirective


class TestRangeMatchDirectiveSingleRange(MatchDirectiveBaseTest):
    """Test cases for RangeMatchDirective with single range using both gte and lte"""

    match_directive = RangeMatchDirective(name="test_range")
    fields = ["price"]
    values_list = []
    values_map = {
        "gte": "match_params.min_price",
        "lte": "match_params.max_price",
    }
    match_params = {"min_price": 100, "max_price": 200}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "range": {
                        "price": {
                            "gte": 100,
                            "lte": 200,
                            "_name": "test_range",
                        }
                    }
                }
            ]
        }
    }


class TestRangeMatchDirectiveGteOnly(MatchDirectiveBaseTest):
    """Test cases for RangeMatchDirective with only gte value"""

    match_directive = RangeMatchDirective(name="test_range")
    fields = ["price"]
    values_list = []
    values_map = {"gte": "match_params.min_price"}
    match_params = {"min_price": 100}
    expected_query = {
        "bool": {
            "filter": [
                {"range": {"price": {"gte": 100, "_name": "test_range"}}}
            ]
        }
    }


class TestRangeMatchDirectiveNestedField(MatchDirectiveBaseTest):
    """Test cases for RangeMatchDirective with nested field"""

    match_directive = RangeMatchDirective(name="test_range")
    fields = [NestedField(nested_path="product", field_name="product.price")]
    values_list = []
    values_map = {
        "gte": "match_params.min_price",
        "lte": "match_params.max_price",
    }
    match_params = {"min_price": 100, "max_price": 200}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "nested": {
                        "path": "product",
                        "query": {
                            "range": {
                                "product.price": {
                                    "gte": 100,
                                    "lte": 200,
                                    "_name": "test_range",
                                }
                            }
                        },
                        "_name": "test_range",
                    }
                }
            ]
        }
    }


class TestRangeMatchDirectiveNullableValue(MatchDirectiveBaseTest):
    """Test cases for RangeMatchDirective with nullable values"""

    match_directive = RangeMatchDirective(
        name="test_range", nullable_value=True
    )
    fields = ["price"]
    values_list = []
    values_map = {
        "gte": "match_params.min_price",
        "lte": "match_params.max_price",
    }
    match_params = {"min_price": None, "max_price": None}
    expected_query = (
        None  # Expect None when all values are null and nullable_value is True
    )


# WaterfallFieldMatchDirective


class TestWaterfallFieldMatchDirectiveGT(MatchDirectiveBaseTest):
    """Test cases for WaterfallFieldMatchDirectiveTest with GT op and ANY match type"""

    match_directive = WaterfallFieldMatchDirective(
        rule=FieldMatchType.ANY,
        waterfall_order=["level1", "level2", "level3", "level4"],
        op=WaterFallMatchOp.GT,
        name="test_waterfall",
    )
    fields = ["field1"]
    values_list = ["match_params.value"]
    match_params = {
        "value": "level2",
    }
    expected_query = {
        "bool": {
            "filter": [
                {
                    "terms": {
                        "field1": ["level3", "level4"],
                        "_name": "test_waterfall",
                    }
                }
            ]
        }
    }


class TestWaterfallFieldMatchDirectiveLTE(MatchDirectiveBaseTest):
    """Test cases for WaterfallFieldMatchDirectiveTest with LTE op and ANY match type"""

    match_directive = WaterfallFieldMatchDirective(
        rule=FieldMatchType.ANY,
        waterfall_order=["level1", "level2", "level3", "level4"],
        op=WaterFallMatchOp.LTE,
        name="test_waterfall",
    )
    fields = ["field1"]
    values_list = ["match_params.value"]
    match_params = {
        "value": "level2",
    }
    expected_query = {
        "bool": {
            "filter": [
                {
                    "terms": {
                        "field1": ["level1", "level2"],
                        "_name": "test_waterfall",
                    }
                }
            ]
        }
    }


# ScriptMatchDirective
class TestScriptMatchDirectiveBasic(MatchDirectiveBaseTest):
    """Test cases for ScriptMatchDirective with basic script"""

    match_directive = ScriptMatchDirective(
        script="doc['field1'].value == params.test_value", name="test_script"
    )
    values_map = {"test_value": "match_params.value"}
    match_params = {"value": "test"}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "script": {
                        "script": {
                            "source": "doc['field1'].value == params.test_value",
                            "params": {"test_value": "test"},
                            "lang": "painless",
                        },
                        "_name": "test_script",
                    }
                }
            ]
        }
    }


class TestScriptMatchDirectiveMultipleParams(MatchDirectiveBaseTest):
    """Test cases for ScriptMatchDirective with multiple parameters"""

    match_directive = ScriptMatchDirective(
        script="doc['price'].value >= params.min && doc['price'].value <= params.max",
        name="test_script",
        mandatory_params_keys=["min", "max"],
    )
    fields = []
    values_list = []
    values_map = {
        "min": "match_params.min_price",
        "max": "match_params.max_price",
    }
    match_params = {"min_price": 100, "max_price": 200}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "script": {
                        "script": {
                            "source": "doc['price'].value >= params.min && doc['price'].value <= params.max",
                            "params": {"min": 100, "max": 200},
                            "lang": "painless",
                        },
                        "_name": "test_script",
                    }
                }
            ]
        }
    }


class TestScriptMatchDirectiveExcludeMode(MatchDirectiveBaseTest):
    """Test cases for ScriptMatchDirective with EXCLUDE mode"""

    match_directive = ScriptMatchDirective(
        script="doc['status'].value == params.status",
        mode=MatchMode.EXCLUDE,
        name="test_script",
    )
    values_map = {"status": "match_params.status"}
    match_params = {"status": "inactive"}
    expected_query = {
        "bool": {
            "must_not": [
                {
                    "script": {
                        "script": {
                            "source": "doc['status'].value == params.status",
                            "params": {"status": "inactive"},
                            "lang": "painless",
                        },
                        "_name": "test_script",
                    }
                }
            ]
        }
    }


class TestScriptMatchDirectiveComplexScript(MatchDirectiveBaseTest):
    """Test cases for ScriptMatchDirective with complex script logic"""

    match_directive = ScriptMatchDirective(
        script="""
            double distance = doc['location'].arcDistance(params.lat, params.lon);
            return distance <= params.radius && doc['category'].value == params.category;
        """,
        name="test_script",
        mandatory_params_keys=["lat", "lon", "radius", "category"],
    )
    fields = []
    values_list = []
    values_map = {
        "lat": "match_params.latitude",
        "lon": "match_params.longitude",
        "radius": "match_params.search_radius",
        "category": "match_params.item_category",
    }
    match_params = {
        "latitude": 40.7128,
        "longitude": -74.0060,
        "search_radius": 5000,
        "item_category": "electronics",
    }
    expected_query = {
        "bool": {
            "filter": [
                {
                    "script": {
                        "script": {
                            "source": """
            double distance = doc['location'].arcDistance(params.lat, params.lon);
            return distance <= params.radius && doc['category'].value == params.category;
        """,
                            "params": {
                                "lat": 40.7128,
                                "lon": -74.0060,
                                "radius": 5000,
                                "category": "electronics",
                            },
                            "lang": "painless",
                        },
                        "_name": "test_script",
                    }
                }
            ]
        }
    }


class TestScriptMatchDirectiveNullableValue(MatchDirectiveBaseTest):
    """Test cases for ScriptMatchDirective with nullable values"""

    match_directive = ScriptMatchDirective(
        script="doc['field1'].value == params.test_value",
        name="test_script",
        nullable_value=True,
        mandatory_params_keys=["test_value"],
    )
    fields = []
    values_list = []
    values_map = {"test_value": "match_params.value"}
    match_params = {"value": None}
    expected_query = None  # Expect None when mandatory param is null and nullable_value is True


class TestScriptMatchDirectiveMissingMandatoryParams(MatchDirectiveBaseTest):
    """Test cases for ScriptMatchDirective with missing mandatory parameters"""

    match_directive = ScriptMatchDirective(
        script="doc['field1'].value == params.test_value",
        name="test_script",
        mandatory_params_keys=["test_value", "other_value"],
    )
    fields = []
    values_list = []
    values_map = {"test_value": "match_params.value"}
    match_params = {"value": "test"}
    expected_query = None
    allowed_exc_cls = ValueError
    exc_message = "Missing mandatory script parameters"


class TestScriptMatchDirectiveEmptyParams(MatchDirectiveBaseTest):
    """Test cases for ScriptMatchDirective with no parameters"""

    match_directive = ScriptMatchDirective(
        script="doc['field1'].value > 0", name="test_script"
    )
    fields = []
    values_list = []
    values_map = {}
    match_params = {}
    expected_query = {
        "bool": {
            "filter": [
                {
                    "script": {
                        "script": {
                            "source": "doc['field1'].value > 0",
                            "lang": "painless",
                        },
                        "_name": "test_script",
                    }
                }
            ]
        }
    }


# FieldExistsDirective


class TestFieldExistsDirectiveSingleField(MatchDirectiveBaseTest):
    """Test cases for FieldExistsDirective with single field"""

    match_directive = FieldExistsDirective(
        rule=FieldMatchType.ANY, name="test_exists"
    )
    fields = ["normal_field"]
    expected_query = {
        "bool": {
            "filter": [
                {"exists": {"field": "normal_field", "_name": "test_exists"}}
            ]
        }
    }


class TestFieldExistsDirectiveMultiFieldAny(MatchDirectiveBaseTest):
    """Test cases for FieldExistsDirective with multiple fields and ANY match type"""

    match_directive = FieldExistsDirective(
        rule=FieldMatchType.ANY, name="test_exists"
    )
    fields = ["field1", "field2", "field3"]
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {
                                "exists": {
                                    "field": "field1",
                                    "_name": "test_exists",
                                }
                            },
                            {
                                "exists": {
                                    "field": "field2",
                                    "_name": "test_exists",
                                }
                            },
                            {
                                "exists": {
                                    "field": "field3",
                                    "_name": "test_exists",
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestFieldExistsDirectiveMultiFieldAll(MatchDirectiveBaseTest):
    """Test cases for FieldExistsDirective with multiple fields and ALL match type"""

    match_directive = FieldExistsDirective(
        rule=FieldMatchType.ALL, name="test_exists"
    )
    fields = ["field1", "field2", "field3"]
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "exists": {
                                    "field": "field1",
                                    "_name": "test_exists",
                                }
                            },
                            {
                                "exists": {
                                    "field": "field2",
                                    "_name": "test_exists",
                                }
                            },
                            {
                                "exists": {
                                    "field": "field3",
                                    "_name": "test_exists",
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestFieldExistsDirectiveSingleNestedField(MatchDirectiveBaseTest):
    """Test cases for FieldExistsDirective with single nested field"""

    match_directive = FieldExistsDirective(
        rule=FieldMatchType.ANY, name="test_exists"
    )
    fields = [
        NestedField(
            nested_path="nested_path", field_name="nested_path.nested_field"
        )
    ]
    expected_query = {
        "bool": {
            "filter": [
                {
                    "nested": {
                        "path": "nested_path",
                        "query": {
                            "exists": {
                                "field": "nested_path.nested_field",
                                "_name": "test_exists",
                            }
                        },
                    }
                }
            ]
        }
    }


class TestFieldExistsDirectiveMixedFieldsAny(MatchDirectiveBaseTest):
    """Test cases for FieldExistsDirective with mixed fields (normal and nested) and ANY match type"""

    match_directive = FieldExistsDirective(
        rule=FieldMatchType.ANY, name="test_exists"
    )
    fields = [
        "normal_field1",
        "normal_field2",
        NestedField(nested_path="path1", field_name="path1.nested1"),
        NestedField(nested_path="path2", field_name="path2.nested2"),
    ]
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "should": [
                            {
                                "exists": {
                                    "field": "normal_field1",
                                    "_name": "test_exists",
                                }
                            },
                            {
                                "exists": {
                                    "field": "normal_field2",
                                    "_name": "test_exists",
                                }
                            },
                            {
                                "nested": {
                                    "path": "path1",
                                    "query": {
                                        "exists": {
                                            "field": "path1.nested1",
                                            "_name": "test_exists",
                                        }
                                    },
                                }
                            },
                            {
                                "nested": {
                                    "path": "path2",
                                    "query": {
                                        "exists": {
                                            "field": "path2.nested2",
                                            "_name": "test_exists",
                                        }
                                    },
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestFieldExistsDirectiveMixedFieldsAll(MatchDirectiveBaseTest):
    """Test cases for FieldExistsDirective with mixed fields (normal and nested) and ALL match type"""

    match_directive = FieldExistsDirective(
        rule=FieldMatchType.ALL, name="test_exists"
    )
    fields = [
        "normal_field1",
        "normal_field2",
        NestedField(nested_path="path1", field_name="path1.nested1"),
        NestedField(nested_path="path2", field_name="path2.nested2"),
    ]
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "exists": {
                                    "field": "normal_field1",
                                    "_name": "test_exists",
                                }
                            },
                            {
                                "exists": {
                                    "field": "normal_field2",
                                    "_name": "test_exists",
                                }
                            },
                            {
                                "nested": {
                                    "path": "path1",
                                    "query": {
                                        "exists": {
                                            "field": "path1.nested1",
                                            "_name": "test_exists",
                                        }
                                    },
                                }
                            },
                            {
                                "nested": {
                                    "path": "path2",
                                    "query": {
                                        "exists": {
                                            "field": "path2.nested2",
                                            "_name": "test_exists",
                                        }
                                    },
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestFieldExistsDirectiveExcludeMode(MatchDirectiveBaseTest):
    """Test cases for FieldExistsDirective with EXCLUDE mode"""

    match_directive = FieldExistsDirective(
        rule=FieldMatchType.ANY, mode=MatchMode.EXCLUDE, name="test_exists"
    )
    fields = ["field1", "field2"]
    expected_query = {
        "bool": {
            "must_not": [
                {
                    "bool": {
                        "should": [
                            {
                                "exists": {
                                    "field": "field1",
                                    "_name": "test_exists",
                                }
                            },
                            {
                                "exists": {
                                    "field": "field2",
                                    "_name": "test_exists",
                                }
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestFieldExistsDirectiveNoFieldError(MatchDirectiveBaseTest):
    """Test cases for FieldExistsDirective with no fields and non-nullable"""

    match_directive = FieldExistsDirective(
        rule=FieldMatchType.ANY, name="test_exists"
    )
    fields = []
    expected_query = None
    allowed_exc_cls = ValueError
    exc_message = " directive requires: `field`. This must be set using `set_field` method."
