from elastictoolkit.queryutils.builder.custommatchdirective import (
    CustomMatchDirective,
)
from elastictoolkit.queryutils.builder.booldirective import (
    AndDirective,
    OrDirective,
)
from elastictoolkit.queryutils.builder.directivevaluemapper import (
    DirectiveValueMapper,
)
from elastictoolkit.queryutils.builder.matchdirective import (
    ConstMatchDirective,
)
from elastictoolkit.queryutils.consts import FieldMatchType
from elastictoolkit.queryutils.types import FieldValue, NestedField
from tests.queryutils.builder.basetestcase import MatchDirectiveBaseTest


class ValueMapper(DirectiveValueMapper):
    experience = FieldValue(
        fields=["experience"], values_list=["*match_params.experience"]
    )
    qualification = FieldValue(
        fields=["qualification"], values_list=["*match_params.qualification"]
    )
    skills = FieldValue(
        fields=["skills"], values_list=["*match_params.skills"]
    )
    nested_field_match = FieldValue(
        fields=[
            NestedField(
                field_name="nested_path.nested_field",
                nested_path="nested_path",
            )
        ],
        values_list=["*match_params.nested_value"],
    )


class TestCustomMatchDirectiveBasic(MatchDirectiveBaseTest):
    """Test basic CustomMatchDirective with single directive"""

    class CustomDirectiveTest(CustomMatchDirective):
        allowed_engine_cls_name = "TestEngine"

        def get_directive(self):
            return OrDirective(
                experience=ConstMatchDirective(
                    FieldMatchType.ANY, name="experience"
                ),
                qualification=ConstMatchDirective(
                    FieldMatchType.ANY, name="qualification"
                ),
            )

    match_directive = CustomDirectiveTest()
    fields = ["experience"]
    values_list = ["match_params.experience"]
    match_params = {
        "experience": ["junior", "middle"],
        "qualification": ["master", "doctor"],
    }
    value_mapper = ValueMapper()
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
                                                "experience": [
                                                    "junior",
                                                    "middle",
                                                ],
                                                "_name": "experience",
                                            }
                                        }
                                    ]
                                }
                            },
                            {
                                "bool": {
                                    "filter": [
                                        {
                                            "terms": {
                                                "qualification": [
                                                    "master",
                                                    "doctor",
                                                ],
                                                "_name": "qualification",
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


class TestCustomMatchDirectiveComplex(MatchDirectiveBaseTest):
    """Test CustomMatchDirective with nested AND/OR directives"""

    class CustomDirectiveTest(CustomMatchDirective):
        allowed_engine_cls_name = "TestEngine"

        def get_directive(self):
            return AndDirective(
                OrDirective(
                    experience=ConstMatchDirective(
                        FieldMatchType.ANY,
                    ),
                    skills=ConstMatchDirective(
                        FieldMatchType.ALL,
                    ),
                ),
                qualification=ConstMatchDirective(
                    FieldMatchType.ANY,
                ),
            )

    match_directive = CustomDirectiveTest()
    match_params = {
        "experience": ["senior"],
        "skills": ["python", "elasticsearch"],
        "qualification": ["master", "doctor"],
    }
    value_mapper = ValueMapper()
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "bool": {
                                    "should": [
                                        {
                                            "bool": {
                                                "filter": [
                                                    {
                                                        "term": {
                                                            "experience": {
                                                                "value": "senior"
                                                            }
                                                        }
                                                    }
                                                ]
                                            }
                                        },
                                        {
                                            "bool": {
                                                "filter": [
                                                    {
                                                        "bool": {
                                                            "filter": [
                                                                {
                                                                    "term": {
                                                                        "skills": {
                                                                            "value": "python"
                                                                        }
                                                                    }
                                                                },
                                                                {
                                                                    "term": {
                                                                        "skills": {
                                                                            "value": "elasticsearch"
                                                                        }
                                                                    }
                                                                },
                                                            ]
                                                        }
                                                    }
                                                ]
                                            }
                                        },
                                    ]
                                }
                            },
                            {
                                "bool": {
                                    "filter": [
                                        {
                                            "terms": {
                                                "qualification": [
                                                    "master",
                                                    "doctor",
                                                ]
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


class TestCustomMatchDirectiveNullableValues(MatchDirectiveBaseTest):
    """Test CustomMatchDirective with nullable values"""

    class CustomDirectiveTest(CustomMatchDirective):
        allowed_engine_cls_name = "TestEngine"

        def get_directive(self):
            if (
                self.match_params.get("experience") is None
                and self.match_params.get("skills") is None
            ):
                return None
            return AndDirective(
                experience=ConstMatchDirective(
                    FieldMatchType.ANY, nullable_value=True
                ),
                skills=ConstMatchDirective(
                    FieldMatchType.ALL, nullable_value=True
                ),
            )

    match_directive = CustomDirectiveTest()
    match_params = {"experience": None, "skills": None}
    value_mapper = ValueMapper()
    expected_query = None  # Expect None when all values are null and nullable


class TestCustomMatchDirectiveNestedFields(MatchDirectiveBaseTest):
    """Test CustomMatchDirective with nested fields"""

    class CustomDirectiveTest(CustomMatchDirective):
        allowed_engine_cls_name = "TestEngine"

        def get_directive(self):
            return AndDirective(
                experience=ConstMatchDirective(FieldMatchType.ANY),
                nested_field_match=ConstMatchDirective(FieldMatchType.ALL),
            )

    match_directive = CustomDirectiveTest()
    match_params = {
        "experience": ["senior"],
        "nested_value": ["value1", "value2"],
    }
    value_mapper = ValueMapper()
    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "bool": {
                                    "filter": [
                                        {
                                            "term": {
                                                "experience": {
                                                    "value": "senior"
                                                }
                                            }
                                        }
                                    ]
                                }
                            },
                            {
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
                                                                        "value": "value1"
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
                                                                        "value": "value2"
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
                            },
                        ]
                    }
                }
            ]
        }
    }


class TestAllowedEngineMissingName(MatchDirectiveBaseTest):
    """Test CustomMatchDirective with allowed engine missing name"""

    allowed_exc_cls = NotImplementedError
    exc_message = "must define its own `allowed_engine_cls_name`"

    def test_query_generation(
        self, _allowed_exc_cls: Exception, _exc_message: str
    ):
        try:
            # This definition will raise an error
            class CustomDirectiveTest(CustomMatchDirective):
                def get_directive(self):
                    return None

        except Exception as e:
            return self._validate_exc(e, _allowed_exc_cls, _exc_message)


class TestCustomMatchDirectiveStaticName(MatchDirectiveBaseTest):
    """Test static name for CustomMatchDirective"""

    class CustomDirectiveWithStaticName(CustomMatchDirective):
        allowed_engine_cls_name = "TestEngine"
        name = "static_name"

        def get_directive(self):
            return AndDirective(
                experience=ConstMatchDirective(FieldMatchType.ANY),
            )

    match_directive = CustomDirectiveWithStaticName()
    match_params = {
        "experience": ["senior"],
        "skills": ["python", "elasticsearch"],
    }
    value_mapper = ValueMapper()

    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "bool": {
                                    "filter": [
                                        {
                                            "term": {
                                                "experience": {
                                                    "value": "senior"
                                                }
                                            }
                                        }
                                    ]
                                }
                            }
                        ],
                        "_name": "static_name",
                    }
                }
            ]
        }
    }


class TestCustomMatchDirectiveConstructorName(MatchDirectiveBaseTest):
    """Test constructor name for CustomMatchDirective"""

    class CustomDirectiveWithConstructorName(CustomMatchDirective):
        allowed_engine_cls_name = "TestEngine"

        def get_directive(self):
            return AndDirective(
                experience=ConstMatchDirective(FieldMatchType.ANY),
            )

    match_directive = CustomDirectiveWithConstructorName(
        name="constructor_name"
    )
    match_params = {
        "experience": ["senior"],
    }
    value_mapper = ValueMapper()

    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "bool": {
                                    "filter": [
                                        {
                                            "term": {
                                                "experience": {
                                                    "value": "senior"
                                                }
                                            }
                                        }
                                    ]
                                }
                            }
                        ],
                        "_name": "constructor_name",
                    }
                }
            ]
        }
    }


class TestCustomMatchDirectiveOverrideName(MatchDirectiveBaseTest):
    """Test override name for CustomMatchDirective"""

    class CustomDirectiveWithOverriddenName(CustomMatchDirective):
        allowed_engine_cls_name = "TestEngine"

        def get_name(self):
            return f"overridden_name_{self._match_params.get('experience')[0]}"

        def get_directive(self):
            return AndDirective(
                experience=ConstMatchDirective(FieldMatchType.ANY),
            )

    match_directive = CustomDirectiveWithOverriddenName()
    match_params = {
        "experience": ["senior"],
    }
    value_mapper = ValueMapper()

    expected_query = {
        "bool": {
            "filter": [
                {
                    "bool": {
                        "filter": [
                            {
                                "bool": {
                                    "filter": [
                                        {
                                            "term": {
                                                "experience": {
                                                    "value": "senior"
                                                }
                                            }
                                        }
                                    ]
                                }
                            }
                        ],
                        "_name": "overridden_name_senior",
                    }
                }
            ]
        }
    }
