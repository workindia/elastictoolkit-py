from elastictoolkit.queryutils.builder.booldirective import AndDirective
from elastictoolkit.queryutils.builder.custommatchdirective import (
    CustomMatchDirective,
)
from elastictoolkit.queryutils.builder.customscorefunctiondirective import (
    CustomScoreFunctionDirective,
)
from elastictoolkit.queryutils.builder.functionscoreengine import (
    FunctionScoreEngine,
)
from elastictoolkit.queryutils.builder.scorefunctiondirective import (
    ScriptScoreDirective,
)
from elastictoolkit.queryutils.builder.matchdirective import (
    ConstMatchDirective,
)
from elastictoolkit.queryutils.consts import (
    FieldMatchType,
    ScoreNullFilterAction,
)
from elastictoolkit.queryutils.types import FieldValue
from tests.queryutils.builder.basetestcase import FunctionScoreEngineBaseTest
from elastictoolkit.queryutils.builder.directivevaluemapper import (
    DirectiveValueMapper,
)


class ValueMapper(DirectiveValueMapper):
    test_score = FieldValue(fields=["field"], values_list=["test"])
    experience = FieldValue(fields=["experience"], values_list=["senior"])
    skills = FieldValue(
        fields=["skills"], values_list=["python", "elasticsearch"]
    )


class TestBasicScriptScore(FunctionScoreEngineBaseTest):
    """Test basic script score function with a filter"""

    class ScoreTestEngine(FunctionScoreEngine):
        test_score = ScriptScoreDirective(
            script="doc['field'].value * params.multiplier",
            filter=ConstMatchDirective(FieldMatchType.ANY),
            weight=2.0,
        ).set_script_params(multiplier=2)

        class Config:
            score_mode = "multiply"
            boost_mode = "replace"
            value_mapper = ValueMapper()

    engine_cls = ScoreTestEngine
    match_params = {}
    match_query = {"term": {"field": "test"}}
    expected_query = {
        "function_score": {
            "query": {"term": {"field": "test"}},
            "functions": [
                {
                    "filter": {
                        "bool": {
                            "filter": [{"term": {"field": {"value": "test"}}}]
                        }
                    },
                    "weight": 2.0,
                    "script_score": {
                        "script": {
                            "source": "doc['field'].value * params.multiplier",
                            "params": {"multiplier": 2},
                        }
                    },
                }
            ],
            "score_mode": "multiply",
            "boost_mode": "replace",
        }
    }


class TestMissingMatchDSL(FunctionScoreEngineBaseTest):
    """Test case when match_dsl is not set"""

    class ScoreTestEngine(FunctionScoreEngine):
        test_score = ScriptScoreDirective(
            script="doc['field'].value",
            weight=1.0,
        )

        class Config:
            value_mapper = None

    engine_cls = ScoreTestEngine
    match_params = {}
    expected_query = {
        "function_score": {
            "query": None,
            "functions": [
                {
                    "script_score": {
                        "script": {"source": "doc['field'].value"}
                    },
                    "weight": 1.0,
                }
            ],
        }
    }


class TestEmptyScoreFunctions(FunctionScoreEngineBaseTest):
    """Test when no score functions are generated"""

    class ScoreTestEngine(FunctionScoreEngine):
        class Config:
            score_mode = "sum"
            value_mapper = None

    engine_cls = ScoreTestEngine
    match_params = {}
    match_query = {"match_all": {}}
    expected_query = {
        "function_score": {
            "query": {"match_all": {}},
            "score_mode": "sum",
        }
    }


class TestComplexConfiguration(FunctionScoreEngineBaseTest):
    """Test function score with multiple configuration options"""

    class ScoreTestEngine(FunctionScoreEngine):
        score1 = ScriptScoreDirective(
            script="doc['field1'].value",
            weight=1.5,
        )
        score2 = ScriptScoreDirective(
            script="doc['field2'].value",
            weight=2.0,
        )

        class Config:
            score_mode = "max"
            boost_mode = "sum"
            max_boost = 3.0
            min_score = 0.5
            value_mapper = ValueMapper()

    engine_cls = ScoreTestEngine
    match_params = {}
    match_query = {"match_all": {}}
    expected_query = {
        "function_score": {
            "query": {"match_all": {}},
            "functions": [
                {
                    "weight": 1.5,
                    "script_score": {
                        "script": {"source": "doc['field1'].value"}
                    },
                },
                {
                    "weight": 2.0,
                    "script_score": {
                        "script": {"source": "doc['field2'].value"}
                    },
                },
            ],
            "score_mode": "max",
            "boost_mode": "sum",
            "max_boost": 3.0,
            "min_score": 0.5,
        }
    }


class CustomScore(CustomScoreFunctionDirective):
    allowed_engine_cls_name = "ScoreTestEngine"

    def get_score_directive(self):
        if not self.match_params.get("enable_score"):
            return None
        return ScriptScoreDirective(
            script="doc['field'].value",
            weight=self._weight,
            filter=ConstMatchDirective(FieldMatchType.ANY),
        )


class TestCustomScoreFunctionDirective(FunctionScoreEngineBaseTest):
    """Test cases for CustomScoreFunctionDirective"""

    class ScoreTestEngine(FunctionScoreEngine):
        test_score = CustomScore(weight=2.0)

        class Config:
            value_mapper = ValueMapper()

    engine_cls = ScoreTestEngine
    match_params = {"enable_score": True}
    match_query = {"term": {"field": "test"}}
    expected_query = {
        "function_score": {
            "query": {"term": {"field": "test"}},
            "functions": [
                {
                    "filter": {
                        "bool": {
                            "filter": [{"term": {"field": {"value": "test"}}}]
                        }
                    },
                    "weight": 2.0,
                    "script_score": {
                        "script": {"source": "doc['field'].value"}
                    },
                }
            ],
        }
    }

    def test_missing_allowed_engine_cls(self):
        """Test that subclass must define allowed_engine_cls_name"""
        try:

            class InvalidCustomScore(CustomScoreFunctionDirective):
                # Intentionally missing allowed_engine_cls_name

                def get_score_directive(self):
                    return None

        except Exception as exc:
            self._validate_exc(
                exc,
                NotImplementedError,
                "must define its own `allowed_engine_cls_name`",
            )

    def test_unresolved_filter_dsl(self):
        """Test accessing filter_dsl before it's resolved"""
        directive = CustomScore(weight=1.0)
        try:
            _ = directive.filter_dsl
        except Exception as exc:
            self._validate_exc(exc, ValueError, "must be resolved externally")

    def test_copy_directive(self):
        """Test copying the directive"""
        original = CustomScore(weight=2.0)
        copied = original.copy()
        assert copied._weight == 2.0
        assert original is not copied


class TestInvalidEngineName(FunctionScoreEngineBaseTest):
    """Test when the engine class name is not allowed"""

    class InvalidEngine(FunctionScoreEngine):
        test_score = CustomScore(weight=2.0)

        class Config:
            value_mapper = ValueMapper()

    engine_cls = InvalidEngine
    match_params = {"enable_score": True}
    match_query = {"term": {"field": "test"}}
    expected_exc_cls = ValueError
    exc_message = "is not allowed for"


class TestNoScoreDirective(FunctionScoreEngineBaseTest):
    class ScoreTestEngine(FunctionScoreEngine):
        test_score = CustomScore(weight=2.0)

        class Config:
            value_mapper = ValueMapper()

    engine_cls = ScoreTestEngine
    match_params = {"enable_score": False}
    expected_query = {"function_score": {"query": None}}


class CustomMatchTest(CustomMatchDirective):
    allowed_engine_cls_name = "ScoreTestEngine"

    def get_directive(self):
        if not self.match_params.get("enable_filter"):
            return None
        return AndDirective(
            experience=ConstMatchDirective(FieldMatchType.ANY),
            skills=ConstMatchDirective(FieldMatchType.ALL),
        )


class CustomScoreWithMatch(CustomScoreFunctionDirective):
    allowed_engine_cls_name = "ScoreTestEngine"

    def get_score_directive(self):
        if not self.match_params.get("enable_score"):
            return None
        return ScriptScoreDirective(
            script="doc['field'].value",
            weight=self._weight,
            filter=CustomMatchTest(),
            null_filter_action=self._null_filter_action,
        )


class TestCustomScoreWithCustomMatch(FunctionScoreEngineBaseTest):
    """Test CustomScoreFunctionDirective with CustomMatchDirective as filter"""

    class ScoreTestEngine(FunctionScoreEngine):
        test_score = CustomScoreWithMatch(weight=2.0)

        class Config:
            value_mapper = ValueMapper()

    engine_cls = ScoreTestEngine
    match_params = {
        "enable_score": True,
        "enable_filter": True,
        "experience": ["senior"],
        "skills": ["python", "elasticsearch"],
    }
    expected_query = {
        "function_score": {
            "query": None,
            "functions": [
                {
                    "filter": {
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
                                }
                            ]
                        }
                    },
                    "weight": 2.0,
                    "script_score": {
                        "script": {"source": "doc['field'].value"}
                    },
                }
            ],
        }
    }


class TestCustomScoreWithCustomMatchDisabled(FunctionScoreEngineBaseTest):
    """Test when custom match directive is disabled"""

    class ScoreTestEngine(FunctionScoreEngine):
        test_score = CustomScoreWithMatch(
            weight=2.0, null_filter_action=ScoreNullFilterAction.ALLOW
        )

        class Config:
            value_mapper = ValueMapper()

    engine_cls = ScoreTestEngine
    match_params = {"enable_score": True, "enable_filter": False}
    expected_query = {
        "function_score": {
            "query": None,
            "functions": [
                {
                    "weight": 2.0,
                    "script_score": {
                        "script": {"source": "doc['field'].value"}
                    },
                }
            ],
        }
    }


class TestCustomScoreDisabled(FunctionScoreEngineBaseTest):
    """Test when score directive is disabled"""

    class ScoreTestEngine(FunctionScoreEngine):
        test_score = CustomScoreWithMatch(
            weight=2.0, null_filter_action=ScoreNullFilterAction.ALLOW
        )

        class Config:
            value_mapper = ValueMapper()

    engine_cls = ScoreTestEngine
    match_params = {"enable_score": False, "enable_filter": True}
    expected_query = {"function_score": {"query": None}}


class TestCustomScoreNullFilterDisabled(FunctionScoreEngineBaseTest):
    """Test when null filter action is DISABLE_FUNCTION"""

    class ScoreTestEngine(FunctionScoreEngine):
        test_score = CustomScoreWithMatch(
            weight=2.0,
            null_filter_action=ScoreNullFilterAction.DISABLE_FUNCTION,
        )

        class Config:
            value_mapper = ValueMapper()

    engine_cls = ScoreTestEngine
    match_params = {"enable_score": True, "enable_filter": False}
    expected_query = {"function_score": {"query": None}}
