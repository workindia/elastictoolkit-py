from elastictoolkit.queryutils.builder.functionscoreengine import (
    FunctionScoreEngine,
)
from elastictoolkit.queryutils.builder.scorefunctiondirective import (
    ScriptScoreDirective,
)
from elastictoolkit.queryutils.builder.matchdirective import (
    ConstMatchDirective,
)
from elastictoolkit.queryutils.consts import FieldMatchType
from elastictoolkit.queryutils.types import FieldValue
from tests.queryutils.builder.basetestcase import FunctionScoreEngineBaseTest
from elastictoolkit.queryutils.builder.directivevaluemapper import (
    DirectiveValueMapper,
)


class ValueMapper(DirectiveValueMapper):
    test_score = FieldValue(fields=["field"], values_list=["test"])


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
