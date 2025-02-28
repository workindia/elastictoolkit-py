import json
import pytest
from typing import Any, Dict, List, Optional, Type, Union

from elasticquerydsl.base import DSLQuery

from elastictoolkit.queryutils.builder.custommatchdirective import (
    CustomMatchDirective,
)
from elastictoolkit.queryutils.builder.directiveengine import DirectiveEngine
from elastictoolkit.queryutils.builder.directivevaluemapper import (
    DirectiveValueMapper,
)
from elastictoolkit.queryutils.builder.functionscoreengine import (
    FunctionScoreEngine,
)
from elastictoolkit.queryutils.builder.matchdirective import MatchDirective
from elastictoolkit.queryutils.types import NestedField


class MatchDirectiveBaseTest:
    """
    Base test class for testing individual match directives.

    This class provides a framework for testing match directives by setting up
    the necessary fixtures and test methods. Subclasses should set the class
    attributes to define the test parameters.

    Class Attributes:
        match_directive (MatchDirective): The directive instance to test
        fields (List[Union[str, NestedField]]): List of fields to match against
        values_list (List[Any]): List of values to match
        values_map (Dict[str, Any]): Dictionary of field-value mappings
        match_params (Dict[str, Any]): Additional match parameters
        expected_query (Dict[str, Any]): Expected query output
    """

    match_directive: MatchDirective
    fields: List[Union[str, NestedField]] = []
    values_list: List[Any] = []
    values_map: Dict[str, Any] = {}
    match_params: Dict[str, Any] = {}
    value_mapper: DirectiveValueMapper = None
    expected_query: Dict[str, Any] = ...
    allowed_exc_cls: Exception = None
    exc_message: str = ""

    @pytest.fixture
    def _match_directive(self) -> MatchDirective:
        """
        Fixture that provides the match directive instance.

        Returns:
            MatchDirective: The configured match directive instance

        Raises:
            NotImplementedError: If match_directive is not set in subclass
        """
        if self.match_directive is None:
            raise NotImplementedError(
                "match_directive must be set in subclass"
            )
        return self.match_directive

    @pytest.fixture
    def _values_list(self) -> List[Any]:
        """
        Fixture that provides the list of values to match.

        Returns:
            List[Any]: List of values for matching
        """
        return self.values_list

    @pytest.fixture
    def _values_map(self) -> Dict[str, Any]:
        """
        Fixture that provides the mapping of field names to values.

        Returns:
            Dict[str, Any]: Dictionary of field-value mappings
        """
        return self.values_map

    @pytest.fixture
    def _fields(self) -> List[Union[str, NestedField]]:
        """
        Fixture that provides the list of fields to match against.

        Returns:
            List[Union[str, NestedField]]: List of field names or nested fields
        """
        return self.fields

    @pytest.fixture
    def _match_params(self) -> Dict[str, Any]:
        """
        Fixture that provides additional match parameters.

        Returns:
            Dict[str, Any]: Dictionary of match parameters
        """
        return self.match_params

    @pytest.fixture
    def _value_mapper(self) -> DirectiveValueMapper:
        return self.value_mapper

    @pytest.fixture
    def _expected_query(self) -> Dict[str, Any]:
        """
        Fixture that provides the expected query output.

        Returns:
            Dict[str, Any]: Expected query structure

        Raises:
            NotImplementedError: If expected_query is not set in subclass
        """
        if self.expected_query is ...:
            raise NotImplementedError("expected_query must be set in subclass")
        return self.expected_query

    @pytest.fixture
    def _allowed_exc_cls(self) -> Exception:
        return self.allowed_exc_cls

    @pytest.fixture
    def _exc_message(self) -> str:
        return self.exc_message

    def test_query_generation(
        self,
        _match_directive: MatchDirective,
        _fields: List[Union[str, NestedField]],
        _values_list: List[Any],
        _values_map: Dict[str, Any],
        _match_params: Dict[str, Any],
        _value_mapper: DirectiveValueMapper,
        _expected_query: Dict[str, Any],
        _allowed_exc_cls: Exception,
        _exc_message: str,
    ):
        """
        Test that the generated query matches the expected output.

        This method configures the match directive with the provided parameters
        and verifies that the generated query matches the expected structure.

        Args:
            match_directive (MatchDirective): The directive to test
            fields (List[Union[str, NestedField]]): Fields to match against
            values_list (List[Any]): Values to match
            values_map (Dict[str, Any]): Field-value mappings
            match_params (Dict[str, Any]): Additional match parameters
            expected_query (Dict[str, Any]): Expected query structure
        """
        _match_directive = _match_directive.copy()
        if _match_params:
            _match_directive.set_match_params(_match_params)
        if _fields:
            _match_directive.set_field(*_fields)
        if _values_list or _values_map:
            _match_directive.set_values(*_values_list, **_values_map)
        if _value_mapper and isinstance(
            _match_directive, CustomMatchDirective
        ):
            _match_directive.set_directive_value_mapper(_value_mapper)

        try:
            dsl = _match_directive.to_dsl(nullable=True)
            if self.allowed_exc_cls:
                pytest.fail(
                    f"Expected exception {self.allowed_exc_cls.__name__} was not raised"
                )
        except Exception as e:
            return self._validate_exc(e, _allowed_exc_cls, _exc_message)

        generated_query = None
        if dsl:
            generated_query = dsl.to_query()

        test_case_info = (
            type(self).__name__ + f" | Description: {self.__doc__}"
            if self.__doc__
            else ""
        )
        assert generated_query == _expected_query, (
            f"TestCase Failed: {test_case_info}\n"
            f"Generated query does not match expected query.\n"
            f"Generated: {json.dumps(generated_query)}\n"
            f"Expected: {json.dumps(_expected_query)}"
        )

    def _validate_exc(
        self,
        exc: Exception,
        allowed_exc_cls: Optional[Exception],
        exc_message: str,
    ):
        if not allowed_exc_cls:
            raise exc
        assert isinstance(exc, allowed_exc_cls), (
            "Raise exception did not match allowed_exc\n",
            f"Raised Exc: {type(exc).__name__}\n",
            f"Allowed Exc: {allowed_exc_cls.__name__}\n",
        )
        if exc_message:
            assert exc_message.lower() in str(exc).lower(), (
                f"Exception message did not match\n"
                f"Expected message: {exc_message}\n"
                f"Actual message: {str(exc)}"
            )


class DirectiveEngineBaseTest:
    """
    Base test class for testing DirectiveEngine implementations.

    This class provides a framework for testing directive engines with various
    match directives and configurations.

    Class Attributes:
        description (str): Test case description
        engine_cls (Type[DirectiveEngine]): DirectiveEngine class to test
        match_params (Dict[str, Any]): Input parameters for the query
        expected_query (Dict[str, Any]): Expected query output
        expected_exc_cls (Optional[Type[Exception]]): Expected exception class
        exc_message (str): Expected exception message
    """

    description: str = ""
    engine_cls: Type[DirectiveEngine] = None
    match_params: Dict[str, Any] = {}
    expected_query: Dict[str, Any] = None
    expected_exc_cls: Optional[Type[Exception]] = None
    exc_message: str = ""

    @pytest.fixture
    def _engine_cls(self) -> Type[DirectiveEngine]:
        return self.engine_cls

    @pytest.fixture
    def _match_params(self) -> Dict[str, Any]:
        return self.match_params

    @pytest.fixture
    def _expected_query(self) -> Dict[str, Any]:
        return self.expected_query

    @pytest.fixture
    def _expected_exc_cls(self) -> Optional[Type[Exception]]:
        return self.expected_exc_cls

    @pytest.fixture
    def _exc_message(self) -> str:
        return self.exc_message

    def test_directive_engine(
        self,
        _engine_cls: Type[DirectiveEngine],
        _match_params: Dict[str, Any],
        _expected_query: Dict[str, Any],
        _expected_exc_cls: Optional[Type[Exception]],
        _exc_message: str,
    ):
        """Test the directive engine query generation"""
        try:
            dsl = _engine_cls().set_match_params(_match_params).to_dsl()
            generated_query = dsl.to_query() if dsl else None

            if _expected_exc_cls:
                pytest.fail(
                    f"Expected exception {_expected_exc_cls.__name__} was not raised"
                )

            test_case_info = (
                type(self).__name__ + f" | Description: {self.__doc__}"
                if self.__doc__
                else ""
            )
            assert generated_query == _expected_query, (
                f"Test case failed: {test_case_info}\n"
                f"Generated query does not match expected query.\n"
                f"Generated: {generated_query}\n"
                f"Expected: {_expected_query}"
            )

        except Exception as exc:
            return self._validate_exc(exc, _expected_exc_cls, _exc_message)

    def _validate_exc(
        self,
        exc: Exception,
        allowed_exc_cls: Optional[Exception],
        exc_message: str,
    ):
        if not allowed_exc_cls:
            raise exc
        assert isinstance(exc, allowed_exc_cls), (
            "Raise exception did not match allowed_exc\n",
            f"Raised Exc: {type(exc).__name__}\n",
            f"Allowed Exc: {allowed_exc_cls.__name__}\n",
        )
        if exc_message:
            assert exc_message.lower() in str(exc).lower(), (
                f"Exception message did not match\n"
                f"Expected message: {exc_message}\n"
                f"Actual message: {str(exc)}"
            )


class FunctionScoreEngineBaseTest(DirectiveEngineBaseTest):
    """
    Base test class for testing FunctionScoreEngine implementations.

    This class extends DirectiveEngineBaseTest to provide a framework for testing
    function score engines with various score functions and configurations.

    Class Attributes:
        description (str): Test case description
        engine_cls (Type[FunctionScoreEngine]): FunctionScoreEngine class to test
        match_params (Dict[str, Any]): Input parameters for the query
        match_query (Dict[str, Any]): Base query to be scored
        expected_query (Dict[str, Any]): Expected query output
        expected_exc_cls (Optional[Type[Exception]]): Expected exception class
        exc_message (str): Expected exception message
    """

    match_query: Dict[str, Any] = None

    @pytest.fixture
    def _match_query(self) -> Dict[str, Any]:
        return self.match_query

    def test_directive_engine(
        self,
        _engine_cls: Type[FunctionScoreEngine],
        _match_params: Dict[str, Any],
        _match_query: Dict[str, Any],
        _expected_query: Dict[str, Any],
        _expected_exc_cls: Optional[Type[Exception]],
        _exc_message: str,
    ):
        """Test the function score engine query generation"""
        try:
            # Create a mock DSLQuery for the match query
            class MockDSLQuery(DSLQuery):
                def to_query(self):
                    return _match_query

            mock_dsl = MockDSLQuery(boost=None, _name=None)

            # Set up and execute the function score engine
            dsl = (
                _engine_cls()
                .set_match_params(_match_params)
                .set_match_dsl(mock_dsl)
                .to_dsl()
            )
            generated_query = dsl.to_query() if dsl else None

            if _expected_exc_cls:
                pytest.fail(
                    f"Expected exception {_expected_exc_cls.__name__} was not raised"
                )

            test_case_info = (
                type(self).__name__ + f" | Description: {self.__doc__}"
                if self.__doc__
                else ""
            )
            assert generated_query == _expected_query, (
                f"Test case failed: {test_case_info}\n"
                f"Generated query does not match expected query.\n"
                f"Generated: {generated_query}\n"
                f"Expected: {_expected_query}"
            )

        except Exception as exc:
            return self._validate_exc(exc, _expected_exc_cls, _exc_message)
