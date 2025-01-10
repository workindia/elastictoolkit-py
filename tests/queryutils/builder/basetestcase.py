import json
import pytest
from typing import Any, Dict, List, Type, Union

from elastictoolkit.queryutils.builder.directiveengine import DirectiveEngine
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

    description: str = ""
    match_directive: MatchDirective
    fields: List[Union[str, NestedField]] = []
    values_list: List[Any] = []
    values_map: Dict[str, Any] = {}
    match_params: Dict[str, Any] = {}
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

    def test_query_generation(
        self,
        _match_directive: MatchDirective,
        _fields: List[Union[str, NestedField]],
        _values_list: List[Any],
        _values_map: Dict[str, Any],
        _match_params: Dict[str, Any],
        _expected_query: Dict[str, Any],
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
        if _match_params:
            _match_directive.set_match_params(_match_params)
        if _fields:
            _match_directive.set_field(*_fields)
        if _values_list or _values_map:
            _match_directive.set_values(*_values_list, **_values_map)

        try:
            dsl = _match_directive.to_dsl(nullable=True)
        except Exception as e:
            return self.validate_exc(e)

        generated_query = None
        if dsl:
            generated_query = dsl.to_query()
        # TODO: Remove
        print(type(self).__name__, ":", generated_query)

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

    def validate_exc(self, exc: Exception):
        if not self.allowed_exc_cls:
            raise exc
        assert isinstance(exc, self.allowed_exc_cls), (
            "Raise exception did not match allowed_exc\n",
            f"Raised Exc: {type(exc).__name__}\n",
            f"Allowed Exc: {self.allowed_exc_cls.__name__}\n",
        )
        if self.exc_message:
            assert self.exc_message.lower() in str(exc).lower(), (
                f"Exception message did not match\n"
                f"Expected message: {self.exc_message}\n"
                f"Actual message: {str(exc)}"
            )


class QueryBuilderBaseTest:
    """
    Base test class for testing complete query builders.

    This class provides a framework for testing directive engines that combine
    multiple match directives. Subclasses should implement the required fixtures.
    """

    @pytest.fixture
    def directive_engine(self) -> Type[DirectiveEngine]:
        """
        Fixture that provides the DirectiveEngine class to test.

        Returns:
            Type[DirectiveEngine]: The engine class to test

        Raises:
            NotImplementedError: If not implemented in subclass
        """
        raise NotImplementedError(
            "directive_engine fixture must be implemented in subclass"
        )

    @pytest.fixture
    def match_params(self) -> Dict[str, Any]:
        """
        Fixture that provides match parameters for the test.

        Returns:
            Dict[str, Any]: Dictionary of match parameters

        Raises:
            NotImplementedError: If not implemented in subclass
        """
        raise NotImplementedError(
            "match_params fixture must be implemented in subclass"
        )

    @pytest.fixture
    def expected_query(self) -> Dict[str, Any]:
        """
        Fixture that provides the expected query output.

        Returns:
            Dict[str, Any]: Expected query structure

        Raises:
            NotImplementedError: If not implemented in subclass
        """
        raise NotImplementedError(
            "expected_query fixture must be implemented in subclass"
        )

    def test_query_generation(
        self,
        directive_engine: Type[DirectiveEngine],
        match_params: Dict,
        expected_query: Dict,
    ):
        """
        Test that the generated query matches the expected output.

        Args:
            directive_engine (Type[DirectiveEngine]): The engine class to test
            match_params (Dict): Input parameters for the query
            expected_query (Dict): Expected query structure
        """
        dsl = directive_engine().set_match_params(match_params).to_dsl()
        generated_query = dsl.to_query()

        assert generated_query == expected_query, (
            f"Generated query does not match expected query.\n"
            f"Generated: {json.dumps(generated_query, indent=2)}\n"
            f"Expected: {json.dumps(expected_query, indent=2)}"
        )
