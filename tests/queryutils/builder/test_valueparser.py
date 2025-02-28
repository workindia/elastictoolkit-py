import pytest
from elastictoolkit.queryutils.builder.helpers.valueparser import (
    RuntimeValueParser,
    ValueParser,
)
from elastictoolkit.queryutils.builder.helpers.valuetransformer import (
    ValueTransformer,
)


class TestRuntimeValueParser:
    @pytest.fixture
    def sample_data(self):
        return {
            "user": {
                "name": "John",
                "age": 30,
                "addresses": ["addr1", "addr2"],
                "contacts": {"email": "john@example.com", "phone": None},
            },
            "settings": {"enabled": True, "values": [1, 2, 3]},
            "user_name": "JohnDoe",
        }

    @pytest.fixture
    def parser(self, sample_data):
        return RuntimeValueParser(data=sample_data, prefix="data")

    def test_parse_simple_string(self, parser):
        """Test parsing of non-prefixed string"""
        assert parser.parse("simple string") == "simple string"

    def test_parse_prefixed_string(self, parser):
        """Test parsing of prefixed string path"""
        assert parser.parse("data.user.name") == "John"
        assert parser.parse("data.user.age") == 30

    def test_parse_nested_path(self, parser):
        """Test parsing of deeply nested paths"""
        assert parser.parse("data.user.contacts.email") == "john@example.com"
        assert parser.parse("data.settings.enabled") is True

    def test_parse_nonexistent_path(self, parser):
        """Test parsing of non-existent paths"""
        assert parser.parse("data.user.nonexistent") is None
        assert parser.parse("data.nonexistent.field") is None

    def test_parse_callable(self, parser, sample_data):
        """Test parsing of callable values"""

        def get_name(data):
            return data["user"]["name"]

        assert parser.parse(get_name) == "John"

    def test_parse_dict(self, parser):
        """Test parsing of dictionary values"""
        test_dict = {
            "name": "data.user.name",
            "email": "data.user.contacts.email",
            "static": "static_value",
        }
        expected = {
            "name": "John",
            "email": "john@example.com",
            "static": "static_value",
        }
        assert parser.parse(test_dict) == expected

    def test_parse_list(self, parser):
        """Test parsing of list values"""
        test_list = ["data.user.name", "static", "data.settings.enabled"]
        expected = ["John", "static", True]
        assert parser.parse(test_list) == expected

    def test_parse_tuple(self, parser):
        """Test parsing of tuple values"""
        test_tuple = ("data.user.name", "static")
        expected = ["John", "static"]
        assert parser.parse(test_tuple) == expected

    def test_parse_with_unpacking(self, parser):
        """Test parsing with unpacking operator"""
        test_list = ["*data.user.addresses", "static"]
        expected = ["addr1", "addr2", "static"]
        assert parser.parse(test_list) == expected

    def test_parse_with_callable_unpacking(self, parser, sample_data):
        """Test parsing with callable unpacking"""

        def get_addresses(data):
            return data["user"]["addresses"]

        get_addresses.unpack = True

        test_list = [get_addresses, "static"]
        expected = ["addr1", "addr2", "static"]
        assert parser.parse(test_list) == expected

    def test_parse_none_values(self, parser):
        """Test parsing of None values"""
        assert parser.parse("data.user.contacts.phone") is None
        assert parser.parse(None) is None

    def test_parse_non_dict_path(self, parser):
        """Test parsing when path traversal hits non-dict value"""
        assert parser.parse("data.user.name.nonexistent") is None

    def test_parse_primitive_values(self, parser):
        """Test parsing of primitive values"""
        assert parser.parse(42) == 42
        assert parser.parse(True) is True
        assert parser.parse(3.14) == 3.14

    def test_unpack_non_iterable(self, parser):
        """Test unpacking of non-iterable values"""
        test_list = ["*data.user.name"]
        expected = ["John"]
        assert parser.parse(test_list) == expected

    def test_parse_empty_collections(self, parser):
        """Test parsing of empty collections"""
        assert parser.parse([]) == []
        assert parser.parse({}) == {}

    def test_parse_nested_collections(self, parser):
        """Test parsing of nested collections"""
        test_data = {
            "list": ["data.user.name", ["data.settings.enabled"]],
            "dict": {"nested": {"value": "data.user.age"}},
        }
        expected = {
            "list": ["John", [True]],
            "dict": {"nested": {"value": 30}},
        }
        assert parser.parse(test_data) == expected

    def test_parse_with_filter_none(self, parser):
        """Test parsing list with None values filtered out"""
        test_list = ["data.user.name", "data.nonexistent", "static"]
        expected = ["John", "static"]
        assert parser.parse(test_list) == expected

    def test_abstract_value_parser(self):
        """Test that ValueParser cannot be instantiated"""
        with pytest.raises(TypeError):
            ValueParser({})

    def test_parse_string_with_asterisk_only(self, parser):
        """Test parsing string with only asterisk"""
        assert parser.parse("*") == "*"

    def test_unpack_string_value(self, parser):
        """Test unpacking string value (should return as single-item list)"""
        result = parser._unpack_value("test")
        assert result == ["test"]

    def test_unpack_list_with_asterisk(self, parser):
        """Test unpacking list with asterisk"""
        result = parser.parse(["*"])
        assert result == ["*"]

    def test_callable(self, parser):
        """Test parsing of callable values"""

        def get_name(data):
            return data["user"]["name"]

        assert parser.parse(get_name) == "John"
        assert (
            parser.parse(ValueTransformer.normalize_str("user_name"))
            == "johndoe"
        )

    def test_callable_with_unpack(self, parser):
        """Test parsing of callable values with unpacking"""

        def get_addresses(data):
            return data["user"]["addresses"]

        assert parser.parse(
            ["data.user.name", ValueTransformer.unpacked(get_addresses)]
        ) == ["John", "addr1", "addr2"]
