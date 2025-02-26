from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Union, Iterable


class ValueParser(ABC):
    def __init__(self, data: Dict[str, Any]) -> None:
        """
        Initializes the ValueParser with the given data.

        Args:
            data (dict): The data dictionary used for resolving values.
        """
        self.data = data
        super().__init__()

    @abstractmethod
    def parse(self, value: Union[str, Callable, Any]) -> Any:
        pass


class RuntimeValueParser(ValueParser):
    def __init__(self, data: dict, prefix: str):
        """
        Initializes the RuntimeValueParser with the given data and prefix.

        Args:
            data (dict): The data dictionary used for resolving key paths.
            prefix (str): The prefix string to identify key paths within `data`.
        """
        super().__init__(data)
        self.prefix = prefix

    def parse(self, value: Union[str, Callable, Any]) -> Any:
        """
        Parses the input `value` based on its type and content, dynamically resolving it if necessary.

        Args:
            value (str | callable | Any): The value to be parsed.

        Returns:
            Any: The resolved value.
        """
        if isinstance(value, str):
            return self._parse_string(value)
        elif callable(value):
            return self._parse_callable(value)
        elif isinstance(value, dict):
            return self._parse_dict(value)
        elif isinstance(value, (list, tuple)):
            return self._parse_iterable(value)
        else:
            return value

    def _parse_string(self, value: str) -> Any:
        """
        Parses a string value, resolving key paths that start with the configured prefix.

        Args:
            value (str): The string to parse.

        Returns:
            Any: The resolved value or the original string if no resolution is needed.
        """
        if value.startswith(f"{self.prefix}."):
            key_path = value[len(f"{self.prefix}.") :]
            return self._resolve_key_path(key_path)
        elif self._should_unpack(value):
            # Handle unpacking in lists
            parsed_value = self.parse(value[1:])
            return self._unpack_value(parsed_value)
        else:
            return value

    def _parse_callable(self, value: Callable) -> Any:
        """
        Executes a callable with `self.data` as its argument.

        Args:
            value (Callable): The function to execute.

        Returns:
            Any: The result of the function call.
        """
        return value(self.data)

    def _parse_dict(self, value: dict) -> dict:
        """
        Recursively parses each value in a dictionary.

        Args:
            value (dict): The dictionary to parse.

        Returns:
            dict: A new dictionary with parsed values.
        """
        return {key: self.parse(val) for key, val in value.items()}

    def _parse_iterable(self, value: Union[list, tuple]) -> list:
        """
        Recursively parses each element in a list or tuple. Values reulting to `null`
        are not appended in the final result

        Args:
            value (list | tuple): The iterable to parse.

        Returns:
            list: A new list with parsed elements.
        """
        result = []
        for item in value:
            # Determine parsed value and whether it needs unpacking
            should_unpack = self._should_unpack(item)
            parsed_value = self.parse(
                item[1:] if isinstance(item, str) and should_unpack else item
            )

            # Append or extend based on unpacking status
            if parsed_value is not None:
                if should_unpack:
                    result.extend(self._unpack_value(parsed_value))
                else:
                    result.append(parsed_value)

        return result

    def _resolve_key_path(self, key_path: str) -> Any:
        """
        Resolves a dotted key path within `self.data`.

        Args:
            key_path (str): The key path to resolve.

        Returns:
            Any: The value found at the key path or None if not found.
        """
        keys = key_path.split(".")
        result = self.data
        for key in keys:
            if isinstance(result, dict):
                result = result.get(key)
                if result is None:
                    return None
            else:
                return None
        return result

    def _should_unpack(self, value: Any) -> bool:
        if isinstance(value, str) and value.startswith("*") and len(value) > 1:
            return True
        if callable(value) and getattr(value, "unpack", False):
            return True
        return False

    def _unpack_value(self, value: Any) -> list:
        """
        Unpacks a value if it's iterable; otherwise, wraps it in a list.

        Args:
            value (Any): The value to unpack.

        Returns:
            list: The unpacked value as a list.
        """
        if isinstance(value, Iterable) and not isinstance(value, str):
            return list(value)
        else:
            return [value]
