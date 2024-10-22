from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from elastictoolkit.adapters import BaseElasticsearchAdapter
from elastictoolkit.indexes.exceptions import ImproperESIndexConfigError


class BaseIndex(ABC):
    """
    Base class for Elasticsearch index representations, providing common functionality
    and enforcing the implementation of essential methods in subclasses.

    Attributes:
        index (str): Name of the Elasticsearch index.
        adapter (BaseElasticsearchAdapter): Adapter for interacting with Elasticsearch.
    """

    index: str = None
    adapter: BaseElasticsearchAdapter = None

    def __init__(
        self, adapter: Optional[BaseElasticsearchAdapter] = None
    ) -> None:
        """
        Initialize the BaseIndex with an optional Elasticsearch adapter.

        Args:
            adapter (Optional[BaseElasticsearchAdapter]): If an adapter is passed as a parameter,
                it will override the adapter set in the class variable `adapter`.
        """
        self.adapter = adapter or self.adapter
        self._validate_params()

    @classmethod
    def set_adapter(cls, adapter: BaseElasticsearchAdapter) -> None:
        """
        Set the Elasticsearch adapter for the index.

        Args:
            adapter (BaseElasticsearchAdapter): The Elasticsearch adapter to set.
        """
        cls.adapter = adapter

    @abstractmethod
    def create_index(self) -> None:
        """
        Abstract method to create the Elasticsearch index.

        Raises:
            NotImplementedError: If the method is not implemented in the subclass.
        """
        pass

    def _validate_params(self) -> None:
        """
        Validate that the required parameters `index` and `adapter` are set.

        Raises:
            ImproperESIndexConfigError: If `index` or `adapter` is not properly configured.
        """
        if not self.index:
            raise ImproperESIndexConfigError(
                f"{type(self).__name__} | `index` parameter is not defined."
            )
        if not self.adapter:
            raise ImproperESIndexConfigError(
                f"{type(self).__name__} | `adapter` must be set as a class variable "
                "or must be passed as an argument during initialization."
            )
        if not isinstance(self.adapter, BaseElasticsearchAdapter):
            raise ImproperESIndexConfigError(
                f"{type(self).__name__} | `adapter` is expected to be of type "
                f"BaseElasticsearchAdapter, got {type(self.adapter)} instead."
            )

    def _get_count_from_response(self, response: Dict[str, Any]) -> int:
        """
        Extract the count of documents from an Elasticsearch response.

        Args:
            response (Dict[str, Any]): The response dictionary from an Elasticsearch query.

        Returns:
            int: The count of documents, or -1 if the response is invalid.
        """
        if not response:
            return -1

        hits = response.get("hits", {})
        total = hits.get("total", -1)

        if isinstance(total, dict):
            # For Elasticsearch versions 7.x and above
            return total.get("value", -1)
        else:
            # For Elasticsearch versions prior to 7.x
            return total
