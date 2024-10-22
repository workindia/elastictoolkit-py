from pydantic import BaseModel
from typing import Any, Dict, Optional, Type

from elastictoolkit.indexes.baseindex import BaseIndex
from elastictoolkit.indexes.exceptions import ImproperESIndexConfigError


class Elasticsearch8Index(BaseIndex):
    """
    Elasticsearch index representation for Elasticsearch 8.x, providing methods to interact with the index.

    Attributes:
        document_definition_class (Optional[Type[BaseModel]]): Pydantic model class used for document validation.
        validate_before_save (bool): Flag indicating whether to validate documents before saving.
    """

    document_definition_class: Optional[Type[BaseModel]] = None
    validate_before_save: bool = False

    def _validate_params(self) -> None:
        """
        Validate that the required parameters are set, including additional checks for document validation.

        Raises:
            ImproperESIndexConfigError: If configuration is invalid.
        """
        super()._validate_params()
        if self.validate_before_save:
            if not self.document_definition_class or not issubclass(
                self.document_definition_class, BaseModel
            ):
                raise ImproperESIndexConfigError(
                    f"{type(self).__name__} | When `validate_before_save` is set to True, "
                    "`document_definition_class` must be set and should be a subclass of pydantic.BaseModel."
                )

    def save(self, _id: str, doc: Dict[str, Any], **kwargs) -> Any:
        """
        Save a document to the Elasticsearch index.

        Args:
            _id (str): ID of the document.
            doc (Dict[str, Any]): The document to save.
            **kwargs: Additional keyword arguments to be passed to parent ES client method

        Returns:
            Any: Response from the index operation.
        """
        self.validate(doc)
        return self.adapter.index(
            index=self.index,
            _id=_id,
            document=doc,
            **kwargs,
        )

    def get(self, _id: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Retrieve a document from the Elasticsearch index by ID.

        Args:
            _id (str): ID of the document.
            **kwargs: Additional keyword arguments to be passed to parent ES client method

        Returns:
            Optional[Dict[str, Any]]: The document if found, None otherwise.
        """
        return self.adapter.get(self.index, _id, **kwargs)

    def search(self, **kwargs) -> Dict[str, Any]:
        """
        Perform a search operation on the Elasticsearch index.

        Args:
            **kwargs: Additional keyword arguments to be passed to parent ES client method

        Returns:
            Dict[str, Any]: Search results.
        """
        return self.adapter.search(index=self.index, **kwargs)

    def scroll(self, scroll_id: str, **kwargs) -> Dict[str, Any]:
        """
        Scroll the search context to retrieve the next batch of results.

        Args:
            scroll_id (str): The scroll identifier.
            **kwargs: Additional keyword arguments to be passed to parent ES client method

        Returns:
            Dict[str, Any]: Scroll results.
        """
        return self.adapter.scroll(scroll_id=scroll_id, **kwargs)

    def clear_scroll(self, scroll_id: str, **kwargs) -> Any:
        """
        Clear the scroll context before its expiration.

        Args:
            scroll_id (str): The scroll identifier.
            **kwargs: Additional keyword arguments to be passed to parent ES client method

        Returns:
            Any: Response from clear_scroll operation.
        """
        return self.adapter.clear_scroll(scroll_id=scroll_id, **kwargs)

    def update(
        self, _id: str, doc: Optional[Dict[str, Any]] = None, **kwargs
    ) -> Any:
        """
        Update a document in the Elasticsearch index.

        Args:
            _id (str): ID of the document.
            doc (Optional[Dict[str, Any]]): Partial document to update.
            **kwargs: Additional keyword arguments to be passed to parent ES client method

        Returns:
            Any: Response from the update operation.
        """
        return self.adapter.update(self.index, _id, doc, **kwargs)

    def update_by_query(self, **kwargs) -> Any:
        """
        Update documents matching a query in the Elasticsearch index.

        Args:
            **kwargs: Additional keyword arguments to be passed to parent ES client method

        Returns:
            Any: Response from the update_by_query operation.
        """
        return self.adapter.update_by_query(self.index, **kwargs)

    def delete(self, _id: str, **kwargs) -> Any:
        """
        Delete a document from the Elasticsearch index.

        Args:
            _id (str): ID of the document to delete.
            **kwargs: Additional keyword arguments to be passed to parent ES client method

        Returns:
            Any: Response from the delete operation.
        """
        return self.adapter.delete(index=self.index, _id=_id, **kwargs)

    def count(self, **kwargs) -> int:
        """
        Get the count of documents matching a query.

        Args:
            **kwargs: Additional keyword arguments to be passed to parent ES client method

        Returns:
            int: Count of documents.
        """
        response = self.adapter.count(index=self.index, **kwargs)
        return response.get("count", 0)

    def delete_by_query(self, **kwargs) -> Any:
        """
        Delete documents matching a query from the Elasticsearch index.

        Args:
            **kwargs: Additional keyword arguments to be passed to parent ES client method

        Returns:
            Any: Response from the delete_by_query operation.
        """
        return self.adapter.delete_by_query(index=self.index, **kwargs)

    def bulk(self, **kwargs) -> Any:
        """
        Perform bulk operations in Elasticsearch.

        Args:
            **kwargs: Additional keyword arguments to be passed to parent ES client method

        Returns:
            Any: Response from the bulk operation.
        """
        return self.adapter.bulk(**kwargs)

    def validate(self, doc: Dict[str, Any]) -> None:
        """
        Validate the document using the document definition class.

        Args:
            doc (Dict[str, Any]): The document to validate.

        Raises:
            ValidationError: If document validation fails.
        """
        if self.validate_before_save:
            # pydantic.ValidationError will be thrown if document validation fails
            self.document_definition_class(**doc)

    def get_task(self, task_id: str, **kwargs) -> Any:
        """
        Get the status of an asynchronous task.

        Args:
            task_id (str): ID of the task.
            **kwargs: Additional keyword arguments to be passed to parent ES client method

        Returns:
            Any: Task status.
        """
        return self.adapter.get_task(task_id=task_id, **kwargs)
