from typing import Any, Dict, List, Optional

from elasticsearch5 import NotFoundError as ESDocNotFoundError

from elastictoolkit.adapters import Elasticsearch5Adapter
from elastictoolkit.constants import ClientType
from elastictoolkit.indexes.exceptions import ImproperESIndexConfigError
from elastictoolkit.indexes.baseindex import BaseIndex


class Elasticsearch5Index(BaseIndex):
    """
    Elasticsearch index representation for Elasticsearch 5.x, providing methods to interact with the index.

    Attributes:
        doc_type (Optional[str]): Document type used in Elasticsearch 5.x indices.
    """

    doc_type: Optional[str] = None
    adapter: Elasticsearch5Adapter = None

    def _validate_params(self) -> None:
        """
        Validate that the required parameters are set, including `doc_type`.

        Raises:
            ImproperESIndexConfigError: If configuration is invalid.
        """
        super()._validate_params()
        if not self.doc_type:
            raise ImproperESIndexConfigError(
                f"{type(self).__name__} | `doc_type` not defined."
            )

    def create_index(self, **kwargs) -> Any:
        """
        Create the Elasticsearch index.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            Any: Response from the create index operation.
        """
        client = self.adapter.get_client(client_type=ClientType.WRITE)
        return client.indices.create(index=self.index, **kwargs)

    def save(self, doc: Dict[str, Any], timeout: str = "5s") -> Any:
        """
        Save a document to the Elasticsearch index.

        Args:
            doc (Dict[str, Any]): The document to save. Must include an 'id' field.
            timeout (str, optional): Timeout for the operation. Defaults to '5s'.

        Returns:
            Any: Response from the index operation.

        Raises:
            ValueError: If 'id' is not provided in the document.
        """
        if "id" not in doc:
            raise ValueError("`id` must be provided in `doc`.")
        client = self.adapter.get_client(client_type=ClientType.WRITE)
        return client.index(
            index=self.index,
            doc_type=self.doc_type,
            body=doc,
            id=doc["id"],
            op_type="index",
            timeout=timeout,
        )

    def save_bulk(self, docs: List[Dict[str, Any]]) -> Any:
        """
        Save multiple documents to the Elasticsearch index in bulk.

        Args:
            docs (List[Dict[str, Any]]): List of documents to save.

        Returns:
            Any: Response from the bulk operation.
        """
        bulk_content = []
        for doc in docs:
            if "id" not in doc:
                continue
            bulk_content.append(
                {
                    "index": {
                        "_index": self.index,
                        "_type": self.doc_type,
                        "_id": doc["id"],
                    }
                }
            )
            bulk_content.append(doc)
        client = self.adapter.get_client(client_type=ClientType.WRITE)
        return client.bulk(
            index=self.index,
            doc_type=self.doc_type,
            body=bulk_content,
        )

    def count(self, query_param: Optional[Dict[str, Any]] = None) -> int:
        """
        Get the count of documents matching a query.

        Args:
            query_param (Optional[Dict[str, Any]]): Query parameters for counting documents.

        Returns:
            int: Count of documents.
        """
        body = {"query": {"match_all": {}}}

        if query_param:
            body = query_param

        client = self.adapter.get_client(client_type=ClientType.READ)
        response = client.count(
            index=self.index,
            doc_type=self.doc_type,
            body=body,
        )

        return response.get("count", 0)

    def get(
        self, doc_id: str, source_exclude: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a document from the Elasticsearch index by ID.

        Args:
            doc_id (str): ID of the document.
            source_exclude (Optional[List[str]]): List of fields to exclude from the source.

        Returns:
            Optional[Dict[str, Any]]: The document if found, None otherwise.
        """
        client = self.adapter.get_client(client_type=ClientType.READ)
        try:
            result = client.get(
                index=self.index,
                doc_type=self.doc_type,
                id=doc_id,
                _source_exclude=source_exclude or [],
            )
            return result.get("_source")
        except ESDocNotFoundError:
            return None

    def update(
        self,
        _id: str,
        partial_doc: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Update a document in the Elasticsearch index.

        Args:
            _id (str): ID of the document.
            partial_doc (Dict[str, Any]): Partial document to update.
            params (Optional[Dict[str, Any]]): Additional parameters.

        Returns:
            Any: Response from the update operation.
        """
        body = {"doc": partial_doc}
        client = self.adapter.get_client(client_type=ClientType.WRITE)
        return client.update(
            index=self.index,
            doc_type=self.doc_type,
            body=body,
            id=_id,
            params=params or {},
        )

    def update_with_body(
        self,
        _id: str,
        body: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Update a document using the provided body.

        Passes the whole body as the request body. Use when updates are done using scripts.
        For partial document updates, use `update()` instead.

        Args:
            _id (str): ID of the document.
            body (Dict[str, Any]): The full update body.
            params (Optional[Dict[str, Any]]): Additional parameters.

        Returns:
            Any: Response from the update operation.
        """
        client = self.adapter.get_client(client_type=ClientType.WRITE)
        return client.update(
            index=self.index,
            doc_type=self.doc_type,
            id=_id,
            body=body,
            params=params or {},
        )

    def update_by_query(
        self,
        body: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Update documents matching a query in the Elasticsearch index.

        Args:
            body (Dict[str, Any]): The query and update definition.
            params (Optional[Dict[str, Any]]): Additional parameters.

        Returns:
            Any: Response from the update_by_query operation.
        """
        client = self.adapter.get_client(client_type=ClientType.WRITE)
        return client.update_by_query(
            index=self.index,
            doc_type=self.doc_type,
            body=body,
            params=params or {},
        )

    def delete(self, doc_id: str) -> Any:
        """
        Delete a document from the Elasticsearch index.

        Args:
            doc_id (str): ID of the document to delete.

        Returns:
            Any: Response from the delete operation.
        """
        client = self.adapter.get_client(client_type=ClientType.WRITE)
        try:
            return client.delete(
                index=self.index,
                doc_type=self.doc_type,
                id=doc_id,
                refresh=True,
            )
        except ESDocNotFoundError:
            return None

    def search(
        self, body: Optional[Dict[str, Any]] = None, **kwargs
    ) -> Dict[str, Any]:
        """
        Perform a search operation on the Elasticsearch index.

        Args:
            body (Optional[Dict[str, Any]]): The search query.
            **kwargs: Additional keyword arguments.

        Returns:
            Dict[str, Any]: Search results.
        """
        client = self.adapter.get_client(client_type=ClientType.READ)
        return client.search(
            index=self.index,
            doc_type=self.doc_type,
            body=body,
            **kwargs,
        )

    def scroll(self, scroll_id: str, **kwargs) -> Dict[str, Any]:
        """
        Scroll the search context to retrieve the next batch of results.

        Args:
            scroll_id (str): The scroll identifier.
            **kwargs: Additional keyword arguments.

        Returns:
            Dict[str, Any]: Scroll results.
        """
        client = self.adapter.get_client(client_type=ClientType.READ)
        return client.scroll(scroll_id=scroll_id, **kwargs)

    def clear_scroll(self, scroll_id: str, **kwargs) -> Any:
        """
        Clear the scroll context before its expiration.

        Args:
            scroll_id (str): The scroll identifier.
            **kwargs: Additional keyword arguments.

        Returns:
            Any: Response from clear_scroll operation.
        """
        client = self.adapter.get_client(client_type=ClientType.WRITE)
        return client.clear_scroll(scroll_id=scroll_id, **kwargs)

    def delete_by_query(
        self,
        body: Dict[str, Any],
        params: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Any:
        """
        Delete documents matching a query from the Elasticsearch index.

        Args:
            body (Dict[str, Any]): The query definition.
            params (Optional[Dict[str, Any]]): Additional parameters.
            **kwargs: Additional keyword arguments.

        Returns:
            Any: Response from the delete_by_query operation.
        """
        client = self.adapter.get_client(client_type=ClientType.WRITE)
        return client.delete_by_query(
            index=self.index,
            doc_type=self.doc_type,
            body=body,
            params=params or {},
            **kwargs,
        )

    def bulk(self, body: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
        """
        Perform bulk operations in Elasticsearch.

        Args:
            body (List[Dict[str, Any]]): Bulk operation definitions.
            **kwargs: Additional keyword arguments.

        Returns:
            Dict[str, Any]: Response from the bulk operation.
        """
        client = self.adapter.get_client(client_type=ClientType.WRITE)
        return client.bulk(body=body, **kwargs)

    def get_task(self, task_id: str, **kwargs) -> Dict[str, Any]:
        """
        Get the status of an asynchronous task.

        Args:
            task_id (str): ID of the task.
            **kwargs: Additional keyword arguments.

        Returns:
            Dict[str, Any]: Task status.
        """
        client = self.adapter.get_client(client_type=ClientType.READ)
        return client.tasks.get(task_id=task_id, **kwargs)

    def validate(self, doc: Dict[str, Any]) -> None:
        """
        Validate the document before saving.

        Args:
            doc (Dict[str, Any]): The document to validate.

        Raises:
            ValueError: If validation fails.
        """
        # Implement validation logic if needed.
        pass

    def query(
        self,
        limit: int = 10,
        offset: int = 0,
        query_param: Optional[Dict[str, Any]] = None,
        sort_params: Optional[List[Dict[str, Any]]] = None,
        timeout: str = "5s",
    ) -> Dict[str, Any]:
        """
        Perform a query on the Elasticsearch index.

        Args:
            limit (int, optional): Number of results to return. Defaults to 10.
            offset (int, optional): Starting point of results. Defaults to 0.
            query_param (Optional[Dict[str, Any]]): Query parameters.
            sort_params (Optional[List[Dict[str, Any]]]): Sorting parameters.
            timeout (str, optional): Timeout for the operation. Defaults to '5s'.

        Returns:
            Dict[str, Any]: Query results.
        """
        client = self.adapter.get_client(client_type=ClientType.READ)
        body = {
            "from": offset,
            "size": limit,
            "query": query_param or {"match_all": {}},
            "sort": sort_params or [],
        }
        return client.search(
            index=self.index,
            doc_type=self.doc_type,
            body=body,
            timeout=timeout,
        )
