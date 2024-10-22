from typing import Any, Dict, List, Mapping, Optional, Union

from elasticsearch8 import Elasticsearch, NotFoundError as ESDocNotFoundError
from elastic_transport import NodeConfig

from elastictoolkit.adapters.baseelasticsearchadapter import (
    BaseElasticsearchAdapter,
)
from elastictoolkit.constants import ClientType

# Define a type alias for hosts parameter
_TYPE_HOSTS = Optional[
    Union[str, List[Union[str, Dict[str, Any], NodeConfig]]]
]


class Elasticsearch8Adapter(BaseElasticsearchAdapter):
    """
    Elasticsearch adapter for Elasticsearch 8.x, implementing the required methods
    from the BaseElasticsearchAdapter for interaction with Elasticsearch clusters.
    """

    write_nodes: _TYPE_HOSTS = None
    read_nodes: _TYPE_HOSTS = None

    def get_client(
        self, client_type: ClientType = ClientType.READ
    ) -> Elasticsearch:
        """
        Retrieve the Elasticsearch client for the specified client type.

        Args:
            client_type (ClientType, optional): Type of client to retrieve (READ or WRITE). Defaults to ClientType.READ.

        Returns:
            Elasticsearch: The Elasticsearch client instance.
        """
        return super().get_client(client_type)

    @classmethod
    def create_client(
        cls, nodes: _TYPE_HOSTS, timeout: float
    ) -> Elasticsearch:
        """
        Create an Elasticsearch client instance.

        Args:
            nodes (_TYPE_HOSTS): Host configurations for the Elasticsearch client.
            timeout (float): Timeout value for client operations.

        Returns:
            Elasticsearch: An instance of Elasticsearch client.
        """
        client = Elasticsearch(hosts=nodes, timeout=timeout)
        return client

    def index(
        self, index: str, _id: str, document: Mapping[str, Any], **kwargs
    ) -> Dict[str, Any]:
        """
        Index a document into a specific Elasticsearch index.

        Args:
            index (str): Elasticsearch index to save the document into.
            _id (str): ID of the document.
            document (Mapping[str, Any]): The document to be saved.
            **kwargs: Additional keyword arguments.

        Returns:
            Dict[str, Any]: Response from Elasticsearch index operation.
        """
        client = self.get_client(client_type=ClientType.WRITE)
        return client.index(index=index, id=_id, document=document, **kwargs)

    def get(
        self, index: str, _id: str, **kwargs
    ) -> Optional[Mapping[str, Any]]:
        """
        Retrieve a document from the Elasticsearch index by ID.

        Args:
            index (str): Index to search on.
            _id (str): ID of the document.
            **kwargs: Additional keyword arguments.

        Returns:
            Optional[Mapping[str, Any]]: Document source if found, None otherwise.
        """
        client = self.get_client(ClientType.READ)
        try:
            response = client.get(index=index, id=_id, **kwargs)
            return response["_source"]
        except ESDocNotFoundError:
            return None

    def search(self, **kwargs) -> Dict[str, Any]:
        """
        Perform a search operation using the read client.

        If 'filter_path' is not provided in kwargs, a default 'filter_path' is set to limit
        the fields returned in the response for efficiency.

        Args:
            **kwargs: Same as Elasticsearch.search parameters.

        Returns:
            Dict[str, Any]: Search results as a dictionary.
        """
        client = self.get_client(ClientType.READ)

        if "filter_path" not in kwargs:
            # Set default filter_path to limit the fields returned in the response
            filter_path = [
                "hits.hits._id",
                "hits.total",
                "hits.hits._score",
            ]

            if kwargs.get("source") or kwargs.get("_source"):
                filter_path.append("hits.hits._source")

            if kwargs.get("explain"):
                filter_path.append("hits.hits._explanation")

            if kwargs.get("aggs") or kwargs.get("aggregations"):
                filter_path.extend(
                    [
                        "aggregations.**.key",
                        "aggregations.**.doc_count",
                    ]
                )

            kwargs["filter_path"] = filter_path

        response = client.search(**kwargs)
        return response

    def scroll(self, scroll_id: str, **kwargs) -> Dict[str, Any]:
        """
        Scroll the search context to retrieve the next batch of results.

        Args:
            scroll_id (str): The scroll identifier.
            **kwargs: Same as Elasticsearch.scroll parameters.

        Returns:
            Dict[str, Any]: Scroll results as a dictionary.
        """
        client = self.get_client(client_type=ClientType.READ)
        return client.scroll(scroll_id=scroll_id, **kwargs)

    def clear_scroll(self, scroll_id: str, **kwargs) -> Dict[str, Any]:
        """
        Clear the scroll context before its expiration.

        Args:
            scroll_id (str): The scroll identifier.
            **kwargs: Same as Elasticsearch.clear_scroll parameters.

        Returns:
            Dict[str, Any]: Response from clear_scroll operation.
        """
        client = self.get_client(client_type=ClientType.WRITE)
        return client.clear_scroll(scroll_id=scroll_id, **kwargs)

    def update(
        self, index: str, _id: str, doc: Mapping[str, Any], **kwargs
    ) -> Dict[str, Any]:
        """
        Update a document in the Elasticsearch index.

        Args:
            index (str): Elasticsearch index.
            _id (str): ID of the document.
            doc (Mapping[str, Any]): Partial document to update.
            **kwargs: Additional keyword arguments.

        Returns:
            Dict[str, Any]: Response from the update operation.
        """
        client = self.get_client(client_type=ClientType.WRITE)
        return client.update(index=index, id=_id, doc=doc, **kwargs)

    def update_by_query(self, index: str, **kwargs) -> Dict[str, Any]:
        """
        Update documents matching a query in the Elasticsearch index.

        Args:
            index (str): Elasticsearch index.
            **kwargs: Additional keyword arguments.

        Returns:
            Dict[str, Any]: Response from the update_by_query operation.
        """
        client = self.get_client(client_type=ClientType.WRITE)
        return client.update_by_query(index=index, **kwargs)

    def delete(
        self, index: str, _id: str, **kwargs
    ) -> Optional[Dict[str, Any]]:
        """
        Delete a document from the Elasticsearch index.

        Args:
            index (str): Index to delete from.
            _id (str): ID of the document to delete.
            **kwargs: Additional keyword arguments.

        Returns:
            Optional[Dict[str, Any]]: Response from the delete operation, or None if document not found and exception not raised.
        """
        client = self.get_client(client_type=ClientType.WRITE)
        try:
            return client.delete(index=index, id=_id, **kwargs)
        except ESDocNotFoundError:
            if kwargs.get("raise_exception", False):
                raise
            return None

    def count(self, **kwargs) -> int:
        """
        Get the count of documents matching a query.

        Args:
            **kwargs: Same as Elasticsearch.count parameters.

        Returns:
            int: Count of documents as an integer.
        """
        client = self.get_client(ClientType.READ)
        response = client.count(**kwargs)
        return response.get("count", 0)

    def delete_by_query(self, **kwargs) -> Dict[str, Any]:
        """
        Delete documents matching a query.

        Args:
            **kwargs: Same as Elasticsearch.delete_by_query parameters.

        Returns:
            Dict[str, Any]: Response from the delete_by_query operation.
        """
        client = self.get_client(client_type=ClientType.WRITE)
        return client.delete_by_query(**kwargs)

    def bulk(self, **kwargs) -> Dict[str, Any]:
        """
        Perform bulk operations in Elasticsearch.

        Args:
            **kwargs: Same as Elasticsearch.bulk parameters.

        Returns:
            Dict[str, Any]: Response from the bulk operation.
        """
        client = self.get_client(client_type=ClientType.WRITE)
        return client.bulk(**kwargs)

    def get_task(self, task_id: str, **kwargs) -> Dict[str, Any]:
        """
        Get the status of an asynchronous task.

        Args:
            task_id (str): ID of the task.
            **kwargs: Additional keyword arguments.

        Returns:
            Dict[str, Any]: Task status as a dictionary.
        """
        client = self.get_client(client_type=ClientType.READ)
        return client.tasks.get(task_id=task_id, **kwargs)
