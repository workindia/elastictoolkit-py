from abc import abstractmethod
from typing import Any, Dict, List, Mapping, Optional

from elastictoolkit.constants import ClientType
from elastictoolkit.adapters.exceptions import (
    ClientNotReadyError,
    ImproperESAdapterConfigError,
)
from elastictoolkit.utils.singleton import SingletonABC


class BaseElasticsearchAdapter(SingletonABC):
    """
    Base class for Elasticsearch adapters providing common functionality
    and enforcing the implementation of essential methods in subclasses.

    Attributes:
        _clients (Dict[str, Dict[ClientType, Any]]): Stores Elasticsearch clients per cluster.
        cluster_name (str): Name of the Elasticsearch cluster.
        write_nodes (List[Mapping[str, Any]]): Configurations for write nodes.
        read_nodes (List[Mapping[str, Any]]): Configurations for read nodes.
        timeout (float): Timeout value for client operations.
    """

    _clients: Dict[str, Dict[ClientType, Any]] = {}
    cluster_name: str = None
    write_nodes: List[Mapping[str, Any]] = None
    read_nodes: List[Mapping[str, Any]] = None
    timeout: float = 3.0

    def __init__(self) -> None:
        """
        Initialize the Elasticsearch adapter and clients.

        If configuration is incomplete, `configure` needs to be called manually
        to re-configure the adapter.
        """
        self._clients = self._init_client()

    @classmethod
    def configure(
        cls,
        cluster_name: Optional[str] = None,
        write_nodes: Optional[List[Mapping[str, Any]]] = None,
        read_nodes: Optional[List[Mapping[str, Any]]] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """
        Configure the Elasticsearch adapter with cluster details.

        Args:
            cluster_name (Optional[str]): Name of the Elasticsearch cluster.
            write_nodes (Optional[List[Mapping[str, Any]]]): List of write node configurations.
            read_nodes (Optional[List[Mapping[str, Any]]]): List of read node configurations.
            timeout (Optional[float]): Timeout value for client operations.
        """
        cls.cluster_name = cluster_name or cls.cluster_name
        cls.write_nodes = write_nodes or cls.write_nodes
        cls.read_nodes = read_nodes or cls.read_nodes
        cls.timeout = timeout or cls.timeout
        cls._init_client()

    @classmethod
    @abstractmethod
    def create_client(
        cls, nodes: List[Mapping[str, Any]], timeout: float
    ) -> Any:
        """
        Abstract method to create an Elasticsearch client.

        Args:
            nodes (List[Mapping[str, Any]]): List of node configurations.
            timeout (float): Timeout value for client operations.

        Returns:
            Any: An Elasticsearch client instance.
        """
        pass

    @classmethod
    def _validate_config(cls, raise_exc: bool = True) -> bool:
        """
        Validate the configuration of the adapter.

        Args:
            raise_exc (bool): Whether to raise an exception if validation fails.

        Returns:
            bool: True if configuration is valid, False otherwise.

        Raises:
            ImproperESAdapterConfigError: If the configuration is invalid and `raise_exc` is True.
        """
        if (
            cls.cluster_name is None
            or cls.write_nodes is None
            or cls.read_nodes is None
        ):
            if raise_exc:
                raise ImproperESAdapterConfigError(
                    f"{cls.__name__} | `cluster_name`, `read_nodes` and `write_nodes` must be defined."
                    " Use the `configure` method for updating configuration."
                )
            return False
        return True

    @classmethod
    def _init_client(cls) -> Dict[str, Dict[ClientType, Any]]:
        """
        Initialize the Elasticsearch clients for the cluster.

        Returns:
            Dict[str, Dict[ClientType, Any]]: A dictionary of clients for each cluster.
        """
        if cls._clients.get(cls.cluster_name) is not None:
            # Already Initialized
            return cls._clients

        if not cls._validate_config(raise_exc=False):
            # Incomplete configuration, cannot initialize
            return cls._clients

        cls._clients[cls.cluster_name] = {
            ClientType.WRITE: cls.create_client(cls.write_nodes, cls.timeout),
            ClientType.READ: cls.create_client(cls.read_nodes, cls.timeout),
        }
        return cls._clients

    def get_client(self, client_type: ClientType = ClientType.READ) -> Any:
        """
        Retrieve the Elasticsearch client for the specified client type.

        Args:
            client_type (ClientType, optional): Type of client to retrieve (READ or WRITE). Defaults to ClientType.READ.

        Returns:
            Any: The Elasticsearch client instance.
        """
        if self.cluster_name not in self._clients:
            self._validate_config()  # Will raise exception if configuration is incomplete
            self._clients = self._init_client()

        cluster = self._clients[self.cluster_name]
        client = cluster.get(client_type, cluster[ClientType.READ])
        return client

    def ready(self, raise_exc: bool = False) -> bool:
        """
        Check if the adapter is ready by verifying connections to both read and write clients.

        Args:
            raise_exc (bool, optional): If True, raises ClientNotReadyError when not ready. Defaults to False.

        Returns:
            bool: True if both clients are ready, False otherwise.

        Raises:
            ClientNotReadyError: If either client is not ready and raise_exc is True.
        """
        try:
            read_client = self.get_client(ClientType.READ)
            write_client = self.get_client(ClientType.WRITE)
            read_ready = read_client.ping()
            write_ready = write_client.ping()
            if not (read_ready and write_ready):
                if raise_exc:
                    raise ClientNotReadyError(
                        "Elasticsearch clients are not ready."
                    )
                return False
            return True
        except Exception as e:
            if raise_exc:
                raise ClientNotReadyError(
                    f"Elasticsearch clients are not ready: {e}"
                ) from e
            return False

    @abstractmethod
    def index(
        self, index: str, _id: str, document: Mapping[str, Any], **kwargs
    ) -> Any:
        """
        Abstract method to index a document into Elasticsearch.

        Args:
            index (str): Elasticsearch index to save the document into.
            _id (str): ID of the document.
            document (Mapping[str, Any]): The document to be saved.
            **kwargs: Additional keyword arguments.

        Returns:
            Any: Response from the index operation.
        """
        pass

    @abstractmethod
    def get(
        self, index: str, _id: str, **kwargs
    ) -> Optional[Mapping[str, Any]]:
        """
        Abstract method to retrieve a document from Elasticsearch.

        Args:
            index (str): Index to search on.
            _id (str): ID of the document.
            **kwargs: Additional keyword arguments.

        Returns:
            Optional[Mapping[str, Any]]: Document source if found, None otherwise.
        """
        pass

    @abstractmethod
    def search(self, **kwargs) -> Any:
        """
        Abstract method to perform a search operation.

        Args:
            **kwargs: Same as Elasticsearch.search parameters.

        Returns:
            Any: Search results.
        """
        pass

    @abstractmethod
    def scroll(self, scroll_id: str, **kwargs) -> Any:
        """
        Abstract method to scroll the existing scroll ID.

        Args:
            scroll_id (str): The scroll identifier.
            **kwargs: Same as Elasticsearch.client.scroll parameters.

        Returns:
            Any: Scroll results.
        """
        pass

    @abstractmethod
    def clear_scroll(self, scroll_id: str, **kwargs) -> Any:
        """
        Abstract method to clear the scroll context before its expiration.

        Args:
            scroll_id (str): The scroll identifier.
            **kwargs: Same as Elasticsearch.client.clear_scroll parameters.

        Returns:
            Any: Response from clear_scroll operation.
        """
        pass

    @abstractmethod
    def update(
        self, index: str, _id: str, doc: Mapping[str, Any], **kwargs
    ) -> Any:
        """
        Abstract method to update a document in Elasticsearch.

        Args:
            index (str): Elasticsearch index.
            _id (str): ID of the document.
            doc (Mapping[str, Any]): Partial document to update.
            **kwargs: Additional keyword arguments.

        Returns:
            Any: Response from the update operation.
        """
        pass

    @abstractmethod
    def update_by_query(self, index: str, **kwargs) -> Any:
        """
        Abstract method to update documents matching a query.

        Args:
            index (str): Elasticsearch index.
            **kwargs: Additional keyword arguments.

        Returns:
            Any: Response from the update_by_query operation.
        """
        pass

    @abstractmethod
    def delete(self, index: str, _id: str, **kwargs) -> Any:
        """
        Abstract method to delete a document from Elasticsearch.

        Args:
            index (str): Index to delete from.
            _id (str): ID of the document to delete.
            **kwargs: Additional keyword arguments.

        Returns:
            Any: Response from the delete operation.
        """
        pass

    @abstractmethod
    def count(self, **kwargs) -> int:
        """
        Abstract method to count the number of documents matching a query.

        Args:
            **kwargs: Same as Elasticsearch.count parameters.

        Returns:
            int: Count of documents.
        """
        pass

    @abstractmethod
    def delete_by_query(self, **kwargs) -> Any:
        """
        Abstract method to delete documents matching a query.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            Any: Response from the delete_by_query operation.
        """
        pass

    @abstractmethod
    def bulk(self, **kwargs) -> Any:
        """
        Abstract method to perform bulk operations in Elasticsearch.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            Any: Response from the bulk operation.
        """
        pass

    @abstractmethod
    def get_task(self, task_id: str, **kwargs) -> Any:
        """
        Abstract method to get the status of an asynchronous task.

        Args:
            task_id (str): ID of the task.
            **kwargs: Additional keyword arguments.

        Returns:
            Any: Task status.
        """
        pass
