from elastictoolkit.exceptions import ElasticToolkitError


class ClientNotReadyError(ElasticToolkitError):
    """
    Raised when a client is not ready to perform an operation.
    """

    pass


class ImproperESAdapterConfigError(ElasticToolkitError):
    """
    Raised when the configuration of the ESAdapter is not correct.
    """

    pass
