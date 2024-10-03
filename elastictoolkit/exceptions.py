# Package Base Exception Class
class ElasticToolkitError(Exception):
    pass


class ClientNotReadyError(ElasticToolkitError):
    pass


class ImproperESAdapterConfigError(ElasticToolkitError):
    pass


class ImproperESIndexConfigError(ElasticToolkitError):
    pass
