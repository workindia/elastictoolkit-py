from enum import Enum


class ClientType(str, Enum):
    READ = "read"
    WRITE = "write"
