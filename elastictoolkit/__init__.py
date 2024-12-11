"""Top-level package for wi-elastictoolkit-py."""

__author__ = """Nikhil Kumar"""
__email__ = "nikhil.kumar@workindia.in"
__version__ = "0.2.0"

# flake8: noqa
from .indexes import BaseIndex
from .adapters import Elasticsearch5Adapter, Elasticsearch8Adapter
