"""Top-level package for wi-elastictoolkit-py."""

__author__ = """Nikhil Jagtap"""
__email__ = "nikhil.jagtap@workindia.in"
__version__ = "0.7.0"

# flake8: noqa
from .indexes import BaseIndex
from .adapters import Elasticsearch5Adapter, Elasticsearch8Adapter
