import io
import os
import re

from setuptools import find_packages
from setuptools import setup


def read(filename):
    filename = os.path.join(os.path.dirname(__file__), filename)
    text_type = type("")
    with io.open(filename, mode="r", encoding="utf-8") as fd:
        return re.sub(
            text_type(r":[a-z]+:`~?(.*?)`"), text_type(r"``\1``"), fd.read()
        )


# Package Dependencies
install_requires = [
    "typing-extensions",
    "elasticsearch5>=5.5.3",
    "elasticsearch8>=8.2.0",
    "elasticquery-dsl-py>=1.0.0,<2.0.0",
    "pydantic==1.9.1,<2.0.0",
]

# Dev Dependencies
develop_requires = install_requires + [
    "nox",
    "pytest",
    "pytest-cov",
    "pre-commit",
    "wheel",
    "pip>=20",
    "bump2version==1.0.0",
]

setup(
    name="elastictoolkit-py",
    version="0.5.0",
    url="https://github.com/workindia/elastictoolkit-py",
    license="MIT license",
    author="Nikhil Kumar",
    author_email="nikhil.kumar@workindia.in",
    description="A Python based DSL for building and managing Elasticsearch queries",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    packages=find_packages(exclude=("tests",)),
    install_requires=install_requires,
    setup_requires=["wheel", "pip>=20"],
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    extras_require={"develop": develop_requires},
)
