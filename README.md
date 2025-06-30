# elastictoolkit-py

[![PyPI version](https://badge.fury.io/py/elastictoolkit-py.svg)](https://pypi.org/project/elastictoolkit-py/)
[![Python versions](https://img.shields.io/pypi/pyversions/elastictoolkit-py.svg)](https://pypi.org/project/elastictoolkit-py/)
[![License](https://img.shields.io/github/license/workindia/elastictoolkit-py.svg)](https://github.com/workindia/elastictoolkit-py/blob/main/LICENSE)


A Python DSL for building and managing Elasticsearch queries with powerful abstraction layers. This toolkit provides unified adapter management for different Elasticsearch versions and an intuitive directive-based system for constructing complex queries without dealing with raw JSON or low-level DSL syntax.

## Key Features

### Adapter Management
- **Multi-version Support**: Unified interface for both Elasticsearch 5.x and 8.x clusters
- **Configuration Management**: Singleton-based adapter configuration with cluster management

### Query Generation
- **Directive-Based Architecture**: Build complex queries using simple, reusable components called directives
- **Dynamic Parameter Handling**: Runtime value injection and parameter mapping for flexible query generation
- **Boolean Logic Composition**: Combine queries with AND/OR logic using intuitive directive composition
- **Function Score Support**: Advanced scoring and boosting capabilities with custom score functions
- **Value Mapping**: Sophisticated field-to-value mapping system supporting static and dynamic values
- **Custom Directives**: Extensible system for creating application-specific query components

## Quick Start Guide

### Installation

```bash
pip install elastictoolkit-py
```

## Usage

### Basic Adapter Usage

```python
from elastictoolkit.adapters import Elasticsearch8Adapter

# Create adapter class with static configuration
class MyElasticsearchAdapter(Elasticsearch8Adapter):
    cluster_name = "my-cluster"
    write_nodes = [{"host": "localhost", "port": 9200, "scheme": "http"}]
    read_nodes = [{"host": "localhost", "port": 9200, "scheme": "http"}]
    timeout = 2.0 # In seconds

# Get adapter instance
adapter = MyElasticsearchAdapter()

# Check if ready
if adapter.ready():
    # Perform operations
    result = adapter.search(index="my-index", body={"query": {"match_all": {}}})
    print(result)
```
### Basic Query Usage

```python
import json
from elastictoolkit.queryutils.builder.directiveengine import DirectiveEngine
from elastictoolkit.queryutils.builder.matchdirective import TextMatchDirective, ConstMatchDirective
from elastictoolkit.queryutils.builder.directivevaluemapper import DirectiveValueMapper
from elastictoolkit.queryutils.types import FieldValue
from elastictoolkit.queryutils.consts import FieldMatchType

# Define value mapper
class ProductSearchMapper(DirectiveValueMapper):
    search_text = FieldValue(
        fields=["name^3", "description"],
        values_list=["match_params.query"]
    )
    category = FieldValue(
        fields=["category"],
        values_list=["*match_params.categories"]
    )

# Create search engine
class ProductSearchEngine(DirectiveEngine):
    search_text = TextMatchDirective(rule=FieldMatchType.ANY)
    category = ConstMatchDirective(rule=FieldMatchType.ANY)

    class Config:
        value_mapper = ProductSearchMapper()

# Use the engine
engine = ProductSearchEngine()
engine.set_match_params({
    "query": "laptop gaming",
    "categories": ["electronics", "computers"]
})

query = engine.to_dsl()
print(json.dumps(query.to_query(), indent=2))
```

**Output:**
```json
{
  "bool": {
    "filter": [
      {
        "terms": {
          "category": ["electronics", "computers"]
        }
      },
      {
        "multi_match": {
          "query": "laptop gaming",
          "fields": ["name^3", "description"]
        }
      }
    ]
  }
}
```

For more comprehensive examples ranging from basic to advanced DirectiveEngine usage patterns, see the [documentation examples](docs/).

## Documentation

The documentation is organized as interactive Jupyter notebooks covering everything from basic concepts to advanced use cases for Query Generation Utils:

📚 **[Documentation Directory](docs/)**

The comprehensive documentation includes:
- **Basic to Advanced Examples**: 4 complete DirectiveEngine usage examples
- **Interactive Notebooks**: 10 detailed Jupyter notebooks covering all aspects
- **Learning Path**: Structured progression from beginner to advanced topics
- **Real-world Use Cases**: Complete e-commerce search implementation

Visit the [docs/](docs/) directory for the complete table of contents and detailed usage examples.

## Supported Elasticsearch Versions

### Adapter Usage
- **Elasticsearch 5.x**: Full support via `Elasticsearch5Adapter` (5.5.3+)
- **Elasticsearch 8.x**: Full support via `Elasticsearch8Adapter` (8.2.0+)

### Query Generation
- **Elasticsearch 8.x**: Optimized for ES8 query DSL and features
- **Backward Compatibility**: Most query patterns work with ES5, but some advanced features require ES8

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

We welcome contributions! Here's how you can help:

### Getting Started
1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes and add tests
4. Ensure all tests pass: `make test`
5. Commit your changes: `git commit -m 'Add amazing feature'`
6. Push to the branch: `git push origin feature/amazing-feature`
7. Open a Pull Request

### Development Setup
```bash
# Clone the repository
git clone https://github.com/workindia/elastictoolkit-py.git
cd elastictoolkit-py

# Initialize development environment
make init

# Install the package in development mode
pip install -e .
```

### Running Tests

To run the tests:

```bash
# Run all tests
pytest

# Run tests with coverage | Requires `pytest-cov`
pytest --cov=elastictoolkit
```

### Guidelines
- Follow PEP 8 coding standards
- Add tests for new features
- Update documentation for API changes
- Ensure backward compatibility when possible
- Write clear, descriptive commit messages

## Contact
For questions or feedback, please open an issue on the [GitHub repository](https://github.com/workindia/elastictoolkit-py/issues).

### Reporting Guidelines
- Use GitHub Issues to report bugs or request features
- Provide detailed reproduction steps for bugs
- Include relevant code examples and error messages

## Authors

elastictoolkit-py was written by [Nikhil Kumar](mailto:nikhil.kumar@workindia.in).
