# Elasticsearch Query Toolkit Documentation

This documentation provides comprehensive examples and guides for using the Elasticsearch Query Toolkit's query generation capabilities. The examples range from basic to advanced usage patterns using DirectiveEngine.

## Table of Contents

| Notebook | Description |
|----------|-------------|
| **[01_Introduction.ipynb](01_Introduction.ipynb)** | Overview and basic setup |
| **[02_CoreConcepts.ipynb](02_CoreConcepts.ipynb)** | Understanding fundamental components |
| **[03_BasicQueries.ipynb](03_BasicQueries.ipynb)** | Working with individual directives |
| **[04_DirectiveEngines.ipynb](04_DirectiveEngines.ipynb)** | Orchestrating multiple directives |
| **[05_ValueMapping.ipynb](05_ValueMapping.ipynb)** | Handling field-value relationships |
| **[06_BooleanLogic.ipynb](06_BooleanLogic.ipynb)** | Combining queries with AND/OR logic |
| **[07_CustomDirectives.ipynb](07_CustomDirectives.ipynb)** | Creating reusable query components |
| **[08_FunctionScore.ipynb](08_FunctionScore.ipynb)** | Boosting and filtering results |
| **[09_CustomScoreFunctions.ipynb](09_CustomScoreFunctions.ipynb)** | Creating reusable scoring functions |
| **[10_ExampleUsecaseEcommerce.ipynb](10_ExampleUsecaseEcommerce.ipynb)** | Complete E-commerce search application |

### Learning Path

1. **Beginners**: Start with notebooks 1-3 to understand basic concepts and individual directives
2. **Intermediate**: Progress to notebooks 4-6 to learn about engines, value mapping, and boolean logic
3. **Advanced**: Explore notebooks 7-9 to master custom directives and function scoring
4. **Implementation**: Study notebook 10 for a complete real-world application example

## Usage Examples

### 1. Basic Query with DirectiveEngine

```python
import json
from elastictoolkit.queryutils.builder.directiveengine import DirectiveEngine
from elastictoolkit.queryutils.builder.matchdirective import ConstMatchDirective
from elastictoolkit.queryutils.builder.directivevaluemapper import DirectiveValueMapper
from elastictoolkit.queryutils.types import FieldValue
from elastictoolkit.queryutils.consts import FieldMatchType

# Define value mapper
class BasicValueMapper(DirectiveValueMapper):
    category = FieldValue(
        fields=["category"],
        values_list=["match_params.category"]
    )
    in_stock = FieldValue(
        fields=["in_stock"],
        values_list=[True]
    )

# Define engine
class BasicSearchEngine(DirectiveEngine):
    category = ConstMatchDirective(rule=FieldMatchType.ANY)
    in_stock = ConstMatchDirective(rule=FieldMatchType.ANY)

    class Config:
        value_mapper = BasicValueMapper()

# Use the engine
engine = BasicSearchEngine()
engine.set_match_params({"category": "electronics"})
query = engine.to_dsl()
print(json.dumps(query.to_query(), indent=2))
```

**Output:**
```json
{
  "bool": {
    "filter": [
      {
        "term": {
          "category": {
            "value": "electronics"
          }
        }
      },
      {
        "term": {
          "in_stock": {
            "value": true
          }
        }
      }
    ]
  }
}
```

### 2. Advanced Multi-Field Search Engine

```python
from elastictoolkit.queryutils.builder.matchdirective import TextMatchDirective, RangeMatchDirective

# Advanced value mapper with multiple field types
class AdvancedValueMapper(DirectiveValueMapper):
    search_text = FieldValue(
        fields=["name^3", "description", "keywords"],
        values_list=["match_params.search_text"]
    )
    category = FieldValue(
        fields=["category"],
        values_list=["*match_params.categories"]  # List of categories
    )
    price_range = FieldValue(
        fields=["price"],
        values_map={
            "gte": "match_params.min_price",
            "lte": "match_params.max_price"
        }
    )
    brand = FieldValue(
        fields=["brand"],
        values_list=["*match_params.brands"]
    )

# Advanced search engine
class AdvancedSearchEngine(DirectiveEngine):
    search_text = TextMatchDirective(rule=FieldMatchType.ANY)
    category = ConstMatchDirective(rule=FieldMatchType.ANY)
    price_range = RangeMatchDirective()
    brand = ConstMatchDirective(rule=FieldMatchType.ANY)

    class Config:
        value_mapper = AdvancedValueMapper()

# Usage with complex parameters
engine = AdvancedSearchEngine()
engine.set_match_params({
    "search_text": "wireless bluetooth headphones",
    "categories": ["electronics", "audio"],
    "min_price": 50,
    "max_price": 300,
    "brands": ["sony", "bose", "apple"]
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
          "brand": ["sony", "bose", "apple"]
        }
      },
      {
        "terms": {
          "category": ["electronics", "audio"]
        }
      },
      {
        "range": {
          "price": {
            "gte": 50,
            "lte": 300
          }
        }
      },
      {
        "multi_match": {
          "query": "wireless bluetooth headphones",
          "fields": ["name^3", "description", "keywords"]
        }
      }
    ]
  }
}
```

### 3. Dynamic Conditional Search Engine

```python
from elastictoolkit.queryutils.builder.matchdirective import FieldExistsDirective

# Conditional value mapper that handles optional parameters
class ConditionalValueMapper(DirectiveValueMapper):
    # Always applied
    active = FieldValue(
        fields=["active"],
        values_list=[True]
    )

    # Conditional filters
    location = FieldValue(
        fields=["location"],
        values_list=["match_params.location"]
    )
    has_discount = FieldValue(
        fields=["discount_percentage"],
        values_list=[]  # FieldExistsDirective doesn't need values
    )
    rating = FieldValue(
        fields=["rating"],
        values_map={"gte": "match_params.min_rating"}
    )

class ConditionalSearchEngine(DirectiveEngine):
    active = ConstMatchDirective(rule=FieldMatchType.ANY)
    location = ConstMatchDirective(rule=FieldMatchType.ANY)
    has_discount = FieldExistsDirective()
    rating = RangeMatchDirective()

    class Config:
        value_mapper = ConditionalValueMapper()

# Engine automatically skips directives when parameters are not provided
engine = ConditionalSearchEngine()
engine.set_match_params({
    "location": "california",
    "min_rating": 4.0
    # Note: has_discount parameter not provided, so that directive won't be applied
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
        "term": {
          "active": {
            "value": true
          }
        }
      },
      {
        "exists": {
          "field": "discount_percentage"
        }
      },
      {
        "term": {
          "location": {
            "value": "california"
          }
        }
      },
      {
        "range": {
          "rating": {
            "gte": 4.0
          }
        }
      }
    ]
  }
}
```

### 4. Complex E-commerce Search Engine

```python
from elastictoolkit.queryutils.builder.booldirective import AndDirective, OrDirective
from elastictoolkit.queryutils.builder.custommatchdirective import CustomMatchDirective

# Complex value mapper for e-commerce
class EcommerceValueMapper(DirectiveValueMapper):
    search_text = FieldValue(
        fields=["name^3", "description", "keywords"],
        values_list=["match_params.search_text"]
    )
    category = FieldValue(
        fields=["category"],
        values_list=["*match_params.categories"]
    )
    brand = FieldValue(
        fields=["brand"],
        values_list=["*match_params.brands"]
    )
    price_range = FieldValue(
        fields=["price"],
        values_map={
            "gte": "match_params.min_price",
            "lte": "match_params.max_price"
        }
    )
    rating = FieldValue(
        fields=["rating"],
        values_map={"gte": "match_params.min_rating"}
    )
    availability = FieldValue(
        fields=["in_stock"],
        values_list=[True]
    )
    on_sale = FieldValue(
        fields=["on_sale"],
        values_list=[True]
    )
    featured = FieldValue(
        fields=["featured"],
        values_list=[True]
    )

# Custom directive for promotional products
class PromotionalProductsDirective(CustomMatchDirective):
    allowed_engine_cls_name = "EcommerceSearchEngine"
    name = "promotional_products"

    def get_directive(self):
        # Only apply if promotional flag is set
        if not self.match_params.get("include_promotional", False):
            return None

        return OrDirective(
            on_sale=ConstMatchDirective(rule=FieldMatchType.ANY),
            featured=ConstMatchDirective(rule=FieldMatchType.ANY)
        )

# Complex e-commerce search engine
class EcommerceSearchEngine(DirectiveEngine):
    # Core search
    search_text = TextMatchDirective(rule=FieldMatchType.ANY)

    # Filters
    category = ConstMatchDirective(rule=FieldMatchType.ANY)
    brand = ConstMatchDirective(rule=FieldMatchType.ANY)
    price_range = RangeMatchDirective()
    rating = RangeMatchDirective()
    availability = ConstMatchDirective(rule=FieldMatchType.ANY)

    # Custom promotional directive
    promotional_products = PromotionalProductsDirective()

    class Config:
        value_mapper = EcommerceValueMapper()

# Complex usage scenario
engine = EcommerceSearchEngine()
engine.set_match_params({
    "search_text": "smartphone",
    "categories": ["electronics", "mobile"],
    "brands": ["apple", "samsung", "google"],
    "min_price": 200,
    "max_price": 1200,
    "min_rating": 4.0,
    "include_promotional": True
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
        "term": {
          "in_stock": {
            "value": true
          }
        }
      },
      {
        "terms": {
          "brand": ["apple", "samsung", "google"]
        }
      },
      {
        "terms": {
          "category": ["electronics", "mobile"]
        }
      },
      {
        "range": {
          "price": {
            "gte": 200,
            "lte": 1200
          }
        }
      },
      {
        "bool": {
          "should": [
            {
              "bool": {
                "filter": [
                  {
                    "term": {
                      "on_sale": {
                        "value": true
                      }
                    }
                  }
                ]
              }
            },
            {
              "bool": {
                "filter": [
                  {
                    "term": {
                      "featured": {
                        "value": true
                      }
                    }
                  }
                ]
              }
            }
          ],
          "_name": "promotional_products"
        }
      },
      {
        "range": {
          "rating": {
            "gte": 4.0
          }
        }
      },
      {
        "multi_match": {
          "query": "smartphone",
          "fields": ["name^3", "description", "keywords"]
        }
      }
    ]
  }
}
```

## Getting Started

1. **Start with the basics**: Review the [Introduction notebook](01_Introduction.ipynb) to understand the fundamental concepts
2. **Try the examples**: Copy and run the examples above to see how DirectiveEngine works
3. **Explore individual notebooks**: Deep dive into specific topics using the notebooks listed in the table of contents
4. **Build your own**: Use the patterns shown here to create custom search engines for your use case

## Key Concepts to Understand

- **DirectiveEngine**: Orchestrates multiple directives to build complex queries
- **DirectiveValueMapper**: Maps directive names to field configurations and values
- **FieldValue**: Defines how fields and values are mapped from parameters
- **Match Parameters**: Runtime values that make queries dynamic and flexible
- **Conditional Logic**: How engines handle optional parameters and skip unused directives

For detailed explanations of these concepts, refer to the [Core Concepts notebook](02_CoreConcepts.ipynb).
