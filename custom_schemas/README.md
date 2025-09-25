# Custom Schemas Guide

This directory contains custom schemas and relational schemas that extend the core functionality of the Synthetic Data Generator.

## Schema Types Supported

### 1. Regular JSON Schemas
Standard JSON Schema files that define single table/entity structures.

**File format:** `schema_name.json`
**Schema type:** `"type": "object"`

Example:
```json
{
  "type": "object",
  "title": "Company",
  "properties": {
    "company_id": {"type": "string"},
    "name": {"type": "string"},
    "industry": {"type": "string"}
  },
  "required": ["company_id", "name"]
}
```

### 2. Relational Schemas (NEW!)
Define multiple related entities with foreign key relationships.

**File format:** `relational_schema_name.json`
**Schema type:** `"type": "relational"`

## Relational Schema Structure

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "relational",
  "title": "Your Relational Schema Title",
  "description": "Description of the relational data structure",
  "schemas": {
    "parent_table": {
      "type": "object",
      "title": "ParentEntity",
      "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"}
      },
      "count": 100
    },
    "child_table": {
      "type": "object",
      "title": "ChildEntity",
      "properties": {
        "id": {"type": "string"},
        "parent_id": {"type": "string"},
        "data": {"type": "string"}
      },
      "count": 300
    }
  },
  "relationships": [
    {
      "parent": "parent_table",
      "parent_key": "id",
      "child": "child_table",
      "child_key": "parent_id",
      "type": "one_to_many",
      "description": "Each parent can have multiple children"
    }
  ]
}
```

## Using Custom Schemas

### Auto-Discovery
1. Place your schema file in this `custom_schemas/` directory
2. The app automatically detects it on next run
3. Use it like any built-in schema

### CLI Usage
```bash
# Interactive mode - option 4 for relational schemas
python3 cli.py interactive

# List schemas (includes custom ones)
python3 cli.py list

# Generate from custom schema
python3 cli.py generate your_custom_schema -n 100
```

## DWP Relational Example

The included `dwp_relational_example.json` demonstrates a complex relational structure for UK benefits data:

- **Citizens** (base table)
- **Households** (linked to citizens)
- **Benefit Claims** (linked to both citizens and households)

This creates realistic relationships where:
- Each household has a head of household (citizen)
- Citizens can have multiple benefit claims
- Claims are associated with both the claimant and their household

## Benefits of Relational Schemas

1. **Data Integrity**: Maintains foreign key relationships
2. **Realistic Test Data**: Proper referential relationships
3. **Complex Scenarios**: Multi-table data generation
4. **DWP Compliance**: Safe test data for benefits systems

## File Formats Supported

| Format | Extension | Example |
|--------|-----------|---------|
| JSON Schema | `.json` | `user.json` |
| YAML Schema | `.yaml/.yml` | `product.yaml` |
| Python Schema | `.py` | `order_schema.py` |

## Advanced Features

### Schema Validation
- Automatic validation of schema structure
- Clear error messages for malformed schemas
- Support for JSON Schema draft-07 features

### Data Coherence
- Names and emails are matched (john.doe@email.com)
- Realistic skills and certifications
- Context-aware field generation
- Proper date relationships

### Performance
- Efficient relational generation
- Batch processing for large datasets
- Memory-optimized for complex relationships

## Examples

See the included example files:
- `customer.json` - Simple customer schema
- `blog_post.json` - Blog post with metadata
- `dwp_relational_example.json` - Complex DWP benefits relationships

For more complex schemas, refer to the main schemas in the parent directory or create your own following these patterns.

## Regular Schema Auto-Discovery

### How it Works
1. **Add Schema File**: Drop any valid JSON schema file (`.json`) into this directory
2. **Auto-Discovery**: The app automatically scans this directory and loads all schema files
3. **Use Schema**: The schema becomes available using the filename (without `.json` extension)

### Example
If you add a file called `my_custom_schema.json`, you can use it with:

```bash
python3 cli.py generate my_custom_schema -n 100
```

### Schema File Format
Your schema files must follow JSON Schema specification:

```json
{
  "type": "object",
  "title": "MySchema",
  "properties": {
    "id": {"type": "string"},
    "name": {"type": "string", "maxLength": 50},
    "age": {"type": "integer", "minimum": 0, "maximum": 120}
  },
  "required": ["id", "name"]
}
```

### Supported Constraints
- **String**: `maxLength`, `pattern`, `enum`
- **Number/Integer**: `minimum`, `maximum`, `decimalPlaces`
- **Date strings**: `start`, `end` (e.g., "2020-01-01", "2024-12-31")
- **Arrays**: `items`, `minItems`, `maxItems`
- **Objects**: Nested properties
- **Required fields**: `required` array

### Usage Examples
```bash
# List all schemas (including custom ones)
python3 cli.py list

# Generate from custom schema
python3 cli.py generate customer -n 500 --format csv

# Generate massive dataset with PySpark
python3 cli.py generate blog_post -n 1000000 --spark --format parquet

# Use with different output formats
python3 cli.py generate my_schema -n 100 --format jsonld
```

Simply add your schema files here and they'll be automatically available!