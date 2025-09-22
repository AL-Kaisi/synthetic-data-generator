# Universal Synthetic Data Generator - Complete Guide

A simple, flexible command-line tool that creates realistic test data from ANY JSON schema. No unnecessary complexity - just straightforward data generation that works.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Installation](#installation)
3. [CLI Usage](#cli-usage)
4. [Interactive Mode](#interactive-mode)
5. [Python API](#python-api)
6. [Predefined Schemas](#predefined-schemas)
7. [Custom Schemas](#custom-schemas)
8. [Examples](#examples)
9. [Supported Data Types](#supported-data-types)
10. [Safety Features](#safety-features)
11. [Performance](#performance)
12. [Architecture](#architecture)
13. [Migration Guide](#migration-guide)
14. [Development](#development)

---

## Quick Start

### For Beginners (Interactive Mode)
```bash
python3 cli.py
```
Follow the menu prompts to generate data interactively.

### For Advanced Users (Direct Commands)
```bash
# List available schemas
python3 cli.py list

# Generate 100 e-commerce products
python3 cli.py generate ecommerce_product -n 100

# Generate from your own schema
python3 cli.py from-file my_schema.json -n 50
```

---

## Installation

**No installation required!** This tool uses only Python standard library.

```bash
# Optional: Install testing dependencies
pip install -r requirements.txt
```

### Requirements
- Python 3.6 or higher
- No external dependencies for core functionality

---

## CLI Usage

### Interactive Mode (Recommended for Beginners)
```bash
python3 cli.py
```

This provides a menu-driven interface with options:
1. Generate from predefined schema
2. Generate from custom JSON schema
3. Create schema interactively
4. List available schemas
5. Exit

### Direct Commands
```bash
# List all available schemas with details
python3 cli.py list

# Generate data from predefined schema (JSON format)
python3 cli.py generate ecommerce_product -n 100 -o products.json

# Generate data in CSV format
python3 cli.py generate ecommerce_product -n 100 --format csv -o products.csv

# Generate data in JSON-LD format
python3 cli.py generate ecommerce_product -n 100 --format jsonld -o products.jsonld

# Generate from custom schema file
python3 cli.py from-file my_schema.json -n 50 -o output.json --format csv

# Generate from JSON string
python3 cli.py from-json '{"type":"object","properties":{"id":{"type":"string"}}}' -n 10 --format csv

# Interactive schema builder
python3 cli.py create

# Get help
python3 cli.py --help
python3 cli.py generate --help
```

### Legacy Commands (Still Supported)
```bash
python3 generate_data.py list
python3 generate_data.py generate healthcare_patient -n 100
python3 generate_data.py from-file schema.json -n 50
python3 generate_data.py create
```

---

## Interactive Mode

The interactive mode provides a guided experience:

### 1. Predefined Schema Selection
- Browse available schemas with descriptions
- See field counts and sample field names
- Specify number of records and output file

### 2. Custom Schema Input
- Paste multi-line JSON schema
- Automatic validation and error reporting
- Generate data immediately after validation

### 3. Interactive Schema Builder
- Step-by-step field addition
- Type selection with constraints
- Required field specification
- Save schema for reuse
- Generate sample data

### 4. Schema Listing
- View all available predefined schemas
- See field counts and sample fields
- Get schema descriptions

---

## Python API

```python
from simple_generator import SchemaDataGenerator
from schemas import SchemaLibrary

# Create generator - that's it!
generator = SchemaDataGenerator()

# Use predefined schema
schemas = SchemaLibrary.get_all_schemas()
data = generator.generate_from_schema(schemas["ecommerce_product"], 100)

# Use custom schema
custom_schema = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string", "maxLength": 50},
        "price": {"type": "number", "minimum": 0, "maximum": 1000}
    },
    "required": ["id", "name"]
}
data = generator.generate_from_schema(custom_schema, 50)

# Convert to different formats
json_ld = generator.to_json_ld(data, custom_schema)
csv_data = generator.to_csv(data)

# Save in different formats
generator.save_data(data, "output.json", "json")
generator.save_data(data, "output.csv", "csv")
generator.save_data(data, "output.jsonld", "jsonld", custom_schema)
```

---

## Predefined Schemas

### Business Schemas
- **ecommerce_product** - Product catalogs with pricing, inventory, ratings (20 fields)
- **financial_transaction** - Banking transactions with fraud detection fields (21 fields)
- **hr_employee** - Employee records with departments, salaries, benefits (26 fields)

### Healthcare & Education
- **healthcare_patient** - Medical records with allergies, medications, vitals (27 fields)
- **education_student** - Academic records with grades, courses, enrollment (25 fields)

### Technical Schemas
- **iot_sensor** - Device readings with timestamps, locations, statuses (17 fields)
- **social_media_post** - Posts with engagement metrics, hashtags, mentions (21 fields)

### Data Engineering Schemas (Perfect for Senior Engineer Demos)
- **data_pipeline_metadata** - ETL/ELT pipeline execution metrics with lineage tracking (29 fields)
- **ml_model_training** - Machine learning training jobs with performance metrics (39 fields)
- **distributed_system_metrics** - Microservice monitoring with SLA compliance (33 fields)
- **real_time_analytics** - Streaming event processing with fraud detection (37 fields)
- **data_warehouse_table** - Table metadata with data quality and lineage (42 fields)
- **cloud_infrastructure** - AWS/Azure/GCP resource monitoring and cost tracking (44 fields)

---

## Custom Schemas

### Schema Format
Schemas follow JSON Schema specification with additional constraints:

```json
{
  "type": "object",
  "title": "MySchema",
  "properties": {
    "id": {"type": "string"},
    "name": {"type": "string", "maxLength": 50},
    "age": {"type": "integer", "minimum": 0, "maximum": 120},
    "score": {"type": "number", "minimum": 0.0, "maximum": 100.0, "decimalPlaces": 2},
    "active": {"type": "boolean"},
    "tags": {"type": "array", "items": {"type": "string"}},
    "created": {"type": "string", "start": "2020-01-01", "end": "2024-12-31"},
    "status": {"type": "string", "enum": ["pending", "active", "completed"]}
  },
  "required": ["id", "name"]
}
```

### Custom Constraints
- `minimum` / `maximum`: Numeric ranges
- `minLength` / `maxLength`: String length limits
- `pattern`: Regex patterns for strings
- `enum`: Fixed set of values
- `start` / `end`: Date ranges
- `decimalPlaces`: Precision for numbers
- `minItems` / `maxItems`: Array size limits

### Schema Builder
Use the interactive schema builder to create schemas without writing JSON:

```bash
python3 cli.py create
```

This guides you through:
- Setting schema title
- Adding fields with types
- Configuring constraints
- Marking required fields
- Saving for reuse

---

## Output Formats

The generator supports multiple output formats:

### JSON (Default)
```bash
python3 cli.py generate ecommerce_product -n 5 --format json
```
Standard JSON format with proper formatting and indentation.

### CSV
```bash
python3 cli.py generate ecommerce_product -n 5 --format csv
```
Comma-separated values format. Nested objects and arrays are serialized as JSON strings within CSV cells.

### JSON-LD
```bash
python3 cli.py generate ecommerce_product -n 5 --format jsonld
```
JSON-LD (Linked Data) format with semantic context mapping to schema.org vocabulary.

### Format Usage Examples
```bash
# Generate data engineering pipeline metrics as CSV for Excel/analysis
python3 cli.py generate data_pipeline_metadata -n 100 --format csv -o pipeline_metrics.csv

# Generate ML training data as JSON-LD for semantic web applications
python3 cli.py generate ml_model_training -n 50 --format jsonld -o ml_training.jsonld

# Generate customer data as JSON for APIs and applications
python3 cli.py generate ecommerce_product -n 1000 --format json -o products.json
```

---

## Examples

### Generate Customer Data
```bash
python3 cli.py from-json '{
  "type": "object",
  "properties": {
    "customer_id": {"type": "string"},
    "email": {"type": "string"},
    "age": {"type": "integer", "minimum": 18, "maximum": 80},
    "total_spent": {"type": "number", "minimum": 0, "maximum": 10000, "decimalPlaces": 2}
  },
  "required": ["customer_id", "email"]
}' -n 100 -o customers.json
```

### Generate IoT Sensor Data
```bash
python3 cli.py generate iot_sensor -n 1000 -o sensor_readings.json
```

### Custom Person Schema File
Create `person_schema.json`:
```json
{
  "type": "object",
  "title": "Person",
  "properties": {
    "id": {"type": "string"},
    "first_name": {"type": "string", "maxLength": 50},
    "last_name": {"type": "string", "maxLength": 50},
    "email": {"type": "string"},
    "age": {"type": "integer", "minimum": 18, "maximum": 65},
    "city": {"type": "string"},
    "active": {"type": "boolean"}
  },
  "required": ["id", "first_name", "last_name", "email"]
}
```

Generate data:
```bash
python3 cli.py from-file person_schema.json -n 50 -o people.json
```

---

## Supported Data Types

### Core Types
- **string**: Text with optional constraints (length, pattern, enum)
- **integer**: Whole numbers with min/max ranges
- **number**: Decimal numbers with precision control
- **boolean**: True/false values
- **array**: Lists of items
- **object**: Nested objects
- **date**: Date strings with custom ranges

### Smart Field Detection
The generator automatically generates appropriate data based on field names:
- `email` → realistic email addresses
- `phone` → phone numbers
- `first_name`, `last_name` → real names
- `city` → city names
- `company` → company names
- `url`, `website` → URLs
- `*_id` → UUIDs

### Special Patterns
- **NINO**: UK National Insurance Numbers (test-safe prefixes only)
- **SKU**: Product codes with patterns
- **Sort codes**: Banking sort codes
- **Account numbers**: Banking account numbers
- **Postcodes**: UK postcodes

---

## Safety Features

### UK National Insurance Numbers (NINO)
For sensitive data like UK National Insurance Numbers, the generator automatically uses test-safe prefixes that cannot be real:

- **Safe Prefixes**: TN, BG, GB, NK, KN, ZZ, AA, AB, AO, FY, NY, OA, PO, OP
- **Automatic Detection**: Any field containing "nino" gets safe treatment
- **Correct Format**: Follows NINO pattern but impossible to be real

Example safe NINO: `TN123456A` (TN prefix = test-only)

### Safety Validation
```python
# Test NINO safety
generator = SchemaDataGenerator()
data = generator.generate_from_schema(nino_schema, 100)

for record in data:
    nino = record.get('nino', '')
    if nino and not nino.startswith(('BG', 'GB', 'NK', 'KN', 'TN', 'NT', 'ZZ')):
        print(f'WARNING: Unsafe NINO: {nino}')
        break
else:
    print('SUCCESS: All NINOs are test-safe!')
```

---

## Performance

The simplified generator is fast and efficient:

- **10K records**: ~1 second
- **100K records**: ~10 seconds
- **1M records**: ~2 minutes

Actual performance depends on schema complexity and hardware.

### Performance Testing
```bash
# Run performance benchmarks
python3 performance_test.py
```

This tests all predefined schemas with various record counts and provides detailed metrics.

---

## Architecture

The generator uses a straightforward approach:

```
Schema → DataGenerator → Generated Data
```

### Key Components
- **DataGenerator**: Single class that handles all field types
- **SchemaDataGenerator**: Orchestrates record generation
- **Direct Type Mapping**: Each type has a simple generation method

### Why Simple?
This tool was originally built with 9 design patterns but was simplified because:

**Removed Complexity:**
- Abstract Factory Pattern → Direct instantiation
- Chain of Responsibility → Simple type mapping
- Singleton Registry → Not needed for this use case
- Template Method Pattern → Direct method calls
- Multiple inheritance layers → Single focused class

**Benefits of Simplification:**
- Easier to understand - Single file, clear logic flow
- Easier to modify - Direct methods, no abstraction layers
- Easier to debug - Straightforward call stack
- Faster performance - No factory overhead
- Less code - 50% reduction in lines of code
- Same functionality - All features preserved

### Extending the Generator
```python
from simple_generator import DataGenerator

# Extend the DataGenerator class
class MyCustomGenerator(DataGenerator):
    def generate_string(self, field_name: str, schema: Dict) -> str:
        # Add your custom string generation logic
        if "product_code" in field_name.lower():
            return f"PROD-{random.randint(1000, 9999)}"

        # Fall back to default behavior
        return super().generate_string(field_name, schema)

# Use your custom generator
from simple_generator import SchemaDataGenerator

class MySchemaGenerator(SchemaDataGenerator):
    def __init__(self):
        super().__init__()
        self.generator = MyCustomGenerator()
```

---

## Migration Guide

### From Web Interface
**Before (Web):**
1. Start server: `python3 app.py`
2. Open browser: `http://localhost:5000`
3. Click through UI

**After (CLI):**
1. Run: `python3 cli.py`
2. Follow menu prompts

### From API Calls
**Before:**
```bash
curl -X POST localhost:5000/api/generate -d '...'
```

**After:**
```bash
python3 cli.py generate ecommerce_product -n 100 -o output.json
```

### Python API (Unchanged)
```python
from simple_generator import SchemaDataGenerator
generator = SchemaDataGenerator()
data = generator.generate_from_schema(schema, 100)
```

### Benefits of CLI-Only Approach
1. **Zero Dependencies**: No Flask, no web server setup
2. **Scriptable**: Easy to integrate into build pipelines
3. **Portable**: Works anywhere Python runs
4. **Fast**: No web browser or HTTP overhead
5. **Secure**: No web attack surface
6. **Simple**: One command to run, no server management

---

## Development

### Project Structure
```
synthetic-data-generator/
├── cli.py                     # Interactive CLI interface
├── simple_generator.py        # Core data generation engine
├── schemas.py                 # Schema library with templates
├── generate_data.py           # Legacy CLI commands
├── dwp_schemas.py            # DWP-specific schemas
├── performance_test.py       # Performance benchmarking
├── test_runner.py            # Test suite runner
├── conftest.py               # Test configuration
├── features/                 # BDD test scenarios
├── requirements.txt          # Optional dependencies
└── COMPLETE_GUIDE.md         # This documentation
```

### Running Tests
```bash
# Run all tests
python3 test_runner.py

# Run performance tests
python3 performance_test.py

# Test specific functionality
python3 -m pytest features/ -v
```

### Code Quality
The codebase follows these principles:
- **Simplicity over complexity**: Direct implementation preferred
- **Readability over cleverness**: Clear code over compact code
- **Functionality over patterns**: Solve real problems, not theoretical ones
- **Documentation**: Comprehensive guides and examples

### Contributing
1. Keep the simple architecture - don't add unnecessary complexity
2. Include tests for new features
3. Update documentation for changes
4. Follow existing code style
5. Ensure safety features remain intact

### Git Workflow
```bash
# Current status
git status              # Clean working tree

# Make changes and commit
git add .
git commit -m "Description of changes"

# Push to GitHub
git push origin main
```

---

## Use Cases

### Testing
Generate realistic test data for applications without using production data.

### Development
Populate development databases with consistent, repeatable data.

### Demos
Create impressive demo data for presentations and POCs.

### Training
Generate training datasets for machine learning or data analysis.

### Compliance
Ensure no real personal data in test environments with built-in safety features.

### CI/CD
Integrate into build pipelines for automated testing with fresh data.

---

## Troubleshooting

### Common Issues

**Error: "No module named 'simple_generator'"**
- Make sure you're running from the project directory
- Check that `simple_generator.py` exists

**Error: "Invalid JSON"**
- Validate your JSON schema using an online validator
- Check for missing commas, brackets, or quotes

**Empty/Weird Data Generated**
- Check field names for automatic type detection
- Verify schema constraints are reasonable
- Use enum values for specific options

**Performance Issues**
- Reduce number of records for testing
- Simplify schema complexity
- Check available memory

### Getting Help
- Run `python3 cli.py --help` for command overview
- Run `python3 cli.py <command> --help` for specific help
- Use interactive mode for guided experience: `python3 cli.py`
- Check examples in this guide

---

## License

MIT License - Use freely in your projects.

---

## Contributing

Contributions welcome! Please:
1. Maintain the simple architecture
2. Include tests for new features
3. Update documentation
4. Follow existing patterns

---

*This tool focuses on what it does best: generating realistic synthetic data from any JSON schema through a simple, powerful command-line interface.*