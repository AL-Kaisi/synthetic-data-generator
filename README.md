# Universal Synthetic Data Generator

A simple, flexible command-line tool that creates realistic test data from ANY JSON schema. No unnecessary complexity - just straightforward data generation that works.

**New to this tool? Start with the [Getting Started Guide](GETTING_STARTED.md)**

## Key Features

- **Universal Schema Support**: Works with ANY valid JSON schema - no restrictions!
- **Multiple Data Types**: Strings, numbers, integers, booleans, arrays, objects, dates
- **Predefined Templates**: E-commerce, healthcare, finance, education, HR, IoT, social media
- **Simple Architecture**: Direct, easy-to-understand code
- **High Performance**: 10K+ records/second
- **Multiple Output Formats**: JSON, CSV, JSON-LD
- **Interactive CLI**: Menu-driven command-line interface
- **Batch Processing**: Scriptable command-line operations
- **DWP Safety Compliant**: Optional safety mode for sensitive data
- **Production Ready**: Reliable and well-tested

## Quick Start

### CLI Usage

```bash
# Interactive mode (recommended for beginners)
python3 cli.py

# Direct commands
python3 cli.py list                                    # List schemas
python3 cli.py generate ecommerce_product -n 100      # Generate data
python3 cli.py from-file my_schema.json -n 50         # From file
python3 cli.py create                                  # Schema builder

# Legacy commands (still supported)
python3 generate_data.py list
python3 generate_data.py generate healthcare_patient -n 100
```

### Interactive CLI

```bash
# Run interactive mode
python3 cli.py

# Or use specific commands directly
python3 cli.py generate ecommerce_product -n 100
```

### Python API

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
    }
}
data = generator.generate_from_schema(custom_schema, 50)
```

## Predefined Schemas

### Business Schemas
- **E-commerce Product** - Product catalogs with pricing, inventory, ratings
- **Financial Transaction** - Banking transactions with fraud detection fields
- **HR Employee** - Employee records with departments, salaries, benefits

### Healthcare & Education
- **Healthcare Patient** - Medical records with allergies, medications, vitals
- **Education Student** - Academic records with grades, courses, enrollment

### Technical Schemas
- **IoT Sensor Data** - Device readings with timestamps, locations, statuses
- **Social Media Post** - Posts with engagement metrics, hashtags, mentions

### Government Schemas (DWP)
- **Child Benefit** - Child benefit claims
- **State Pension** - State pension records
- **Universal Credit** - Universal credit claims
- **ESA** - Employment Support Allowance
- **PIP** - Personal Independence Payment

## Safety Features

For sensitive data like UK National Insurance Numbers (NINOs), the generator automatically uses test-safe prefixes that cannot be real (TN, BG, ZZ, etc.). Any field containing "nino" in the name gets this safe treatment automatically.

## Simple Architecture

The generator uses a straightforward approach:

```
Schema → DataGenerator → Generated Data
```

- **DataGenerator**: Single class that handles all field types
- **SchemaDataGenerator**: Orchestrates record generation
- **Direct Type Mapping**: Each type has a simple generation method

## Performance

The simplified generator is fast and efficient:

- **10K records**: ~1 second
- **100K records**: ~10 seconds
- **1M records**: ~2 minutes

Actual performance depends on schema complexity and your hardware.

## Testing

### Run All Tests
```bash
python test_runner.py
```

### BDD Features
- NINO safety validation
- Schema compliance testing
- Performance requirements
- Error handling scenarios
- Business rule validation

### Safety Validation
```bash
# Specific safety tests
python -c "
from schema_data_generator import SchemaDataGenerator
from dwp_schemas import dwp_schemas

generator = SchemaDataGenerator(domain='dwp')
data = generator.generate_from_schema(dwp_schemas['child_benefit'], 100)

for record in data:
    nino = record.get('nino', '')
    if nino and not nino.startswith(('BG', 'GB', 'NK', 'KN', 'TN', 'NT', 'ZZ', 'AA', 'AB', 'AO', 'FY', 'NY', 'OA', 'PO', 'OP')):
        print(f'WARNING: Unsafe NINO: {nino}')
        break
else:
    print('SUCCESS: All NINOs are test-safe!')
"
```

## Custom Schemas

```python
custom_schema = {
    "type": "object",
    "title": "Employee",
    "properties": {
        "employee_id": {"type": "string"},
        "nino": {"type": "string", "pattern": "^[A-Z]{2}[0-9]{6}[A-D]$"},
        "first_name": {"type": "string", "maxLength": 30},
        "salary": {"type": "integer", "minimum": 20000, "maximum": 80000},
        "department": {"type": "string", "enum": ["IT", "HR", "Finance"]},
        "start_date": {"type": "string", "start": "2020-01-01", "end": "2024-12-31"},
        "email": {"type": "string"},
        "active": {"type": "boolean"}
    },
    "required": ["employee_id", "nino", "first_name"]
}

data = generator.generate_from_schema(custom_schema, 500)
```

## Documentation

- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Detailed system architecture
- **[CODE_EXPLANATION.md](CODE_EXPLANATION.md)** - Step-by-step code walkthrough
- **[PATTERN_DIAGRAM.md](PATTERN_DIAGRAM.md)** - Visual design pattern diagrams

## Extending the Generator

The simple generator is easy to extend:

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

## Supported Data Types

- **string**: Text with optional constraints (length, pattern, enum)
- **integer**: Whole numbers with min/max ranges
- **number**: Decimal numbers with precision control
- **boolean**: True/false values
- **array**: Lists of items
- **object**: Nested objects
- **date**: Date strings with custom ranges

Smart field detection automatically generates appropriate data based on field names (email, phone, first_name, etc.).

## Performance Testing

```bash
# Run performance tests
python3 performance_test.py
```

## Use Cases

- **Testing**: Generate realistic test data for applications
- **Development**: Populate development databases
- **Demo**: Create demo data for presentations
- **Training**: Generate training datasets
- **Compliance**: Ensure no real personal data in test environments

## Safety Guarantees

1. **NINO Safety**: Cannot generate real National Insurance Numbers
2. **Pattern Validation**: All generated data follows specified patterns
3. **Business Rules**: DWP-specific rules prevent realistic personal data
4. **Automatic Detection**: Field name recognition applies appropriate safety
5. **Validation Chain**: Multiple validation layers ensure compliance

## Contributing

1. All contributions must maintain DWP safety standards
2. New generators must implement the DataGenerator interface
3. Include BDD tests for new features
4. Update documentation for new patterns or schemas

## License

Enterprise-grade data generator for DWP compliance testing.

---

**Remember**: This tool is designed to be safe for testing. All generated personal data (especially NINOs) uses invalid patterns that cannot correspond to real individuals.# synthetic-data-generator
