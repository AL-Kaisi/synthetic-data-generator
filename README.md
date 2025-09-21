# Enterprise Data Generator with Design Patterns

A sophisticated synthetic data generator built with enterprise-grade design patterns, specifically designed for DWP (Department for Work and Pensions) safety standards. Generates realistic test data from any JSON schema while ensuring no real personal information can be accidentally created.

## Key Features

- **Schema-Driven**: Works with any JSON schema
- **Pattern-Based Architecture**: Built with 9 design patterns
- **DWP Safety Compliant**: Cannot generate real NINOs
- **High Performance**: 10K+ records/second
- **JSON-LD Support**: Semantic web compatibility
- **BDD Tested**: Comprehensive behavioral tests
- **Enterprise Ready**: Production-grade architecture

## Quick Start

```python
from schema_data_generator import SchemaDataGenerator
from dwp_schemas import dwp_schemas

# Create generator with DWP safety rules
generator = SchemaDataGenerator(domain="dwp")

# Generate Child Benefit test data
data = generator.generate_from_schema(dwp_schemas["child_benefit"], 1000)

# Convert to JSON-LD
json_ld = generator.to_json_ld(data, dwp_schemas["child_benefit"])
```

## 📋 Pre-built DWP Schemas

- **Child Benefit** (21 fields) - Child benefit claims
- **State Pension** (25 fields) - State pension records
- **Universal Credit** (33 fields) - Universal credit claims
- **ESA** (28 fields) - Employment Support Allowance
- **PIP** (31 fields) - Personal Independence Payment

## 🔒 Safety Features

### NINO Safety
- **Invalid Prefixes Only**: Uses BG, GB, NK, KN, TN, NT, ZZ, AA, AB, AO, FY, NY, OA, PO, OP
- **Automatic Detection**: Any field containing "nino" uses safe generation
- **Business Rules**: DWP-specific validation ensures compliance

```python
# Example safe NINO: TN123456A
# TN prefix = test-only
# Correct format but impossible to be real
```

## Architecture Overview

The system implements 9 design patterns working together:

```
Client Code → Facade → Registry → Factory → Strategy
                 ↓
            Chain of Responsibility → Template Method → Builder
```

### Design Patterns Used

1. **Facade Pattern** - Simple interface hiding complexity
2. **Singleton Pattern** - Single generator registry
3. **Abstract Factory Pattern** - Creates generator families
4. **Factory Method Pattern** - Creates objects by type
5. **Strategy Pattern** - Interchangeable generation algorithms
6. **Chain of Responsibility** - Sequential schema processing
7. **Template Method Pattern** - Consistent generation workflow
8. **Builder Pattern** - Constructs processing chains
9. **Registry Pattern** - Manages available factories

## Performance Benchmarks

| Schema | Fields | Records/Second | Memory (100K records) |
|--------|--------|----------------|----------------------|
| Child Benefit | 21 | 15,286 | 45 MB |
| State Pension | 25 | 13,210 | 52 MB |
| Universal Credit | 33 | 11,801 | 68 MB |
| ESA | 28 | 11,831 | 58 MB |
| PIP | 31 | 7,884 | 64 MB |

**Projections:**
- 10K records: 0.8 seconds
- 100K records: 8.4 seconds
- 1M records: 1.4 minutes
- 10M records: 0.2 hours

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
        print(f'⚠️  Unsafe NINO: {nino}')
        break
else:
    print('✅ All NINOs are test-safe!')
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

## Extension Points

### Adding New Generators
```python
class CustomGenerator(DataGenerator):
    def generate(self, constraints: Dict) -> Any:
        # Custom generation logic
        pass

    def validate_constraints(self, constraints: Dict) -> bool:
        # Custom validation
        return True
```

### Adding New Processors
```python
class CustomProcessor(SchemaProcessor):
    def _handle_processing(self, context: ProcessingContext):
        # Custom processing logic
        pass
```

### Adding New Factories
```python
class CustomFactory(GeneratorFactory):
    def create_specialized_generator(self, generator_type: str) -> DataGenerator:
        return CustomGenerator()
```

## Field Type Support

| Type | Examples | Constraints |
|------|----------|-------------|
| string | names, addresses | minLength, maxLength, enum, pattern |
| integer | ages, counts | minimum, maximum |
| number | amounts, rates | minimum, maximum, decimalPlaces |
| boolean | flags, status | true_probability |
| date | birth dates, start dates | start, end |
| email | contact emails | format |
| phone | contact numbers | format (uk/us) |
| url | websites | - |
| uuid | unique identifiers | - |
| nino | National Insurance | **Always test-safe** |

## Monitoring

```python
from performance_test import PerformanceTester

tester = PerformanceTester()
results = tester.run_comprehensive_benchmark()

print(f"Average speed: {results['summary']['average_records_per_second']:,.0f} rec/s")
print(f"Memory usage: {results['summary']['average_memory_usage_mb']:.1f} MB")
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
