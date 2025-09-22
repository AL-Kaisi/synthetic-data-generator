# Universal Synthetic Data Generator

A simple, flexible command-line tool that creates realistic test data from ANY JSON schema. No unnecessary complexity - just straightforward data generation that works.

📖 **[Complete Documentation & Guide](COMPLETE_GUIDE.md)**

## Quick Start

### Interactive Mode (Recommended)
```bash
python3 cli.py
```

### Direct Commands
```bash
# List available schemas
python3 cli.py list

# Generate 100 e-commerce products as JSON (default)
python3 cli.py generate ecommerce_product -n 100

# Generate as CSV format
python3 cli.py generate ecommerce_product -n 100 --format csv

# Generate as JSON-LD format
python3 cli.py generate ecommerce_product -n 100 --format jsonld

# Generate from your own schema
python3 cli.py from-file my_schema.json -n 50 --format csv
```

## Key Features

- **Universal Schema Support**: Works with ANY valid JSON schema
- **Zero Dependencies**: Pure Python standard library
- **Interactive CLI**: Menu-driven interface for beginners
- **13+ Predefined Templates**: E-commerce, healthcare, finance, education, HR, IoT, social media, data engineering
- **Smart Data Generation**: Context-aware realistic data
- **Safety Features**: Test-safe generation for sensitive data (NINO, etc.)
- **High Performance**: 10K+ records/second
- **Multiple Output Formats**: JSON, CSV, JSON-LD

## Python API

```python
from simple_generator import SchemaDataGenerator

generator = SchemaDataGenerator()
data = generator.generate_from_schema(your_schema, 100)
```

## Examples

```bash
# Generate healthcare patient records as JSON
python3 cli.py generate healthcare_patient -n 50

# Generate pipeline metadata as CSV
python3 cli.py generate data_pipeline_metadata -n 100 --format csv

# Generate from custom schema file as JSON-LD
python3 cli.py from-file person_schema.json -n 100 --format jsonld

# Interactive schema builder
python3 cli.py create
```

## Documentation

- **[Complete Guide](COMPLETE_GUIDE.md)** - Full documentation with examples
- **[Requirements](requirements.txt)** - Optional testing dependencies

## Installation

No installation required! Uses only Python standard library.

```bash
# Optional: Install testing dependencies
pip install -r requirements.txt
```

## License

MIT License - Use freely in your projects.

---

*Generate realistic test data from any JSON schema through a simple, powerful command-line interface.*