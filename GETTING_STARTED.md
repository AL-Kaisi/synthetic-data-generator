# Getting Started with Synthetic Data Generator

This guide will help you quickly start generating synthetic data using the command-line interface.

## Quick Start

### 1. Interactive Mode (Recommended for Beginners)

Run the interactive CLI to get started:

```bash
python3 cli.py
```

This will show you a menu with options:
1. Generate from predefined schema
2. Generate from custom JSON schema
3. Create schema interactively
4. List available schemas
5. Exit

### 2. Direct Commands

If you know what you want, use direct commands:

```bash
# List all available schemas
python3 cli.py list

# Generate 100 e-commerce products
python3 cli.py generate ecommerce_product -n 100

# Generate from your own schema file
python3 cli.py from-file my_schema.json -n 50

# Create a schema interactively
python3 cli.py create
```

## Examples

### Generate Sample Data

```bash
# Generate 10 healthcare patient records
python3 cli.py generate healthcare_patient -n 10 -o patients.json

# Generate 50 financial transactions
python3 cli.py generate financial_transaction -n 50

# Generate IoT sensor data
python3 cli.py generate iot_sensor -n 100 -o sensor_data.json
```

### Custom Schema Example

Create a file called `person_schema.json`:

```json
{
  "type": "object",
  "title": "Person",
  "properties": {
    "id": {"type": "string"},
    "name": {"type": "string", "maxLength": 50},
    "age": {"type": "integer", "minimum": 18, "maximum": 65},
    "email": {"type": "string"},
    "active": {"type": "boolean"}
  },
  "required": ["id", "name", "age"]
}
```

Then generate data:

```bash
python3 cli.py from-file person_schema.json -n 20
```

### Interactive Schema Builder

For a guided experience creating your own schema:

```bash
python3 cli.py create
```

This will walk you through:
- Setting a schema title
- Adding fields with types and constraints
- Marking fields as required
- Saving the schema for reuse
- Generating sample data

## Available Schema Types

The generator includes ready-to-use schemas for:

- **ecommerce_product**: Product catalogs
- **healthcare_patient**: Medical records
- **financial_transaction**: Banking data
- **education_student**: Academic records
- **hr_employee**: Employee information
- **iot_sensor**: Sensor readings
- **social_media_post**: Social media data

## Output Formats

Generated data is saved as JSON by default. The data includes realistic values based on field names and constraints.

## Next Steps

- Check out the full [README.md](README.md) for advanced features
- Look at [SIMPLIFICATION.md](SIMPLIFICATION.md) to understand the architecture
- Run tests with `python3 test_runner.py`

## Need Help?

Run any command with `--help` for detailed usage:

```bash
python3 cli.py --help
python3 cli.py generate --help
```