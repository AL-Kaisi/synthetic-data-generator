# Universal Synthetic Data Generator

A powerful, flexible command-line tool that creates realistic test data from ANY JSON schema. Built with simplicity in mind, enhanced with Faker library for professional-quality synthetic data generation.

📖 **[Complete Documentation & Guide](COMPLETE_GUIDE.md)**

## Key Features

- **Universal Schema Support**: Works with ANY valid JSON schema
- **Faker-Powered**: Generates realistic, coherent data with proper names, emails, addresses, skills, and more
- **Interactive CLI**: User-friendly menu-driven interface
- **20+ Predefined Templates**: E-commerce, healthcare, finance, education, HR, IoT, social media, data engineering, DWP
- **Auto-Discovery**: Drop JSON/YAML schema files into `custom_schemas/` directory - instantly available
- **Context-Aware Generation**: Emails match names, realistic skills and certifications
- **Multiple Output Formats**: JSON, CSV, JSON-LD, Parquet (with PySpark)
- **Massive Dataset Support**: Generate billions of records using PySpark
- **High Performance**: 10K-20K+ records/second (standard), millions with PySpark
- **Safety Features**: Test-safe generation for sensitive data (NINO, etc.)

## Installation

```bash
# Basic installation (includes Faker)
pip3 install faker

# Optional: For massive datasets (PySpark)
pip3 install pyspark

# Optional: For testing
pip3 install -r requirements.txt
```

## Quick Start

### Interactive Mode (Easiest)
```bash
python3 cli.py
```
Then follow the menu to:
1. Choose from 20+ predefined schemas
2. Load your custom schema file
3. Build a schema interactively
4. Select output format and filename

### Command Line Mode
```bash
# List all available schemas
python3 cli.py list

# Generate 100 records using a predefined schema
python3 cli.py generate hr_employee -n 100

# Generate with specific output format
python3 cli.py generate ecommerce_product -n 1000 --format csv
```

## Usage Examples

### 1. Basic Data Generation

```bash
# Generate employee records as JSON (default)
python3 cli.py generate hr_employee -n 50

# Output sample:
# {
#   "first_name": "Sarah",
#   "last_name": "Johnson",
#   "email": "sarah.johnson@gmail.com",  # Email matches name!
#   "skills": ["Python", "Docker", "AWS", "Leadership"],  # Real skills!
#   "certifications": ["AWS Certified Solutions Architect", "Scrum Master"]
# }
```

### 2. Different Output Formats

```bash
# Generate as CSV
python3 cli.py generate healthcare_patient -n 100 --format csv -o patients.csv

# Generate as JSON-LD with semantic context
python3 cli.py generate ecommerce_product -n 50 --format jsonld -o products.jsonld

# Generate as Parquet for big data processing
python3 cli.py generate financial_transaction -n 10000 --format parquet --spark
```

### 3. Custom Schema Files

```bash
# From JSON file
python3 cli.py from-file my_schema.json -n 100

# From JSON string
python3 cli.py from-json '{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}}}'

# Auto-discovered schemas (place in custom_schemas/ folder)
python3 cli.py generate my_custom_schema -n 50
```

### 4. Interactive Schema Builder

```bash
# Build a schema interactively
python3 cli.py create

# Follow prompts to:
# - Name your schema
# - Add fields with types
# - Set constraints
# - Generate data immediately
```

### 5. Massive Datasets with PySpark

```bash
# Generate 1 million records
python3 cli.py generate data_pipeline_metadata -n 1000000 --spark

# Generate 10 million with custom Spark settings
python3 cli.py generate iot_sensor -n 10000000 --spark --partitions 100 --spark-memory 8g

# Output to Parquet for Spark/Hadoop processing
python3 cli.py generate financial_transaction -n 50000000 --spark --format parquet
```

### 6. Python API Usage

```python
from simple_generator import SchemaDataGenerator
from schemas import SchemaLibrary

# Using predefined schema
generator = SchemaDataGenerator()
schemas = SchemaLibrary.get_all_schemas()
data = generator.generate_from_schema(schemas['hr_employee'], 100)

# Save in different formats
csv_output = generator.to_csv(data)
jsonld_output = generator.to_json_ld(data, schemas['hr_employee'])

# Using custom schema
custom_schema = {
    "type": "object",
    "properties": {
        "user_id": {"type": "string"},
        "username": {"type": "string"},
        "email": {"type": "string"},
        "age": {"type": "integer", "minimum": 18, "maximum": 65},
        "is_active": {"type": "boolean"}
    }
}
data = generator.generate_from_schema(custom_schema, 50)
```

### 7. Specialized Data Scenarios

```bash
# Generate UK-specific data (DWP schemas)
python3 cli.py generate child_benefit -n 100  # Safe test NINOs included

# Generate ML training metadata
python3 cli.py generate ml_model_training -n 50 --format csv

# Generate real-time streaming events
python3 cli.py generate real_time_analytics -n 10000

# Generate cloud infrastructure configs
python3 cli.py generate cloud_infrastructure -n 200 --format jsonld
```

### 8. Batch Processing

```bash
# Generate multiple schemas at once
for schema in ecommerce_product healthcare_patient financial_transaction; do
    python3 cli.py generate $schema -n 1000 --format csv -o "${schema}_data.csv"
done

# Generate with timestamp
python3 cli.py generate iot_sensor -n 5000 -o "sensor_$(date +%Y%m%d_%H%M%S).json"
```

### 9. Custom Schema Auto-Discovery

```bash
# Step 1: Add your schema file
cp my_company_schema.json custom_schemas/

# Step 2: It's automatically available!
python3 cli.py list  # Will show your schema
python3 cli.py generate my_company_schema -n 100
```

### 10. Advanced Features

```bash
# Pipe to other tools
python3 cli.py generate ecommerce_product -n 1000 | jq '.[] | .product_name'

# Generate and compress
python3 cli.py generate healthcare_patient -n 10000 | gzip > patients.json.gz

# Generate specific fields only
python3 generate_data.py --list-schemas  # See all fields
python3 cli.py generate hr_employee -n 50 --format csv | cut -d',' -f1,2,3  # Extract specific columns
```

## Available Predefined Schemas

### Business & Commerce
- `ecommerce_product` - Product catalog with pricing, inventory
- `financial_transaction` - Banking and payment transactions
- `customer` - Customer profiles with contact info

### Healthcare & Services
- `healthcare_patient` - Patient records with medical history
- `child_benefit` - UK benefit claims (test-safe NINOs)
- `universal_credit` - UK universal credit claims
- `pip` - Personal Independence Payment claims

### Technology & Engineering
- `data_pipeline_metadata` - ETL pipeline execution logs
- `ml_model_training` - Machine learning training metrics
- `distributed_system_metrics` - System performance data
- `real_time_analytics` - Streaming event data
- `iot_sensor` - IoT device readings

### HR & Education
- `hr_employee` - Employee records with skills, certifications
- `education_student` - Student academic records

### Social & Content
- `social_media_post` - Social media content with engagement
- `blog_post` - Blog articles with metadata

## Data Quality Features

### Faker Integration
- **Realistic Names**: Proper first/last names from various cultures
- **Coherent Emails**: Emails match person's name (john.smith@gmail.com)
- **Professional Skills**: Real technical and soft skills
- **Valid Certifications**: Industry-standard certification names
- **Proper Addresses**: Real-looking street addresses and postcodes
- **Smart Phone Numbers**: Correctly formatted for regions

### Context-Aware Generation
- Fields are processed in order for data coherence
- Names generated before emails for matching
- Related fields use consistent data

## Performance

| Records | Standard Mode | PySpark Mode | Output Format |
|---------|--------------|--------------|---------------|
| 1K      | <1 second    | N/A          | JSON/CSV      |
| 10K     | 1-2 seconds  | N/A          | JSON/CSV      |
| 100K    | 5-10 seconds | 2-3 seconds  | JSON/CSV      |
| 1M      | 50-100 sec   | 10-20 sec    | Parquet       |
| 10M     | N/A          | 1-2 minutes  | Parquet       |
| 100M    | N/A          | 10-15 min    | Parquet       |

## Output Format Examples

### JSON (Default)
```json
{
  "employee_id": "550e8400-e29b-41d4-a716-446655440000",
  "first_name": "Emma",
  "last_name": "Watson",
  "email": "emma.watson@company.com",
  "department": "Engineering"
}
```

### CSV
```csv
employee_id,first_name,last_name,email,department
550e8400-e29b-41d4-a716-446655440000,Emma,Watson,emma.watson@company.com,Engineering
```

### JSON-LD (Semantic Web)
```json
{
  "@context": "http://schema.org/",
  "@type": "Employee",
  "@graph": [
    {
      "employee_id": "550e8400-e29b-41d4-a716-446655440000",
      "first_name": "Emma",
      "last_name": "Watson"
    }
  ]
}
```

## Testing

```bash
# Run all tests
python3 test_runner.py

# Test specific functionality
python3 quick_test.py
python3 test_faker_integration.py
python3 simple_performance_test.py
```

