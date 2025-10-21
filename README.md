# Universal Synthetic Data Generator

A command-line tool that generates realistic test data from JSON schemas, CSV schemas, Excel schemas, and SQL schemas. This tool helps data engineers create test datasets for development, testing, and learning without needing real data.

Complete Documentation: [COMPLETE_GUIDE.md](COMPLETE_GUIDE.md)

## What This Tool Does

This generator creates fake but realistic data that looks like real data. For example:
- Employee records with names, emails, departments, salaries
- Customer data with addresses and phone numbers
- Financial transactions, healthcare records, IoT sensor data
- Any custom data structure you define

There are two ways to generate data:

1. **Simple Generator** - For small datasets (up to 100,000 records)
2. **Spark Generator** - For large datasets (100,000+ records) using Apache Spark

## Key Features

### Schema Support
- **JSON schemas** - Standard JSON format
- **CSV schemas** - Simple 3-column format
- **Excel schemas** - Easy-to-use spreadsheet format
- **SQL schemas** - CREATE TABLE DDL statements
- **Description-based generation** - The tool reads your column descriptions to understand what data to generate

### Data Generation Options
- **Simple mode** - Fast startup, good for development and testing
- **Spark mode** - Distributed processing for massive datasets (requires Java and PySpark)
- **Multiple output formats** - JSON, CSV, Parquet (columnar format used in big data)
- **Realistic data** - Uses Faker library to generate names, emails, addresses that look real

### Built-in Templates
The tool includes 20+ ready-to-use schemas:
- Business: E-commerce products, customers, financial transactions
- HR: Employee records with skills and certifications
- Healthcare: Patient records
- Technology: IoT sensors, data pipelines, ML training logs
- Government: UK benefits data (with safe test NINOs)

## Installation

```bash
# Basic installation (includes Faker)
pip3 install faker

# For Excel schema support
pip3 install pandas openpyxl

# For PySpark - Massive Dataset Generation
pip3 install pyspark

# Java 11+ required for PySpark
# macOS:
brew install openjdk@17

# Ubuntu/Debian:
sudo apt install openjdk-17-jdk

# Set JAVA_HOME (add to ~/.bashrc or ~/.zshrc)
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"  # macOS with Homebrew
export JAVA_HOME="/usr/lib/jvm/java-17-openjdk"  # Linux
export PATH="$JAVA_HOME/bin:$PATH"

# Optional: For testing
pip3 install -r requirements.txt
```

## Quick Start

### Which Generator Should I Use?

Choose based on how many records you need:

| Number of Records | Tool to Use | Why |
|-------------------|-------------|-----|
| Less than 1,000 | Simple generator (interactive) | Fastest way to start, no setup needed |
| 1,000 - 100,000 | Simple generator (command line) | Fast enough, no Java/Spark required |
| 100,000 - 1,000,000 | Spark generator (local mode) | Uses your computer's multiple cores |
| Over 1,000,000 | Spark generator (cluster) | Distributes work across multiple machines |

### Method 1: Interactive Mode (Easiest for Beginners)

Run this command and follow the menu:
```bash
python3 cli.py
```

You'll see options to:
1. Pick from built-in templates (employee, customer, etc.)
2. Use your own schema file
3. Build a new schema step-by-step
4. Choose output format (JSON, CSV, etc.)

### Method 2: Command Line Mode

```bash
# See what schemas are available
python3 cli.py list

# Generate 100 employee records
python3 cli.py generate hr_employee -n 100

# Generate as CSV instead of JSON
python3 cli.py generate ecommerce_product -n 1000 --format csv
```

### Method 3: Python API (For Large Datasets with Spark)

This is the recommended way for datasets over 100,000 records:

```python
from spark_generator import SparkDataGenerator
from schema_parser import SchemaParser

# Load your schema from Excel
parser = SchemaParser()
schema = parser.parse_schema_file('custom_schemas/employee_schema_3col.xlsx')

# Set up Spark generator
# local[*] means use all CPU cores on your machine
generator = SparkDataGenerator(master="local[*]", memory="4g")

# Generate data and save as Parquet files
df = generator.generate_and_save(
    schema=schema,
    num_records=1000000,
    output_path="output/data",
    output_format="parquet"
)

# Always close when done
generator.close()
```

## Usage Examples

### 1. Basic Data Generation

```bash
# Generate employee records as JSON (default)
python3 cli.py generate hr_employee -n 50

# Output location: generated_data/2025/09-September/hr_employee/hr_employee_50_records_20250926_143022.json

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

The generator supports multiple schema formats, with automatic type inference from descriptions.

```bash
# From JSON file
python3 cli.py from-file my_schema.json -n 100

# From JSON string
python3 cli.py from-json '{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}}}'

# From Excel schema (description-based generation)
python3 cli.py from-file employee_schema.xlsx -n 1000 --spark

# From CSV schema
python3 cli.py from-file schema.csv -n 500

# From SQL schema (CREATE TABLE statement)
python3 cli.py from-file employee_table.sql -n 1000 --spark

# Auto-discovered schemas (place in custom_schemas/ folder)
python3 cli.py generate my_custom_schema -n 50
```

#### Creating Excel Schemas (Recommended for Beginners)

Excel schemas are the easiest way to define your data structure. Create a spreadsheet with exactly three columns:

**Column 1: column_name** - The name of each field
**Column 2: values** - Optional list of choices (comma-separated)
**Column 3: description** - What type of data to generate

Example Excel schema:

| column_name | values | description |
|-------------|--------|-------------|
| employee_id | | unique identifier for employee |
| nino | | National Insurance Number |
| first_name | | employee first name |
| email | | employee email address |
| department | IT,HR,Finance,Sales | department name |
| salary | | annual salary amount in GBP |
| is_active | | employment status active or not |

**How the generator reads this:**

1. **column_name** → Becomes the field name in your data
2. **values** → If you provide comma-separated options, it picks one randomly
3. **description** → The generator reads this to understand what data to create

**Smart description parsing:**

The generator looks for keywords in your descriptions:

- "identifier", "id" → Generates UUID or number
- "National Insurance Number", "NINO" → Valid UK NINO format
- "postcode", "zip" → UK postcode format
- "email" → Email addresses
- "first name" → Realistic first names
- "last name" → Realistic last names
- "date of birth", "birth date" → Date format
- "phone" → Phone numbers
- "salary", "amount", "price" → Numbers with decimals
- "age", "count", "quantity" → Whole numbers (integers)
- "active", "enabled", "flag" → True/False (boolean)

**The values column:**
- If empty: Generator decides based on description
- If filled: Generator picks randomly from your list
- Example: "IT,HR,Finance,Sales" → Each record gets one of these

**Tips for writing descriptions:**
- Be specific: "UK postcode" is better than just "location"
- Use common terms: "email address" works, "electronic mail" might not
- For numbers, mention the unit: "salary in GBP" or "age in years"

#### Creating SQL Schemas

SQL schemas use standard CREATE TABLE statements that you may already have from existing databases. The generator parses the DDL (Data Definition Language) and converts it into test data.

**Example SQL schema:**

```sql
CREATE TABLE employees (
    employee_id INT NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    department ENUM('IT', 'HR', 'Finance', 'Marketing', 'Sales', 'Operations'),
    job_title VARCHAR(100),
    salary DECIMAL(10, 2),
    years_of_service INT,
    is_active BOOLEAN DEFAULT TRUE,
    hire_date TIMESTAMP,
    PRIMARY KEY (employee_id)
);
```

**What the generator understands:**

1. **Column names** → Field names in generated data
2. **Data types** → Mapped to appropriate generators:
   - `INT`, `INTEGER`, `BIGINT` → Whole numbers
   - `DECIMAL(10,2)`, `NUMERIC`, `FLOAT` → Decimal numbers
   - `VARCHAR(100)`, `TEXT` → Text strings (respects length limits)
   - `ENUM('val1','val2')` → Picks randomly from the list
   - `BOOLEAN`, `BOOL` → True/False values
   - `DATE`, `DATETIME`, `TIMESTAMP` → Date/time values
   - `UUID`, `GUID` → Unique identifiers
3. **Constraints** → Enforced in generated data:
   - `NOT NULL` → Field always has a value (added to required fields)
   - `DEFAULT value` → Noted but not used (random data generated)
   - `PRIMARY KEY` → Noted but not enforced (each record gets unique ID)
4. **Comments** → Ignored (both `--` and `/* */` styles)

**Supported SQL dialects:**
- PostgreSQL
- MySQL
- SQL Server
- SQLite
- Most standard SQL DDL syntax

**Usage with Spark:**

```python
from schema_parser import SchemaParser
from spark_generator import SparkDataGenerator

# Parse SQL schema
parser = SchemaParser()
schema = parser.parse_schema_file('custom_schemas/employee_table.sql')

# Generate data
generator = SparkDataGenerator(master="local[*]", memory="4g")
df = generator.generate_and_save(
    schema=schema,
    num_records=10000,
    output_path="output/employees",
    output_format="parquet"
)
generator.close()
```

**Why use SQL schemas:**
- Already have database schemas you want to test
- Database developers are familiar with DDL syntax
- Can copy CREATE TABLE statements directly from database
- Type constraints and lengths are automatically enforced

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

### 5. Using Spark Generator for Large Datasets

When you need to generate more than 100,000 records, use the Spark generator. Spark is a distributed computing framework that splits the work across multiple CPU cores or machines.

#### Why Use Spark?

**Without Spark (Simple Generator):**
- Generates records one-by-one on a single CPU core
- Works well for small datasets
- Becomes slow for 100,000+ records

**With Spark (Spark Generator):**
- Splits work across all your CPU cores (or multiple machines)
- Generates records in parallel
- Much faster for large datasets
- Saves data in efficient formats like Parquet

#### How Spark Generator Works

```mermaid
flowchart TD
    A[Schema File<br/>JSON/Excel/CSV/SQL] --> B[Schema Parser]
    B --> C[JSON Schema Format]
    C --> D[Spark Generator Init]
    D --> E[Create Spark Session<br/>master=local[*] or cluster]
    E --> F[Broadcast Schema<br/>to all workers]

    F --> G[Split Work into Partitions]
    G --> H1[Partition 1<br/>Generate N/P records]
    G --> H2[Partition 2<br/>Generate N/P records]
    G --> H3[Partition 3<br/>Generate N/P records]
    G --> H4[Partition N<br/>Generate N/P records]

    H1 --> I1[Use Faker<br/>Generate realistic data]
    H2 --> I2[Use Faker<br/>Generate realistic data]
    H3 --> I3[Use Faker<br/>Generate realistic data]
    H4 --> I4[Use Faker<br/>Generate realistic data]

    I1 --> J[Collect Results<br/>Spark DataFrame]
    I2 --> J
    I3 --> J
    I4 --> J

    J --> K{Output Format?}
    K -->|Parquet| L1[Write Parquet Files<br/>Compressed columnar]
    K -->|CSV| L2[Write CSV Files<br/>Text format]
    K -->|JSON| L3[Write JSON Files<br/>Line-delimited]

    L1 --> M[Output Directory<br/>part-00000.parquet<br/>part-00001.parquet<br/>...]
    L2 --> M
    L3 --> M

    style A fill:#e1f5ff
    style C fill:#fff4e1
    style E fill:#ffe1f5
    style J fill:#e1ffe1
    style M fill:#f0f0f0

    classDef parallel fill:#ffcccc
    class H1,H2,H3,H4,I1,I2,I3,I4 parallel
```

**Key Steps:**

1. **Schema Parsing** - Reads your schema file (any format) and converts to standard JSON Schema
2. **Spark Session** - Initializes Spark with your specified cores and memory
3. **Broadcasting** - Sends schema to all worker nodes (one copy shared by all)
4. **Partitioning** - Divides total records across N partitions (workers work in parallel)
5. **Generation** - Each partition uses Faker library to create realistic fake data
6. **Collection** - Results combined into a single Spark DataFrame
7. **Writing** - Saves to disk in chosen format (Parquet, CSV, or JSON)

**Parallelization Example:**
- 10,000 records with 4 partitions
- Partition 1: Generates records 1-2,500
- Partition 2: Generates records 2,501-5,000
- Partition 3: Generates records 5,001-7,500
- Partition 4: Generates records 7,501-10,000
- All 4 run at the same time on different CPU cores

#### Basic Python Example

```python
from spark_generator import SparkDataGenerator
from schema_parser import SchemaParser

# Step 1: Load your schema
parser = SchemaParser()
schema = parser.parse_schema_file('custom_schemas/employee_schema_3col.xlsx')

# Step 2: Set up Spark generator
generator = SparkDataGenerator(
    app_name="EmployeeDataGeneration",  # Name for your Spark job
    master="local[*]",                  # local[*] = use all CPU cores
    memory="4g",                        # How much RAM to use
    error_rate=0.0                      # Don't inject errors in data
)

# Step 3: Generate data
df = generator.generate_and_save(
    schema=schema,
    num_records=1000000,                # 1 million records
    output_path="output/employees",     # Where to save files
    output_format="parquet",            # Columnar format for big data
    num_partitions=10,                  # Split into 10 parts
    show_sample=True                    # Show example records
)

# Step 4: Check results
print(f"Generated {df.count():,} records")
print(f"Saved in {df.rdd.getNumPartitions()} files")

# Step 5: Clean up
generator.close()
```

#### Understanding the Parameters

**master="local[*]"**
- `local[N]` means run on your computer with N cores
- `local[*]` means use all available cores
- For a cluster, use `yarn` or `spark://hostname:port`

**memory="4g"**
- How much RAM the Spark driver can use
- Driver coordinates the work
- For 1M records, 4GB is plenty

**num_partitions=10**
- How many pieces to split the data into
- More partitions = more parallel work
- Good rule: 2-4 partitions per CPU core
- For 1M records on 4 cores, use 10-16 partitions

**output_format="parquet"**
- Parquet is a columnar format (stores data by column, not row)
- Much faster to read than CSV for analytics
- Automatically compressed (uses less disk space)
- Standard format for Spark, Hadoop, and modern data warehouses

#### How Excel Schemas Work

Your Excel file needs three columns:

1. **column_name** - The field name
2. **values** - Optional: comma-separated options (like "IT,HR,Finance")
3. **description** - Tells the generator what type of data to make

Example Excel schema:

| column_name | values | description |
|-------------|--------|-------------|
| nino | | National Insurance Number |
| postcode | | UK postcode |
| department | IT,HR,Finance,Sales | department name |

The generator reads the **description** to understand what data to generate:
- "National Insurance Number" generates valid UK NINOs
- "postcode" generates UK postcodes
- "email" generates email addresses
- "salary" or "amount" generates numbers
- If **values** has comma-separated items, picks randomly from those

#### Performance You Can Expect

Based on real tests (MacBook Pro with 8GB RAM):

| Records | Partitions | Time | Speed | File Size |
|---------|-----------|------|-------|-----------|
| 5,000 | 2 | 11 seconds | 450 per second | 313 KB |
| 10,000 | 4 | 9 seconds | 1,100 per second | 524 KB |
| 100,000 | 10 | ~90 seconds | 1,100 per second | ~5 MB |
| 1,000,000 | 100 | ~15 minutes | 1,100 per second | ~50 MB |

**On a cluster:**
- 4 computers = about 4x faster
- 10 computers = about 10x faster
- Speed scales linearly with resources

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

### 11. Relational Data Generation

```bash
# Interactive mode - Option 4
python3 cli.py
# Choose "4. Generate DWP Relational Data"

# Generates related tables with foreign key relationships:
# - citizens table (base records)
# - households table (linked to citizens)
# - benefit_claims table (linked to both)

# Output structure:
# generated_data/
#   └── 2025/
#       └── 09-September/
#           └── dwp_relational/
#               ├── citizens/
#               ├── households/
#               └── benefit_claims/
```

### 12. Organized Output Structure

All generated data is automatically organized in a date-based folder structure:

```
generated_data/
├── 2025/
│   └── 09-September/
│       ├── hr_employee/
│       │   ├── hr_employee_1K_records_20250926_143022.json
│       │   └── hr_employee_10K_records_20250926_145512.csv
│       ├── ecommerce_product/
│       │   └── ecommerce_product_5K_records_20250926_150033.json
│       └── financial_transaction/
│           └── financial_transaction_1M_records_20250926_151245.parquet
```

**Naming Convention:**
- Small datasets: `schema_name_100_records_timestamp.format`
- Thousands: `schema_name_5K_records_timestamp.format`
- Millions: `schema_name_1M_records_timestamp.format`

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

## Why the Generated Data Looks Real

### Using the Faker Library

This tool uses a Python library called "Faker" that specializes in creating realistic fake data. This means:

**Names look real:**
- First names: John, Sarah, Mohammed, Aisha, Wei
- Last names: Smith, Johnson, Patel, Chen, O'Brien
- From different cultures and countries

**Emails match names:**
- If name is "John Smith", email might be: john.smith@gmail.com
- If name is "Sarah Jones", email might be: sjones@outlook.com
- Domains include gmail.com, yahoo.com, hotmail.com, etc.

**Professional details are realistic:**
- Technical skills: Python, JavaScript, AWS, Docker, Kubernetes
- Soft skills: Leadership, Communication, Problem Solving
- Certifications: AWS Certified, Scrum Master, PMP
- Job titles: Data Engineer, Software Developer, Project Manager

**Location data looks proper:**
- UK postcodes: SW1A 1AA, M1 1AE, B33 8TH
- US addresses: "123 Main St, New York, NY 10001"
- Phone numbers: Formatted correctly for the region

### How Context-Aware Generation Works

The generator is smart about relationships between fields:

1. **Names before emails**: Generates first and last name first, then creates matching email
2. **Dates make sense**: Birth date won't be in the future, start date won't be before birth
3. **Numbers are in range**: Salaries are realistic for job types, ages are reasonable

Example of coherent data:
```json
{
  "first_name": "Emma",
  "last_name": "Wilson",
  "email": "emma.wilson@gmail.com",  ← Matches the name!
  "age": 28,
  "job_title": "Junior Data Engineer",
  "years_experience": 3  ← Makes sense with age
}
```

## Performance: How Fast Is It?

### Simple Generator Performance

The simple generator runs on a single CPU core:

| Number of Records | Time | When to Use |
|-------------------|------|-------------|
| 100 | Less than 1 second | Quick testing during development |
| 1,000 | Less than 1 second | Unit tests |
| 10,000 | 1-2 seconds | Integration tests |
| 100,000 | 10-15 seconds | Maximum recommended size |

**When to use:** Any time you need less than 100,000 records.

### Spark Generator Performance

The Spark generator uses multiple CPU cores (tested on laptop with 8GB RAM):

| Number of Records | Partitions | Time | Output Size |
|-------------------|-----------|------|-------------|
| 5,000 | 2 | 11 seconds | 313 KB |
| 10,000 | 4 | 9 seconds | 524 KB |
| 100,000 | 10 | 90 seconds (~1.5 min) | 5 MB |
| 1,000,000 | 100 | 15 minutes | 50 MB |
| 10,000,000 | 1,000 | 2.5 hours | 500 MB |

**Important notes:**
- All files are Parquet format with Snappy compression
- Times are for local mode (single computer)
- On a cluster with multiple machines, speed multiplies

**What affects speed:**
1. **Number of partitions** - More partitions = more parallel work (but not infinite)
2. **CPU cores** - More cores = faster generation
3. **Memory** - More RAM = can process larger batches
4. **Output format** - Parquet is fastest, CSV is slowest

### Understanding Partitions

Think of partitions like splitting a job among workers:
- 1 partition = 1 worker does all the work
- 10 partitions = 10 workers split the work
- If you have 4 CPU cores, use 8-16 partitions (2-4x cores)
- Too few partitions = not using all your cores
- Too many partitions = overhead from managing all the splits

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

## Spark Generator - Learning More

### Understanding Spark Configuration

When you create a Spark generator, you can control how it works:

```python
from spark_generator import SparkDataGenerator

generator = SparkDataGenerator(
    app_name="MyDataGen",      # Name shown in Spark UI (monitoring tool)
    master="local[4]",         # Where to run: local[4] = 4 cores on your computer
    memory="4g",               # RAM for the main Spark process (driver)
    cores=4,                   # How many cores each worker uses
    error_rate=0.0             # 0.0 = perfect data, 0.1 = 10% has errors
)
```

**What each setting means:**

**app_name** - Just a label to identify your job in monitoring tools

**master** - Where Spark runs:
- `local[1]` - Run on your computer with 1 core
- `local[4]` - Run on your computer with 4 cores
- `local[*]` - Run on your computer with all cores
- `yarn` - Run on a Hadoop cluster
- `spark://hostname:7077` - Run on a Spark cluster

**memory** - RAM for the Spark driver (the main coordinator):
- `2g` = 2 gigabytes (good for under 1M records)
- `4g` = 4 gigabytes (good for 1M-10M records)
- `8g` = 8 gigabytes (good for 10M+ records)

**error_rate** - Intentionally add bad data for testing data quality tools:
- `0.0` = No errors, all data is valid
- `0.1` = 10% of fields will have issues (nulls, wrong types, etc.)
- `0.5` = 50% of fields will have issues

### Choosing Output Format

```python
# Parquet - RECOMMENDED for big data
df = generator.generate_and_save(
    schema=schema,
    num_records=1000000,
    output_path="data/output",
    output_format="parquet"  # Options: parquet, csv, json, orc
)
```

**Format comparison:**

| Format | Speed | Size | Best For |
|--------|-------|------|----------|
| Parquet | Fast | Small (compressed) | Spark, Hadoop, data analysis |
| CSV | Slow | Large | Excel, simple tools, humans reading |
| JSON | Medium | Large | Web APIs, simple structures |
| ORC | Fast | Small (compressed) | Hadoop Hive |

**Why Parquet is best:**
- Stores data by column (columnar format)
- Only reads columns you need (efficient)
- Automatically compressed
- Industry standard for big data

### Loading Different Schema Types

The schema parser can read four formats:

```python
from schema_parser import SchemaParser

parser = SchemaParser()

# From Excel (easiest for non-programmers)
schema = parser.parse_schema_file('schemas/employee.xlsx')

# From CSV (simple text format)
schema = parser.parse_schema_file('schemas/customer.csv')

# From JSON (standard data format)
schema = parser.parse_schema_file('schemas/product.json')

# From SQL (CREATE TABLE statements)
schema = parser.parse_schema_file('schemas/employee_table.sql')

# All four formats work the same way after parsing!
```

### Reading and Analyzing Generated Data

After generating data, you can read it back and analyze it:

```python
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Analysis").getOrCreate()

# Read the Parquet files you generated
df = spark.read.parquet("output/employees")

# Count employees by department
df.groupBy("department").count().show()

# Get salary statistics
df.select("salary").summary("count", "mean", "min", "max").show()

# Save in different format if needed
df.write.csv("output/employees_csv", header=True)
```

### Guidelines for Different Dataset Sizes

**Under 100,000 records:**
- Use the simple generator (not Spark)
- Why: Spark has startup overhead (15-30 seconds)
- Simple generator is faster for small data

**100,000 to 1,000,000 records:**
- Use Spark in local mode
- Set partitions to 2-4 times your CPU cores
- Example: 4 cores → use 10-16 partitions
- Use Parquet format

**Over 1,000,000 records:**
- Consider using a cluster if available
- Set partitions to 100-1000 (depends on data size)
- Definitely use Parquet format
- Set error_rate to 0.0 (errors slow things down)

**Memory recommendations:**
- Under 1M records: 2GB is enough
- 1M-10M records: Use 4GB
- Over 10M records: Use 8GB or more
- Watch the Spark UI at http://localhost:4040 while it runs

## Testing

```bash
# Run all tests
python3 test_runner.py

# Test specific functionality
python3 quick_test.py
python3 test_faker_integration.py
python3 simple_performance_test.py

# Test Spark generator with custom schemas
python3 test_spark_custom_schema.py  # Employees dataset (Excel schema)
python3 test_spark_benefits.py        # Benefits claimant dataset (Excel schema)
python3 test_spark_sql_schema.py      # Employees dataset (SQL schema)

# Test simple generator with custom schemas
python3 test_simple_custom_schema.py
```

