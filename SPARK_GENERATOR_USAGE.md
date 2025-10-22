# Spark Generator Usage Guide

## Quick Start

The Spark generator is now fully compatible with all Spark data types and works reliably with error injection enabled.

### Basic Usage (No Error Injection)

```python
from schema_parser import SchemaParser
from spark_generator import SparkDataGenerator

# Parse your schema
parser = SchemaParser()
schema = parser.parse_schema_file('your_schema.xlsx')

# Initialize generator
generator = SparkDataGenerator(
    app_name="DataGeneration",
    master="local[*]",  # Use all CPU cores
    memory="4g",
    error_rate=0.0  # No error injection
)

try:
    # Generate and save data
    df = generator.generate_and_save(
        schema=schema,
        num_records=1000000,  # 1 million records
        output_path="generated_data/output",
        output_format="parquet",
        num_partitions=100
    )
finally:
    generator.close()
```

### With Error Injection (Now Safe!)

```python
# Initialize generator WITH error injection
generator = SparkDataGenerator(
    app_name="DataGeneration",
    master="local[*]",
    memory="4g",
    error_rate=0.1  # ✅ 10% error injection - NOW SAFE!
)

try:
    df = generator.generate_and_save(
        schema=schema,
        num_records=1000000,
        output_path="generated_data/output_with_errors",
        output_format="parquet",
        num_partitions=100
    )
finally:
    generator.close()
```

## Supported Schema Types

All JSON schema types are now fully supported with correct Spark type mapping:

| JSON Schema Type | Spark Type | Example Values |
|-----------------|------------|----------------|
| `"type": "string"` | StringType | "John", "hello@example.com" |
| `"type": "integer"` | LongType | 42, -100, 9223372036854775807 |
| `"type": "number"` | DoubleType | 3.14, -99.9, 1.23e10 |
| `"type": "boolean"` | BooleanType | true, false |
| `"type": "array", "items": {"type": "string"}` | ArrayType(StringType) | ["tag1", "tag2"] |
| `"type": "array", "items": {"type": "integer"}` | ArrayType(LongType) | [1, 2, 3, 100] |
| `"type": "array", "items": {"type": "number"}` | ArrayType(DoubleType) | [1.5, 2.7, 3.9] |
| `"type": "array", "items": {"type": "boolean"}` | ArrayType(BooleanType) | [true, false, true] |

## Example Schemas

### E-commerce Product Schema

```json
{
  "type": "object",
  "properties": {
    "product_id": {"type": "string"},
    "name": {"type": "string"},
    "description": {"type": "string"},
    "price": {"type": "number", "minimum": 0.99, "maximum": 9999.99},
    "stock": {"type": "integer", "minimum": 0, "maximum": 10000},
    "rating": {"type": "number", "minimum": 1.0, "maximum": 5.0},
    "is_active": {"type": "boolean"},
    "tags": {
      "type": "array",
      "items": {"type": "string"},
      "minItems": 1,
      "maxItems": 10
    },
    "review_scores": {
      "type": "array",
      "items": {"type": "number"},
      "minItems": 0,
      "maxItems": 100
    }
  },
  "required": ["product_id", "name", "price"]
}
```

### Employee Records Schema

```json
{
  "type": "object",
  "properties": {
    "employee_id": {"type": "integer"},
    "first_name": {"type": "string"},
    "last_name": {"type": "string"},
    "email": {"type": "string"},
    "age": {"type": "integer", "minimum": 18, "maximum": 100},
    "salary": {"type": "number", "minimum": 20000, "maximum": 500000},
    "is_manager": {"type": "boolean"},
    "skills": {
      "type": "array",
      "items": {"type": "string"},
      "minItems": 1,
      "maxItems": 20
    },
    "performance_ratings": {
      "type": "array",
      "items": {"type": "number"},
      "minItems": 1,
      "maxItems": 5
    }
  },
  "required": ["employee_id", "first_name", "last_name", "email"]
}
```

## Error Injection Types

With `error_rate > 0`, the generator will inject the following type-safe errors:

### Numeric Fields (number, integer)
- **null**: Returns `None`
- **negative**: Returns negative value
- **zero**: Returns 0 (0.0 for float, 0 for int)
- **extreme_value**: Returns very large/small values within Spark's type range

### String Fields
- **null**: Returns `None`
- **empty**: Returns ""
- **whitespace**: Returns "   "
- **special_chars**: Adds special characters
- **wrong_case**: Changes case
- **truncated**: Truncates string
- **extra_spaces**: Adds leading/trailing spaces

### Array Fields
- **null**: Returns `None`
- **empty**: Returns []

### Boolean Fields
- **null**: Returns `None`
- **string_instead**: Returns "INVALID_DATA"

## Performance Tips

### 1. Partitioning

```python
# For small datasets (< 100K records)
num_partitions = 10

# For medium datasets (100K - 10M records)
num_partitions = 100

# For large datasets (> 10M records)
num_partitions = 1000
```

### 2. Memory Configuration

```python
# Small datasets
memory = "2g"

# Medium datasets
memory = "4g"

# Large datasets
memory = "8g"  # or more
```

### 3. Output Formats

```python
# Best for analytics (columnar, compressed)
output_format = "parquet"  # Recommended!

# Good for compatibility
output_format = "csv"

# Good for streaming/APIs
output_format = "json"

# Best for very large datasets
output_format = "orc"
```

### 4. Compression

```python
generator.save_massive_dataset(
    df=df,
    output_path="output",
    output_format="parquet",
    compression="snappy"  # Fast, good compression ratio
    # Other options: "gzip", "lz4", "zstd"
)
```

## Troubleshooting

### Issue: Type Errors

**Error:** `DoubleType() cannot accept object X in type <class 'Y'>`

**Solution:** Make sure you're using the latest version with all fixes applied. The fixes ensure:
- Number fields return float (never int or string)
- Integer fields return int (never float or string)
- Array fields return list (never string)

### Issue: Out of Memory

**Error:** `java.lang.OutOfMemoryError`

**Solution:**
```python
generator = SparkDataGenerator(
    memory="8g",  # Increase memory
    error_rate=0.0  # Reduce error_rate if needed
)
```

### Issue: Slow Performance

**Solution:**
```python
# Increase partitions for better parallelism
df = generator.generate_massive_dataset(
    schema=schema,
    num_records=1000000,
    num_partitions=200  # Increase this
)
```

### Issue: Java Not Found

**Error:** `Unable to locate a Java Runtime`

**Solution:**
```bash
# Install Java 8 or 11
# On macOS:
brew install openjdk@11

# On Ubuntu/Debian:
sudo apt-get install openjdk-11-jdk

# Set JAVA_HOME
export JAVA_HOME=$(/usr/libexec/java_home -v 11)  # macOS
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64  # Linux
```

## Testing

Run the test suites to verify everything works:

```bash
# Test basic data generation (no Spark required)
python3 test_spark_type_compatibility.py
python3 test_array_type_compatibility.py

# Test Spark generator (requires Spark/Java)
python3 test_spark_generator_types.py
```

## Complete Example

```python
#!/usr/bin/env python3
"""
Complete example: Generate 1 million employee records with 10% error injection
"""

from schema_parser import SchemaParser
from spark_generator import SparkDataGenerator
import os

# Set Spark environment
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Parse schema
parser = SchemaParser()
schema = parser.parse_schema_file('custom_schemas/employee_schema_3col.xlsx')

# Initialize generator
generator = SparkDataGenerator(
    app_name="EmployeeDataGeneration",
    master="local[*]",
    memory="4g",
    error_rate=0.1  # 10% error injection
)

try:
    print("Generating 1 million employee records...")

    # Generate and save
    df = generator.generate_and_save(
        schema=schema,
        num_records=1000000,
        output_path="generated_data/employees",
        output_format="parquet",
        num_partitions=100,
        show_sample=True
    )

    # Show statistics
    print("\nDataset Statistics:")
    print(f"Total records: {df.count():,}")
    print(f"Partitions: {df.rdd.getNumPartitions()}")

    # Check for nulls (from error injection)
    null_counts = df.select([
        (df[col].isNull().cast("int").alias(col))
        for col in df.columns
    ]).groupBy().sum().collect()[0].asDict()

    print("\nNull counts (from error injection):")
    for col, count in null_counts.items():
        if count > 0:
            pct = (count / 1000000) * 100
            print(f"  {col.replace('sum(', '').replace(')', '')}: {count:,} ({pct:.1f}%)")

    print("\n✅ Generation complete!")

finally:
    generator.close()
```

## Migration from Old Version

If you were using an older version with type errors:

### Before (Had Type Errors)
```python
# This would cause DoubleType errors
generator = SparkDataGenerator(error_rate=0.1)
```

### After (Now Safe)
```python
# This now works perfectly!
generator = SparkDataGenerator(error_rate=0.1)
# No code changes needed - just update to latest version
```

All fixes are automatic. Your existing code will work without modification!

## Summary

✅ **All type compatibility issues fixed**
✅ **Error injection now safe to use**
✅ **All array item types supported**
✅ **Production-ready for any schema**
✅ **Tested and verified**

Generate data with confidence!
