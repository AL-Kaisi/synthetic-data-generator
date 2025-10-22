# Single File Output Feature

## Overview

The Spark generator now supports outputting data as a single file instead of multiple Spark part files. This is useful for:
- Smaller datasets that need to be easily shared
- CSV exports for Excel or other tools
- Single JSON files for APIs
- Creating clean, single-file outputs

## Usage

### Basic Example

```python
from spark_generator import SparkDataGenerator

generator = SparkDataGenerator(
    master="local[*]",
    memory="2g"
)

try:
    # Generate single CSV file
    df = generator.generate_and_save(
        schema=your_schema,
        num_records=10000,
        output_path="output/data",  # Extension added automatically
        output_format="csv",
        single_file=True  # ✨ New parameter!
    )
    # Creates: output/data.csv (single file)

finally:
    generator.close()
```

### Multiple Part Files (Default)

```python
# Default behavior: multiple part files (good for big data)
df = generator.generate_and_save(
    schema=schema,
    num_records=1000000,
    output_path="output/big_data",
    output_format="parquet",
    single_file=False  # or omit (default)
)

# Creates:
# output/big_data/
#   ├── part-00000-xxx.parquet
#   ├── part-00001-xxx.parquet
#   ├── part-00002-xxx.parquet
#   └── _SUCCESS
```

### Single File Output

```python
# Single file output: one clean file (good for smaller data)
df = generator.generate_and_save(
    schema=schema,
    num_records=10000,
    output_path="output/data",
    output_format="csv",
    single_file=True  # ✨ Consolidate to single file
)

# Creates:
# output/data.csv (single file, no part-* files)
```

## Supported Formats

### CSV (Single File)

```python
df = generator.generate_and_save(
    schema=schema,
    num_records=5000,
    output_path="exports/employees",
    output_format="csv",
    single_file=True
)
# Creates: exports/employees.csv
```

**Use case:** Excel import, data analysis tools, sharing with non-technical users

### JSON (Single File)

```python
df = generator.generate_and_save(
    schema=schema,
    num_records=1000,
    output_path="api/data",
    output_format="json",
    single_file=True
)
# Creates: api/data.json
```

**Use case:** API responses, web applications, configuration files

### Parquet (Single File)

```python
df = generator.generate_and_save(
    schema=schema,
    num_records=50000,
    output_path="analytics/dataset",
    output_format="parquet",
    single_file=True
)
# Creates: analytics/dataset.parquet
```

**Use case:** Analytics tools, data warehouses, efficient storage

### ORC (Single File)

```python
df = generator.generate_and_save(
    schema=schema,
    num_records=20000,
    output_path="hadoop/data",
    output_format="orc",
    single_file=True
)
# Creates: hadoop/data.orc
```

**Use case:** Hadoop ecosystems, Hive tables, columnar storage

## When to Use Single File vs Multiple Files

### Use Single File (`single_file=True`) When:

✅ **Small to medium datasets** (< 100K records)
✅ **Sharing with others** (easier to send one file)
✅ **Excel/CSV imports** (Excel expects single files)
✅ **API responses** (single JSON file)
✅ **Simple downloads** (users expect one file)
✅ **Testing/development** (easier to inspect)

### Use Multiple Files (`single_file=False`) When:

✅ **Large datasets** (> 100K records)
✅ **Parallel processing** (faster writes)
✅ **Big data pipelines** (HDFS, S3)
✅ **Distributed systems** (Spark, Hadoop)
✅ **Scalability** (can process in parallel)
✅ **Production workloads** (better performance)

## Performance Considerations

### Single File
- **Pros:** Clean output, easy to share, single file
- **Cons:** Slower for large datasets (uses coalesce(1))
- **Best for:** < 100K records

### Multiple Files
- **Pros:** Fast, parallel writing, scalable
- **Cons:** Multiple part files, needs consolidation
- **Best for:** > 100K records

### Recommendations by Dataset Size

| Records | Recommendation | Reason |
|---------|----------------|--------|
| < 1K | Single file ✅ | Fast, clean output |
| 1K - 50K | Single file ✅ | Still fast, easy to share |
| 50K - 100K | Either ⚖️ | Trade-off between speed and convenience |
| 100K - 1M | Multiple files ✅ | Better performance |
| > 1M | Multiple files ✅ | Significantly faster |

## Complete Examples

### Example 1: Single CSV for Excel

```python
from schema_parser import SchemaParser
from spark_generator import SparkDataGenerator

# Parse schema
parser = SchemaParser()
schema = parser.parse_schema_file('employee_schema.xlsx')

# Generate single CSV file for Excel
generator = SparkDataGenerator(
    master="local[2]",
    memory="2g"
)

try:
    df = generator.generate_and_save(
        schema=schema,
        num_records=5000,
        output_path="exports/employees_2024",
        output_format="csv",
        single_file=True
    )
    print("✅ Created: exports/employees_2024.csv")

finally:
    generator.close()
```

### Example 2: Both Single and Multiple Files

```python
generator = SparkDataGenerator(
    master="local[*]",
    memory="4g"
)

try:
    # Generate large dataset with multiple files (fast)
    print("Generating large dataset...")
    df_large = generator.generate_and_save(
        schema=schema,
        num_records=1000000,
        output_path="data/full_dataset",
        output_format="parquet",
        num_partitions=100,
        single_file=False  # Multiple files for speed
    )

    # Generate sample as single CSV (easy to share)
    print("\nGenerating sample CSV...")
    df_sample = generator.generate_and_save(
        schema=schema,
        num_records=1000,
        output_path="samples/sample_data",
        output_format="csv",
        single_file=True  # Single file for sharing
    )

    print("✅ Created both datasets!")

finally:
    generator.close()
```

### Example 3: Different Formats

```python
generator = SparkDataGenerator(
    master="local[*]",
    memory="2g"
)

try:
    # CSV for Excel
    generator.generate_and_save(
        schema=schema,
        num_records=5000,
        output_path="exports/data",
        output_format="csv",
        single_file=True
    )
    # Creates: exports/data.csv

    # JSON for API
    generator.generate_and_save(
        schema=schema,
        num_records=1000,
        output_path="api/response",
        output_format="json",
        single_file=True
    )
    # Creates: api/response.json

    # Parquet for analytics
    generator.generate_and_save(
        schema=schema,
        num_records=50000,
        output_path="analytics/dataset",
        output_format="parquet",
        single_file=True
    )
    # Creates: analytics/dataset.parquet

finally:
    generator.close()
```

## How It Works

### Behind the Scenes

1. **Coalesce to 1 partition:** `df.coalesce(1)` merges all partitions
2. **Write to temp directory:** Spark writes `part-00000-xxx.csv` to temp dir
3. **Find part file:** Locate the single part file
4. **Rename file:** Move `part-00000-xxx.csv` → `data.csv`
5. **Clean up:** Remove temp directory and `_SUCCESS` files

### File Extension Handling

The generator automatically adds the correct extension:

```python
# You provide path without extension
output_path="data/employees"

# Generator adds extension based on format
single_file=True, output_format="csv"  → "data/employees.csv"
single_file=True, output_format="json" → "data/employees.json"
single_file=True, output_format="parquet" → "data/employees.parquet"
```

## Troubleshooting

### Issue: "No part files found"

**Cause:** DataFrame might be empty or write failed

**Solution:**
```python
# Check if DataFrame has data
print(f"Record count: {df.count()}")

# Verify write succeeded
if df.count() > 0:
    generator.save_massive_dataset(df, output_path, "csv", single_file=True)
```

### Issue: Slow performance with single file

**Cause:** coalesce(1) on large datasets

**Solution:**
```python
# For large datasets, use multiple files instead
if num_records > 100000:
    single_file = False  # Use multiple files for speed
```

### Issue: Out of memory with single file

**Cause:** Entire dataset must fit in single partition

**Solution:**
```python
# Increase driver memory
generator = SparkDataGenerator(
    master="local[*]",
    memory="8g"  # Increase memory
)

# Or use multiple files for very large datasets
single_file = False
```

## API Reference

### generate_and_save()

```python
def generate_and_save(
    schema: Dict,
    num_records: int,
    output_path: str,
    output_format: str = "parquet",
    num_partitions: int = None,
    show_sample: bool = True,
    single_file: bool = False  # ✨ New parameter
) -> DataFrame
```

**Parameters:**
- `single_file` (bool): If True, output single file instead of part files

### save_massive_dataset()

```python
def save_massive_dataset(
    df: DataFrame,
    output_path: str,
    output_format: str = "parquet",
    mode: str = "overwrite",
    compression: str = "snappy",
    coalesce_partitions: int = None,
    single_file: bool = False  # ✨ New parameter
)
```

**Parameters:**
- `single_file` (bool): If True, consolidate to single file
- `coalesce_partitions` (int): Overrides single_file if specified

## Best Practices

### 1. Choose Right Output Mode

```python
# Small dataset → Single file
if num_records < 50000:
    single_file = True

# Large dataset → Multiple files
else:
    single_file = False
```

### 2. Use Appropriate Formats

```python
# CSV: Good for Excel, human-readable
output_format = "csv", single_file = True

# Parquet: Good for analytics, compressed
output_format = "parquet", single_file = False

# JSON: Good for APIs, web apps
output_format = "json", single_file = True
```

### 3. Consider Your Use Case

```python
# Sharing with users → Single file
single_file = True

# Processing pipelines → Multiple files
single_file = False
```

## Summary

✅ **New Feature:** `single_file` parameter for clean output
✅ **Automatic:** Extension added automatically
✅ **Clean:** No part-* files or _SUCCESS files
✅ **Flexible:** Works with CSV, JSON, Parquet, ORC
✅ **Simple:** Just set `single_file=True`

Now you can generate clean, single-file outputs for easy sharing! 🎉
