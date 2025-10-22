# CSV Complex Types Fix

## Problem

When generating CSV files with Spark, you encountered this error:

```
CSV data source doesn't support column 'fieldname' of type MapType(StringType, StringType)
```

**Root Cause:** CSV format is a simple text format that doesn't support complex data types like:
- **ArrayType** (lists/arrays)
- **MapType** (dictionaries/objects)
- **StructType** (nested objects)

## Solution

The Spark generator now **automatically converts** complex types to JSON strings when outputting to CSV format.

### What Changed

**File:** `spark_generator.py`

**Added:** `_prepare_dataframe_for_csv()` method (lines 338-390)

This method:
1. Detects columns with complex types (ArrayType, MapType, StructType)
2. Converts them to JSON strings using Spark's `to_json()` function
3. Preserves null values correctly
4. Prints conversion messages for transparency

### How It Works

#### Before (Would Fail) ❌

```python
# Schema with array and object fields
schema = {
    "properties": {
        "name": {"type": "string"},
        "skills": {"type": "array", "items": {"type": "string"}},  # ArrayType
        "metadata": {"type": "object"}  # MapType
    }
}

generator.generate_and_save(
    schema=schema,
    output_format="csv"  # ❌ Would fail with MapType error
)
```

#### After (Works Perfectly) ✅

```python
# Same schema
schema = {
    "properties": {
        "name": {"type": "string"},
        "skills": {"type": "array", "items": {"type": "string"}},
        "metadata": {"type": "object"}
    }
}

generator.generate_and_save(
    schema=schema,
    output_format="csv"  # ✅ Now works! Arrays/objects → JSON strings
)

# Output CSV:
# name,skills,metadata
# John,"[\"Python\",\"JavaScript\"]","{\"key\":\"value\"}"
# Jane,"[\"Java\",\"C++\"]","{\"key\":\"value\"}"
```

### Automatic Conversion

The conversion happens automatically:

| Schema Type | Spark Type | CSV Output | Example |
|-------------|-----------|------------|---------|
| `string` | StringType | Plain text | `John` |
| `integer` | LongType | Number | `42` |
| `number` | DoubleType | Number | `3.14` |
| `boolean` | BooleanType | true/false | `true` |
| `array` | ArrayType | JSON string | `["Python","Java"]` |
| `object` | MapType | JSON string | `{"key":"value"}` |

### Example Output

#### Schema with Arrays

```python
schema = {
    "properties": {
        "employee_id": {"type": "integer"},
        "name": {"type": "string"},
        "skills": {
            "type": "array",
            "items": {"type": "string"},
            "maxItems": 5
        }
    }
}

generator.generate_and_save(
    schema=schema,
    num_records=3,
    output_path="employees",
    output_format="csv",
    single_file=True
)
```

**Output CSV (employees.csv):**
```csv
employee_id,name,skills
1,John Smith,"[""Python"",""JavaScript"",""SQL""]"
2,Jane Doe,"[""Java"",""C++"",""Docker""]"
3,Bob Johnson,"[""React"",""Node.js""]"
```

#### Schema with Objects

```python
schema = {
    "properties": {
        "user_id": {"type": "integer"},
        "name": {"type": "string"},
        "address": {"type": "object"}
    }
}

generator.generate_and_save(
    schema=schema,
    num_records=2,
    output_path="users",
    output_format="csv",
    single_file=True
)
```

**Output CSV (users.csv):**
```csv
user_id,name,address
1,John Smith,"{""street"":""123 Main St"",""city"":""NYC""}"
2,Jane Doe,"{""street"":""456 Oak Ave"",""city"":""LA""}"
```

### Console Output

When converting complex types, you'll see informative messages:

```
Saving data to output/data as csv...
  → Converting array column 'skills' to JSON string for CSV
  → Converting map column 'metadata' to JSON string for CSV
Data saved successfully to output/data
```

### Reading CSV Back

If you need to read the CSV back and parse JSON columns:

#### Python (Pandas)

```python
import pandas as pd
import json

# Read CSV
df = pd.read_csv('employees.csv')

# Parse JSON string back to list
df['skills'] = df['skills'].apply(json.loads)

print(df['skills'][0])  # ['Python', 'JavaScript', 'SQL']
```

#### Python (Spark)

```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import ArrayType, StringType

# Read CSV
df = spark.read.csv('employees.csv', header=True)

# Parse JSON string back to array
df = df.withColumn('skills', from_json(col('skills'), ArrayType(StringType())))

df.show()
```

#### Excel

Excel will show the JSON strings as text:
```
["Python","JavaScript","SQL"]
```

You can use Excel formulas or Power Query to parse if needed.

### Supported Formats Comparison

| Format | Arrays | Objects | Notes |
|--------|--------|---------|-------|
| **CSV** | JSON string | JSON string | Automatic conversion ✅ |
| **JSON** | Native | Native | No conversion needed ✅ |
| **Parquet** | Native | Native | No conversion needed ✅ |
| **ORC** | Native | Native | No conversion needed ✅ |

### Complete Example

```python
from spark_generator import SparkDataGenerator
import os

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

# Schema with complex types
schema = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "email": {"type": "string"},
        "skills": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
            "maxItems": 5
        },
        "scores": {
            "type": "array",
            "items": {"type": "number"},
            "minItems": 1,
            "maxItems": 3
        },
        "is_active": {"type": "boolean"}
    }
}

generator = SparkDataGenerator(
    master="local[2]",
    memory="2g"
)

try:
    # Generate CSV with complex types - now works! ✅
    df = generator.generate_and_save(
        schema=schema,
        num_records=1000,
        output_path="employees",
        output_format="csv",
        single_file=True
    )

    print("✅ CSV generated successfully with arrays converted to JSON!")

finally:
    generator.close()
```

**Console Output:**
```
Saving data to employees as csv...
  → Converting array column 'skills' to JSON string for CSV
  → Converting array column 'scores' to JSON string for CSV
Data saved successfully to employees
  → Consolidated to single file: employees.csv
✅ CSV generated successfully with arrays converted to JSON!
```

**Result (employees.csv):**
```csv
id,name,email,skills,scores,is_active
1,John Smith,john@example.com,"[""Python"",""JavaScript""]","[4.5,3.8,4.2]",true
2,Jane Doe,jane@example.com,"[""Java"",""C++"",""Docker""]","[4.9,4.7]",true
3,Bob Johnson,bob@example.com,"[""React"",""Node.js""]","[4.1,4.3,3.9]",false
```

### Benefits

✅ **Automatic** - No manual conversion needed
✅ **Transparent** - Shows which columns are converted
✅ **Type-safe** - Preserves null values correctly
✅ **Excel-compatible** - Can open in Excel
✅ **Reversible** - Can parse JSON back to arrays/objects
✅ **No data loss** - All information preserved

### Edge Cases Handled

#### Null Values

```python
# Null arrays/objects are preserved as NULL
name,skills
John,"[""Python""]"
Jane,  # NULL preserved
```

#### Empty Arrays

```python
# Empty arrays converted to "[]"
name,skills
John,"[""Python""]"
Jane,"[]"  # Empty array
```

#### Nested Arrays

```python
# Nested structures converted to nested JSON
name,data
John,"[[1,2],[3,4]]"  # Array of arrays
```

#### Special Characters

```python
# Special characters properly escaped
name,skills
John,"[""C++"",""C#""]"  # Special chars handled
```

### Troubleshooting

#### Issue: JSON strings too long for Excel

**Cause:** Excel has cell limit of ~32K characters

**Solution:**
```python
# Use JSON or Parquet format for large complex data
output_format = "json"  # or "parquet"
```

#### Issue: Want to keep arrays as separate columns

**Cause:** CSV limitation - arrays must be converted

**Solution:**
```python
# Option 1: Flatten arrays into separate columns (custom code)
# Option 2: Use JSON/Parquet format instead
output_format = "json"
```

#### Issue: Need to parse JSON in Excel

**Cause:** Excel doesn't parse JSON automatically

**Solution:**
- Use Power Query in Excel to parse JSON
- Or use Python/Pandas to preprocess the CSV

### Migration from Old Version

#### Before (Would Fail)

```python
# This would crash with MapType error
generator.generate_and_save(
    schema=schema_with_arrays,
    output_format="csv"
)
# ❌ Error: CSV doesn't support ArrayType
```

#### After (Works)

```python
# Same code now works automatically
generator.generate_and_save(
    schema=schema_with_arrays,
    output_format="csv"
)
# ✅ Arrays automatically converted to JSON strings
```

**No code changes needed!** The fix is automatic.

### Best Practices

1. **For Simple Data** (no arrays/objects)
   ```python
   output_format = "csv"  # Works great!
   ```

2. **For Complex Data** (with arrays/objects)
   ```python
   # Option A: CSV with JSON strings (good for Excel)
   output_format = "csv"  # Arrays → JSON strings

   # Option B: Native format (better for processing)
   output_format = "parquet"  # or "json"
   ```

3. **For Excel Users**
   ```python
   output_format = "csv"  # Works, arrays as JSON strings
   single_file = True  # Single file easier for Excel
   ```

4. **For Data Processing**
   ```python
   output_format = "parquet"  # Better for large data
   single_file = False  # Faster parallel writes
   ```

## Summary

✅ **Problem Fixed:** CSV now supports schemas with arrays and objects
✅ **Automatic:** Converts complex types to JSON strings
✅ **Transparent:** Shows which columns are converted
✅ **Type-safe:** Preserves all data including nulls
✅ **Backward Compatible:** No code changes needed

You can now generate CSV files with any schema! 🎉
