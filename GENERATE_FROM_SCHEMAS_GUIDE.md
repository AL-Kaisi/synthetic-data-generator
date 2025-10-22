# How to Generate Data from Custom Schemas

## Quick Start

You have **two easy ways** to generate data from your custom schemas:

### Method 1: Quick Generate (Fastest)

Use `quick_generate.py` for fast, one-command data generation:

```bash
# Basic usage (generates 1000 records as CSV)
python3 quick_generate.py custom_schemas/employee_schema_3col.xlsx

# Specify number of records
python3 quick_generate.py custom_schemas/employee_schema_3col.xlsx 5000

# Specify format (csv, json, parquet, orc)
python3 quick_generate.py custom_schemas/employee_schema_3col.xlsx 5000 csv

# Different examples
python3 quick_generate.py custom_schemas/customer_schema_3col.xlsx 10000 json
python3 quick_generate.py custom_schemas/benefits_claimant_3col.xlsx 2000 parquet
```

**Output**: Creates `generated_data/<schema_name>_output.<format>`

---

### Method 2: Interactive Generator (More Options)

Use `generate_from_custom_schemas.py` for interactive mode with more options:

```bash
python3 generate_from_custom_schemas.py
```

This will:
1. Show all available schemas in `custom_schemas/` folder
2. Let you choose which one to use
3. Ask how many records to generate
4. Ask which format (CSV, JSON, Parquet, ORC)
5. Ask if you want single file or multiple files
6. Ask if you want error injection (for testing)
7. Generate the data with progress output

---

## CSV Complex Type Handling ✅

**IMPORTANT**: The CSV format doesn't support arrays or objects natively.

### Automatic Conversion

When you generate CSV output, the generator **automatically converts**:
- **Arrays** → JSON strings: `["item1","item2","item3"]`
- **Objects** → JSON strings: `{"key1":"value1","key2":"value2"}`

### Example

If your schema has:
```
skills: array of strings
metadata: object
```

The CSV output will look like:
```csv
name,skills,metadata
John,"[""Python"",""Java"",""SQL""]","{""department"":""IT""}"
Jane,"[""Marketing"",""Sales""]","{""department"":""Sales""}"
```

You'll see this message during generation:
```
  → Converting array column 'skills' to JSON string for CSV
  → Converting map column 'metadata' to JSON string for CSV
```

### Reading CSV Back

If you need to parse the JSON strings back to arrays/objects:

**Python (Pandas)**:
```python
import pandas as pd
import json

df = pd.read_csv('generated_data/employees_output.csv')

# Parse JSON strings back to lists/dicts
df['skills'] = df['skills'].apply(json.loads)
df['metadata'] = df['metadata'].apply(json.loads)
```

**Excel**:
- Arrays/objects appear as text: `["Python","Java"]`
- You can use Excel's Power Query to parse JSON if needed
- Or just use them as text values for viewing

---

## Available Schemas

Your `custom_schemas/` folder contains:

```
custom_schemas/
├── employee_schema_3col.xlsx       ← Employee data schema
├── customer_schema_3col.xlsx       ← Customer data schema
├── benefits_claimant_3col.xlsx     ← Benefits claimant schema
├── customer.json                   ← Customer JSON schema
├── blog_post.json                  ← Blog post schema
├── employee_table.sql              ← Employee SQL schema
├── customer_orders.sql             ← Customer orders SQL schema
└── dwp_relational_example.json     ← Relational schema example
```

All of these work with both generation methods!

---

## Examples

### Example 1: Generate Employee CSV

```bash
python3 quick_generate.py custom_schemas/employee_schema_3col.xlsx 1000 csv
```

**Output**: `generated_data/employee_schema_3col_output.csv`

---

### Example 2: Generate Customer JSON

```bash
python3 quick_generate.py custom_schemas/customer_schema_3col.xlsx 5000 json
```

**Output**: `generated_data/customer_schema_3col_output.json`

---

### Example 3: Generate Benefits Data with Errors (Interactive)

```bash
python3 generate_from_custom_schemas.py
```

Then choose:
- Schema: `benefits_claimant_3col.xlsx`
- Records: `10000`
- Format: `CSV`
- Single file: `Yes`
- Error injection: `0.1` (10% errors for testing)

**Output**: `generated_data/benefits_claimant_3col_output.csv`

---

### Example 4: Generate from SQL Schema

```bash
python3 quick_generate.py custom_schemas/employee_table.sql 2000 parquet
```

**Output**: `generated_data/employee_table_output.parquet`

---

## Output Formats

### CSV
- ✅ Excel-compatible
- ✅ Arrays/objects auto-converted to JSON strings
- ✅ Human-readable
- ❌ Larger file size
- **Best for**: Excel users, small datasets, sharing

### JSON
- ✅ Native array/object support
- ✅ No conversion needed
- ✅ Human-readable
- ❌ Larger file size than Parquet
- **Best for**: APIs, web apps, JavaScript

### Parquet
- ✅ Native array/object support
- ✅ Highly compressed
- ✅ Fast for analytics
- ❌ Not human-readable (binary)
- **Best for**: Big data, analytics, Spark/Pandas

### ORC
- ✅ Native array/object support
- ✅ Highly compressed
- ✅ Optimized for Hadoop
- ❌ Not human-readable (binary)
- **Best for**: Hadoop ecosystem, Hive tables

---

## Troubleshooting

### Error: "CSV doesn't support MapType"

**This should NOT happen** - the fix is already in place!

If you still see this error:
1. Make sure you're using the latest `spark_generator.py`
2. Check that you're running the script (not calling methods directly)
3. Verify the conversion messages appear:
   ```
   → Converting array column 'skills' to JSON string for CSV
   → Converting map column 'metadata' to JSON string for CSV
   ```

### Error: "Java not found"

Spark requires Java:

```bash
# macOS
brew install openjdk@11

# Linux
sudo apt-get install openjdk-11-jdk

# Verify
java -version
```

### Error: "Schema file not found"

Make sure:
1. You're in the project root directory
2. The schema file exists in `custom_schemas/`
3. You're using the correct filename (check spelling)

```bash
# List available schemas
ls -la custom_schemas/
```

### Error: "PySpark not installed"

Install PySpark:

```bash
pip install pyspark
```

### Memory Errors

For large datasets (>100K records), increase memory:

Edit the script and change:
```python
generator = SparkDataGenerator(
    memory="8g"  # Increase from 4g to 8g or 16g
)
```

---

## Advanced Usage

### Generate Multiple Formats at Once

```bash
# Generate CSV
python3 quick_generate.py custom_schemas/employee_schema_3col.xlsx 5000 csv

# Generate JSON
python3 quick_generate.py custom_schemas/employee_schema_3col.xlsx 5000 json

# Generate Parquet
python3 quick_generate.py custom_schemas/employee_schema_3col.xlsx 5000 parquet
```

Now you have the same data in 3 formats!

---

### Use with Error Injection (Testing)

Use interactive mode to inject data quality issues:

```bash
python3 generate_from_custom_schemas.py
```

Set error rate to `0.1` for 10% errors (good for testing data quality tools)

---

### Create Custom Schema

Create your own Excel schema in `custom_schemas/`:

**Format** (use 3 columns):
| column_name | values | description |
|-------------|--------|-------------|
| employee_id | | id |
| name | | name |
| department | IT,Sales,HR,Marketing | |
| salary | | number |
| is_active | true,false | boolean |
| skills | | array |

Then generate:
```bash
python3 quick_generate.py custom_schemas/my_schema.xlsx 1000 csv
```

---

## Summary

### Quick Method
```bash
# One command, fast results
python3 quick_generate.py <schema_file> [records] [format]
```

### Interactive Method
```bash
# More options, guided process
python3 generate_from_custom_schemas.py
```

### CSV Complex Types
✅ **Automatic conversion** - arrays and objects become JSON strings
✅ **Excel compatible** - opens in Excel without errors
✅ **Reversible** - can parse JSON strings back if needed

---

**Questions?**
- Check the schema files in `custom_schemas/` for examples
- Review `CSV_COMPLEX_TYPES_FIX.md` for technical details
- Review `SINGLE_FILE_OUTPUT.md` for output options

---

**Ready to generate!** 🚀
