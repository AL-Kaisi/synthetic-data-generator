# How to Use - Simple Steps

## Step 1: Drop Schema File

Put your schema file in `custom_schemas/` folder:

```
custom_schemas/
├── your_schema.xlsx
├── another_schema.json
└── employee_schema_3col.xlsx
```

**Supported formats:**
- Excel: `.xlsx`, `.xls`
- JSON: `.json`
- CSV: `.csv`
- SQL: `.sql`

---

## Step 2: Run Spark Generator

Simply run:

```bash
python3 spark_generator.py
```

That's it!

---

## What Happens

The script will automatically:

1. ✅ **Scan** `custom_schemas/` folder
2. ✅ **Find** all schema files
3. ✅ **Parse** each schema
4. ✅ **Generate** 1000 records from each
5. ✅ **Save** as CSV in `generated_data/` folder

---

## Example Output

```
======================================================================
Spark Data Generator - Auto Mode
======================================================================

Scanning custom_schemas/ folder for schema files...

✓ Found 3 schema file(s):
  1. employee_schema_3col.xlsx
  2. customer_schema_3col.xlsx
  3. benefits_claimant_3col.xlsx

======================================================================
Initializing Spark Generator
======================================================================

======================================================================
Processing: employee_schema_3col.xlsx
======================================================================
  Parsing schema...
  ✓ Parsed successfully (13 fields)
  ℹ️  Contains arrays/objects (will convert to JSON for CSV)
  Generating 1000 records...

Saving data to generated_data/employee_schema_3col as csv...
  → Converting array column 'skills' to JSON string for CSV
Data saved successfully to generated_data/employee_schema_3col
  → Consolidated to single file: generated_data/employee_schema_3col.csv

  ✅ SUCCESS: generated_data/employee_schema_3col.csv (45.2 KB)

======================================================================
Processing: customer_schema_3col.xlsx
======================================================================
  Parsing schema...
  ✓ Parsed successfully (10 fields)
  Generating 1000 records...

Saving data to generated_data/customer_schema_3col as csv...
Data saved successfully to generated_data/customer_schema_3col
  → Consolidated to single file: generated_data/customer_schema_3col.csv

  ✅ SUCCESS: generated_data/customer_schema_3col.csv (38.7 KB)

======================================================================
SUMMARY
======================================================================
Total schemas: 3
Successful: 3
Failed: 0

✓ Generated files in: generated_data/
  - generated_data/employee_schema_3col.csv
  - generated_data/customer_schema_3col.csv
  - generated_data/benefits_claimant_3col.csv

✓ Spark session closed
```

---

## Output Location

All generated CSV files are saved in:

```
generated_data/
├── employee_schema_3col.csv
├── customer_schema_3col.csv
└── benefits_claimant_3col.csv
```

---

## CSV with Arrays/Objects

If your schema has arrays or objects, they're **automatically converted** to JSON strings:

**Schema:**
```
skills: array of strings
metadata: object
```

**CSV Output:**
```csv
name,skills,metadata
John,"[""Python"",""Java""]","{""dept"":""IT""}"
Jane,"[""Marketing""]","{""dept"":""Sales""}"
```

**You'll see this message:**
```
→ Converting array column 'skills' to JSON string for CSV
→ Converting map column 'metadata' to JSON string for CSV
```

---

## Your Current Schemas

You already have these schemas in `custom_schemas/`:

```
✓ employee_schema_3col.xlsx
✓ customer_schema_3col.xlsx
✓ benefits_claimant_3col.xlsx
✓ customer.json
✓ blog_post.json
✓ employee_table.sql
✓ customer_orders.sql
✓ dwp_relational_example.json
```

Just run:
```bash
python3 spark_generator.py
```

And all of them will be processed! 🎉

---

## Troubleshooting

### Error: "Java not found"

Install Java:
```bash
# macOS
brew install openjdk@11

# Linux
sudo apt-get install openjdk-11-jdk
```

### Error: "No schema files found"

Make sure:
1. `custom_schemas/` folder exists
2. Schema files are in supported formats (.xlsx, .json, .csv, .sql)
3. You're running from the project root directory

### Error: "CSV doesn't support MapType"

This should NOT happen anymore - the fix is in place!

If you still see it, check that you have the latest `spark_generator.py` (the fix is on line 308-309).

---

## Summary

**Workflow:**
1. Drop schema → `custom_schemas/your_schema.xlsx`
2. Run → `python3 spark_generator.py`
3. Get CSV → `generated_data/your_schema.csv`

**That's it!** ✅
