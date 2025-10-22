#!/usr/bin/env python3
"""
Quick Generate - Generate data from a schema file with one command

Usage:
    python3 quick_generate.py <schema_file> [num_records] [output_format]

Examples:
    python3 quick_generate.py custom_schemas/employee_schema_3col.xlsx
    python3 quick_generate.py custom_schemas/employee_schema_3col.xlsx 5000
    python3 quick_generate.py custom_schemas/employee_schema_3col.xlsx 5000 csv
    python3 quick_generate.py custom_schemas/customer_schema_3col.xlsx 10000 json
"""

import os
import sys
from pathlib import Path

# Set Spark environment
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from schema_parser import SchemaParser
from spark_generator import SparkDataGenerator


def main():
    """Generate data from schema file"""

    # Parse arguments
    if len(sys.argv) < 2:
        print("Usage: python3 quick_generate.py <schema_file> [num_records] [output_format]")
        print("\nExamples:")
        print("  python3 quick_generate.py custom_schemas/employee_schema_3col.xlsx")
        print("  python3 quick_generate.py custom_schemas/employee_schema_3col.xlsx 5000")
        print("  python3 quick_generate.py custom_schemas/employee_schema_3col.xlsx 5000 csv")
        print("\nSupported formats: csv, json, parquet, orc")
        sys.exit(1)

    schema_file = sys.argv[1]
    num_records = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    output_format = sys.argv[3].lower() if len(sys.argv) > 3 else "csv"

    # Validate schema file
    if not os.path.exists(schema_file):
        print(f"❌ Schema file not found: {schema_file}")
        sys.exit(1)

    # Validate format
    if output_format not in ['csv', 'json', 'parquet', 'orc']:
        print(f"❌ Unsupported format: {output_format}")
        print("Supported formats: csv, json, parquet, orc")
        sys.exit(1)

    print("="*70)
    print("Quick Data Generation")
    print("="*70)
    print(f"Schema: {schema_file}")
    print(f"Records: {num_records:,}")
    print(f"Format: {output_format.upper()}")

    # Parse schema
    print("\n✓ Parsing schema...")
    parser = SchemaParser()

    try:
        schema = parser.parse_schema_file(schema_file)
        print(f"✓ Schema parsed successfully")
        print(f"  Fields: {len(schema.get('properties', {}))}")

        # Show if there are complex types (arrays, objects)
        has_arrays = any(f.get('type') == 'array' for f in schema.get('properties', {}).values())
        has_objects = any(f.get('type') == 'object' for f in schema.get('properties', {}).values())

        if (has_arrays or has_objects) and output_format == 'csv':
            print(f"\n  ℹ️  Note: Schema contains arrays/objects")
            print(f"     They will be converted to JSON strings for CSV")

    except Exception as e:
        print(f"❌ Error parsing schema: {e}")
        sys.exit(1)

    # Generate output path
    output_dir = "generated_data"
    os.makedirs(output_dir, exist_ok=True)

    schema_name = Path(schema_file).stem
    output_path = f"{output_dir}/{schema_name}_output"

    # Initialize generator
    print(f"\n✓ Initializing Spark generator...")

    generator = SparkDataGenerator(
        app_name=f"QuickGen_{schema_name}",
        master="local[*]",
        memory="4g",
        error_rate=0.0  # No errors for quick generation
    )

    try:
        # Generate data
        print(f"\n✓ Generating {num_records:,} records...")

        df = generator.generate_and_save(
            schema=schema,
            num_records=num_records,
            output_path=output_path,
            output_format=output_format,
            num_partitions=max(1, num_records // 1000),
            show_sample=True,
            single_file=True  # Always single file for quick generation
        )

        # Success
        final_file = f"{output_path}.{output_format}"
        print(f"\n{'='*70}")
        print("✅ SUCCESS!")
        print("="*70)
        print(f"\n✓ Generated: {final_file}")

        if os.path.exists(final_file):
            file_size = os.path.getsize(final_file)
            size_mb = file_size / (1024 * 1024)
            print(f"  Size: {size_mb:.2f} MB")

        print(f"  Records: {num_records:,}")

        if output_format == 'csv':
            print(f"\n💡 Open in Excel or any CSV reader")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        generator.close()

    print(f"\n✓ Done!")


if __name__ == "__main__":
    main()
