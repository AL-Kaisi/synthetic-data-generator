#!/usr/bin/env python3
"""
Generate Synthetic Data from Custom Schemas

This script:
1. Lists all schemas in the custom_schemas folder
2. Lets you choose which schema to use
3. Generates data using Spark with proper CSV MapType/ArrayType handling
4. Outputs as single CSV file (or other formats)
"""

import os
import sys
from pathlib import Path

# Set Spark environment
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from schema_parser import SchemaParser
from spark_generator import SparkDataGenerator


def list_schema_files(schema_dir: str = "custom_schemas") -> list:
    """
    List all schema files in the custom_schemas directory

    Args:
        schema_dir: Directory containing schema files

    Returns:
        List of schema file paths
    """
    schema_path = Path(schema_dir)

    if not schema_path.exists():
        print(f"❌ Schema directory not found: {schema_dir}")
        return []

    # Find all supported schema files
    schema_files = []
    for ext in ['.xlsx', '.xls', '.json', '.csv', '.sql']:
        schema_files.extend(schema_path.glob(f'*{ext}'))

    # Filter out README and other documentation files
    schema_files = [f for f in schema_files if 'README' not in f.name.upper()]

    return sorted(schema_files)


def choose_schema(schema_files: list) -> Path:
    """
    Let user choose a schema file interactively

    Args:
        schema_files: List of available schema files

    Returns:
        Chosen schema file path
    """
    print("\n" + "="*70)
    print("Available Schema Files")
    print("="*70)

    for i, schema_file in enumerate(schema_files, 1):
        print(f"{i}. {schema_file.name}")

    while True:
        try:
            choice = input(f"\nChoose a schema (1-{len(schema_files)}) or 'q' to quit: ").strip()

            if choice.lower() == 'q':
                print("Exiting...")
                sys.exit(0)

            choice_num = int(choice)
            if 1 <= choice_num <= len(schema_files):
                return schema_files[choice_num - 1]
            else:
                print(f"❌ Please enter a number between 1 and {len(schema_files)}")
        except ValueError:
            print("❌ Please enter a valid number or 'q' to quit")


def get_output_config():
    """
    Get output configuration from user

    Returns:
        Tuple of (num_records, output_format, single_file, error_rate)
    """
    print("\n" + "="*70)
    print("Output Configuration")
    print("="*70)

    # Number of records
    while True:
        try:
            num_records = input("Number of records to generate (default: 1000): ").strip()
            num_records = int(num_records) if num_records else 1000
            if num_records > 0:
                break
            print("❌ Please enter a positive number")
        except ValueError:
            print("❌ Please enter a valid number")

    # Output format
    print("\nOutput formats:")
    print("  1. CSV (Excel-compatible, with automatic array/object conversion)")
    print("  2. JSON (Native array/object support)")
    print("  3. Parquet (Columnar, compressed, best for analytics)")
    print("  4. ORC (Optimized row columnar)")

    format_map = {
        '1': 'csv',
        '2': 'json',
        '3': 'parquet',
        '4': 'orc'
    }

    while True:
        format_choice = input("Choose output format (1-4, default: 1 CSV): ").strip()
        format_choice = format_choice if format_choice else '1'
        if format_choice in format_map:
            output_format = format_map[format_choice]
            break
        print("❌ Please enter 1, 2, 3, or 4")

    # Single file or multiple files
    if num_records <= 100000:
        default_single = 'y'
        print(f"\nFor {num_records:,} records, single file is recommended")
    else:
        default_single = 'n'
        print(f"\nFor {num_records:,} records, multiple files may be faster")

    single_input = input(f"Generate single file? (y/n, default: {default_single}): ").strip().lower()
    single_input = single_input if single_input else default_single
    single_file = single_input == 'y'

    # Error injection rate
    error_input = input("Error injection rate (0.0-1.0, default: 0.0): ").strip()
    try:
        error_rate = float(error_input) if error_input else 0.0
        error_rate = max(0.0, min(1.0, error_rate))  # Clamp between 0 and 1
    except ValueError:
        error_rate = 0.0

    return num_records, output_format, single_file, error_rate


def main():
    """Main execution function"""
    print("="*70)
    print("Synthetic Data Generator - Custom Schemas")
    print("="*70)
    print("\nThis tool generates synthetic data from your custom schema files")
    print("with automatic CSV complex type handling (arrays/objects → JSON strings)")

    # List available schemas
    schema_files = list_schema_files()

    if not schema_files:
        print("\n❌ No schema files found in custom_schemas directory")
        print("\nSupported formats: .xlsx, .xls, .json, .csv, .sql")
        sys.exit(1)

    # Choose schema
    chosen_schema = choose_schema(schema_files)
    print(f"\n✓ Selected schema: {chosen_schema.name}")

    # Parse schema
    print(f"\n{'='*70}")
    print("Parsing Schema")
    print("="*70)

    parser = SchemaParser()
    try:
        schema = parser.parse_schema_file(str(chosen_schema))
        print(f"✓ Successfully parsed schema")
        print(f"\n  Schema name: {schema.get('title', 'Untitled')}")
        print(f"  Number of fields: {len(schema.get('properties', {}))}")

        # Show field types
        print(f"\n  Fields:")
        for field_name, field_def in schema.get('properties', {}).items():
            field_type = field_def.get('type', 'unknown')
            if field_type == 'array':
                item_type = field_def.get('items', {}).get('type', 'unknown')
                print(f"    - {field_name}: array of {item_type}")
            else:
                print(f"    - {field_name}: {field_type}")

    except Exception as e:
        print(f"❌ Error parsing schema: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Get output configuration
    num_records, output_format, single_file, error_rate = get_output_config()

    # Generate output path
    output_dir = "generated_data"
    output_name = chosen_schema.stem  # filename without extension
    output_path = f"{output_dir}/{output_name}_output"

    # Summary
    print(f"\n{'='*70}")
    print("Generation Summary")
    print("="*70)
    print(f"  Schema: {chosen_schema.name}")
    print(f"  Records: {num_records:,}")
    print(f"  Format: {output_format.upper()}")
    print(f"  Single file: {'Yes' if single_file else 'No'}")
    print(f"  Error injection: {error_rate*100:.1f}%")
    print(f"  Output: {output_path}.{output_format}")

    if output_format == 'csv':
        print("\n  ℹ️  CSV Note: Arrays and objects will be automatically")
        print("     converted to JSON strings for CSV compatibility")

    confirm = input(f"\nProceed with generation? (y/n, default: y): ").strip().lower()
    if confirm and confirm != 'y':
        print("Cancelled.")
        sys.exit(0)

    # Initialize Spark generator
    print(f"\n{'='*70}")
    print("Initializing Spark Generator")
    print("="*70)

    generator = SparkDataGenerator(
        app_name=f"Generate_{output_name}",
        master="local[*]",  # Use all available cores
        memory="4g",
        error_rate=error_rate
    )

    try:
        # Generate and save data
        print(f"\n{'='*70}")
        print("Generating Data")
        print("="*70)

        df = generator.generate_and_save(
            schema=schema,
            num_records=num_records,
            output_path=output_path,
            output_format=output_format,
            num_partitions=max(1, num_records // 1000),  # Auto partition
            show_sample=True,
            single_file=single_file
        )

        # Success message
        print(f"\n{'='*70}")
        print("✅ SUCCESS!")
        print("="*70)

        if single_file:
            final_file = f"{output_path}.{output_format}"
            print(f"\n✓ Generated single file: {final_file}")

            # Show file size
            if os.path.exists(final_file):
                file_size = os.path.getsize(final_file)
                size_mb = file_size / (1024 * 1024)
                print(f"  File size: {size_mb:.2f} MB ({file_size:,} bytes)")
        else:
            print(f"\n✓ Generated files in: {output_path}/")

            # Count part files
            if os.path.exists(output_path):
                part_files = list(Path(output_path).glob("part-*"))
                print(f"  Part files: {len(part_files)}")

        print(f"\n📊 Data Statistics:")
        print(f"  Total records: {num_records:,}")
        if error_rate > 0:
            print(f"  Error injection rate: {error_rate*100:.1f}%")
            print(f"  Expected errors: ~{int(num_records * error_rate * len(schema.get('properties', {})))} field errors")

        # CSV-specific instructions
        if output_format == 'csv':
            print(f"\n💡 CSV Tips:")
            print(f"  - Open in Excel, Google Sheets, or any CSV reader")
            print(f"  - Arrays/objects are stored as JSON strings")
            print(f"  - Example array: [\"value1\",\"value2\"]")
            print(f"  - Example object: {{\"key\":\"value\"}}")
            print(f"  - Use Python/Pandas json.loads() to parse back if needed")

    except Exception as e:
        print(f"\n❌ Error during generation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        generator.close()
        print(f"\n✓ Spark session closed")

    print(f"\n{'='*70}")
    print("Generation Complete!")
    print("="*70)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nCancelled by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
