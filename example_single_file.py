#!/usr/bin/env python3
"""
Example: Generate single file outputs with Spark

This example demonstrates the new single_file feature for creating
clean, single-file outputs instead of Spark's default part files.
"""

from spark_generator import SparkDataGenerator
from schema_parser import SchemaParser
import os

# Set Spark environment
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

def main():
    print("="*70)
    print("Single File Output Example")
    print("="*70)

    # Simple schema for demonstration
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "email": {"type": "string"},
            "age": {"type": "integer", "minimum": 18, "maximum": 80},
            "salary": {"type": "number", "minimum": 30000, "maximum": 150000},
            "is_active": {"type": "boolean"},
            "skills": {"type": "array", "items": {"type": "string"}, "maxItems": 5}
        },
        "required": ["id", "name", "email"]
    }

    # Initialize Spark generator
    generator = SparkDataGenerator(
        app_name="SingleFileExample",
        master="local[2]",
        memory="2g",
        error_rate=0.05  # 5% error injection
    )

    try:
        # Example 1: Single CSV file (perfect for Excel)
        print("\n1. Generating single CSV file for Excel...")
        print("-" * 70)
        df_csv = generator.generate_and_save(
            schema=schema,
            num_records=1000,
            output_path="generated_data/employees",
            output_format="csv",
            show_sample=False,
            single_file=True  # ✨ Creates single file!
        )
        print("✅ Created: generated_data/employees.csv")

        # Example 2: Single JSON file (perfect for APIs)
        print("\n2. Generating single JSON file for API...")
        print("-" * 70)
        df_json = generator.generate_and_save(
            schema=schema,
            num_records=500,
            output_path="generated_data/api_data",
            output_format="json",
            show_sample=False,
            single_file=True  # ✨ Creates single file!
        )
        print("✅ Created: generated_data/api_data.json")

        # Example 3: Single Parquet file (perfect for analytics)
        print("\n3. Generating single Parquet file for analytics...")
        print("-" * 70)
        df_parquet = generator.generate_and_save(
            schema=schema,
            num_records=5000,
            output_path="generated_data/analytics_data",
            output_format="parquet",
            show_sample=False,
            single_file=True  # ✨ Creates single file!
        )
        print("✅ Created: generated_data/analytics_data.parquet")

        # Example 4: Multiple files (for comparison)
        print("\n4. Generating multiple files (default behavior)...")
        print("-" * 70)
        df_multi = generator.generate_and_save(
            schema=schema,
            num_records=5000,
            output_path="generated_data/multi_files",
            output_format="parquet",
            num_partitions=4,
            show_sample=False,
            single_file=False  # Default: multiple part files
        )
        print("✅ Created: generated_data/multi_files/ (with part-* files)")

        print("\n" + "="*70)
        print("Summary")
        print("="*70)
        print("✅ Single CSV file:     generated_data/employees.csv")
        print("✅ Single JSON file:    generated_data/api_data.json")
        print("✅ Single Parquet file: generated_data/analytics_data.parquet")
        print("✅ Multiple files:      generated_data/multi_files/")
        print("\n💡 Tip: Use single_file=True for small datasets (< 50K records)")
        print("💡 Tip: Use single_file=False for large datasets (> 100K records)")

    finally:
        generator.close()

    print("\n✨ Example complete!")


if __name__ == "__main__":
    main()
