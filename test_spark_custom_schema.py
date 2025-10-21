#!/usr/bin/env python3
"""
Test script for Spark generator with custom Excel schema
"""

import json
import sys
from schema_parser import SchemaParser
from spark_generator import SparkDataGenerator

def main():
    print("=" * 80)
    print("Testing Spark Generator with Custom Excel Schema")
    print("=" * 80)

    # Parse the employee Excel schema
    print("\n1. Parsing employee Excel schema...")
    parser = SchemaParser()
    schema = parser.parse_schema_file('custom_schemas/employee_schema_3col.xlsx')

    print("\nParsed schema:")
    print(json.dumps(schema, indent=2))

    # Initialize Spark generator
    print("\n2. Initializing Spark generator...")
    import os
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

    generator = SparkDataGenerator(
        app_name="EmployeeDataGeneration",
        master="local[2]",  # Use 2 cores to avoid resource issues
        memory="2g",  # Reduced memory for local testing
        error_rate=0.0  # No error injection for Spark (strict type checking)
    )

    try:
        # Generate test dataset
        print("\n3. Generating employee dataset...")
        num_records = 10000
        output_path = "generated_data/spark_employees"

        df = generator.generate_and_save(
            schema=schema,
            num_records=num_records,
            output_path=output_path,
            output_format="parquet",
            num_partitions=4,
            show_sample=True
        )

        # Show some statistics
        print("\n4. Dataset Statistics:")
        print(f"   Partitions: {df.rdd.getNumPartitions()}")
        print(f"   Error injection rate: {generator.error_rate * 100:.1f}%")

        # Show schema details
        print("\n5. Generated DataFrame Schema:")
        df.printSchema()

        # Show value counts for some categorical fields
        print("\n6. Department distribution:")
        df.groupBy("department").count().show()

        print("\n7. Office location distribution:")
        df.groupBy("office_location").count().show()

        print("\n" + "=" * 80)
        print("Test completed successfully!")
        print("=" * 80)

    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        generator.close()

if __name__ == "__main__":
    main()
