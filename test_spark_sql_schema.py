#!/usr/bin/env python3
"""
Test Spark generator with SQL schema
Demonstrates SQL DDL schema parsing and data generation
"""

import json
import sys
import os
from schema_parser import SchemaParser
from spark_generator import SparkDataGenerator

def main():
    print("=" * 80)
    print("Testing Spark Generator with SQL Schema")
    print("=" * 80)

    # Parse the employee SQL schema
    print("\n1. Parsing employee SQL schema...")
    parser = SchemaParser()
    schema = parser.parse_schema_file('custom_schemas/employee_table.sql')

    print("\nParsed schema from SQL:")
    print(json.dumps(schema, indent=2))

    # Initialize Spark generator
    print("\n2. Initializing Spark generator...")
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

    generator = SparkDataGenerator(
        app_name="EmployeeDataGeneration",
        master="local[2]",
        memory="2g",
        error_rate=0.0  # No error injection for Spark
    )

    try:
        # Generate test dataset
        print("\n3. Generating employee dataset from SQL schema...")
        num_records = 10000
        output_path = "generated_data/spark_sql_employees"

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
        print(f"   Total records: {df.count()}")
        print(f"   Partitions: {df.rdd.getNumPartitions()}")

        # Show schema details
        print("\n5. Generated DataFrame Schema:")
        df.printSchema()

        # Show value distributions
        print("\n6. Department distribution:")
        df.groupBy("department").count().orderBy("count", ascending=False).show()

        print("\n7. Active status distribution:")
        df.groupBy("is_active").count().show()

        print("\n8. Salary statistics:")
        df.select("salary").summary("count", "mean", "stddev", "min", "max").show()

        print("\n9. Years of service statistics:")
        df.select("years_of_service").summary("count", "mean", "stddev", "min", "max").show()

        # Show sample records
        print("\n10. Sample employee records:")
        df.select("employee_id", "first_name", "last_name", "department", "job_title", "salary").show(10, truncate=False)

        print("\n" + "=" * 80)
        print("Test completed successfully!")
        print(f"Generated {num_records} employee records from SQL schema")
        print(f"Output saved to: {output_path}")
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
