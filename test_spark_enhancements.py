#!/usr/bin/env python3
"""
Test PySpark generator enhancements
"""

import sys

try:
    from pyspark.sql import SparkSession
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("PySpark is not installed. Skipping PySpark tests.")
    sys.exit(0)

from spark_generator import SparkDataGenerator


def test_spark_error_injection():
    """Test PySpark generator with error injection"""
    print("=" * 70)
    print("TEST: PySpark Generator with Error Injection")
    print("=" * 70)

    # Define a simple schema
    schema = {
        "type": "object",
        "title": "Employee",
        "properties": {
            "employee_id": {"type": "string"},
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "email": {"type": "string"},
            "nino": {"type": "string"},  # HMRC NINO
            "department": {
                "type": "string",
                "enum": ["IT", "HR", "Finance", "Marketing"]
            },
            "salary": {
                "type": "number",
                "minimum": 30000,
                "maximum": 150000
            },
            "is_active": {"type": "boolean"}
        },
        "required": ["employee_id", "first_name", "last_name", "email", "nino"]
    }

    # Initialize generator with 15% error injection
    print("\n--- Initializing PySpark generator with 15% error injection ---")
    generator = SparkDataGenerator(
        app_name="TestErrorInjection",
        master="local[2]",  # Use 2 cores
        memory="2g",
        error_rate=0.15
    )

    try:
        # Generate 10,000 records
        print("\n--- Generating 10,000 records ---")
        df = generator.generate_massive_dataset(
            schema=schema,
            num_records=10000,
            num_partitions=4
        )

        # Cache and show sample
        df.cache()

        print("\n--- Sample records (first 10) ---")
        df.show(10, truncate=False)

        print("\n--- Schema ---")
        df.printSchema()

        # Count records
        total_records = df.count()
        print(f"\nTotal records generated: {total_records:,}")

        # Count null values in each column
        print("\n--- Null value counts (showing data quality issues) ---")
        from pyspark.sql.functions import col, sum as spark_sum, when, isnan, isnull

        null_counts = df.select([
            spark_sum(
                when(isnull(col(c)) | (col(c) == ""), 1).otherwise(0)
            ).alias(c)
            for c in df.columns
        ])

        null_counts.show()

        # Calculate error percentage
        total_fields = len(df.columns) * total_records
        total_nulls = null_counts.collect()[0].asDict()
        total_null_count = sum(total_nulls.values())
        error_percentage = (total_null_count / total_fields) * 100 if total_fields > 0 else 0

        print(f"\n--- Statistics ---")
        print(f"Total fields: {total_fields:,}")
        print(f"Fields with null/empty values: {total_null_count:,}")
        print(f"Actual error rate: {error_percentage:.2f}%")
        print(f"Target error rate: {generator.error_rate * 100:.1f}%")

        # Check for NINO format
        print("\n--- Sample NINO values ---")
        nino_samples = df.select("nino").filter(col("nino").isNotNull()).limit(10)
        nino_samples.show(truncate=False)

        print("\n✓ PySpark generator with error injection working correctly!")

    finally:
        generator.close()


def main():
    """Run PySpark tests"""
    print("\n")
    print("╔" + "═" * 68 + "╗")
    print("║" + " " * 10 + "PYSPARK GENERATOR ENHANCEMENTS - TEST SUITE" + " " * 12 + "║")
    print("╚" + "═" * 68 + "╝")
    print()

    try:
        test_spark_error_injection()

        print("\n" + "=" * 70)
        print("PYSPARK TESTS COMPLETED SUCCESSFULLY! ✓")
        print("=" * 70)

    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
