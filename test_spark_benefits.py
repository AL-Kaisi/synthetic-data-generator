#!/usr/bin/env python3
"""
Test Spark generator with benefits claimant schema
Demonstrates schema with meaningful descriptions
"""

import json
import sys
import os
from schema_parser import SchemaParser
from spark_generator import SparkDataGenerator

def main():
    print("=" * 80)
    print("Testing Spark Generator with Benefits Claimant Schema")
    print("=" * 80)

    # Parse the benefits claimant Excel schema
    print("\n1. Parsing benefits claimant Excel schema...")
    parser = SchemaParser()
    schema = parser.parse_schema_file('custom_schemas/benefits_claimant_3col.xlsx')

    print("\nParsed schema:")
    print(json.dumps(schema, indent=2))

    # Initialize Spark generator
    print("\n2. Initializing Spark generator...")
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

    generator = SparkDataGenerator(
        app_name="BenefitsDataGeneration",
        master="local[2]",
        memory="2g",
        error_rate=0.0  # No error injection for Spark
    )

    try:
        # Generate test dataset
        print("\n3. Generating benefits claimant dataset...")
        num_records = 5000
        output_path = "generated_data/spark_benefits"

        df = generator.generate_and_save(
            schema=schema,
            num_records=num_records,
            output_path=output_path,
            output_format="parquet",
            num_partitions=2,
            show_sample=True
        )

        # Show some statistics
        print("\n4. Dataset Statistics:")
        print(f"   Partitions: {df.rdd.getNumPartitions()}")

        # Show schema details
        print("\n5. Generated DataFrame Schema:")
        df.printSchema()

        # Show value distributions
        print("\n6. Benefit type distribution:")
        df.groupBy("benefit_type").count().orderBy("count", ascending=False).show()

        print("\n7. Claim status distribution:")
        df.groupBy("claim_status").count().orderBy("count", ascending=False).show()

        print("\n8. Payment frequency distribution:")
        df.groupBy("payment_frequency").count().orderBy("count", ascending=False).show()

        print("\n9. Sample claim amounts (statistics):")
        df.select("claim_amount").summary("count", "mean", "stddev", "min", "max").show()

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
