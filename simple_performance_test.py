#!/usr/bin/env python3
"""
Simple performance test for key schemas
"""

import time
from simple_generator import SchemaDataGenerator
from schemas import SchemaLibrary

def test_performance():
    """Test performance of key schemas"""
    generator = SchemaDataGenerator()
    all_schemas = SchemaLibrary.get_all_schemas()

    # Test key schemas with different record counts
    test_cases = [
        ('ecommerce_product', [100, 1000, 5000]),
        ('data_pipeline_metadata', [100, 1000, 5000]),
        ('child_benefit', [100, 1000, 5000]) if 'child_benefit' in all_schemas else ('healthcare_patient', [100, 1000, 5000])
    ]

    print("Performance Test Results")
    print("=" * 50)

    for schema_name, record_counts in test_cases:
        if schema_name not in all_schemas:
            continue

        schema = all_schemas[schema_name]
        print(f"\n{schema_name} ({len(schema['properties'])} fields):")

        for count in record_counts:
            start_time = time.time()
            data = generator.generate_from_schema(schema, count)
            duration = time.time() - start_time

            records_per_sec = count / duration if duration > 0 else 0
            print(f"  {count:,} records: {duration:.3f}s ({records_per_sec:,.0f} records/sec)")

if __name__ == "__main__":
    test_performance()