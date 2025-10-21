#!/usr/bin/env python3
"""
Test script for simple generator with custom Excel schema
"""

import json
import sys
from schema_parser import SchemaParser
from simple_generator import SchemaDataGenerator

def main():
    print("=" * 80)
    print("Testing Simple Generator with Custom Excel Schema")
    print("=" * 80)

    # Parse the employee Excel schema
    print("\n1. Parsing employee Excel schema...")
    parser = SchemaParser()
    schema = parser.parse_schema_file('custom_schemas/employee_schema_3col.xlsx')

    print("\nParsed schema:")
    print(json.dumps(schema, indent=2))

    # Initialize simple generator
    print("\n2. Initializing data generator...")
    generator = SchemaDataGenerator(error_rate=0.05)  # 5% error injection

    # Generate test dataset
    print("\n3. Generating employee dataset...")
    num_records = 100

    # Generate data
    data = generator.generate_from_schema(schema, num_records)

    print(f"\nGenerated {len(data)} records")

    # Show first 5 records
    print("\n4. Sample records:")
    for i, record in enumerate(data[:5], 1):
        print(f"\nRecord {i}:")
        print(json.dumps(record, indent=2, default=str))

    # Save to CSV
    print("\n5. Saving to CSV...")
    output_file = "generated_data/employees_test.csv"
    generator.save_data(data, output_file, output_format="csv")
    print(f"   Saved to: {output_file}")

    # Save to JSON
    print("\n6. Saving to JSON...")
    output_file = "generated_data/employees_test.json"
    generator.save_data(data, output_file, output_format="json")
    print(f"   Saved to: {output_file}")

    # Show statistics
    print("\n7. Data Statistics:")
    print(f"   Total records: {len(data)}")

    # Count by department
    dept_counts = {}
    for record in data:
        dept = record.get('department')
        dept_counts[dept] = dept_counts.get(dept, 0) + 1

    print("\n   Department distribution:")
    for dept, count in sorted([(k, v) for k, v in dept_counts.items() if k is not None]):
        print(f"      {dept}: {count}")
    if None in dept_counts:
        print(f"      None (errors): {dept_counts[None]}")

    # Count by office location
    location_counts = {}
    for record in data:
        location = record.get('office_location')
        location_counts[location] = location_counts.get(location, 0) + 1

    print("\n   Office location distribution:")
    for location, count in sorted([(k, v) for k, v in location_counts.items() if k is not None]):
        print(f"      {location}: {count}")
    if None in location_counts:
        print(f"      None (errors): {location_counts[None]}")

    print("\n" + "=" * 80)
    print("Test completed successfully!")
    print("=" * 80)

if __name__ == "__main__":
    main()
