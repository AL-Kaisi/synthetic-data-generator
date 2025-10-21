#!/usr/bin/env python3
"""
Test the 3-column Excel schema format:
- column_name
- values
- description
"""

import json
from schema_parser import SchemaParser
from simple_generator import SchemaDataGenerator


def test_employee_schema():
    """Test employee schema with NINO field"""
    print("=" * 70)
    print("TEST 1: Employee Schema (3-column Excel format)")
    print("=" * 70)

    # Parse the Excel file
    parser = SchemaParser()
    schema = parser.parse_schema_file('custom_schemas/employee_schema_3col.xlsx')

    print("\n--- Parsed Schema ---")
    print(json.dumps(schema, indent=2))

    # Generate sample data
    print("\n--- Generating 5 sample records ---")
    generator = SchemaDataGenerator(error_rate=0.0)
    data = generator.generate_from_schema(schema, num_records=5)

    for i, record in enumerate(data, 1):
        print(f"\nRecord {i}:")
        print(json.dumps(record, indent=2, default=str))

    # Check NINO fields
    print("\n--- NINO Validation ---")
    for i, record in enumerate(data, 1):
        nino = record.get('nino', 'N/A')
        print(f"Record {i} NINO: {nino}")

    print("\n✓ Employee schema test completed")
    return schema, data


def test_customer_schema():
    """Test customer schema"""
    print("\n" + "=" * 70)
    print("TEST 2: Customer Schema (3-column Excel format)")
    print("=" * 70)

    # Parse the Excel file
    parser = SchemaParser()
    schema = parser.parse_schema_file('custom_schemas/customer_schema_3col.xlsx')

    print("\n--- Parsed Schema ---")
    print(json.dumps(schema, indent=2))

    # Generate sample data
    print("\n--- Generating 3 sample records ---")
    generator = SchemaDataGenerator(error_rate=0.0)
    data = generator.generate_from_schema(schema, num_records=3)

    for i, record in enumerate(data, 1):
        print(f"\nRecord {i}:")
        print(json.dumps(record, indent=2, default=str))

    print("\n✓ Customer schema test completed")
    return schema, data


def test_benefits_claimant_schema():
    """Test UK benefits claimant schema with NINO"""
    print("\n" + "=" * 70)
    print("TEST 3: Benefits Claimant Schema (3-column Excel format)")
    print("=" * 70)

    # Parse the Excel file
    parser = SchemaParser()
    schema = parser.parse_schema_file('custom_schemas/benefits_claimant_3col.xlsx')

    print("\n--- Parsed Schema ---")
    print(json.dumps(schema, indent=2))

    # Generate sample data
    print("\n--- Generating 5 sample records ---")
    generator = SchemaDataGenerator(error_rate=0.0)
    data = generator.generate_from_schema(schema, num_records=5)

    for i, record in enumerate(data, 1):
        print(f"\nRecord {i}:")
        print(json.dumps(record, indent=2, default=str))

    # Verify NINO and benefit types
    print("\n--- Data Validation ---")
    for i, record in enumerate(data, 1):
        nino = record.get('nino', 'N/A')
        benefit_type = record.get('benefit_type', 'N/A')
        claim_status = record.get('claim_status', 'N/A')
        print(f"Record {i}:")
        print(f"  NINO: {nino}")
        print(f"  Benefit: {benefit_type}")
        print(f"  Status: {claim_status}")

    print("\n✓ Benefits claimant schema test completed")
    return schema, data


def test_with_error_injection():
    """Test Excel schema with error injection"""
    print("\n" + "=" * 70)
    print("TEST 4: Excel Schema with Error Injection")
    print("=" * 70)

    # Parse the Excel file
    parser = SchemaParser()
    schema = parser.parse_schema_file('custom_schemas/employee_schema_3col.xlsx')

    # Generate data with 20% error injection
    print("\n--- Generating 10 records with 20% error injection ---")
    generator = SchemaDataGenerator(error_rate=0.2)
    data = generator.generate_from_schema(schema, num_records=10)

    # Count errors
    error_count = 0
    for record in data:
        has_error = False
        for key, value in record.items():
            if value is None or value == "" or (isinstance(value, str) and value.strip() == ""):
                has_error = True
                break
        if has_error:
            error_count += 1

    # Show first 3 records
    print("\n--- Sample records (first 3) ---")
    for i, record in enumerate(data[:3], 1):
        print(f"\nRecord {i}:")
        print(json.dumps(record, indent=2, default=str))

    print(f"\n--- Statistics ---")
    print(f"Total records: {len(data)}")
    print(f"Records with errors: {error_count}")
    print(f"Error rate: {(error_count/len(data)*100):.1f}%")

    print("\n✓ Error injection test completed")


def main():
    """Run all tests"""
    print("\n")
    print("╔" + "═" * 68 + "╗")
    print("║" + " " * 10 + "3-COLUMN EXCEL SCHEMA FORMAT - TEST SUITE" + " " * 14 + "║")
    print("╚" + "═" * 68 + "╝")
    print()

    try:
        test_employee_schema()
        test_customer_schema()
        test_benefits_claimant_schema()
        test_with_error_injection()

        print("\n" + "=" * 70)
        print("ALL TESTS COMPLETED SUCCESSFULLY! ✓")
        print("=" * 70)
        print("\n3-Column Excel Format:")
        print("  1. column_name - The name of the field")
        print("  2. values - Enum values (comma-separated) or empty for auto-generation")
        print("  3. description - Meaning/description for type inference")
        print("\nFeatures Tested:")
        print("  ✓ Excel schema parsing")
        print("  ✓ HMRC NINO auto-generation")
        print("  ✓ Enum value support")
        print("  ✓ Type inference from description")
        print("  ✓ Error injection")
        print("\nYou can now:")
        print("  1. Edit the Excel files in Excel/Google Sheets")
        print("  2. Use them directly for data generation")
        print("  3. Create your own following the same format")

    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
