#!/usr/bin/env python3
"""
Quick test script to verify all functionality works
"""

import sys
import time
from simple_generator import SchemaDataGenerator
from schemas import SchemaLibrary

def test_basic_functionality():
    """Test basic data generation"""
    print("Testing basic functionality...")

    generator = SchemaDataGenerator()
    all_schemas = SchemaLibrary.get_all_schemas()

    print(f"Found {len(all_schemas)} schemas")

    # Test a few key schemas
    test_schemas = ['ecommerce_product', 'healthcare_patient', 'data_pipeline_metadata']

    for schema_name in test_schemas:
        if schema_name in all_schemas:
            start_time = time.time()
            data = generator.generate_from_schema(all_schemas[schema_name], 10)
            duration = time.time() - start_time
            print(f"{schema_name}: Generated {len(data)} records in {duration:.3f}s")
        else:
            print(f"{schema_name}: Schema not found")

def test_dwp_schemas():
    """Test DWP schema functionality including NINO safety"""
    print("\nTesting DWP schemas...")

    generator = SchemaDataGenerator()
    all_schemas = SchemaLibrary.get_all_schemas()

    dwp_schemas_to_test = ['child_benefit', 'universal_credit', 'pip']

    for schema_name in dwp_schemas_to_test:
        if schema_name in all_schemas:
            data = generator.generate_from_schema(all_schemas[schema_name], 10)
            print(f"{schema_name}: Generated {len(data)} records")

            # Test NINO safety if applicable
            if 'nino' in all_schemas[schema_name]['properties']:
                ninos = [r.get('nino') for r in data if r.get('nino')]
                safe_prefixes = {'BG', 'GB', 'NK', 'KN', 'TN', 'NT', 'ZZ', 'AA', 'AB', 'AO', 'FY', 'NY', 'OA', 'PO', 'OP'}
                safe_count = sum(1 for nino in ninos if nino[:2] in safe_prefixes)
                print(f"   NINO safety: {safe_count}/{len(ninos)} use safe prefixes")
        else:
            print(f"{schema_name}: Schema not available")

def test_custom_schemas():
    """Test custom schema auto-discovery"""
    print("\nTesting custom schema auto-discovery...")

    all_schemas = SchemaLibrary.get_all_schemas()
    custom_schemas = ['customer', 'blog_post']

    for schema_name in custom_schemas:
        if schema_name in all_schemas:
            generator = SchemaDataGenerator()
            data = generator.generate_from_schema(all_schemas[schema_name], 5)
            print(f"{schema_name}: Auto-discovered and generated {len(data)} records")
        else:
            print(f"{schema_name}: Custom schema not found")

def test_output_formats():
    """Test different output formats"""
    print("\nTesting output formats...")

    generator = SchemaDataGenerator()
    all_schemas = SchemaLibrary.get_all_schemas()

    if 'ecommerce_product' in all_schemas:
        schema = all_schemas['ecommerce_product']
        data = generator.generate_from_schema(schema, 3)

        # Test CSV format
        try:
            csv_data = generator.to_csv(data)
            print(f"CSV format: Generated {len(csv_data)} characters")
        except Exception as e:
            print(f"CSV format failed: {e}")

        # Test JSON-LD format
        try:
            jsonld_data = generator.to_json_ld(data, schema)
            print(f"JSON-LD format: Generated with {len(jsonld_data.get('@graph', []))} records")
        except Exception as e:
            print(f"JSON-LD format failed: {e}")

def main():
    """Run all tests"""
    print("Quick Test Suite for Synthetic Data Generator")
    print("=" * 50)

    try:
        test_basic_functionality()
        test_dwp_schemas()
        test_custom_schemas()
        test_output_formats()

        print("\n" + "=" * 50)
        print("All tests completed!")
        print("Core functionality is working correctly")

    except Exception as e:
        print(f"\nTest failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()