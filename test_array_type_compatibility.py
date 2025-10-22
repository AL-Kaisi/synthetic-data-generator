#!/usr/bin/env python3
"""
Test for Spark ArrayType compatibility
Verifies that generate_array always returns list or None
"""

from simple_generator import DataGenerator

def test_array_type_compatibility():
    """Test that generate_array is Spark ArrayType compatible"""
    print("Testing generate_array (ArrayType compatibility)...")

    # Test with no error injection
    generator = DataGenerator(error_rate=0.0)
    schema = {"items": {"type": "string"}, "minItems": 1, "maxItems": 5}

    for _ in range(100):
        value = generator.generate_array(schema, "tags")
        assert isinstance(value, list), f"Expected list, got {type(value).__name__}: {value}"
        assert len(value) >= 1 and len(value) <= 5, f"Array length {len(value)} out of range"
    print("✓ No errors: All values are list")

    # Test with error injection
    generator_errors = DataGenerator(error_rate=0.5)
    valid_types = (list, type(None))  # Can be list or None, NEVER string

    for _ in range(200):
        value = generator_errors.generate_array(schema, "tags")
        assert isinstance(value, valid_types), \
            f"Expected list or None, got {type(value).__name__}: {value}"

        # If not None, must be list
        if value is not None:
            assert isinstance(value, list), \
                f"Non-None value must be list, got {type(value).__name__}: {value}"

            # Ensure it's not a string (common error)
            assert not isinstance(value, str), \
                f"Array field must NEVER return string, got: {value}"

    print("✓ With errors: All values are list or None (NEVER string)")


def test_array_error_injection():
    """Test that array error injection is type-safe for Spark"""
    print("\nTesting array error injection type safety...")

    generator = DataGenerator(error_rate=1.0)  # 100% errors

    schema = {
        "items": {"type": "string"},
        "minItems": 2,
        "maxItems": 8
    }

    string_errors = 0
    for _ in range(100):
        value = generator.generate_array(schema, "categories")

        # Spark ArrayType accepts: list, None
        # Spark ArrayType REJECTS: string, int, float, bool
        assert value is None or isinstance(value, list), \
            f"Array error must be None or list, got {type(value).__name__}: {value}"

        # The critical test: ensure NO string values like "not_an_array"
        if isinstance(value, str):
            string_errors += 1
            print(f"  ❌ FAILED: Got string value '{value}' for array field")

    assert string_errors == 0, f"Found {string_errors} string values in array field (should be 0)"
    print("✓ Array error injection never returns strings")


def test_array_null_handling():
    """Test that arrays can properly return None values"""
    print("\nTesting array null value handling...")

    generator = DataGenerator(error_rate=0.5)

    schema = {"items": {"type": "string"}, "minItems": 1, "maxItems": 5}

    null_count = 0
    empty_count = 0

    for _ in range(1000):
        value = generator.generate_array(schema, "tags")

        if value is None:
            null_count += 1
        elif isinstance(value, list) and len(value) == 0:
            empty_count += 1

    # With 50% error rate and "null" being one of 2 error types,
    # we should see some nulls (roughly 25% of values)
    assert null_count > 0, "Should have some null values for arrays"
    assert empty_count > 0, "Should have some empty arrays"

    print(f"✓ Null handling works: {null_count} null arrays, {empty_count} empty arrays out of 1000")


def test_array_with_different_item_types():
    """Test arrays with different item types"""
    print("\nTesting arrays with different item types...")

    generator = DataGenerator(error_rate=0.3)

    # String items
    schema_string = {"items": {"type": "string"}, "minItems": 1, "maxItems": 3}
    for _ in range(50):
        value = generator.generate_array(schema_string, "tags")
        if value is not None:
            assert isinstance(value, list)
            for item in value:
                if item is not None:  # Individual items might have errors
                    assert isinstance(item, str), f"Array item should be string, got {type(item)}"

    # Number items
    schema_number = {"items": {"type": "number"}, "minItems": 1, "maxItems": 3}
    for _ in range(50):
        value = generator.generate_array(schema_number, "ratings")
        if value is not None:
            assert isinstance(value, list)
            for item in value:
                if item is not None:
                    assert isinstance(item, float), f"Array item should be float, got {type(item)}"

    # Integer items
    schema_integer = {"items": {"type": "integer"}, "minItems": 1, "maxItems": 3}
    for _ in range(50):
        value = generator.generate_array(schema_integer, "counts")
        if value is not None:
            assert isinstance(value, list)
            for item in value:
                if item is not None:
                    assert isinstance(item, int), f"Array item should be int, got {type(item)}"

    print("✓ Arrays with different item types work correctly")


def main():
    print("=" * 70)
    print("Spark ArrayType Compatibility Test Suite")
    print("=" * 70)

    test_array_type_compatibility()
    test_array_error_injection()
    test_array_null_handling()
    test_array_with_different_item_types()

    print("\n" + "=" * 70)
    print("✅ ALL TESTS PASSED - ArrayType Spark compatibility verified!")
    print("=" * 70)
    print("\nSummary:")
    print("  - Array fields: Always return list or None (NEVER string)")
    print("  - Error injection: Type-safe for Spark ArrayType")
    print("  - No 'not_an_array' strings or other type mismatches")
    print("  - Null and empty arrays properly handled")


if __name__ == "__main__":
    main()
