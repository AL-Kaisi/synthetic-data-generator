#!/usr/bin/env python3
"""
Test for Spark BooleanType compatibility
Verifies that generate_boolean always returns bool or None
"""

from simple_generator import DataGenerator

def test_boolean_type_compatibility():
    """Test that generate_boolean is Spark BooleanType compatible"""
    print("Testing generate_boolean (BooleanType compatibility)...")

    # Test with no error injection
    generator = DataGenerator(error_rate=0.0)
    schema = {}

    for _ in range(100):
        value = generator.generate_boolean(schema)
        assert isinstance(value, bool), f"Expected bool, got {type(value).__name__}: {value}"
    print("✓ No errors: All values are bool")

    # Test with error injection
    generator_errors = DataGenerator(error_rate=0.5)
    valid_types = (bool, type(None))  # Can be bool or None, NEVER string

    for _ in range(200):
        value = generator_errors.generate_boolean(schema)
        assert isinstance(value, valid_types), \
            f"Expected bool or None, got {type(value).__name__}: {value}"

        # If not None, must be bool
        if value is not None:
            assert isinstance(value, bool), \
                f"Non-None value must be bool, got {type(value).__name__}: {value}"

            # Ensure it's never a string like "INVALID_DATA"
            assert not isinstance(value, str), \
                f"Boolean field must NEVER return string, got: {value}"

    print("✓ With errors: All values are bool or None (NEVER string)")


def test_boolean_error_injection():
    """Test that boolean error injection is type-safe for Spark"""
    print("\nTesting boolean error injection type safety...")

    generator = DataGenerator(error_rate=1.0)  # 100% errors

    schema = {}

    string_errors = 0
    for _ in range(100):
        value = generator.generate_boolean(schema)

        # Spark BooleanType accepts: bool, None
        # Spark BooleanType REJECTS: string, int, float
        assert value is None or isinstance(value, bool), \
            f"Boolean error must be None or bool, got {type(value).__name__}: {value}"

        # The critical test: ensure NO string values like "INVALID_DATA"
        if isinstance(value, str):
            string_errors += 1
            print(f"  ❌ FAILED: Got string value '{value}' for boolean field")

    assert string_errors == 0, f"Found {string_errors} string values in boolean field (should be 0)"
    print("✓ Boolean error injection never returns strings")


def test_boolean_null_handling():
    """Test that booleans can properly return None values"""
    print("\nTesting boolean null value handling...")

    generator = DataGenerator(error_rate=0.5)

    schema = {}

    null_count = 0
    true_count = 0
    false_count = 0

    for _ in range(1000):
        value = generator.generate_boolean(schema)

        if value is None:
            null_count += 1
        elif value is True:
            true_count += 1
        elif value is False:
            false_count += 1

    # With 50% error rate and "null" being the ONLY error type for boolean,
    # we should see roughly 50% nulls
    assert null_count > 0, "Should have some null values for booleans"
    assert true_count > 0, "Should have some True values"
    assert false_count > 0, "Should have some False values"

    print(f"✓ Null handling works: {null_count} nulls, {true_count} True, {false_count} False out of 1000")


def main():
    print("=" * 70)
    print("Spark BooleanType Compatibility Test Suite")
    print("=" * 70)

    test_boolean_type_compatibility()
    test_boolean_error_injection()
    test_boolean_null_handling()

    print("\n" + "=" * 70)
    print("✅ ALL TESTS PASSED - BooleanType Spark compatibility verified!")
    print("=" * 70)
    print("\nSummary:")
    print("  - Boolean fields: Always return bool or None (NEVER string)")
    print("  - Error injection: Type-safe for Spark BooleanType")
    print("  - No 'INVALID_DATA' strings or other type mismatches")
    print("  - Null values properly handled")


if __name__ == "__main__":
    main()
