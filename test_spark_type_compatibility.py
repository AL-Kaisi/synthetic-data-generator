#!/usr/bin/env python3
"""
Comprehensive test for Spark type compatibility
Tests all numeric types with and without error injection
"""

from simple_generator import DataGenerator

def test_number_type_compatibility():
    """Test that generate_number is Spark DoubleType compatible"""
    print("Testing generate_number (DoubleType compatibility)...")

    # Test with no error injection
    generator = DataGenerator(error_rate=0.0)
    schema = {"minimum": 0, "maximum": 1000000}

    for _ in range(100):
        value = generator.generate_number(schema)
        assert isinstance(value, float), f"Expected float, got {type(value).__name__}: {value}"
    print("✓ No errors: All values are float")

    # Test with error injection
    generator_errors = DataGenerator(error_rate=0.5)
    valid_types = (float, type(None))  # Can be float or None, NEVER string or int

    for _ in range(200):
        value = generator_errors.generate_number(schema)
        assert isinstance(value, valid_types), \
            f"Expected float or None, got {type(value).__name__}: {value}"

        # If not None, must be float
        if value is not None:
            assert isinstance(value, float), \
                f"Non-None value must be float, got {type(value).__name__}: {value}"

    print("✓ With errors: All values are float or None (NEVER string or int)")

    # Test with enum
    schema_enum = {"enum": [1, 2, 3, 999999999]}
    for _ in range(100):
        value = generator.generate_number(schema_enum)
        assert isinstance(value, float), \
            f"Enum value must be float, got {type(value).__name__}: {value}"
        assert value in [1.0, 2.0, 3.0, 999999999.0], f"Invalid enum value: {value}"
    print("✓ Enum values: All converted to float")


def test_integer_type_compatibility():
    """Test that generate_integer is Spark LongType compatible"""
    print("\nTesting generate_integer (LongType compatibility)...")

    # Test with no error injection
    generator = DataGenerator(error_rate=0.0)
    schema = {"minimum": 0, "maximum": 1000000}

    for _ in range(100):
        value = generator.generate_integer(schema)
        assert isinstance(value, int), f"Expected int, got {type(value).__name__}: {value}"
    print("✓ No errors: All values are int")

    # Test with error injection
    generator_errors = DataGenerator(error_rate=0.5)
    valid_types = (int, type(None))  # Can be int or None, NEVER string or float

    for _ in range(200):
        value = generator_errors.generate_integer(schema)
        assert isinstance(value, valid_types), \
            f"Expected int or None, got {type(value).__name__}: {value}"

        # If not None, must be int
        if value is not None:
            assert isinstance(value, int), \
                f"Non-None value must be int, got {type(value).__name__}: {value}"
            # bool is a subclass of int in Python, so exclude it
            assert type(value) is int, \
                f"Value must be exactly int type, got {type(value).__name__}: {value}"

    print("✓ With errors: All values are int or None (NEVER string or float)")

    # Test with enum
    schema_enum = {"enum": [10, 20, 30, 999999999]}
    for _ in range(100):
        value = generator.generate_integer(schema_enum)
        if value is not None:
            assert isinstance(value, int), \
                f"Enum value must be int, got {type(value).__name__}: {value}"
            assert value in [10, 20, 30, 999999999], f"Invalid enum value: {value}"
    print("✓ Enum values: All are int or None")


def test_error_injection_types():
    """Test that error injection never returns invalid types for Spark"""
    print("\nTesting error injection type safety...")

    generator = DataGenerator(error_rate=1.0)  # 100% errors

    # Test number field errors
    print("  Testing number field errors...")
    for _ in range(100):
        value = generator.generate_number({"minimum": 0, "maximum": 1000})
        # Spark DoubleType accepts: float, None
        # Spark DoubleType REJECTS: int, string, bool
        assert value is None or isinstance(value, float), \
            f"Number error must be None or float, got {type(value).__name__}: {value}"

        # Ensure no string values like "INVALID_DATA"
        assert not isinstance(value, str), \
            f"Number field must NEVER return string, got: {value}"

    # Test integer field errors
    print("  Testing integer field errors...")
    for _ in range(100):
        value = generator.generate_integer({"minimum": 0, "maximum": 1000})
        # Spark LongType accepts: int, None
        # Spark LongType REJECTS: float, string, bool
        assert value is None or isinstance(value, int), \
            f"Integer error must be None or int, got {type(value).__name__}: {value}"

        # Ensure exactly int (not bool which is subclass of int)
        if value is not None:
            assert type(value) is int, \
                f"Integer field must be exactly int, got {type(value).__name__}: {value}"

        # Ensure no string values
        assert not isinstance(value, str), \
            f"Integer field must NEVER return string, got: {value}"

    print("✓ Error injection is type-safe for Spark")


def test_null_value_handling():
    """Test that null values are properly handled"""
    print("\nTesting null value handling...")

    generator = DataGenerator(error_rate=0.5)

    # Check that we can get None values from error injection
    null_count_number = 0
    null_count_integer = 0

    for _ in range(1000):
        value = generator.generate_number({"minimum": 0, "maximum": 100})
        if value is None:
            null_count_number += 1

    for _ in range(1000):
        value = generator.generate_integer({"minimum": 0, "maximum": 100})
        if value is None:
            null_count_integer += 1

    # With 50% error rate and "null" being one of 4 error types,
    # we should see some nulls (roughly 12.5% of values)
    assert null_count_number > 0, "Should have some null values for numbers"
    assert null_count_integer > 0, "Should have some null values for integers"

    print(f"✓ Null handling works: {null_count_number} null numbers, {null_count_integer} null integers out of 1000 each")


def test_extreme_values():
    """Test that extreme values don't use float('inf') or invalid values"""
    print("\nTesting extreme value handling...")

    generator = DataGenerator(error_rate=1.0)

    # Generate many values, some should be extreme
    values_number = [generator.generate_number({"minimum": 0, "maximum": 100}) for _ in range(500)]
    values_integer = [generator.generate_integer({"minimum": 0, "maximum": 100}) for _ in range(500)]

    # Check no infinity values (Spark can't handle them)
    for value in values_number:
        if value is not None:
            assert value != float('inf') and value != float('-inf'), \
                f"Number must not be infinity, got: {value}"
            assert not (value != value), f"Number must not be NaN, got: {value}"  # NaN check

    print("✓ No infinity or NaN values")

    # Check integer extreme values are within Spark's LongType range
    for value in values_integer:
        if value is not None:
            assert -9223372036854775808 <= value <= 9223372036854775807, \
                f"Integer must be within LongType range, got: {value}"

    print("✓ Integer values within Spark LongType range")


def main():
    print("=" * 70)
    print("Spark Type Compatibility Test Suite")
    print("=" * 70)

    test_number_type_compatibility()
    test_integer_type_compatibility()
    test_error_injection_types()
    test_null_value_handling()
    test_extreme_values()

    print("\n" + "=" * 70)
    print("✅ ALL TESTS PASSED - Spark compatibility verified!")
    print("=" * 70)
    print("\nSummary:")
    print("  - Number fields: Always return float or None (NEVER string or int)")
    print("  - Integer fields: Always return int or None (NEVER string or float)")
    print("  - Error injection: Type-safe for Spark DoubleType/LongType")
    print("  - Null values: Properly handled")
    print("  - Extreme values: No infinity, NaN, or out-of-range values")


if __name__ == "__main__":
    main()
