#!/usr/bin/env python3
"""
Comprehensive test for ALL Spark type compatibility
Tests all data types with and without error injection
"""

from simple_generator import DataGenerator

def test_all_types_without_errors():
    """Test all types return correct types without error injection"""
    print("Testing all types without error injection...")

    generator = DataGenerator(error_rate=0.0)

    # Test string
    for _ in range(20):
        value = generator.generate_string("name", {})
        assert isinstance(value, str), f"String must return str, got {type(value)}"

    # Test integer
    for _ in range(20):
        value = generator.generate_integer({})
        assert isinstance(value, int), f"Integer must return int, got {type(value)}"

    # Test number
    for _ in range(20):
        value = generator.generate_number({})
        assert isinstance(value, float), f"Number must return float, got {type(value)}"

    # Test boolean
    for _ in range(20):
        value = generator.generate_boolean({})
        assert isinstance(value, bool), f"Boolean must return bool, got {type(value)}"

    # Test array
    for _ in range(20):
        value = generator.generate_array({"items": {"type": "string"}}, "tags")
        assert isinstance(value, list), f"Array must return list, got {type(value)}"

    print("✓ All types return correct types without errors")


def test_all_types_with_errors():
    """Test all types are type-safe with error injection"""
    print("\nTesting all types WITH error injection...")

    generator = DataGenerator(error_rate=0.5)

    print("  Testing string with errors...")
    for _ in range(50):
        value = generator.generate_string("name", {})
        assert value is None or isinstance(value, str), \
            f"String must be None or str, got {type(value)}: {value}"

    print("  Testing integer with errors...")
    for _ in range(50):
        value = generator.generate_integer({})
        assert value is None or isinstance(value, int), \
            f"Integer must be None or int, got {type(value)}: {value}"
        if isinstance(value, str):
            raise AssertionError(f"Integer returned string: {value}")

    print("  Testing number with errors...")
    for _ in range(50):
        value = generator.generate_number({})
        assert value is None or isinstance(value, float), \
            f"Number must be None or float, got {type(value)}: {value}"
        if isinstance(value, str):
            raise AssertionError(f"Number returned string: {value}")
        if isinstance(value, int) and not isinstance(value, bool):
            raise AssertionError(f"Number returned int instead of float: {value}")

    print("  Testing boolean with errors...")
    for _ in range(50):
        value = generator.generate_boolean({})
        assert value is None or isinstance(value, bool), \
            f"Boolean must be None or bool, got {type(value)}: {value}"
        if isinstance(value, str):
            raise AssertionError(f"Boolean returned string: {value}")

    print("  Testing array with errors...")
    for _ in range(50):
        value = generator.generate_array({"items": {"type": "string"}}, "tags")
        assert value is None or isinstance(value, list), \
            f"Array must be None or list, got {type(value)}: {value}"
        if isinstance(value, str):
            raise AssertionError(f"Array returned string: {value}")

    print("✓ All types are type-safe with error injection")


def test_array_item_types():
    """Test arrays with different item types"""
    print("\nTesting array item types...")

    generator = DataGenerator(error_rate=0.2)

    # String array items
    print("  Testing array of strings...")
    for _ in range(30):
        value = generator.generate_array({"items": {"type": "string"}, "minItems": 1, "maxItems": 3}, "tags")
        if value is not None and len(value) > 0:
            for item in value:
                if item is not None:
                    assert isinstance(item, str), f"String array item must be str, got {type(item)}"

    # Integer array items
    print("  Testing array of integers...")
    for _ in range(30):
        value = generator.generate_array({"items": {"type": "integer"}, "minItems": 1, "maxItems": 3}, "counts")
        if value is not None and len(value) > 0:
            for item in value:
                if item is not None:
                    assert isinstance(item, int), f"Integer array item must be int, got {type(item)}"

    # Number array items
    print("  Testing array of numbers...")
    for _ in range(30):
        value = generator.generate_array({"items": {"type": "number"}, "minItems": 1, "maxItems": 3}, "scores")
        if value is not None and len(value) > 0:
            for item in value:
                if item is not None:
                    assert isinstance(item, float), f"Number array item must be float, got {type(item)}"

    # Boolean array items
    print("  Testing array of booleans...")
    for _ in range(30):
        value = generator.generate_array({"items": {"type": "boolean"}, "minItems": 1, "maxItems": 3}, "flags")
        if value is not None and len(value) > 0:
            for item in value:
                if item is not None:
                    assert isinstance(item, bool), f"Boolean array item must be bool, got {type(item)}"

    print("✓ All array item types are correct")


def test_extreme_error_rate():
    """Test with 100% error rate"""
    print("\nTesting with 100% error rate...")

    generator = DataGenerator(error_rate=1.0)

    # All types should still return valid types or None
    for _ in range(50):
        # String
        value = generator.generate_string("name", {})
        assert value is None or isinstance(value, str)

        # Integer
        value = generator.generate_integer({})
        assert value is None or isinstance(value, int)
        assert not isinstance(value, str), f"Integer error returned string: {value}"

        # Number
        value = generator.generate_number({})
        assert value is None or isinstance(value, float)
        assert not isinstance(value, str), f"Number error returned string: {value}"

        # Boolean
        value = generator.generate_boolean({})
        assert value is None or isinstance(value, bool)
        assert not isinstance(value, str), f"Boolean error returned string: {value}"

        # Array
        value = generator.generate_array({"items": {"type": "string"}}, "tags")
        assert value is None or isinstance(value, list)
        assert not isinstance(value, str), f"Array error returned string: {value}"

    print("✓ 100% error rate still maintains type safety")


def test_enum_values():
    """Test enum values maintain correct types"""
    print("\nTesting enum values...")

    # Test without error injection first
    generator_no_errors = DataGenerator(error_rate=0.0)

    # Integer enums - no errors
    print("  Testing integer enums without errors...")
    for _ in range(30):
        value = generator_no_errors.generate_integer({"enum": [1, 2, 3, 4, 5]})
        assert isinstance(value, int), f"Integer enum must be int, got {type(value)}"
        assert value in [1, 2, 3, 4, 5], f"Invalid enum value: {value}"

    # Number enums - no errors
    print("  Testing number enums without errors...")
    for _ in range(30):
        value = generator_no_errors.generate_number({"enum": [1, 2, 3, 4, 5]})
        assert isinstance(value, float), f"Number enum must be float, got {type(value)}"
        assert value in [1.0, 2.0, 3.0, 4.0, 5.0], f"Invalid enum value: {value}"

    # Test WITH error injection - values might be outside enum, but type must be correct
    generator_with_errors = DataGenerator(error_rate=0.3)

    print("  Testing integer enums with error injection...")
    for _ in range(30):
        value = generator_with_errors.generate_integer({"enum": [1, 2, 3, 4, 5]})
        if value is not None:
            assert isinstance(value, int), f"Integer enum must be int, got {type(value)}"
            # Note: With error injection, value might be negative, zero, or extreme - that's OK!

    print("  Testing number enums with error injection...")
    for _ in range(30):
        value = generator_with_errors.generate_number({"enum": [1, 2, 3, 4, 5]})
        if value is not None:
            assert isinstance(value, float), f"Number enum must be float, got {type(value)}"
            # Note: With error injection, value might be negative, zero, or extreme - that's OK!

    print("✓ Enum values maintain correct types (even with error injection)")


def main():
    print("=" * 70)
    print("Comprehensive Spark Type Compatibility Test Suite")
    print("=" * 70)

    test_all_types_without_errors()
    test_all_types_with_errors()
    test_array_item_types()
    test_extreme_error_rate()
    test_enum_values()

    print("\n" + "=" * 70)
    print("✅ ALL COMPREHENSIVE TESTS PASSED!")
    print("=" * 70)
    print("\nAll Spark type guarantees verified:")
    print("  ✓ String → str or None")
    print("  ✓ Integer → int or None (NEVER float or string)")
    print("  ✓ Number → float or None (NEVER int or string)")
    print("  ✓ Boolean → bool or None (NEVER string)")
    print("  ✓ Array → list or None (NEVER string)")
    print("  ✓ Array items maintain type safety")
    print("  ✓ Enum values maintain type safety")
    print("  ✓ Works with any error rate (0% to 100%)")
    print("\n🎉 Your Spark generator is fully type-safe!")


if __name__ == "__main__":
    main()
