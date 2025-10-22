#!/usr/bin/env python3
"""
Test to verify that generate_number always returns float type
This test ensures Spark DoubleType compatibility
"""

from simple_generator import DataGenerator

def test_generate_number_returns_float():
    """Test that generate_number always returns float, never int"""
    generator = DataGenerator(error_rate=0.0)

    # Test 1: Default schema (no constraints)
    schema1 = {}
    for _ in range(100):
        value = generator.generate_number(schema1)
        assert isinstance(value, float), f"Expected float, got {type(value)} with value {value}"
    print("✓ Test 1 passed: Default schema returns float")

    # Test 2: Schema with enum values (potentially integers)
    schema2 = {"enum": [1, 2, 3, 4, 5]}
    for _ in range(50):
        value = generator.generate_number(schema2)
        if value is not None:
            assert isinstance(value, float), f"Expected float, got {type(value)} with value {value}"
    print("✓ Test 2 passed: Enum values converted to float")

    # Test 3: Schema with min/max
    schema3 = {"minimum": 100, "maximum": 999999999}
    for _ in range(100):
        value = generator.generate_number(schema3)
        assert isinstance(value, float), f"Expected float, got {type(value)} with value {value}"
        assert 100.0 <= value <= 999999999.0, f"Value {value} out of range"
    print("✓ Test 3 passed: Large min/max values return float")

    # Test 4: Schema with decimal places
    schema4 = {"decimalPlaces": 2, "minimum": 0, "maximum": 1000}
    for _ in range(100):
        value = generator.generate_number(schema4)
        assert isinstance(value, float), f"Expected float, got {type(value)} with value {value}"
        # Check decimal places (allowing for floating point precision)
        assert round(value, 2) == value, f"Value {value} has more than 2 decimal places"
    print("✓ Test 4 passed: Decimal places respected, returns float")

    # Test 5: With error injection enabled
    generator_with_errors = DataGenerator(error_rate=0.5)
    schema5 = {"minimum": 0, "maximum": 1000}
    for _ in range(100):
        value = generator_with_errors.generate_number(schema5)
        # Value can be None, string, or float, but never int
        if value is not None and not isinstance(value, str):
            assert isinstance(value, float), f"Expected float, got {type(value)} with value {value}"
    print("✓ Test 5 passed: Error injection returns float or acceptable error values")

    print("\n✅ All tests passed! generate_number always returns float type")
    print("This ensures compatibility with Spark's DoubleType")

if __name__ == "__main__":
    test_generate_number_returns_float()
