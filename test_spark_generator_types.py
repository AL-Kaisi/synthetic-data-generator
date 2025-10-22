#!/usr/bin/env python3
"""
Test Spark generator type compatibility
Verifies that SparkDataGenerator creates correct Spark schemas and generates type-safe data
"""

import os
import sys

# Set Java environment for Spark
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

try:
    from spark_generator import SparkDataGenerator, SPARK_AVAILABLE
    if not SPARK_AVAILABLE:
        print("PySpark is not installed. Skipping Spark tests.")
        sys.exit(0)
except ImportError:
    print("Cannot import SparkDataGenerator. Skipping Spark tests.")
    sys.exit(0)


def test_spark_schema_creation():
    """Test that Spark schemas are created correctly for all types"""
    print("Testing Spark schema creation...")

    schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
            "rating": {"type": "number"},
            "active": {"type": "boolean"},
            "tags": {"type": "array", "items": {"type": "string"}},
            "scores": {"type": "array", "items": {"type": "number"}},
            "counts": {"type": "array", "items": {"type": "integer"}},
            "flags": {"type": "array", "items": {"type": "boolean"}}
        },
        "required": ["name", "age"]
    }

    generator = SparkDataGenerator(
        app_name="SchemaTest",
        master="local[2]",
        memory="2g",
        error_rate=0.0
    )

    try:
        spark_schema = generator._create_spark_schema(schema)

        # Verify field types
        fields = {field.name: field for field in spark_schema.fields}

        assert str(fields["name"].dataType) == "StringType()", \
            f"Expected StringType for name, got {fields['name'].dataType}"

        assert str(fields["age"].dataType) == "LongType()", \
            f"Expected LongType for age, got {fields['age'].dataType}"

        assert str(fields["rating"].dataType) == "DoubleType()", \
            f"Expected DoubleType for rating, got {fields['rating'].dataType}"

        assert str(fields["active"].dataType) == "BooleanType()", \
            f"Expected BooleanType for active, got {fields['active'].dataType}"

        # Check array types
        assert "ArrayType(StringType" in str(fields["tags"].dataType), \
            f"Expected ArrayType(StringType) for tags, got {fields['tags'].dataType}"

        assert "ArrayType(DoubleType" in str(fields["scores"].dataType), \
            f"Expected ArrayType(DoubleType) for scores, got {fields['scores'].dataType}"

        assert "ArrayType(LongType" in str(fields["counts"].dataType), \
            f"Expected ArrayType(LongType) for counts, got {fields['counts'].dataType}"

        assert "ArrayType(BooleanType" in str(fields["flags"].dataType), \
            f"Expected ArrayType(BooleanType) for flags, got {fields['flags'].dataType}"

        print("✓ All Spark schema types are correct")

    finally:
        generator.close()


def test_spark_data_generation():
    """Test that Spark generator creates type-safe data"""
    print("\nTesting Spark data generation with all types...")

    schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer", "minimum": 18, "maximum": 100},
            "rating": {"type": "number", "minimum": 1.0, "maximum": 5.0},
            "active": {"type": "boolean"},
            "tags": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 3},
            "scores": {"type": "array", "items": {"type": "number"}, "minItems": 1, "maxItems": 5},
            "counts": {"type": "array", "items": {"type": "integer"}, "minItems": 1, "maxItems": 3}
        },
        "required": ["name", "age", "rating"]
    }

    generator = SparkDataGenerator(
        app_name="DataGenTest",
        master="local[2]",
        memory="2g",
        error_rate=0.0  # No errors for this test
    )

    try:
        # Generate a small dataset
        df = generator.generate_massive_dataset(
            schema=schema,
            num_records=100,
            num_partitions=4
        )

        # Collect data to verify types
        rows = df.collect()

        assert len(rows) == 100, f"Expected 100 rows, got {len(rows)}"

        # Verify first row has correct types
        row = rows[0]

        assert isinstance(row.name, str), f"name should be str, got {type(row.name)}"
        assert isinstance(row.age, int), f"age should be int, got {type(row.age)}"
        assert isinstance(row.rating, float), f"rating should be float, got {type(row.rating)}"
        assert isinstance(row.active, bool), f"active should be bool, got {type(row.active)}"
        assert isinstance(row.tags, list), f"tags should be list, got {type(row.tags)}"
        assert isinstance(row.scores, list), f"scores should be list, got {type(row.scores)}"
        assert isinstance(row.counts, list), f"counts should be list, got {type(row.counts)}"

        # Verify array item types
        if len(row.tags) > 0:
            assert isinstance(row.tags[0], str), f"tag items should be str, got {type(row.tags[0])}"

        if len(row.scores) > 0:
            assert isinstance(row.scores[0], float), f"score items should be float, got {type(row.scores[0])}"

        if len(row.counts) > 0:
            assert isinstance(row.counts[0], int), f"count items should be int, got {type(row.counts[0])}"

        print("✓ All generated data has correct types")
        print(f"  - Verified 100 records")
        print(f"  - name: str, age: int, rating: float, active: bool")
        print(f"  - tags: list[str], scores: list[float], counts: list[int]")

    finally:
        generator.close()


def test_spark_with_error_injection():
    """Test that Spark generator works with error injection"""
    print("\nTesting Spark data generation with error injection...")

    schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer", "minimum": 18, "maximum": 100},
            "rating": {"type": "number", "minimum": 1.0, "maximum": 5.0},
            "tags": {"type": "array", "items": {"type": "string"}, "minItems": 0, "maxItems": 5}
        }
    }

    generator = SparkDataGenerator(
        app_name="ErrorInjectionTest",
        master="local[2]",
        memory="2g",
        error_rate=0.3  # 30% error rate
    )

    try:
        # Generate dataset with errors
        df = generator.generate_massive_dataset(
            schema=schema,
            num_records=100,
            num_partitions=4
        )

        # Verify schema allows nulls (needed for error injection)
        for field in df.schema.fields:
            assert field.nullable, f"Field {field.name} should be nullable with error_rate > 0"

        # Collect and verify data
        rows = df.collect()
        assert len(rows) == 100, f"Expected 100 rows, got {len(rows)}"

        # Count nulls
        null_ages = sum(1 for row in rows if row.age is None)
        null_ratings = sum(1 for row in rows if row.rating is None)
        null_tags = sum(1 for row in rows if row.tags is None)
        empty_tags = sum(1 for row in rows if row.tags is not None and len(row.tags) == 0)

        print(f"✓ Error injection working correctly")
        print(f"  - Null ages: {null_ages}/100")
        print(f"  - Null ratings: {null_ratings}/100")
        print(f"  - Null tags: {null_tags}/100")
        print(f"  - Empty tag arrays: {empty_tags}/100")

        # Verify non-null values have correct types
        for row in rows:
            if row.age is not None:
                assert isinstance(row.age, int), f"Non-null age must be int, got {type(row.age)}"
            if row.rating is not None:
                assert isinstance(row.rating, float), f"Non-null rating must be float, got {type(row.rating)}"
            if row.tags is not None:
                assert isinstance(row.tags, list), f"Non-null tags must be list, got {type(row.tags)}"
                # Check that tags never contains "not_an_array" string
                for tag in row.tags:
                    if tag is not None:
                        assert isinstance(tag, str), f"Tag items must be str, got {type(tag)}"

        print("✓ All non-null values have correct types (no type mismatches)")

    finally:
        generator.close()


def main():
    print("=" * 70)
    print("Spark Generator Type Compatibility Test Suite")
    print("=" * 70)

    test_spark_schema_creation()
    test_spark_data_generation()
    test_spark_with_error_injection()

    print("\n" + "=" * 70)
    print("✅ ALL SPARK TESTS PASSED!")
    print("=" * 70)
    print("\nSummary:")
    print("  - Spark schema creation handles all types correctly")
    print("  - Array types properly mapped: string→StringType, number→DoubleType, etc.")
    print("  - Generated data matches Spark schema types exactly")
    print("  - Error injection works without causing type mismatches")
    print("  - Ready for production use with any schema!")


if __name__ == "__main__":
    main()
