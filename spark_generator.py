#!/usr/bin/env python3
"""
PySpark-based Synthetic Data Generator for Massive Datasets
Generates millions/billions of records using distributed computing
"""

import json
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import *
    from pyspark.sql import Row
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("Warning: PySpark not installed. Use 'pip install pyspark' for massive dataset generation.")

from simple_generator import DataGenerator


class SparkDataGenerator:
    """
    Distributed data generator using PySpark for massive datasets

    This generator is fully type-safe for all Spark DataTypes and supports
    error injection without causing type compatibility issues.

    Type Safety Guarantees:
    ----------------------
    - Integer fields (LongType): Always returns int or None, NEVER float or string
    - Number fields (DoubleType): Always returns float or None, NEVER int or string
    - Boolean fields (BooleanType): Always returns bool or None, NEVER string
    - Array fields (ArrayType): Always returns list or None, NEVER string
    - String fields (StringType): Always returns str or None
    - Object fields (MapType): Returns dict with string keys/values

    Features:
    ---------
    - Parallel data generation across multiple partitions
    - Type-safe error injection (0% to 100% error rate)
    - Support for all JSON schema types
    - Streaming data generation
    - Multiple output formats (Parquet, CSV, JSON, ORC, Delta)
    - Automatic schema inference and validation

    Error Injection:
    ---------------
    When error_rate > 0, the generator injects type-safe errors:
    - Numeric fields: null, negative, zero, extreme values (type-safe)
    - Boolean fields: null only
    - Array fields: null or empty list
    - String fields: null, empty, whitespace, malformed, etc.

    All error injection maintains Spark type compatibility!

    Usage Example:
    -------------
    >>> generator = SparkDataGenerator(
    ...     master="local[*]",
    ...     memory="4g",
    ...     error_rate=0.1  # 10% error injection - fully safe!
    ... )
    >>> df = generator.generate_and_save(
    ...     schema=my_schema,
    ...     num_records=1000000,
    ...     output_path="output",
    ...     output_format="parquet"
    ... )
    >>> generator.close()
    """

    def __init__(self, app_name: str = "SyntheticDataGenerator",
                 master: str = None,
                 memory: str = "4g",
                 cores: int = None,
                 error_rate: float = 0.0):
        """
        Initialize Spark session for distributed generation

        Args:
            app_name: Spark application name
            master: Spark master URL (local[*] for local mode, yarn for cluster)
            memory: Driver memory allocation
            cores: Number of cores to use
            error_rate: Percentage of fields to inject with data quality issues (0.0 to 1.0)
        """
        if not SPARK_AVAILABLE:
            raise ImportError("PySpark is not installed. Install with: pip install pyspark")

        builder = SparkSession.builder.appName(app_name)

        if master:
            builder = builder.master(master)
        else:
            # Use all available cores locally by default
            builder = builder.master("local[*]")

        # Configuration for better performance
        builder = builder.config("spark.sql.adaptive.enabled", "true") \
                        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                        .config("spark.driver.memory", memory) \
                        .config("spark.driver.host", "127.0.0.1") \
                        .config("spark.driver.bindAddress", "127.0.0.1")

        if cores:
            builder = builder.config("spark.executor.cores", str(cores))

        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

        # Store error rate for use in data generation
        self.error_rate = error_rate

        # Broadcast the generator for use in UDFs
        self.data_generator = DataGenerator(error_rate=error_rate)

    def _create_spark_schema(self, json_schema: Dict) -> StructType:
        """
        Convert JSON schema to Spark StructType with full type safety

        This method ensures all Spark DataTypes are correctly mapped and nullable
        when error injection is enabled.

        Type Safety Guarantees:
        - string → StringType (can be None)
        - integer → LongType (can be None, NEVER float or string)
        - number → DoubleType (can be None, NEVER int or string)
        - boolean → BooleanType (can be None, NEVER string)
        - array → ArrayType with correct item types (can be None, NEVER string)
        - object → MapType(StringType, StringType, nullable)

        Args:
            json_schema: JSON schema dictionary

        Returns:
            Spark StructType with type-safe field definitions
        """
        spark_fields = []
        properties = json_schema.get("properties", {})
        required_fields = json_schema.get("required", [])

        for field_name, field_schema in properties.items():
            field_type = field_schema.get("type", "string")

            # If error injection is enabled, all fields must be nullable
            # since we can inject None values
            if self.error_rate > 0:
                nullable = True
            else:
                nullable = field_name not in required_fields

            if field_type == "string":
                spark_type = StringType()
            elif field_type == "integer":
                # LongType: accepts int or None, REJECTS float/string
                spark_type = LongType()
            elif field_type == "number":
                # DoubleType: accepts float or None, REJECTS int/string
                spark_type = DoubleType()
            elif field_type == "boolean":
                # BooleanType: accepts bool or None, REJECTS string
                spark_type = BooleanType()
            elif field_type == "array":
                # ArrayType: accepts list or None, REJECTS string like "not_an_array"
                # Handle all array item types for Spark compatibility
                item_type = field_schema.get("items", {}).get("type", "string")
                if item_type == "string":
                    spark_type = ArrayType(StringType(), True)  # True = nullable items
                elif item_type == "integer":
                    spark_type = ArrayType(LongType(), True)
                elif item_type == "number":
                    spark_type = ArrayType(DoubleType(), True)
                elif item_type == "boolean":
                    spark_type = ArrayType(BooleanType(), True)
                else:
                    # Default to string for unknown types
                    spark_type = ArrayType(StringType(), True)
            elif field_type == "object":
                # For nested objects, use MapType with nullable values
                spark_type = MapType(StringType(), StringType(), valueContainsNull=True)
            else:
                # Default to StringType for unknown types
                spark_type = StringType()

            spark_fields.append(StructField(field_name, spark_type, nullable))

        return StructType(spark_fields)

    def generate_massive_dataset(self,
                                schema: Dict,
                                num_records: int,
                                num_partitions: int = None,
                                batch_size: int = 10000) -> 'DataFrame':
        """
        Generate massive dataset using PySpark

        Args:
            schema: JSON schema for data generation
            num_records: Total number of records to generate
            num_partitions: Number of partitions (default: auto)
            batch_size: Records per partition batch

        Returns:
            Spark DataFrame with generated data
        """
        if num_partitions is None:
            # Auto-calculate partitions based on records and batch size
            num_partitions = max(1, num_records // batch_size)
            # Cap at 10000 partitions for very large datasets
            num_partitions = min(num_partitions, 10000)

        print(f"Generating {num_records:,} records across {num_partitions} partitions...")

        # Broadcast the schema and error_rate (not the generator to avoid serialization issues)
        broadcast_schema = self.spark.sparkContext.broadcast(schema)
        broadcast_error_rate = self.spark.sparkContext.broadcast(self.error_rate)

        def generate_batch(partition_id, iterator):
            """
            Generate data for a partition with type-safe data generation

            Each partition gets its own DataGenerator instance to ensure:
            - Thread safety
            - Independent random seeds
            - Type-safe error injection
            - Correct Spark DataType compatibility
            """
            import random
            import uuid
            from datetime import datetime, timedelta

            # Each partition gets its own generator instance with error_rate
            # This ensures type-safe generation: int for LongType, float for DoubleType, etc.
            gen = DataGenerator(error_rate=broadcast_error_rate.value)
            schema_val = broadcast_schema.value

            for row_num in iterator:
                record = {}
                properties = schema_val.get("properties", {})

                for field_name, field_schema in properties.items():
                    # generate_field ensures type safety:
                    # - integer → int or None
                    # - number → float or None
                    # - boolean → bool or None
                    # - array → list or None
                    record[field_name] = gen.generate_field(field_name, field_schema)

                yield Row(**record)

        # Create RDD with row numbers
        rdd = self.spark.sparkContext.range(0, num_records, numSlices=num_partitions)

        # Generate data in parallel
        data_rdd = rdd.mapPartitionsWithIndex(generate_batch)

        # Convert to DataFrame with schema
        spark_schema = self._create_spark_schema(schema)
        df = self.spark.createDataFrame(data_rdd, spark_schema)

        return df

    def save_massive_dataset(self,
                            df: 'DataFrame',
                            output_path: str,
                            output_format: str = "parquet",
                            mode: str = "overwrite",
                            compression: str = "snappy",
                            coalesce_partitions: int = None,
                            single_file: bool = False):
        """
        Save massive dataset efficiently

        Args:
            df: Spark DataFrame to save
            output_path: Output path (local or HDFS/S3)
            output_format: Format (parquet, csv, json, orc, delta)
            mode: Save mode (overwrite, append, ignore, error)
            compression: Compression codec
            coalesce_partitions: Number of output files (optional, overrides single_file)
            single_file: If True, output a single file instead of multiple part files
        """
        import os
        import shutil
        import glob

        # Optimize partitions for output
        if coalesce_partitions:
            df = df.coalesce(coalesce_partitions)
        elif single_file:
            # Coalesce to 1 partition for single file output
            df = df.coalesce(1)

        print(f"Saving data to {output_path} as {output_format}...")

        # Determine if we need temporary directory for single file
        if single_file:
            temp_output = output_path + "_temp"
        else:
            temp_output = output_path

        # Convert complex types to strings for CSV format
        # IMPORTANT: Do this BEFORE creating the writer!
        if output_format == "csv":
            df = self._prepare_dataframe_for_csv(df)

        # Create writer AFTER all dataframe transformations
        writer = df.write.mode(mode)

        # Set compression
        if compression:
            writer = writer.option("compression", compression)

        # Write data
        if output_format == "parquet":
            writer.parquet(temp_output)
        elif output_format == "csv":
            writer.option("header", "true").csv(temp_output)
        elif output_format == "json":
            writer.json(temp_output)
        elif output_format == "orc":
            writer.orc(temp_output)
        elif output_format == "delta":
            # Requires delta-spark package
            writer.format("delta").save(temp_output)
            single_file = False  # Delta doesn't support single file
        else:
            raise ValueError(f"Unsupported format: {output_format}")

        # If single file requested, rename and clean up
        if single_file:
            self._consolidate_to_single_file(temp_output, output_path, output_format)

        print(f"Data saved successfully to {output_path}")

    def _prepare_dataframe_for_csv(self, df: 'DataFrame') -> 'DataFrame':
        """
        Prepare DataFrame for CSV output by converting complex types to JSON strings

        CSV format doesn't support:
        - ArrayType (lists)
        - MapType (dictionaries)
        - StructType (nested objects)

        This method converts these to JSON strings so CSV can handle them.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with complex types converted to JSON strings
        """
        from pyspark.sql.functions import col, to_json, when
        from pyspark.sql.types import ArrayType, MapType, StructType

        # Check each column and convert complex types to JSON strings
        for field in df.schema.fields:
            col_name = field.name
            col_type = field.dataType

            # Convert ArrayType to JSON string
            if isinstance(col_type, ArrayType):
                print(f"  → Converting array column '{col_name}' to JSON string for CSV")
                df = df.withColumn(
                    col_name,
                    when(col(col_name).isNull(), None)
                    .otherwise(to_json(col(col_name)))
                )

            # Convert MapType to JSON string
            elif isinstance(col_type, MapType):
                print(f"  → Converting map column '{col_name}' to JSON string for CSV")
                df = df.withColumn(
                    col_name,
                    when(col(col_name).isNull(), None)
                    .otherwise(to_json(col(col_name)))
                )

            # Convert StructType to JSON string
            elif isinstance(col_type, StructType):
                print(f"  → Converting struct column '{col_name}' to JSON string for CSV")
                df = df.withColumn(
                    col_name,
                    when(col(col_name).isNull(), None)
                    .otherwise(to_json(col(col_name)))
                )

        return df

    def _consolidate_to_single_file(self, temp_dir: str, final_path: str, output_format: str):
        """
        Consolidate Spark part files into a single file

        Args:
            temp_dir: Temporary directory with part files
            final_path: Final output file path
            output_format: File format (csv, json, etc.)
        """
        import os
        import shutil
        import glob

        # Find the part file (there should be only one with coalesce(1))
        part_files = glob.glob(os.path.join(temp_dir, f"part-*"))

        if not part_files:
            raise FileNotFoundError(f"No part files found in {temp_dir}")

        # Get the first (and should be only) part file
        part_file = part_files[0]

        # Ensure final_path has correct extension
        if not final_path.endswith(f".{output_format}"):
            final_path = f"{final_path}.{output_format}"

        # Create parent directory if it doesn't exist
        os.makedirs(os.path.dirname(os.path.abspath(final_path)), exist_ok=True)

        # Move the part file to final location
        shutil.move(part_file, final_path)

        # Clean up temporary directory
        shutil.rmtree(temp_dir, ignore_errors=True)

        print(f"  → Consolidated to single file: {final_path}")

    def generate_and_save(self,
                         schema: Dict,
                         num_records: int,
                         output_path: str,
                         output_format: str = "parquet",
                         num_partitions: int = None,
                         show_sample: bool = True,
                         single_file: bool = False):
        """
        Generate and save massive dataset in one operation

        Args:
            schema: JSON schema
            num_records: Number of records
            output_path: Output path (without extension if single_file=True)
            output_format: Output format (parquet, csv, json, orc, delta)
            num_partitions: Number of partitions for generation
            show_sample: Whether to show sample records
            single_file: If True, output a single file instead of multiple part files
        """
        start_time = datetime.now()

        # Generate data
        df = self.generate_massive_dataset(schema, num_records, num_partitions)

        # Cache for performance if showing sample
        if show_sample:
            df.cache()
            print("\nSample records:")
            df.show(5, truncate=False)
            print(f"\nSchema:")
            df.printSchema()
            print(f"\nTotal records: {df.count():,}")

        # Save data
        self.save_massive_dataset(df, output_path, output_format, single_file=single_file)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        records_per_sec = num_records / duration if duration > 0 else 0

        print(f"\nGeneration complete!")
        print(f"Time taken: {duration:.2f} seconds")
        print(f"Throughput: {records_per_sec:,.0f} records/second")

        return df

    def generate_streaming_data(self,
                              schema: Dict,
                              records_per_batch: int = 1000,
                              interval_seconds: int = 1):
        """
        Generate streaming data for real-time processing

        Args:
            schema: JSON schema
            records_per_batch: Records per micro-batch
            interval_seconds: Interval between batches

        Returns:
            Spark streaming DataFrame
        """
        from pyspark.sql.functions import expr

        # Create a rate source for continuous generation
        stream_df = self.spark.readStream \
            .format("rate") \
            .option("rowsPerSecond", records_per_batch) \
            .load()

        # Broadcast schema
        broadcast_schema = self.spark.sparkContext.broadcast(schema)

        # UDF to generate records with type safety
        @F.udf(returnType=self._create_spark_schema(schema))
        def generate_record(value):
            """
            Type-safe record generation for streaming data

            Uses DataGenerator which guarantees:
            - integer fields return int or None
            - number fields return float or None
            - boolean fields return bool or None
            - array fields return list or None
            """
            gen = DataGenerator(error_rate=self.error_rate)
            record = {}
            properties = broadcast_schema.value.get("properties", {})

            for field_name, field_schema in properties.items():
                # Type-safe generation with error injection support
                record[field_name] = gen.generate_field(field_name, field_schema)

            return Row(**record)

        # Apply generation
        result_df = stream_df.select(
            generate_record(F.col("value")).alias("data")
        ).select("data.*")

        return result_df

    def close(self):
        """Close Spark session"""
        if self.spark:
            self.spark.stop()


def main():
    """
    Auto-generate data from schemas in custom_schemas/ folder

    Usage:
        python3 spark_generator.py

    This will automatically:
    1. Find all schema files in custom_schemas/ folder
    2. Parse each schema (.xlsx, .json, .csv, .sql)
    3. Generate 1000 records from each schema
    4. Save as CSV in generated_data/ folder
    """
    import os
    from pathlib import Path

    # Check if PySpark is available
    if not SPARK_AVAILABLE:
        print("PySpark is not installed. Install with:")
        print("pip install pyspark")
        sys.exit(1)

    print("="*70)
    print("Spark Data Generator - Auto Mode")
    print("="*70)
    print("\nScanning custom_schemas/ folder for schema files...")

    # Find schema files in custom_schemas folder
    schema_dir = Path("custom_schemas")

    if not schema_dir.exists():
        print(f"\n❌ Folder not found: custom_schemas/")
        print("Please create custom_schemas/ folder and add your schema files")
        sys.exit(1)

    # Find all supported schema files
    schema_files = []
    for ext in ['.xlsx', '.xls', '.json', '.csv', '.sql']:
        schema_files.extend(schema_dir.glob(f'*{ext}'))

    # Filter out README files
    schema_files = [f for f in schema_files if 'README' not in f.name.upper()]
    schema_files = sorted(schema_files)

    if not schema_files:
        print("\n❌ No schema files found in custom_schemas/")
        print("\nSupported formats:")
        print("  - Excel: .xlsx, .xls")
        print("  - JSON: .json")
        print("  - CSV: .csv")
        print("  - SQL: .sql")
        print("\nDrop your schema files in custom_schemas/ and run again!")
        sys.exit(1)

    print(f"\n✓ Found {len(schema_files)} schema file(s):")
    for i, f in enumerate(schema_files, 1):
        print(f"  {i}. {f.name}")

    # Create output directory
    output_dir = Path("generated_data")
    output_dir.mkdir(exist_ok=True)

    # Import schema parser
    try:
        from schema_parser import SchemaParser
    except ImportError:
        print("\n❌ schema_parser.py not found")
        sys.exit(1)

    parser = SchemaParser()

    # Initialize generator
    print(f"\n{'='*70}")
    print("Initializing Spark Generator")
    print("="*70)

    generator = SparkDataGenerator(
        app_name="AutoDataGenerator",
        master="local[*]",  # Use all local cores
        memory="4g",
        error_rate=0.0  # No errors by default
    )

    success_count = 0

    try:
        # Process each schema file
        for schema_file in schema_files:
            print(f"\n{'='*70}")
            print(f"Processing: {schema_file.name}")
            print("="*70)

            try:
                # Parse schema
                print(f"  Parsing schema...")
                schema = parser.parse_schema_file(str(schema_file))

                num_fields = len(schema.get('properties', {}))
                print(f"  ✓ Parsed successfully ({num_fields} fields)")

                # Check for complex types
                properties = schema.get('properties', {})
                has_arrays = any(f.get('type') == 'array' for f in properties.values())
                has_objects = any(f.get('type') == 'object' for f in properties.values())

                if has_arrays or has_objects:
                    print(f"  ℹ️  Contains arrays/objects (will convert to JSON for CSV)")

                # Generate output path
                output_name = schema_file.stem
                output_path = f"generated_data/{output_name}"

                # Generate data
                print(f"  Generating 1000 records...")

                df = generator.generate_and_save(
                    schema=schema,
                    num_records=1000,
                    output_path=output_path,
                    output_format="csv",
                    show_sample=False,
                    single_file=True
                )

                # Verify output
                final_file = f"{output_path}.csv"
                if os.path.exists(final_file):
                    size_kb = os.path.getsize(final_file) / 1024
                    print(f"  ✅ SUCCESS: {final_file} ({size_kb:.1f} KB)")
                    success_count += 1
                else:
                    print(f"  ❌ Output file not created")

            except Exception as e:
                print(f"  ❌ Error: {e}")
                import traceback
                traceback.print_exc()

        # Summary
        print(f"\n{'='*70}")
        print("SUMMARY")
        print("="*70)
        print(f"Total schemas: {len(schema_files)}")
        print(f"Successful: {success_count}")
        print(f"Failed: {len(schema_files) - success_count}")

        if success_count > 0:
            print(f"\n✓ Generated files in: generated_data/")
            for schema_file in schema_files:
                csv_file = f"generated_data/{schema_file.stem}.csv"
                if os.path.exists(csv_file):
                    print(f"  - {csv_file}")

    finally:
        generator.close()
        print(f"\n✓ Spark session closed")


if __name__ == "__main__":
    main()