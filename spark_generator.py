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

        writer = df.write.mode(mode)

        # Optimize partitions for output
        if coalesce_partitions:
            df = df.coalesce(coalesce_partitions)
        elif single_file:
            # Coalesce to 1 partition for single file output
            df = df.coalesce(1)

        # Set compression
        if compression:
            writer = writer.option("compression", compression)

        print(f"Saving data to {output_path} as {output_format}...")

        # Determine if we need temporary directory for single file
        if single_file:
            temp_output = output_path + "_temp"
        else:
            temp_output = output_path

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
    """Example usage of Spark generator"""

    # Check if PySpark is available
    if not SPARK_AVAILABLE:
        print("PySpark is not installed. Install with:")
        print("pip install pyspark")
        sys.exit(1)

    # Example schema
    from schemas import SchemaLibrary
    schema = SchemaLibrary.ecommerce_product_schema()

    # Initialize generator with error injection
    generator = SparkDataGenerator(
        app_name="MassiveDataGeneration",
        master="local[*]",  # Use all local cores
        memory="8g",
        error_rate=0.1  # 10% error injection for data quality testing
    )

    try:
        # Generate 1 million records (multiple files for big data)
        df = generator.generate_and_save(
            schema=schema,
            num_records=1000000,
            output_path="output/massive_products",
            output_format="parquet",
            num_partitions=100,
            show_sample=True,
            single_file=False  # Multiple part files for large datasets
        )

        # Show statistics
        print(f"\nDataset statistics:")
        print(f"Partitions: {df.rdd.getNumPartitions()}")

        if generator.error_rate > 0:
            print(f"Error injection rate: {generator.error_rate * 100:.1f}%")

        # Example: Generate smaller dataset as single CSV file
        print("\n" + "="*70)
        print("Generating single CSV file...")
        print("="*70)

        small_df = generator.generate_and_save(
            schema=schema,
            num_records=10000,  # Smaller dataset
            output_path="output/products_single",  # Extension added automatically
            output_format="csv",
            show_sample=False,
            single_file=True  # Output as single CSV file
        )

    finally:
        generator.close()


if __name__ == "__main__":
    main()