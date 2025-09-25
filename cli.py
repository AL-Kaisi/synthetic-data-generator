#!/usr/bin/env python3
"""
Enhanced CLI for Synthetic Data Generator
Pure command-line interface with interactive features
"""

import json
import argparse
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List

from simple_generator import SchemaDataGenerator
from schemas import SchemaLibrary, RelationalSchemaSupport
from relational_generator import RelationalDataGenerator, create_dwp_relational_config


class InteractiveCLI:
    """Interactive command-line interface for data generation"""

    def __init__(self):
        self.generator = SchemaDataGenerator()
        self.library = SchemaLibrary()

    def run_interactive_mode(self):
        """Run in interactive mode with menu-driven interface"""
        print("=" * 60)
        print("SYNTHETIC DATA GENERATOR")
        print("=" * 60)
        print()

        while True:
            print("Choose an option:")
            print("1. Generate from predefined schema")
            print("2. Generate from custom JSON schema")
            print("3. Create schema interactively")
            print("4. Generate relational data (DWP/linked schemas)")
            print("5. List available schemas")
            print("6. Exit")
            print()

            choice = input("Enter your choice (1-6): ").strip()

            if choice == "1":
                self._interactive_predefined()
            elif choice == "2":
                self._interactive_custom()
            elif choice == "3":
                self._interactive_builder()
            elif choice == "4":
                self._interactive_relational()
            elif choice == "5":
                self._list_schemas()
            elif choice == "6":
                print("Goodbye!")
                break
            else:
                print("Invalid choice. Please enter 1-6.")

            print()

    def _interactive_predefined(self):
        """Interactive predefined schema selection"""
        schemas = SchemaLibrary.get_all_schemas()

        print("\nAvailable Schemas:")
        schema_list = list(schemas.keys())
        for i, name in enumerate(schema_list, 1):
            schema = schemas[name]
            title = schema.get("title", name)
            props = len(schema.get("properties", {}))
            print(f"{i:2d}. {name} - {title} ({props} fields)")

        print()
        try:
            choice = int(input(f"Select schema (1-{len(schema_list)}): ")) - 1
            if 0 <= choice < len(schema_list):
                schema_name = schema_list[choice]
                schema = schemas[schema_name]

                num_records = int(input("Number of records to generate: "))
                output_file = input("Output filename (press Enter for auto): ").strip()

                if not output_file:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    output_file = f"data_{schema_name}_{timestamp}.json"

                self._generate_and_save(schema, num_records, output_file, schema_name)
            else:
                print("Invalid selection.")
        except (ValueError, KeyboardInterrupt):
            print("Operation cancelled.")

    def _interactive_custom(self):
        """Interactive custom schema input"""
        print("\nEnter your JSON schema:")
        print("(You can paste multi-line JSON, press Ctrl+D when done)")

        lines = []
        try:
            while True:
                line = input()
                lines.append(line)
        except EOFError:
            pass
        except KeyboardInterrupt:
            print("\nOperation cancelled.")
            return

        schema_text = "\n".join(lines)

        try:
            schema = json.loads(schema_text)
            schema = SchemaLibrary.custom_schema_from_json(schema)

            num_records = int(input("Number of records to generate: "))
            output_file = input("Output filename (press Enter for auto): ").strip()

            if not output_file:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                schema_name = schema.get("title", "custom")
                output_file = f"data_{schema_name}_{timestamp}.json"

            self._generate_and_save(schema, num_records, output_file, "custom")

        except json.JSONDecodeError as e:
            print(f"Invalid JSON: {e}")
        except Exception as e:
            print(f"Error: {e}")

    def _interactive_builder(self):
        """Interactive schema builder"""
        print("\nInteractive Schema Builder")
        print("-" * 30)

        title = input("Schema title: ").strip() or "CustomSchema"

        from schemas import create_custom_schema_builder
        builder = create_custom_schema_builder()
        builder.set_title(title)

        print("\nAdd fields (type 'done' when finished):")
        print("Available types: string, number, integer, boolean, array, date")

        required_fields = []

        while True:
            print()
            field_name = input("Field name (or 'done'): ").strip()
            if field_name.lower() == 'done':
                break

            if not field_name:
                continue

            field_type = input(f"Type for '{field_name}': ").strip().lower()
            is_required = input(f"Is '{field_name}' required? (y/n): ").strip().lower() == 'y'

            if is_required:
                required_fields.append(field_name)

            # Add constraints based on type
            if field_type == "string":
                max_length = input("Max length (optional): ").strip()
                pattern = input("Pattern regex (optional): ").strip()
                enum_values = input("Enum values (comma-separated, optional): ").strip()

                kwargs = {}
                if max_length:
                    kwargs["maxLength"] = int(max_length)
                if pattern:
                    kwargs["pattern"] = pattern
                if enum_values:
                    kwargs["enum"] = [v.strip() for v in enum_values.split(",")]

                builder.add_string_field(field_name, **kwargs)

            elif field_type == "number":
                minimum = input("Minimum value (optional): ").strip()
                maximum = input("Maximum value (optional): ").strip()
                decimal = input("Decimal places (optional): ").strip()

                kwargs = {}
                if minimum:
                    kwargs["minimum"] = float(minimum)
                if maximum:
                    kwargs["maximum"] = float(maximum)
                if decimal:
                    kwargs["decimalPlaces"] = int(decimal)

                builder.add_number_field(field_name, **kwargs)

            elif field_type == "integer":
                minimum = input("Minimum value (optional): ").strip()
                maximum = input("Maximum value (optional): ").strip()

                kwargs = {}
                if minimum:
                    kwargs["minimum"] = int(minimum)
                if maximum:
                    kwargs["maximum"] = int(maximum)

                builder.add_integer_field(field_name, **kwargs)

            elif field_type == "boolean":
                builder.add_boolean_field(field_name)

            elif field_type == "array":
                item_type = input("Item type (default: string): ").strip() or "string"
                builder.add_array_field(field_name, item_type)

            elif field_type == "date":
                start = input("Start date YYYY-MM-DD (optional): ").strip()
                end = input("End date YYYY-MM-DD (optional): ").strip()
                builder.add_date_field(field_name, start or None, end or None)

            else:
                print(f"Unknown type: {field_type}")
                continue

            print(f"Added field: {field_name}")

        # Set required fields
        if required_fields:
            builder.set_required(*required_fields)

        # Build schema
        schema = builder.build()

        print(f"\nGenerated schema with {len(schema.get('properties', {}))} fields")

        # Ask if user wants to save the schema
        save_schema = input("Save schema to file? (y/n): ").strip().lower() == 'y'
        if save_schema:
            filepath = SchemaLibrary.save_schema(schema, title)
            print(f"Schema saved to: {filepath}")

        # Ask if user wants to generate data
        generate_data = input("Generate data from this schema? (y/n): ").strip().lower() == 'y'
        if generate_data:
            try:
                num_records = int(input("Number of records: "))
                output_file = input("Output filename (press Enter for auto): ").strip()

                if not output_file:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    output_file = f"data_{title}_{timestamp}.json"

                self._generate_and_save(schema, num_records, output_file, title)
            except ValueError:
                print("Invalid number of records.")

    def _list_schemas(self):
        """List all available schemas with details"""
        schemas = SchemaLibrary.get_all_schemas()

        print("\nAvailable Predefined Schemas:")
        print("=" * 50)

        for name, schema in schemas.items():
            title = schema.get("title", name)
            props = len(schema.get("properties", {}))
            required = len(schema.get("required", []))

            print(f"\nSchema: {name}")
            print(f"  Title: {title}")
            print(f"  Fields: {props} total, {required} required")

            # Show first few field names
            properties = list(schema.get("properties", {}).keys())
            if properties:
                preview = properties[:5]
                if len(properties) > 5:
                    preview.append("...")
                print(f"  Sample fields: {', '.join(preview)}")

    def _interactive_relational(self):
        """Interactive relational data generation"""
        print("\nRelational Data Generation")
        print("=" * 50)

        # Check for available relational schemas
        relational_schemas = RelationalSchemaSupport.get_relational_schemas()

        print("Available options:")
        print("1. Generate DWP relational data (predefined)")
        print("2. Use custom relational schema")

        if relational_schemas:
            print("3. Use discovered relational schemas")

        choice = input("\nEnter your choice: ").strip()

        if choice == "1":
            self._generate_dwp_relational()
        elif choice == "2":
            self._generate_custom_relational()
        elif choice == "3" and relational_schemas:
            self._generate_discovered_relational(relational_schemas)
        else:
            print("Invalid choice.")

    def _generate_dwp_relational(self):
        """Generate DWP relational data using predefined configuration"""
        print("\nGenerating DWP Relational Data...")
        print("This will create linked data across citizens, benefits, and claims")

        try:
            # Get number of base citizens
            num_citizens = int(input("Number of base citizens (default: 500): ") or "500")

            # Create relational generator
            generator = RelationalDataGenerator()

            # Get DWP configuration and adjust citizen count
            config = create_dwp_relational_config()
            config['citizens']['count'] = num_citizens

            # Adjust other counts proportionally
            config['child_benefit']['count'] = int(num_citizens * 0.3)  # 30% have child benefit
            config['universal_credit']['count'] = int(num_citizens * 0.2)  # 20% have UC
            config['pip']['count'] = int(num_citizens * 0.15)  # 15% have PIP
            config['state_pension']['count'] = int(num_citizens * 0.1)  # 10% have pension

            print(f"\nGenerating:")
            for schema_name, schema_config in config.items():
                count = schema_config['count']
                description = schema_config.get('description', '')
                print(f"  - {schema_name}: {count} records ({description})")

            # Generate relational data
            data = generator.generate_relational_data(config)

            # Save data to separate files
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"dwp_relational_{timestamp}"

            print(f"\nSaving relational data...")
            for schema_name, records in data.items():
                filename = f"{base_filename}_{schema_name}.json"
                with open(filename, 'w') as f:
                    json.dump(records, f, indent=2)
                print(f"Saved {len(records)} {schema_name} records to {filename}")

            # Show relationships summary
            print(f"\nRelationship Summary:")
            if 'citizens' in data and 'child_benefit' in data:
                citizens_with_cb = len(set(cb.get('nino') for cb in data['child_benefit'] if cb.get('nino')))
                print(f"  - {citizens_with_cb} citizens have child benefit claims")

            if 'citizens' in data and 'universal_credit' in data:
                citizens_with_uc = len(set(uc.get('nino') for uc in data['universal_credit'] if uc.get('nino')))
                print(f"  - {citizens_with_uc} citizens have universal credit claims")

            print(f"\nRelational data generated successfully!")

        except ValueError:
            print("Invalid number entered.")
        except Exception as e:
            print(f"Error generating relational data: {e}")

    def _generate_custom_relational(self):
        """Generate relational data from custom schema file"""
        print("\nCustom Relational Schema")
        print("Place your relational schema JSON file in the custom_schemas/ directory")
        print("It should have 'type': 'relational' and define schemas and relationships")

        schema_file = input("Enter relational schema filename: ").strip()
        if not schema_file.endswith('.json'):
            schema_file += '.json'

        try:
            from relational_generator import RelationalSchemaLoader

            file_path = f"custom_schemas/{schema_file}"
            if not os.path.exists(file_path):
                print(f"File not found: {file_path}")
                return

            # Load relational schema
            config = RelationalSchemaLoader.load_relational_schema(file_path)

            # Create generator and generate data
            generator = RelationalDataGenerator()

            # Convert relational schema format to generator config
            schema_config = {}
            for schema_name, schema_def in config['schemas'].items():
                schema_config[schema_name] = {
                    'count': schema_def.get('count', 100),
                    'schema': schema_def,
                    'description': schema_def.get('description', '')
                }

            # Generate data
            data = generator.generate_relational_data(schema_config)

            # Save data
            base_filename = f"custom_relational_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            RelationalSchemaLoader.save_relational_data(data, base_filename)

            print("Custom relational data generated successfully!")

        except Exception as e:
            print(f"Error: {e}")

    def _generate_discovered_relational(self, relational_schemas):
        """Generate data from discovered relational schemas"""
        print("\nDiscovered Relational Schemas:")

        schema_list = list(relational_schemas.keys())
        for i, schema_name in enumerate(schema_list, 1):
            schema = relational_schemas[schema_name]
            title = schema.get('title', schema_name)
            description = schema.get('description', 'No description')
            print(f"{i}. {schema_name} - {title}")
            print(f"   {description}")

        try:
            choice = int(input(f"\nSelect schema (1-{len(schema_list)}): ")) - 1
            if 0 <= choice < len(schema_list):
                selected_schema = schema_list[choice]
                config = relational_schemas[selected_schema]

                # Generate using the selected relational schema
                generator = RelationalDataGenerator()

                # Convert format
                schema_config = {}
                for schema_name, schema_def in config['schemas'].items():
                    schema_config[schema_name] = {
                        'count': schema_def.get('count', 100),
                        'schema': schema_def
                    }

                data = generator.generate_relational_data(schema_config)

                # Save data
                base_filename = f"{selected_schema}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                for schema_name, records in data.items():
                    filename = f"{base_filename}_{schema_name}.json"
                    with open(filename, 'w') as f:
                        json.dump(records, f, indent=2)
                    print(f"Saved {len(records)} {schema_name} records to {filename}")

                print("Relational data generated successfully!")
            else:
                print("Invalid choice.")

        except ValueError:
            print("Invalid number entered.")

    def _generate_and_save(self, schema: Dict, num_records: int, output_file: str, schema_name: str):
        """Generate data and save to file"""
        print(f"\nGenerating {num_records} records from '{schema_name}' schema...")

        try:
            # Generate data
            data = self.generator.generate_from_schema(schema, num_records)
            print(f"Generated {len(data)} records successfully")

            # Save data
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            with open(output_path, 'w') as f:
                json.dump(data, f, indent=2)

            print(f"Data saved to: {output_path}")

            # Show sample
            if data:
                print("\nSample record:")
                print(json.dumps(data[0], indent=2))

            return True

        except Exception as e:
            print(f"Generation failed: {e}")
            return False

    def _generate_from_predefined(self, schema_name: str, num_records: int, output_file: str = None, output_format: str = "json") -> bool:
        """Generate data from predefined schema with format support"""
        try:
            from schemas import SchemaLibrary

            schemas = SchemaLibrary.get_all_schemas()
            if schema_name not in schemas:
                print(f"Schema '{schema_name}' not found")
                print("Available schemas:", ", ".join(schemas.keys()))
                return False

            schema = schemas[schema_name]
            print(f"Generating {num_records} records from '{schema_name}' schema...")

            # Generate data
            data = self.generator.generate_from_schema(schema, num_records)
            print(f"Generated {len(data)} records successfully")

            # Save data with format support
            if output_file:
                saved_file = self.generator.save_data(data, output_file, output_format, schema)
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                saved_file = self.generator.save_data(data, f"data_{schema_name}_{timestamp}", output_format, schema)

            print(f"Data saved to: {saved_file}")

            # Show sample
            if data:
                print("\nSample record:")
                print(json.dumps(data[0], indent=2))

            return True

        except Exception as e:
            print(f"Generation failed: {e}")
            return False

    def _generate_from_file(self, schema_file: str, num_records: int, output_file: str = None, output_format: str = "json") -> bool:
        """Generate data from schema file with format support"""
        try:
            from schemas import SchemaLibrary

            # Load schema from file
            schema = SchemaLibrary.load_schema(schema_file)
            schema_name = Path(schema_file).stem
            print(f"Generating {num_records} records from schema file '{schema_file}'...")

            # Generate data
            data = self.generator.generate_from_schema(schema, num_records)
            print(f"Generated {len(data)} records successfully")

            # Save data with format support
            if output_file:
                saved_file = self.generator.save_data(data, output_file, output_format, schema)
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                saved_file = self.generator.save_data(data, f"data_{schema_name}_{timestamp}", output_format, schema)

            print(f"Data saved to: {saved_file}")

            # Show sample
            if data:
                print("\nSample record:")
                print(json.dumps(data[0], indent=2))

            return True

        except Exception as e:
            print(f"Generation failed: {e}")
            return False

    def _generate_from_json(self, json_string: str, num_records: int, output_file: str = None, output_format: str = "json") -> bool:
        """Generate data from JSON string with format support"""
        try:
            from schemas import SchemaLibrary

            # Parse JSON schema
            schema = json.loads(json_string)
            schema = SchemaLibrary.custom_schema_from_json(schema)
            schema_name = schema.get("title", "custom")
            print(f"Generating {num_records} records from JSON schema...")

            # Generate data
            data = self.generator.generate_from_schema(schema, num_records)
            print(f"Generated {len(data)} records successfully")

            # Save data with format support
            if output_file:
                saved_file = self.generator.save_data(data, output_file, output_format, schema)
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                saved_file = self.generator.save_data(data, f"data_{schema_name}_{timestamp}", output_format, schema)

            print(f"Data saved to: {saved_file}")

            # Show sample
            if data:
                print("\nSample record:")
                print(json.dumps(data[0], indent=2))

            return True

        except Exception as e:
            print(f"Generation failed: {e}")
            return False

    def _generate_with_spark(self,
                            schema_name: str = None,
                            schema: Dict = None,
                            num_records: int = 1000000,
                            output_file: str = None,
                            output_format: str = "parquet",
                            partitions: int = None,
                            spark_master: str = None,
                            spark_memory: str = "4g") -> bool:
        """Generate massive dataset using PySpark"""
        try:
            # Try to import Spark generator
            try:
                from spark_generator import SparkDataGenerator, SPARK_AVAILABLE
                if not SPARK_AVAILABLE:
                    print("\nPySpark is not installed. For massive dataset generation (1M+ records), install PySpark:")
                    print("pip install pyspark")
                    print("\nFalling back to standard generation (may be slow for large datasets)...")

                    # Fallback to standard generation
                    if schema_name:
                        return self._generate_from_predefined(schema_name, num_records, output_file, "json")
                    else:
                        return False
            except ImportError:
                print("\nPySpark module not found. Install with: pip install pyspark")
                return False

            from schemas import SchemaLibrary

            # Get schema
            if schema_name:
                schemas = SchemaLibrary.get_all_schemas()
                if schema_name not in schemas:
                    print(f"Schema '{schema_name}' not found")
                    return False
                schema = schemas[schema_name]
                print(f"\nGenerating {num_records:,} records from '{schema_name}' schema using PySpark...")
            else:
                schema_name = schema.get("title", "custom")
                print(f"\nGenerating {num_records:,} records using PySpark...")

            # Suggest optimal settings for massive datasets
            if num_records >= 10000000:
                print(f"Tip: For {num_records:,} records, consider increasing memory with --spark-memory 8g")

            # Initialize Spark generator
            print(f"Initializing Spark (driver memory: {spark_memory})...")
            generator = SparkDataGenerator(
                app_name=f"DataGen_{schema_name}",
                master=spark_master,
                memory=spark_memory
            )

            # Generate output path
            if not output_file:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"spark_output/{schema_name}_{timestamp}_{num_records}"

            # Adjust format for Spark (json-ld not supported in Spark)
            spark_format = output_format
            if output_format == "jsonld":
                spark_format = "json"
                print("Note: JSON-LD format not supported in Spark mode, using JSON instead")

            # Generate and save
            df = generator.generate_and_save(
                schema=schema,
                num_records=num_records,
                output_path=output_file,
                output_format=spark_format,
                num_partitions=partitions,
                show_sample=True
            )

            # Clean up
            generator.close()
            return True

        except Exception as e:
            print(f"Spark generation failed: {e}")
            print("Try reducing the number of records or increasing memory allocation")
            return False


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Synthetic Data Generator - Generate realistic test data from JSON schemas",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Interactive mode
  %(prog)s list                               # List schemas
  %(prog)s generate ecommerce_product -n 100  # Generate 100 products
  %(prog)s from-file schema.json -n 50       # Generate from file
  %(prog)s create                             # Interactive schema builder

For more help on specific commands, use: %(prog)s <command> --help
        """
    )

    # Add global options
    parser.add_argument('--version', action='version', version='Synthetic Data Generator 2.0')

    # Create subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Interactive mode (default)
    interactive_parser = subparsers.add_parser('interactive', help='Run in interactive mode')

    # List schemas
    list_parser = subparsers.add_parser('list', help='List available predefined schemas')

    # Generate from predefined
    gen_parser = subparsers.add_parser('generate', help='Generate data from a predefined schema')
    gen_parser.add_argument('schema', help='Schema name')
    gen_parser.add_argument('-n', '--records', type=int, default=100, help='Number of records (default: 100)')
    gen_parser.add_argument('-o', '--output', help='Output file path')
    gen_parser.add_argument('--format', choices=['json', 'csv', 'jsonld', 'parquet'], default='json', help='Output format (default: json)')
    gen_parser.add_argument('--spark', action='store_true', help='Use PySpark for massive datasets (1M+ records)')
    gen_parser.add_argument('--partitions', type=int, help='Number of Spark partitions (auto-calculated if not specified)')
    gen_parser.add_argument('--spark-master', help='Spark master URL (default: local[*])')
    gen_parser.add_argument('--spark-memory', default='4g', help='Spark driver memory (default: 4g)')

    # Generate from file
    file_parser = subparsers.add_parser('from-file', help='Generate data from a schema file')
    file_parser.add_argument('file', help='Path to JSON schema file')
    file_parser.add_argument('-n', '--records', type=int, default=100, help='Number of records (default: 100)')
    file_parser.add_argument('-o', '--output', help='Output file path')
    file_parser.add_argument('--format', choices=['json', 'csv', 'jsonld', 'parquet'], default='json', help='Output format (default: json)')
    file_parser.add_argument('--spark', action='store_true', help='Use PySpark for massive datasets (1M+ records)')
    file_parser.add_argument('--partitions', type=int, help='Number of Spark partitions (auto-calculated if not specified)')
    file_parser.add_argument('--spark-master', help='Spark master URL (default: local[*])')
    file_parser.add_argument('--spark-memory', default='4g', help='Spark driver memory (default: 4g)')

    # Generate from JSON string
    json_parser = subparsers.add_parser('from-json', help='Generate data from JSON string')
    json_parser.add_argument('json', help='JSON schema string')
    json_parser.add_argument('-n', '--records', type=int, default=100, help='Number of records (default: 100)')
    json_parser.add_argument('-o', '--output', help='Output file path')
    json_parser.add_argument('--format', choices=['json', 'csv', 'jsonld', 'parquet'], default='json', help='Output format (default: json)')
    json_parser.add_argument('--spark', action='store_true', help='Use PySpark for massive datasets (1M+ records)')
    json_parser.add_argument('--partitions', type=int, help='Number of Spark partitions (auto-calculated if not specified)')
    json_parser.add_argument('--spark-master', help='Spark master URL (default: local[*])')
    json_parser.add_argument('--spark-memory', default='4g', help='Spark driver memory (default: 4g)')

    # Create schema
    create_parser = subparsers.add_parser('create', help='Create a new schema interactively')

    args = parser.parse_args()

    # Create CLI instance
    cli = InteractiveCLI()

    # Handle commands
    if args.command is None or args.command == 'interactive':
        # Run interactive mode
        cli.run_interactive_mode()

    elif args.command == 'list':
        cli._list_schemas()

    elif args.command == 'generate':
        # Check if using Spark for massive datasets
        if args.spark or args.records >= 1000000:
            success = cli._generate_with_spark(
                schema_name=args.schema,
                num_records=args.records,
                output_file=args.output,
                output_format=args.format,
                partitions=args.partitions,
                spark_master=getattr(args, 'spark_master', None),
                spark_memory=getattr(args, 'spark_memory', '4g')
            )
        else:
            success = cli._generate_from_predefined(args.schema, args.records, args.output, args.format)
        sys.exit(0 if success else 1)

    elif args.command == 'from-file':
        # Check if using Spark for massive datasets
        if args.spark or args.records >= 1000000:
            from schemas import SchemaLibrary
            schema = SchemaLibrary.load_schema(args.file)
            success = cli._generate_with_spark(
                schema=schema,
                num_records=args.records,
                output_file=args.output,
                output_format=args.format,
                partitions=args.partitions,
                spark_master=getattr(args, 'spark_master', None),
                spark_memory=getattr(args, 'spark_memory', '4g')
            )
        else:
            success = cli._generate_from_file(args.file, args.records, args.output, args.format)
        sys.exit(0 if success else 1)

    elif args.command == 'from-json':
        # Check if using Spark for massive datasets
        if args.spark or args.records >= 1000000:
            schema = json.loads(args.json)
            success = cli._generate_with_spark(
                schema=schema,
                num_records=args.records,
                output_file=args.output,
                output_format=args.format,
                partitions=args.partitions,
                spark_master=getattr(args, 'spark_master', None),
                spark_memory=getattr(args, 'spark_memory', '4g')
            )
        else:
            success = cli._generate_from_json(args.json, args.records, args.output, args.format)
        sys.exit(0 if success else 1)

    elif args.command == 'create':
        cli._interactive_builder()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)