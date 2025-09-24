#!/usr/bin/env python3
"""
Command-line interface for flexible synthetic data generation
Supports both predefined schemas and custom schemas
"""

import json
import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

from simple_generator import SchemaDataGenerator
from schemas import SchemaLibrary


class DataGeneratorCLI:
    """Command-line interface for data generation"""

    def __init__(self):
        self.generator = SchemaDataGenerator()
        self.library = SchemaLibrary()

    def list_schemas(self):
        """List all available predefined schemas"""
        print("\nAvailable Predefined Schemas:\n")
        schemas = SchemaLibrary.get_all_schemas()

        for name, schema in schemas.items():
            title = schema.get("title", name)
            props = len(schema.get("properties", {}))
            required = len(schema.get("required", []))

            print(f"  - {name}")
            print(f"    Title: {title}")
            print(f"    Properties: {props} fields ({required} required)")
            print()

    def generate_from_predefined(self, schema_name: str, num_records: int, output_file: str = None):
        """Generate data from a predefined schema"""
        schemas = SchemaLibrary.get_all_schemas()

        if schema_name not in schemas:
            print(f"Error: Schema '{schema_name}' not found")
            print("Use --list to see available schemas")
            return False

        schema = schemas[schema_name]
        return self._generate_and_save(schema, num_records, output_file, schema_name)

    def generate_from_file(self, schema_file: str, num_records: int, output_file: str = None):
        """Generate data from a custom schema file"""
        try:
            schema = SchemaLibrary.load_schema(schema_file)
            schema_name = Path(schema_file).stem
            return self._generate_and_save(schema, num_records, output_file, schema_name)
        except Exception as e:
            print(f"Error loading schema: {e}")
            return False

    def generate_from_json(self, json_string: str, num_records: int, output_file: str = None):
        """Generate data from a JSON string schema"""
        try:
            schema = json.loads(json_string)
            schema = SchemaLibrary.custom_schema_from_json(schema)
            schema_name = schema.get("title", "custom")
            return self._generate_and_save(schema, num_records, output_file, schema_name)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON: {e}")
            return False
        except Exception as e:
            print(f"Error processing schema: {e}")
            return False

    def _generate_and_save(self, schema: Dict, num_records: int, output_file: str, schema_name: str):
        """Generate data and save to file"""
        print(f"\nGenerating {num_records} records from '{schema_name}' schema...")

        try:
            # Generate data
            data = self.generator.generate_from_schema(schema, num_records)
            print(f"Generated {len(data)} records successfully")

            # Determine output file
            if output_file is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"data_{schema_name}_{timestamp}.json"

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

    def create_schema_interactively(self):
        """Interactive schema creation"""
        from schemas import create_custom_schema_builder

        print("\nInteractive Schema Builder\n")

        builder = create_custom_schema_builder()

        # Get schema title
        title = input("Enter schema title: ").strip() or "CustomSchema"
        builder.set_title(title)

        # Add fields
        print("\nAdd fields (type 'done' when finished):")
        print("Available types: string, number, integer, boolean, array, date\n")

        while True:
            field_name = input("\nField name (or 'done'): ").strip()
            if field_name.lower() == 'done':
                break

            if not field_name:
                continue

            field_type = input(f"Type for '{field_name}': ").strip().lower()

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

            print(f"✓ Added field: {field_name}")

        # Set required fields
        required = input("\nRequired fields (comma-separated): ").strip()
        if required:
            builder.set_required(*[f.strip() for f in required.split(",")])

        # Build schema
        schema = builder.build()

        # Save schema
        save = input("\nSave schema to file? (y/n): ").strip().lower()
        if save == 'y':
            filepath = SchemaLibrary.save_schema(schema, title)
            print(f"Schema saved to: {filepath}")

        # Generate data
        generate = input("Generate data from this schema? (y/n): ").strip().lower()
        if generate == 'y':
            num = int(input("Number of records: ").strip() or "10")
            self._generate_and_save(schema, num, None, title)

        return schema


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Flexible Synthetic Data Generator - Generate realistic test data from any JSON schema"
    )

    # Commands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # List schemas
    list_parser = subparsers.add_parser('list', help='List available predefined schemas')

    # Generate from predefined
    gen_parser = subparsers.add_parser('generate', help='Generate data from a predefined schema')
    gen_parser.add_argument('schema', help='Schema name (use "list" to see options)')
    gen_parser.add_argument('-n', '--records', type=int, default=100, help='Number of records to generate')
    gen_parser.add_argument('-o', '--output', help='Output file path')

    # Generate from file
    file_parser = subparsers.add_parser('from-file', help='Generate data from a schema file')
    file_parser.add_argument('file', help='Path to JSON schema file')
    file_parser.add_argument('-n', '--records', type=int, default=100, help='Number of records to generate')
    file_parser.add_argument('-o', '--output', help='Output file path')

    # Generate from JSON
    json_parser = subparsers.add_parser('from-json', help='Generate data from JSON string')
    json_parser.add_argument('json', help='JSON schema string')
    json_parser.add_argument('-n', '--records', type=int, default=100, help='Number of records to generate')
    json_parser.add_argument('-o', '--output', help='Output file path')

    # Create schema
    create_parser = subparsers.add_parser('create', help='Create a new schema interactively')

    # Example schemas
    example_parser = subparsers.add_parser('examples', help='Show example schemas')

    args = parser.parse_args()

    cli = DataGeneratorCLI()

    if args.command == 'list':
        cli.list_schemas()

    elif args.command == 'generate':
        success = cli.generate_from_predefined(args.schema, args.records, args.output)
        sys.exit(0 if success else 1)

    elif args.command == 'from-file':
        success = cli.generate_from_file(args.file, args.records, args.output)
        sys.exit(0 if success else 1)

    elif args.command == 'from-json':
        success = cli.generate_from_json(args.json, args.records, args.output)
        sys.exit(0 if success else 1)

    elif args.command == 'create':
        cli.create_schema_interactively()

    elif args.command == 'examples':
        print("\nExample Usage:\n")
        print("  # List available schemas")
        print("  python generate_data.py list\n")
        print("  # Generate 100 e-commerce products")
        print("  python generate_data.py generate ecommerce_product -n 100\n")
        print("  # Generate from custom schema file")
        print("  python generate_data.py from-file my_schema.json -n 50 -o output.json\n")
        print("  # Create a new schema interactively")
        print("  python generate_data.py create\n")
        print("  # Generate from inline JSON")
        print('  python generate_data.py from-json \'{"type":"object","properties":{"id":{"type":"string"}}}\' -n 10\n')

    else:
        parser.print_help()


if __name__ == "__main__":
    main()