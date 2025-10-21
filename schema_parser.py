#!/usr/bin/env python3
"""
Schema Parser for Multiple Formats
Supports JSON, CSV, and Excel schema definitions
"""

import json
import os
from typing import Dict, List, Any, Optional
from pathlib import Path


class SchemaParser:
    """Parse schemas from various file formats"""

    def __init__(self):
        """Initialize schema parser"""
        self.supported_formats = {
            '.json': self.parse_json_schema,
            '.csv': self.parse_csv_schema,
            '.xlsx': self.parse_excel_schema,
            '.xls': self.parse_excel_schema,
            '.sql': self.parse_sql_schema
        }

    def parse_schema_file(self, file_path: str) -> Dict[str, Any]:
        """
        Parse a schema file and convert it to standard JSON Schema format

        Args:
            file_path: Path to the schema file

        Returns:
            JSON Schema dictionary

        Raises:
            ValueError: If file format is not supported
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Schema file not found: {file_path}")

        file_ext = path.suffix.lower()

        if file_ext not in self.supported_formats:
            raise ValueError(
                f"Unsupported schema format: {file_ext}. "
                f"Supported formats: {', '.join(self.supported_formats.keys())}"
            )

        parser_func = self.supported_formats[file_ext]
        return parser_func(file_path)

    def parse_json_schema(self, file_path: str) -> Dict[str, Any]:
        """
        Parse JSON schema file

        Args:
            file_path: Path to JSON schema file

        Returns:
            JSON Schema dictionary
        """
        with open(file_path, 'r') as f:
            schema = json.load(f)

        # Validate basic structure
        if not isinstance(schema, dict):
            raise ValueError("JSON schema must be a dictionary")

        # If it's not already a proper schema, wrap it
        if schema.get("type") != "object":
            # Check if it's a relational schema
            if schema.get("type") == "relational":
                return schema  # Return as-is for relational schemas
            # Otherwise assume it's properties only
            schema = {
                "type": "object",
                "title": Path(file_path).stem.replace('_', ' ').title(),
                "properties": schema
            }

        return schema

    def parse_csv_schema(self, file_path: str) -> Dict[str, Any]:
        """
        Parse CSV schema file

        Expected CSV format:
        column_name,type,description,values,required,min,max

        Args:
            file_path: Path to CSV schema file

        Returns:
            JSON Schema dictionary
        """
        import csv

        properties = {}
        required_fields = []

        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            # Validate headers
            if not reader.fieldnames or 'column_name' not in reader.fieldnames:
                raise ValueError("CSV schema must have 'column_name' column")

            for row in reader:
                column_name = row.get('column_name', '').strip()
                if not column_name:
                    continue

                # Parse field schema
                field_schema = self._parse_csv_row_to_schema(row)
                properties[column_name] = field_schema

                # Check if required
                if row.get('required', '').lower() in ['true', 'yes', '1']:
                    required_fields.append(column_name)

        schema = {
            "type": "object",
            "title": Path(file_path).stem.replace('_', ' ').title(),
            "properties": properties
        }

        if required_fields:
            schema["required"] = required_fields

        return schema

    def parse_excel_schema(self, file_path: str) -> Dict[str, Any]:
        """
        Parse Excel schema file

        Expected Excel format with columns:
        - column_name: Name of the field
        - values: Optional comma-separated values for enum (e.g., "red,green,blue")
        - description: Description of data type (e.g., "name", "date", "email", "number")
        - required: true/false or yes/no (optional)
        - type: Override type (string, number, integer, boolean, array) (optional)

        Args:
            file_path: Path to Excel schema file

        Returns:
            JSON Schema dictionary
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas and openpyxl are required for Excel schema parsing. "
                "Install with: pip install pandas openpyxl"
            )

        # Read Excel file
        df = pd.read_excel(file_path)

        # Validate required columns
        if 'column_name' not in df.columns:
            raise ValueError("Excel schema must have 'column_name' column")

        properties = {}
        required_fields = []

        for _, row in df.iterrows():
            column_name = str(row.get('column_name', '')).strip()
            if not column_name or pd.isna(row.get('column_name')):
                continue

            # Parse field schema from Excel row
            field_schema = self._parse_excel_row_to_schema(row)
            properties[column_name] = field_schema

            # Check if required
            required = str(row.get('required', 'false')).lower()
            if required in ['true', 'yes', '1']:
                required_fields.append(column_name)

        schema = {
            "type": "object",
            "title": Path(file_path).stem.replace('_', ' ').title(),
            "properties": properties
        }

        if required_fields:
            schema["required"] = required_fields

        return schema

    def parse_sql_schema(self, file_path: str) -> Dict[str, Any]:
        """
        Parse SQL CREATE TABLE statement

        Expected SQL format:
        CREATE TABLE table_name (
            column_name data_type [constraints],
            ...
        );

        Supports single-line and multi-line comments

        Args:
            file_path: Path to SQL file

        Returns:
            JSON Schema dictionary
        """
        import re

        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # Remove SQL comments
        # Remove single-line comments (-- comment)
        sql_content = re.sub(r'--[^\n]*', '', sql_content)
        # Remove multi-line comments (/* comment */)
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)

        # Extract table name
        table_match = re.search(
            r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([`"\']?(\w+)[`"\']?)',
            sql_content,
            re.IGNORECASE
        )
        table_name = table_match.group(2) if table_match else Path(file_path).stem

        # Extract column definitions
        # Find the content between parentheses (use greedy match to get all columns)
        # Start after CREATE TABLE and capture everything until the final );
        columns_match = re.search(
            r'CREATE\s+TABLE[^(]*\((.*)\);',
            sql_content,
            re.IGNORECASE | re.DOTALL
        )

        # If no semicolon, try without it
        if not columns_match:
            columns_match = re.search(
                r'CREATE\s+TABLE[^(]*\((.*)\)\s*$',
                sql_content,
                re.IGNORECASE | re.DOTALL
            )

        if not columns_match:
            raise ValueError("Could not parse CREATE TABLE statement")

        columns_text = columns_match.group(1)

        # Split by comma, but be careful with nested parentheses (e.g., DECIMAL(10,2))
        column_lines = []
        current_line = ""
        paren_depth = 0

        for char in columns_text:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
                column_lines.append(current_line.strip())
                current_line = ""
                continue
            current_line += char

        if current_line.strip():
            column_lines.append(current_line.strip())

        # Parse each column
        properties = {}
        required_fields = []

        for line in column_lines:
            line = line.strip()

            # Skip constraint definitions (PRIMARY KEY, FOREIGN KEY, etc.)
            if any(line.upper().startswith(keyword) for keyword in [
                'PRIMARY KEY', 'FOREIGN KEY', 'UNIQUE', 'CHECK', 'CONSTRAINT', 'INDEX', 'KEY'
            ]):
                continue

            # Parse column definition
            # Format: column_name data_type [constraints]
            # Use regex to handle types with parentheses (ENUM, DECIMAL, VARCHAR, etc.)
            col_match = re.match(
                r'^\s*([`"\']?\w+[`"\']?)\s+(\w+(?:\([^)]*\))?)\s*(.*?)$',
                line
            )
            if not col_match:
                continue

            column_name = col_match.group(1).strip('`"\' ')
            sql_type = col_match.group(2)  # Keep original case for ENUM values
            constraints = col_match.group(3)

            # Map SQL type to JSON schema type
            field_schema = self._sql_type_to_json_schema(sql_type, constraints, column_name)
            properties[column_name] = field_schema

            # Check if NOT NULL
            if 'NOT NULL' in constraints.upper():
                required_fields.append(column_name)

        schema = {
            "type": "object",
            "title": table_name.replace('_', ' ').title(),
            "properties": properties
        }

        if required_fields:
            schema["required"] = required_fields

        return schema

    def _sql_type_to_json_schema(self, sql_type: str, constraints: str, column_name: str) -> Dict[str, Any]:
        """
        Convert SQL data type to JSON schema field definition

        Args:
            sql_type: SQL data type (e.g., VARCHAR(255), INT, DECIMAL(10,2))
            constraints: Column constraints (e.g., NOT NULL, DEFAULT, CHECK)
            column_name: Column name for intelligent inference

        Returns:
            JSON Schema field definition
        """
        import re

        field_schema = {}

        # Extract base type and parameters
        type_match = re.match(r'(\w+)(?:\(([^)]+)\))?', sql_type)
        if not type_match:
            field_schema['type'] = 'string'
            return field_schema

        base_type = type_match.group(1).upper()
        type_params = type_match.group(2)

        # Map SQL types to JSON schema types
        # Integer types
        if base_type in ['INT', 'INTEGER', 'SMALLINT', 'TINYINT', 'MEDIUMINT', 'BIGINT', 'SERIAL']:
            field_schema['type'] = 'integer'
            if 'UNSIGNED' in sql_type.upper():
                field_schema['minimum'] = 0

        # Decimal/Float types
        elif base_type in ['DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'REAL', 'MONEY']:
            field_schema['type'] = 'number'
            if type_params and ',' in type_params:
                # DECIMAL(10,2) means 2 decimal places
                precision, scale = type_params.split(',')
                field_schema['decimalPlaces'] = int(scale.strip())

        # Boolean types
        elif base_type in ['BOOLEAN', 'BOOL', 'BIT']:
            field_schema['type'] = 'boolean'

        # Date/Time types
        elif base_type in ['DATE', 'DATETIME', 'TIMESTAMP', 'TIME']:
            field_schema['type'] = 'string'
            field_schema['description'] = base_type.lower()

        # Text types
        elif base_type in ['VARCHAR', 'CHAR', 'TEXT', 'MEDIUMTEXT', 'LONGTEXT', 'NVARCHAR', 'NCHAR']:
            field_schema['type'] = 'string'
            if type_params and type_params.isdigit():
                field_schema['maxLength'] = int(type_params)

        # JSON type
        elif base_type == 'JSON':
            field_schema['type'] = 'object'

        # Array/Set types
        elif base_type in ['ARRAY', 'SET']:
            field_schema['type'] = 'array'

        # ENUM type
        elif base_type == 'ENUM':
            field_schema['type'] = 'string'
            if type_params:
                # Extract values from ENUM('val1', 'val2', 'val3')
                enum_values = re.findall(r"'([^']*)'", type_params)
                if enum_values:
                    field_schema['enum'] = enum_values

        # UUID/GUID
        elif base_type in ['UUID', 'GUID', 'UNIQUEIDENTIFIER']:
            field_schema['type'] = 'string'
            field_schema['description'] = 'uuid'

        # Blob/Binary types
        elif base_type in ['BLOB', 'BINARY', 'VARBINARY', 'BYTEA']:
            field_schema['type'] = 'string'
            field_schema['description'] = 'binary data'

        # Default to string
        else:
            field_schema['type'] = 'string'

        # Use column name for additional inference
        field_schema['description'] = column_name.replace('_', ' ')

        # Extract DEFAULT value for enums or constraints
        default_match = re.search(r"DEFAULT\s+(?:'([^']*)'|(\d+))", constraints, re.IGNORECASE)
        if default_match and 'enum' not in field_schema:
            # We could use this for generating more realistic defaults

            pass

        return field_schema

    def _parse_csv_row_to_schema(self, row: Dict[str, str]) -> Dict[str, Any]:
        """
        Convert CSV row to JSON schema field definition

        Args:
            row: Dictionary representing a CSV row

        Returns:
            JSON Schema field definition
        """
        field_schema = {}

        # Get type
        field_type = row.get('type', 'string').strip().lower()
        description = row.get('description', '').strip().lower()

        # Infer type from description if not explicitly set
        if not field_type or field_type == 'string':
            field_type = self._infer_type_from_description(description)

        field_schema['type'] = field_type

        # Handle values (enum)
        values = row.get('values', '').strip()
        if values:
            enum_values = [v.strip() for v in values.split(',') if v.strip()]
            if enum_values:
                field_schema['enum'] = enum_values

        # Handle min/max constraints
        if 'min' in row and row['min'].strip():
            try:
                field_schema['minimum'] = float(row['min'])
            except ValueError:
                pass

        if 'max' in row and row['max'].strip():
            try:
                field_schema['maximum'] = float(row['max'])
            except ValueError:
                pass

        # Add description if provided
        if description:
            field_schema['description'] = description

        return field_schema

    def _parse_excel_row_to_schema(self, row) -> Dict[str, Any]:
        """
        Convert Excel row to JSON schema field definition

        Args:
            row: Pandas Series representing an Excel row

        Returns:
            JSON Schema field definition
        """
        import pandas as pd

        field_schema = {}

        # Get type from explicit type column or infer from description
        field_type = str(row.get('type', '')).strip().lower() if pd.notna(row.get('type')) else ''
        description = str(row.get('description', '')).strip().lower() if pd.notna(row.get('description')) else ''

        # Infer type from description if not explicitly set
        if not field_type or field_type == 'string' or field_type == 'nan':
            field_type = self._infer_type_from_description(description)

        field_schema['type'] = field_type

        # Handle values (enum)
        values = str(row.get('values', '')).strip() if pd.notna(row.get('values')) else ''
        if values and values != 'nan':
            enum_values = [v.strip() for v in values.split(',') if v.strip()]
            if enum_values:
                field_schema['enum'] = enum_values

        # Handle min/max constraints
        if 'min' in row and pd.notna(row.get('min')):
            try:
                field_schema['minimum'] = float(row['min'])
            except (ValueError, TypeError):
                pass

        if 'max' in row and pd.notna(row.get('max')):
            try:
                field_schema['maximum'] = float(row['max'])
            except (ValueError, TypeError):
                pass

        # Add description if provided and not the default
        if description and description != 'nan':
            field_schema['description'] = description

        return field_schema

    def _infer_type_from_description(self, description: str) -> str:
        """
        Infer JSON schema type from description text

        Args:
            description: Description of the field

        Returns:
            JSON schema type string
        """
        description_lower = description.lower()

        # Number types
        if any(keyword in description_lower for keyword in [
            'age', 'count', 'quantity', 'number', 'integer', 'int', 'id'
        ]) and 'insurance' not in description_lower:
            return 'integer'

        if any(keyword in description_lower for keyword in [
            'price', 'amount', 'salary', 'revenue', 'cost', 'rate', 'percentage'
        ]):
            return 'number'

        # Boolean types
        if any(keyword in description_lower for keyword in [
            'is_', 'has_', 'active', 'enabled', 'flag', 'boolean', 'bool'
        ]):
            return 'boolean'

        # Array types
        if any(keyword in description_lower for keyword in [
            'list', 'array', 'tags', 'skills', 'items'
        ]):
            return 'array'

        # Default to string
        return 'string'


def main():
    """Example usage"""
    parser = SchemaParser()

    # Example: Parse different schema formats
    example_files = [
        "custom_schemas/customer.json",
        # Add more examples here
    ]

    for file_path in example_files:
        if os.path.exists(file_path):
            try:
                schema = parser.parse_schema_file(file_path)
                print(f"\nParsed schema from {file_path}:")
                print(json.dumps(schema, indent=2))
            except Exception as e:
                print(f"Error parsing {file_path}: {e}")


if __name__ == "__main__":
    main()
