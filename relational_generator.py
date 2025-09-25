#!/usr/bin/env python3
"""
Relational Schema Data Generator
Generates related data across multiple tables/schemas maintaining referential integrity
"""

import json
import random
import uuid
from typing import Dict, List, Any, Optional, Tuple
from simple_generator import DataGenerator
from schemas import SchemaLibrary


class RelationalDataGenerator:
    """Generate related data across multiple schemas with foreign key relationships"""

    def __init__(self):
        self.generator = DataGenerator()
        self.generated_records = {}  # Store generated records by schema name
        self.foreign_keys = {}  # Track foreign key relationships

    def define_relationship(self, parent_schema: str, parent_key: str,
                          child_schema: str, child_key: str, relationship_type: str = "one_to_many"):
        """Define a relationship between two schemas"""
        if child_schema not in self.foreign_keys:
            self.foreign_keys[child_schema] = []

        self.foreign_keys[child_schema].append({
            'parent_schema': parent_schema,
            'parent_key': parent_key,
            'child_key': child_key,
            'relationship_type': relationship_type
        })

    def generate_relational_data(self, schema_config: Dict) -> Dict[str, List[Dict]]:
        """
        Generate related data based on schema configuration

        Args:
            schema_config: Dictionary with schema names as keys and generation config as values
            Example:
            {
                "citizens": {"count": 100, "schema": citizen_schema},
                "child_benefit": {"count": 200, "schema": child_benefit_schema,
                                "parent": "citizens", "ratio": 2}
            }
        """
        all_schemas = SchemaLibrary.get_all_schemas()
        results = {}

        # Generate data in dependency order
        generation_order = self._determine_generation_order(schema_config)

        for schema_name in generation_order:
            config = schema_config[schema_name]
            schema = config.get('schema') or all_schemas.get(schema_name)

            if not schema:
                continue

            count = config.get('count', 100)

            # Generate records for this schema
            records = []
            for i in range(count):
                # Reset context for each record
                self.generator.current_record_context = {}
                record = self._generate_related_record(schema_name, schema, config)
                records.append(record)

            results[schema_name] = records
            self.generated_records[schema_name] = records

        return results

    def _generate_related_record(self, schema_name: str, schema: Dict, config: Dict) -> Dict:
        """Generate a single record with proper foreign key relationships"""
        record = {}
        properties = schema.get('properties', {})

        # Handle foreign key relationships first
        if schema_name in self.foreign_keys:
            for fk_relationship in self.foreign_keys[schema_name]:
                parent_schema = fk_relationship['parent_schema']
                parent_key = fk_relationship['parent_key']
                child_key = fk_relationship['child_key']

                if parent_schema in self.generated_records:
                    # Pick a random parent record
                    parent_record = random.choice(self.generated_records[parent_schema])
                    record[child_key] = parent_record[parent_key]

        # Handle parent relationships (when this record should reference existing data)
        if 'parent' in config and config['parent'] in self.generated_records:
            parent_schema = config['parent']
            parent_records = self.generated_records[parent_schema]

            if parent_records:
                parent_record = random.choice(parent_records)
                # Copy key identifying fields from parent
                if 'nino' in parent_record and 'nino' in properties:
                    record['nino'] = parent_record['nino']
                    # Also copy personal details for consistency
                    if 'claimant_first_name' in properties and 'first_name' in parent_record:
                        record['claimant_first_name'] = parent_record['first_name']
                    if 'claimant_last_name' in properties and 'last_name' in parent_record:
                        record['claimant_last_name'] = parent_record['last_name']
                    if 'date_of_birth' in properties and 'date_of_birth' in parent_record:
                        record['date_of_birth'] = parent_record['date_of_birth']

        # Generate remaining fields
        ordered_fields = sorted(properties.items(), key=lambda x: (
            0 if 'first_name' in x[0].lower() else
            1 if 'last_name' in x[0].lower() else
            2 if 'email' in x[0].lower() else
            3
        ))

        for field_name, field_schema in ordered_fields:
            if field_name not in record:  # Don't override foreign keys
                record[field_name] = self.generator.generate_field(field_name, field_schema)

        return record

    def _determine_generation_order(self, schema_config: Dict) -> List[str]:
        """Determine the order to generate schemas based on dependencies"""
        ordered = []
        remaining = set(schema_config.keys())

        while remaining:
            # Find schemas with no unresolved dependencies
            ready = []
            for schema_name in remaining:
                config = schema_config[schema_name]
                parent = config.get('parent')
                if not parent or parent in ordered or parent not in remaining:
                    ready.append(schema_name)

            if not ready:
                # If no schema is ready, pick one arbitrarily (circular dependency or error)
                ready.append(next(iter(remaining)))

            for schema_name in ready:
                ordered.append(schema_name)
                remaining.remove(schema_name)

        return ordered


def create_dwp_relational_config() -> Dict:
    """Create a DWP relational schema configuration"""
    all_schemas = SchemaLibrary.get_all_schemas()

    # Base citizen record (parent for all benefits)
    citizen_schema = {
        "type": "object",
        "title": "UKCitizen",
        "properties": {
            "citizen_id": {"type": "string"},
            "nino": {"type": "string", "pattern": "^[A-Z]{2}[0-9]{6}[A-D]$"},
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "date_of_birth": {"type": "string", "start": "1940-01-01", "end": "2000-12-31"},
            "gender": {"type": "string", "enum": ["Male", "Female", "Other"]},
            "address_line_1": {"type": "string"},
            "address_line_2": {"type": "string"},
            "city": {"type": "string"},
            "postcode": {"type": "string"},
            "email": {"type": "string"},
            "phone": {"type": "string"},
            "marital_status": {"type": "string", "enum": ["Single", "Married", "Divorced", "Widowed"]},
            "employment_status": {"type": "string", "enum": ["Employed", "Unemployed", "Retired", "Student", "Self-Employed"]},
            "registration_date": {"type": "string"}
        },
        "required": ["citizen_id", "nino", "first_name", "last_name", "date_of_birth"]
    }

    return {
        "citizens": {
            "count": 1000,
            "schema": citizen_schema,
            "description": "Base citizen records"
        },
        "child_benefit": {
            "count": 300,
            "schema": all_schemas.get('child_benefit'),
            "parent": "citizens",
            "description": "Child benefit claims linked to citizens"
        },
        "universal_credit": {
            "count": 200,
            "schema": all_schemas.get('universal_credit'),
            "parent": "citizens",
            "description": "Universal credit claims linked to citizens"
        },
        "pip": {
            "count": 150,
            "schema": all_schemas.get('pip'),
            "parent": "citizens",
            "description": "PIP claims linked to citizens"
        },
        "state_pension": {
            "count": 100,
            "schema": all_schemas.get('state_pension'),
            "parent": "citizens",
            "description": "State pension records for elderly citizens"
        }
    }


class RelationalSchemaLoader:
    """Load and validate relational schemas from files"""

    @staticmethod
    def load_relational_schema(file_path: str) -> Dict:
        """Load a relational schema configuration from JSON file"""
        with open(file_path, 'r') as f:
            config = json.load(f)

        # Validate relational schema format
        if not isinstance(config, dict):
            raise ValueError("Relational schema must be a dictionary")

        required_keys = ['schemas', 'relationships']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Relational schema must have '{key}' key")

        return config

    @staticmethod
    def save_relational_data(data: Dict[str, List[Dict]], base_filename: str):
        """Save relational data to separate files"""
        for schema_name, records in data.items():
            filename = f"{base_filename}_{schema_name}.json"
            with open(filename, 'w') as f:
                json.dump(records, f, indent=2)
            print(f"Saved {len(records)} {schema_name} records to {filename}")


def main():
    """Example usage of relational data generation"""
    print("Generating DWP Relational Data...")

    # Create relational generator
    generator = RelationalDataGenerator()

    # Get DWP configuration
    config = create_dwp_relational_config()

    # Generate related data
    data = generator.generate_relational_data(config)

    # Display summary
    print("\\nGenerated relational data:")
    for schema_name, records in data.items():
        print(f"- {schema_name}: {len(records)} records")

    # Save data
    RelationalSchemaLoader.save_relational_data(data, "dwp_relational")

    # Show sample relationships
    if 'citizens' in data and 'child_benefit' in data:
        print("\\nSample relationship:")
        citizen = data['citizens'][0]
        matching_benefits = [cb for cb in data['child_benefit'] if cb.get('nino') == citizen.get('nino')]

        print(f"Citizen: {citizen['first_name']} {citizen['last_name']} (NINO: {citizen['nino']})")
        print(f"Child benefit claims: {len(matching_benefits)}")


if __name__ == "__main__":
    main()