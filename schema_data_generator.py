#!/usr/bin/env python3
"""
Flexible synthetic data generator that works with any JSON schema
Refactored with Design Patterns:
- Strategy Pattern for data generation
- Factory Pattern for generator creation
- Chain of Responsibility for schema processing
- Facade Pattern for simple interface
"""

import json
from datetime import datetime
from typing import Dict, List, Any

from generators import GeneratorRegistry
from schema_processor import SchemaProcessorChain


class SchemaDataGenerator:
    """
    Facade Pattern - Provides simple interface to complex data generation system

    This class orchestrates the entire data generation process using:
    - Registry Pattern for generator management
    - Chain of Responsibility for schema processing
    - Strategy Pattern for different generation strategies
    """

    def __init__(self, domain: str = "standard"):
        """
        Initialize the data generator

        Args:
            domain: Domain-specific rules to apply (standard, dwp, etc.)
        """
        # Registry Pattern - get generator registry singleton
        self.generator_registry = GeneratorRegistry()

        # Get appropriate factory for domain
        self.factory = self.generator_registry.get_factory(
            "dwp" if domain == "dwp" else "standard"
        )

        # Chain of Responsibility - create processing chain
        self.schema_processor = SchemaProcessorChain(domain)

        # Cache for generators to avoid recreation
        self._generator_cache = {}

    def _get_generator(self, generator_type: str):
        """
        Get generator instance with caching
        Factory Method Pattern - creates appropriate generator
        """
        if generator_type not in self._generator_cache:
            if generator_type in ["string", "number", "date"]:
                # Use factory methods for basic types
                if generator_type == "string":
                    self._generator_cache[generator_type] = self.factory.create_string_generator()
                elif generator_type == "number":
                    self._generator_cache[generator_type] = self.factory.create_number_generator()
                elif generator_type == "date":
                    self._generator_cache[generator_type] = self.factory.create_date_generator()
            else:
                # Use specialized generator factory
                self._generator_cache[generator_type] = self.factory.create_specialized_generator(generator_type)

        return self._generator_cache[generator_type]

    def generate_field_value(self, field_name: str, field_schema: Dict) -> Any:
        """
        Generate value for a single field
        Template Method Pattern - defines the generation workflow
        """
        # Process field through chain of responsibility
        processing_result = self.schema_processor.process_field(field_name, field_schema)

        if not processing_result["valid"]:
            raise ValueError(f"Invalid field schema for '{field_name}': {processing_result['errors']}")

        constraints = processing_result["constraints"]
        generator_type = constraints.get("generator_type", "string")

        # Get appropriate generator (Factory Pattern)
        generator = self._get_generator(generator_type)

        # Generate value (Strategy Pattern)
        return generator.generate(constraints)

    def generate_from_schema(self, schema: Dict, num_records: int = 100) -> List[Dict]:
        """
        Generate data based on a JSON schema
        Template Method Pattern - defines the overall generation process
        """
        if "type" not in schema or schema["type"] != "object":
            raise ValueError("Schema must be an object type with properties")

        properties = schema.get("properties", {})
        required_fields = schema.get("required", [])

        records = []
        for i in range(num_records):
            record = self._generate_single_record(properties, required_fields)
            records.append(record)

        return records

    def _generate_single_record(self, properties: Dict, required_fields: List[str]) -> Dict:
        """
        Generate a single record
        Template Method Pattern - defines record generation steps
        """
        record = {}

        for field_name, field_schema in properties.items():
            # Skip optional fields sometimes (10% chance)
            if field_name not in required_fields and self._should_skip_optional_field():
                continue

            # Generate field value using the new pattern-based approach
            try:
                record[field_name] = self.generate_field_value(field_name, field_schema)
            except Exception as e:
                # Fallback to basic string if generation fails
                print(f"Warning: Failed to generate field '{field_name}': {e}")
                record[field_name] = f"default_value_{field_name}"

        return record

    def _should_skip_optional_field(self) -> bool:
        """Template Method - decides whether to skip optional fields"""
        import random
        return random.random() < 0.1


    def to_json_ld(self, data: List[Dict], schema: Dict, context_url: str = None) -> Dict:
        """Convert generated data to JSON-LD format"""

        # Create context based on schema
        context = {
            "@vocab": "https://schema.org/",
            "custom": context_url or "https://example.com/schema/"
        }

        # Add field mappings to context
        properties = schema.get("properties", {})
        for field_name in properties:
            if field_name in ["id", "name", "email", "phone", "url"]:
                # Map to schema.org properties where possible
                context[field_name] = field_name
            else:
                context[field_name] = f"custom:{field_name}"

        # Convert records to JSON-LD
        json_ld_records = []
        for i, record in enumerate(data):
            ld_record = {
                "@context": context,
                "@type": schema.get("title", "Thing"),
                "@id": f"{context_url or 'https://example.com'}/records/{i+1}"
            }

            for field, value in record.items():
                ld_record[field] = value

            json_ld_records.append(ld_record)

        # Create complete JSON-LD document
        json_ld_document = {
            "@context": context,
            "@graph": json_ld_records,
            "generatedAt": datetime.now().isoformat(),
            "totalRecords": len(data),
            "schema": schema.get("title", "Generated Data"),
            "description": f"Synthetic data generated from schema: {schema.get('title', 'Unknown')}"
        }

        return json_ld_document

def main():
    """Example usage with sample schemas"""
    generator = SchemaDataGenerator()

    # Example schema 1: User profile
    user_schema = {
        "type": "object",
        "title": "User",
        "properties": {
            "id": {"type": "string"},
            "first_name": {"type": "string", "maxLength": 20},
            "last_name": {"type": "string", "maxLength": 20},
            "email": {"type": "string"},
            "age": {"type": "integer", "minimum": 18, "maximum": 80},
            "city": {"type": "string"},
            "country": {"type": "string"},
            "is_active": {"type": "boolean"},
            "join_date": {"type": "string"},
            "phone": {"type": "string"}
        },
        "required": ["id", "first_name", "last_name", "email"]
    }

    print("🚀 Generating data from schema...")

    # Generate data
    data = generator.generate_from_schema(user_schema, 10)
    print(f"✅ Generated {len(data)} records")

    # Convert to JSON-LD
    json_ld_data = generator.to_json_ld(data, user_schema)

    # Save outputs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save regular JSON
    with open(f"generated_data_{timestamp}.json", 'w') as f:
        json.dump(data, f, indent=2)

    # Save JSON-LD
    with open(f"generated_data_{timestamp}.jsonld", 'w') as f:
        json.dump(json_ld_data, f, indent=2)

    print(f"💾 Data saved as JSON and JSON-LD")
    print("\n📋 Sample record:")
    print(json.dumps(data[0], indent=2))

    print("\n📋 Sample JSON-LD record:")
    print(json.dumps(json_ld_data["@graph"][0], indent=2))

if __name__ == "__main__":
    main()