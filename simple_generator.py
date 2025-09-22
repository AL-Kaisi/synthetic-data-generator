#!/usr/bin/env python3
"""
Simple Synthetic Data Generator
Generates realistic test data from JSON schemas without unnecessary complexity
"""

import json
import random
import string
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional


class DataGenerator:
    """Simple data generator for various field types"""

    def __init__(self):
        self.fake_data = {
            "first_names": ["James", "Mary", "John", "Patricia", "Robert", "Jennifer",
                          "Michael", "Linda", "William", "Elizabeth", "David", "Barbara"],
            "last_names": ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
                         "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez"],
            "cities": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow",
                      "Sheffield", "Bradford", "Liverpool", "Edinburgh", "Bristol"],
            "companies": ["Tech Corp", "Global Systems", "Innovation Ltd", "Digital Solutions",
                        "Modern Enterprises", "Future Technologies", "Smart Industries"],
            "email_domains": ["gmail.com", "yahoo.com", "hotmail.com", "company.com"]
        }

    def generate_string(self, field_name: str, schema: Dict) -> str:
        """Generate a string value based on field name and constraints"""

        # Handle enum values
        if "enum" in schema:
            return random.choice(schema["enum"])

        # Handle patterns
        if "pattern" in schema:
            return self._generate_from_pattern(schema["pattern"])

        # Smart generation based on field name
        field_lower = field_name.lower()
        if "first" in field_lower and "name" in field_lower:
            return random.choice(self.fake_data["first_names"])
        elif "last" in field_lower and "name" in field_lower:
            return random.choice(self.fake_data["last_names"])
        elif "city" in field_lower:
            return random.choice(self.fake_data["cities"])
        elif "company" in field_lower or "brand" in field_lower:
            return random.choice(self.fake_data["companies"])
        elif "email" in field_lower:
            return self._generate_email()
        elif "phone" in field_lower:
            return self._generate_phone()
        elif "url" in field_lower or "website" in field_lower:
            return f"https://example-{random.randint(1, 999)}.com"
        elif "id" in field_lower or field_lower.endswith("_id"):
            return str(uuid.uuid4())

        # Default string generation
        min_length = schema.get("minLength", 1)
        max_length = schema.get("maxLength", 50)
        length = random.randint(min_length, min(max_length, 50))

        return ''.join(random.choices(string.ascii_letters + ' ', k=length)).strip()

    def generate_number(self, schema: Dict) -> float:
        """Generate a number value"""
        if "enum" in schema:
            return random.choice(schema["enum"])

        minimum = schema.get("minimum", 0)
        maximum = schema.get("maximum", 1000)
        value = random.uniform(minimum, maximum)

        decimal_places = schema.get("decimalPlaces", 2)
        return round(value, decimal_places)

    def generate_integer(self, schema: Dict) -> int:
        """Generate an integer value"""
        if "enum" in schema:
            return random.choice(schema["enum"])

        minimum = schema.get("minimum", 0)
        maximum = schema.get("maximum", 1000)
        return random.randint(int(minimum), int(maximum))

    def generate_boolean(self, schema: Dict) -> bool:
        """Generate a boolean value"""
        return random.choice([True, False])

    def generate_array(self, schema: Dict) -> List[Any]:
        """Generate an array of values"""
        min_items = schema.get("minItems", 0)
        max_items = schema.get("maxItems", 10)
        items_schema = schema.get("items", {"type": "string"})

        array_size = random.randint(min_items, max_items)
        result = []

        for _ in range(array_size):
            item_type = items_schema.get("type", "string")
            if item_type == "string":
                result.append(self.generate_string("item", items_schema))
            elif item_type == "number":
                result.append(self.generate_number(items_schema))
            elif item_type == "integer":
                result.append(self.generate_integer(items_schema))
            elif item_type == "boolean":
                result.append(self.generate_boolean(items_schema))

        return result

    def generate_object(self, schema: Dict) -> Dict[str, Any]:
        """Generate a nested object"""
        properties = schema.get("properties", {})
        result = {}

        for prop_name, prop_schema in properties.items():
            result[prop_name] = self.generate_field(prop_name, prop_schema)

        return result

    def generate_date(self, field_name: str, schema: Dict) -> str:
        """Generate a date string"""
        start_str = schema.get("start", "1970-01-01")
        end_str = schema.get("end", "2030-12-31")

        start = datetime.strptime(start_str, "%Y-%m-%d")
        end = datetime.strptime(end_str, "%Y-%m-%d")

        random_date = start + timedelta(days=random.randint(0, (end - start).days))

        # Check if field name suggests datetime
        if "timestamp" in field_name.lower() or "datetime" in field_name.lower():
            time_part = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
            return f"{random_date.strftime('%Y-%m-%d')}T{time_part}Z"

        return random_date.strftime("%Y-%m-%d")

    def generate_field(self, field_name: str, schema: Dict) -> Any:
        """Generate a value for a single field"""
        field_type = schema.get("type", "string")

        # Check if field name suggests a date (even if type is string)
        if "date" in field_name.lower() and field_type == "string":
            return self.generate_date(field_name, schema)

        # Generate based on type
        if field_type == "string":
            return self.generate_string(field_name, schema)
        elif field_type == "number":
            return self.generate_number(schema)
        elif field_type == "integer":
            return self.generate_integer(schema)
        elif field_type == "boolean":
            return self.generate_boolean(schema)
        elif field_type == "array":
            return self.generate_array(schema)
        elif field_type == "object":
            return self.generate_object(schema)
        else:
            # Default to string
            return self.generate_string(field_name, schema)

    def _generate_email(self) -> str:
        """Generate an email address"""
        first = random.choice(self.fake_data["first_names"]).lower()
        last = random.choice(self.fake_data["last_names"]).lower()
        domain = random.choice(self.fake_data["email_domains"])
        return f"{first}.{last}@{domain}"

    def _generate_phone(self) -> str:
        """Generate a phone number"""
        return f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"

    def _generate_from_pattern(self, pattern: str) -> str:
        """Generate string from common patterns"""

        # Common patterns
        if pattern == "^[A-Z]{2}[0-9]{6}[A-D]$":  # NINO pattern
            # Use test-only prefixes for safety
            prefix = random.choice(["TN", "BG", "GB", "NK", "ZZ"])
            return f"{prefix}{random.randint(100000, 999999)}{random.choice('ABCD')}"
        elif pattern == "^[A-Z]{3}-[0-9]{6}$":  # SKU pattern
            letters = ''.join(random.choices(string.ascii_uppercase, k=3))
            numbers = f"{random.randint(100000, 999999)}"
            return f"{letters}-{numbers}"
        elif "^[0-9]{8}$" in pattern:  # 8-digit number
            return str(random.randint(10000000, 99999999))
        elif "^[0-9]{2}-[0-9]{2}-[0-9]{2}$" in pattern:  # Sort code
            return f"{random.randint(10, 99)}-{random.randint(10, 99)}-{random.randint(10, 99)}"
        else:
            # Default pattern generation
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))


class SchemaDataGenerator:
    """Main generator that creates data from JSON schemas"""

    def __init__(self):
        self.generator = DataGenerator()

    def generate_from_schema(self, schema: Dict, num_records: int = 100) -> List[Dict]:
        """Generate multiple records from a schema"""

        if schema.get("type") != "object":
            raise ValueError("Schema must be of type 'object'")

        properties = schema.get("properties", {})
        required_fields = schema.get("required", [])

        records = []
        for _ in range(num_records):
            record = {}

            for field_name, field_schema in properties.items():
                # Skip optional fields sometimes (10% chance)
                if field_name not in required_fields and random.random() < 0.1:
                    continue

                record[field_name] = self.generator.generate_field(field_name, field_schema)

            records.append(record)

        return records

    def to_json_ld(self, data: List[Dict], schema: Dict, context_url: str = None) -> Dict:
        """Convert data to JSON-LD format"""

        context = {
            "@vocab": "https://schema.org/",
            "custom": context_url or "https://example.com/schema/"
        }

        # Add field mappings
        properties = schema.get("properties", {})
        for field_name in properties:
            if field_name not in ["id", "name", "email", "phone", "url"]:
                context[field_name] = f"custom:{field_name}"

        # Convert records
        json_ld_records = []
        for i, record in enumerate(data):
            ld_record = {
                "@context": context,
                "@type": schema.get("title", "Thing"),
                "@id": f"{context_url or 'https://example.com'}/records/{i+1}",
                **record
            }
            json_ld_records.append(ld_record)

        return {
            "@context": context,
            "@graph": json_ld_records,
            "generatedAt": datetime.now().isoformat(),
            "totalRecords": len(data)
        }


def main():
    """Example usage"""

    # Example schema
    schema = {
        "type": "object",
        "title": "Product",
        "properties": {
            "product_id": {"type": "string"},
            "name": {"type": "string", "maxLength": 50},
            "description": {"type": "string", "maxLength": 200},
            "price": {"type": "number", "minimum": 0.99, "maximum": 999.99, "decimalPlaces": 2},
            "stock": {"type": "integer", "minimum": 0, "maximum": 1000},
            "category": {"type": "string", "enum": ["Electronics", "Clothing", "Books", "Food"]},
            "tags": {"type": "array", "items": {"type": "string"}, "maxItems": 5},
            "is_active": {"type": "boolean"},
            "created_date": {"type": "string", "start": "2020-01-01", "end": "2024-12-31"}
        },
        "required": ["product_id", "name", "price"]
    }

    # Generate data
    generator = SchemaDataGenerator()
    data = generator.generate_from_schema(schema, 5)

    # Print results
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()