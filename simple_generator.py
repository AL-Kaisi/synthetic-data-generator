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
from faker import Faker


class DataGenerator:
    """Simple data generator for various field types"""

    def __init__(self, error_rate: float = 0.0):
        """
        Initialize DataGenerator

        Args:
            error_rate: Percentage of records to inject with data quality issues (0.0 to 1.0)
        """
        # Initialize Faker for more realistic data
        self.faker = Faker('en_GB')  # Using UK locale as default
        Faker.seed(random.randint(0, 10000))  # Random seed for variety

        # Error injection configuration
        self.error_rate = min(max(error_rate, 0.0), 1.0)  # Clamp between 0 and 1

        # Context storage for coherent record generation
        self.current_record_context = {}

        # Realistic skills and certifications
        self.tech_skills = [
            "Python", "JavaScript", "TypeScript", "Java", "C++", "SQL", "NoSQL",
            "Docker", "Kubernetes", "AWS", "Azure", "GCP", "React", "Angular",
            "Node.js", "Machine Learning", "Data Analysis", "DevOps", "CI/CD",
            "Agile", "Scrum", "Git", "REST APIs", "GraphQL", "Microservices"
        ]

        self.certifications = [
            "AWS Certified Solutions Architect", "Microsoft Azure Administrator",
            "Google Cloud Professional", "PMP Certification", "Scrum Master",
            "ITIL Foundation", "CompTIA Security+", "Cisco CCNA", "Six Sigma",
            "Data Science Certificate", "Certified Kubernetes Administrator",
            "Oracle Database Administrator", "Salesforce Administrator"
        ]

        self.soft_skills = [
            "Leadership", "Communication", "Problem Solving", "Team Collaboration",
            "Time Management", "Critical Thinking", "Project Management",
            "Customer Service", "Presentation Skills", "Negotiation"
        ]

        # Keep some custom data for specific needs
        self.fake_data = {
            "email_domains": ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "company.com"]
        }

        # HMRC NINO validation - invalid first letters
        self.nino_invalid_first_letters = ['D', 'F', 'I', 'Q', 'U', 'V']
        # Invalid second letters
        self.nino_invalid_second_letters = ['D', 'F', 'I', 'O', 'Q', 'U', 'V']
        # Invalid prefix combinations (real or reserved)
        self.nino_invalid_prefixes = [
            'BG', 'GB', 'NK', 'KN', 'TN', 'NT', 'ZZ',
            'FY', 'MW', 'NC', 'PP', 'PZ', 'TF'
        ]

    def generate_hmrc_nino(self) -> str:
        """
        Generate a valid HMRC National Insurance Number that follows official rules
        but is guaranteed not to be real (uses test-safe patterns)

        NINO Format: 2 letters + 6 digits + 1 letter (A, B, C, or D)

        Rules:
        - First letter cannot be: D, F, I, Q, U, V
        - Second letter cannot be: D, F, I, O, Q, U, V
        - Prefix cannot be: BG, GB, NK, KN, TN, NT, ZZ, FY, MW, NC, PP, PZ, TF
        - Suffix must be: A, B, C, or D

        Returns:
            Valid test NINO string
        """
        # Valid letters for first position (excluding invalid ones)
        valid_first = [chr(i) for i in range(65, 91) if chr(i) not in self.nino_invalid_first_letters]
        # Valid letters for second position (excluding invalid ones)
        valid_second = [chr(i) for i in range(65, 91) if chr(i) not in self.nino_invalid_second_letters]

        # Generate prefix until we get a valid one
        max_attempts = 100
        for _ in range(max_attempts):
            first_letter = random.choice(valid_first)
            second_letter = random.choice(valid_second)
            prefix = f"{first_letter}{second_letter}"

            # Check if this prefix is valid
            if prefix not in self.nino_invalid_prefixes:
                break
        else:
            # Fallback to a known test-safe prefix if we couldn't generate one
            prefix = "AA"  # AA is always valid for test data

        # Generate 6 random digits
        digits = f"{random.randint(0, 999999):06d}"

        # Generate suffix (A, B, C, or D)
        suffix = random.choice(['A', 'B', 'C', 'D'])

        return f"{prefix}{digits}{suffix}"

    def _should_inject_error(self) -> bool:
        """Determine if an error should be injected based on error_rate"""
        return random.random() < self.error_rate

    def _inject_error(self, value: Any, field_type: str, field_name: str) -> Any:
        """
        Inject data quality issues into a value

        Args:
            value: The original value
            field_type: Type of the field (string, number, integer, boolean, etc.)
            field_name: Name of the field

        Returns:
            Value with injected error
        """
        error_types = []

        # Different error types based on field type
        if field_type == "string":
            error_types = [
                "null",
                "empty",
                "whitespace",
                "special_chars",
                "wrong_case",
                "truncated",
                "extra_spaces"
            ]
        elif field_type in ["number", "integer"]:
            # For Spark compatibility: NEVER return strings for numeric fields
            # Spark's DoubleType/LongType cannot accept string values
            error_types = [
                "null",
                "negative",
                "zero",
                "extreme_value"
            ]
        elif field_type == "boolean":
            # For Spark compatibility: NEVER return strings for boolean fields
            # Spark's BooleanType cannot accept string values like "INVALID_DATA"
            error_types = [
                "null"
            ]
        elif field_type == "array":
            # For Spark compatibility: NEVER return strings for array fields
            # Spark's ArrayType cannot accept string values
            error_types = [
                "null",
                "empty"
            ]
        else:
            error_types = ["null"]

        error_type = random.choice(error_types)

        # Apply the selected error type
        if error_type == "null":
            return None
        elif error_type == "empty" and field_type == "string":
            return ""
        elif error_type == "empty" and field_type == "array":
            return []
        elif error_type == "whitespace":
            return "   "
        elif error_type == "special_chars":
            return value + random.choice(["!@#$", "///", "<<<", ">>>", "***"])
        elif error_type == "wrong_case" and isinstance(value, str):
            return value.upper() if value.islower() else value.lower()
        elif error_type == "truncated" and isinstance(value, str) and len(value) > 2:
            return value[:len(value)//2]
        elif error_type == "extra_spaces" and isinstance(value, str):
            return "  " + value + "  "
        elif error_type == "negative" and isinstance(value, (int, float)):
            return -abs(value)
        elif error_type == "zero":
            # Return 0 with correct type based on original value
            return 0.0 if isinstance(value, float) else 0
        elif error_type == "extreme_value":
            # For number fields, return floats; for integer fields, return ints
            # Avoid float('inf') as Spark cannot handle it
            if isinstance(value, float):
                return random.choice([999999999.0, -999999999.0, 1.7976931348623157e+308, -1.7976931348623157e+308])
            else:
                return random.choice([2147483647, -2147483648, 9223372036854775807, -9223372036854775808])
        elif error_type == "string_instead":
            return "INVALID_DATA"
        elif error_type == "number_instead":
            return random.randint(0, 1)
        elif error_type == "wrong_type":
            return "not_an_array"

        return value

    def generate_string(self, field_name: str, schema: Dict) -> str:
        """Generate a string value based on field name and constraints"""

        # Handle enum values
        if "enum" in schema:
            value = random.choice(schema["enum"])
            if self._should_inject_error():
                return self._inject_error(value, "string", field_name)
            return value

        # Handle patterns
        if "pattern" in schema:
            value = self._generate_from_pattern(schema["pattern"])
            if self._should_inject_error():
                return self._inject_error(value, "string", field_name)
            return value

        # Smart generation based on field name using Faker
        field_lower = field_name.lower()

        # Check for NINO field
        if "nino" in field_lower or "national_insurance" in field_lower:
            value = self.generate_hmrc_nino()
            if self._should_inject_error():
                return self._inject_error(value, "string", field_name)
            return value

        # Generate value based on field name
        value = None
        if "first" in field_lower and "name" in field_lower:
            value = self.faker.first_name()
            self.current_record_context['first_name'] = value
        elif "last" in field_lower and "name" in field_lower:
            value = self.faker.last_name()
            self.current_record_context['last_name'] = value
        elif "name" in field_lower and "full" not in field_lower:
            value = self.faker.name()
        elif "city" in field_lower:
            value = self.faker.city()
        elif "address" in field_lower:
            value = self.faker.address().replace('\n', ', ')
        elif "street" in field_lower:
            value = self.faker.street_address()
        elif "postcode" in field_lower or "zip" in field_lower:
            value = self.faker.postcode()
        elif "country" in field_lower:
            value = self.faker.country()
        elif "company" in field_lower or "brand" in field_lower:
            value = self.faker.company()
        elif "job" in field_lower or "position" in field_lower:
            value = self.faker.job()
        elif "email" in field_lower:
            # Generate email based on context (first/last name if available)
            if 'first_name' in self.current_record_context and 'last_name' in self.current_record_context:
                first = self.current_record_context['first_name'].lower()
                last = self.current_record_context['last_name'].lower()
                domain = random.choice(self.fake_data['email_domains'])
                value = random.choice([
                    f"{first}.{last}@{domain}",
                    f"{first}{last}@{domain}",
                    f"{first[0]}{last}@{domain}",
                    f"{first}_{last}@{domain}"
                ])
            else:
                value = self.faker.email()
        elif "phone" in field_lower:
            value = self.faker.phone_number()
        elif "url" in field_lower or "website" in field_lower:
            value = self.faker.url()
        elif "username" in field_lower:
            value = self.faker.user_name()
        elif "description" in field_lower or "summary" in field_lower:
            value = self.faker.text(max_nb_chars=schema.get("maxLength", 200))
        elif "title" in field_lower and "job" not in field_lower:
            value = self.faker.sentence(nb_words=4).rstrip('.')
        elif "category" in field_lower:
            value = self.faker.word().capitalize()
        elif "status" in field_lower:
            value = random.choice(["active", "pending", "completed", "inactive", "archived"])
        elif "currency" in field_lower:
            value = self.faker.currency_code()
        elif "iban" in field_lower:
            value = self.faker.iban()
        elif "isbn" in field_lower:
            value = self.faker.isbn13()
        elif "ipv4" in field_lower or ("ip" in field_lower and "address" in field_lower):
            value = self.faker.ipv4()
        elif "mac" in field_lower:
            value = self.faker.mac_address()
        elif "user_agent" in field_lower:
            value = self.faker.user_agent()
        elif "color" in field_lower or "colour" in field_lower:
            value = self.faker.color_name()
        elif "department" in field_lower:
            value = self.faker.bs().title()
        elif "id" in field_lower or field_lower.endswith("_id"):
            value = str(uuid.uuid4())
        else:
            # Default string generation with Faker
            min_length = schema.get("minLength", 1)
            max_length = schema.get("maxLength", 50)

            # Use Faker's text generation for better quality
            if max_length > 20:
                value = self.faker.text(max_nb_chars=max_length)[:max_length]
            else:
                value = self.faker.lexify('?' * random.randint(min_length, max_length))

        # Apply error injection if needed
        if self._should_inject_error():
            return self._inject_error(value, "string", field_name)
        return value

    def generate_number(self, schema: Dict) -> float:
        """
        Generate a number value using Faker for better distributions - ALWAYS returns float or None

        For Spark compatibility:
        - Always returns float type (never int)
        - Never returns strings (error injection only returns None or valid floats)
        - Handles null values properly
        """
        if "enum" in schema:
            value = random.choice(schema["enum"])
            # Ensure enum values are converted to float
            value = float(value) if value is not None else None
            if self._should_inject_error():
                error_value = self._inject_error(value, "number", "")
                # Error injection for numbers only returns None or float (never string)
                return float(error_value) if error_value is not None else None
            return value

        minimum = float(schema.get("minimum", 0))
        maximum = float(schema.get("maximum", 1000))

        # Use Faker's random float for better distribution
        value = self.faker.random.uniform(minimum, maximum)

        decimal_places = schema.get("decimalPlaces", 2)
        value = round(value, decimal_places)

        # Apply error injection if needed
        if self._should_inject_error():
            error_value = self._inject_error(value, "number", "")
            # Error injection for numbers only returns None or float (never string)
            return float(error_value) if error_value is not None else None

        # Final safeguard: ensure we always return a float type
        return float(value)

    def generate_integer(self, schema: Dict) -> int:
        """
        Generate an integer value using Faker - ALWAYS returns int or None

        For Spark compatibility:
        - Always returns int type (never float or string)
        - Never returns strings (error injection only returns None or valid ints)
        - Handles null values properly
        """
        if "enum" in schema:
            value = random.choice(schema["enum"])
            value = int(value) if value is not None else None
            if self._should_inject_error():
                error_value = self._inject_error(value, "integer", "")
                # Error injection for integers only returns None or int (never string)
                return int(error_value) if error_value is not None else None
            return value

        minimum = int(schema.get("minimum", 0))
        maximum = int(schema.get("maximum", 1000))

        # Use Faker's random int for consistency
        value = self.faker.random_int(min=minimum, max=maximum)

        # Apply error injection if needed
        if self._should_inject_error():
            error_value = self._inject_error(value, "integer", "")
            # Error injection for integers only returns None or int (never string)
            return int(error_value) if error_value is not None else None

        return value

    def generate_boolean(self, schema: Dict) -> bool:
        """
        Generate a boolean value using Faker - ALWAYS returns bool or None

        For Spark compatibility:
        - Always returns bool type (never string)
        - Never returns strings (error injection only returns None)
        - Handles null values properly
        """
        # Faker can provide weighted boolean generation if needed
        value = self.faker.boolean()

        # Apply error injection if needed
        if self._should_inject_error():
            return self._inject_error(value, "boolean", "")
        return value

    def generate_array(self, schema: Dict, field_name: str = "item") -> List[Any]:
        """
        Generate an array of values - ALWAYS returns list or None

        For Spark compatibility:
        - Always returns list type (never string or other types)
        - Never returns strings (error injection only returns None or empty list)
        - Handles null values properly
        """
        min_items = schema.get("minItems", 0)
        max_items = schema.get("maxItems", 10)
        items_schema = schema.get("items", {"type": "string"})

        array_size = random.randint(min_items, max_items)
        result = []

        # Generate realistic arrays based on field name
        field_lower = field_name.lower()
        value = None
        if "skill" in field_lower:
            # Generate realistic skills
            num_skills = random.randint(min_items or 3, min(max_items, 10))
            tech_skills = random.sample(self.tech_skills, min(num_skills - 2, len(self.tech_skills)))
            soft_skills = random.sample(self.soft_skills, min(2, len(self.soft_skills)))
            value = tech_skills + soft_skills
        elif "certification" in field_lower or "certificate" in field_lower:
            # Generate realistic certifications
            num_certs = random.randint(min_items or 1, min(max_items, 5))
            value = random.sample(self.certifications, min(num_certs, len(self.certifications)))
        elif "tag" in field_lower or "category" in field_lower:
            # Generate realistic tags/categories
            value = [self.faker.word().capitalize() for _ in range(array_size)]
        elif "course" in field_lower:
            # Generate course names
            value = [
                f"{self.faker.word().capitalize()} {random.choice(['101', '201', '301', 'Advanced', 'Introduction to'])}"
                for _ in range(array_size)
            ]
        else:
            # Default array generation
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
            value = result

        # Apply error injection if needed
        if self._should_inject_error():
            return self._inject_error(value, "array", field_name)
        return value

    def generate_object(self, schema: Dict) -> Dict[str, Any]:
        """Generate a nested object"""
        properties = schema.get("properties", {})
        result = {}

        for prop_name, prop_schema in properties.items():
            result[prop_name] = self.generate_field(prop_name, prop_schema)

        return result

    def generate_date(self, field_name: str, schema: Dict) -> str:
        """Generate a date string using Faker"""
        start_str = schema.get("start", "1970-01-01")
        end_str = schema.get("end", "2030-12-31")

        start = datetime.strptime(start_str, "%Y-%m-%d")
        end = datetime.strptime(end_str, "%Y-%m-%d")

        # Use Faker's date_between for more realistic dates
        random_date = self.faker.date_between(start_date=start, end_date=end)

        # Check if field name suggests datetime
        if "timestamp" in field_name.lower() or "datetime" in field_name.lower():
            # Use Faker for time component too
            time_obj = self.faker.time_object()
            return f"{random_date.strftime('%Y-%m-%d')}T{time_obj.strftime('%H:%M:%S')}Z"

        return str(random_date)

    def generate_field(self, field_name: str, schema: Dict) -> Any:
        """Generate a value for a single field"""
        field_type = schema.get("type", "string")

        # Check if field name suggests a date or timestamp (even if type is string)
        if ("date" in field_name.lower() or "timestamp" in field_name.lower()) and field_type == "string":
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
            return self.generate_array(schema, field_name)
        elif field_type == "object":
            return self.generate_object(schema)
        else:
            # Default to string
            return self.generate_string(field_name, schema)

    def _generate_email(self) -> str:
        """Generate an email address using Faker"""
        return self.faker.email()

    def _generate_phone(self) -> str:
        """Generate a phone number using Faker"""
        return self.faker.phone_number()

    def _generate_from_pattern(self, pattern: str) -> str:
        """Generate string from common patterns"""

        # Common patterns
        if pattern == "^[A-Z]{2}[0-9]{6}[A-D]$":  # NINO pattern
            # Use proper HMRC NINO generator
            return self.generate_hmrc_nino()
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

    def __init__(self, error_rate: float = 0.0):
        """
        Initialize SchemaDataGenerator

        Args:
            error_rate: Percentage of fields to inject with data quality issues (0.0 to 1.0)
        """
        self.generator = DataGenerator(error_rate=error_rate)

    def generate_from_schema(self, schema: Dict, num_records: int = 100) -> List[Dict]:
        """Generate multiple records from a schema"""

        if schema.get("type") != "object":
            raise ValueError("Schema must be of type 'object'")

        properties = schema.get("properties", {})
        required_fields = schema.get("required", [])

        records = []
        for _ in range(num_records):
            # Reset context for each new record
            self.generator.current_record_context = {}
            record = {}

            # Process fields in order to ensure names come before email
            ordered_fields = sorted(properties.items(), key=lambda x: (
                0 if 'first_name' in x[0].lower() else
                1 if 'last_name' in x[0].lower() else
                2 if 'email' in x[0].lower() else
                3
            ))

            for field_name, field_schema in ordered_fields:
                # Skip optional fields sometimes (10% chance)
                if field_name not in required_fields and random.random() < 0.1:
                    continue

                record[field_name] = self.generator.generate_field(field_name, field_schema)

            records.append(record)

        return records

    def to_csv(self, data: List[Dict]) -> str:
        """Convert data to CSV format"""
        import csv
        import io

        if not data:
            return ""

        # Collect all unique field names from all records
        all_fieldnames = set()
        for record in data:
            all_fieldnames.update(record.keys())

        fieldnames = sorted(list(all_fieldnames))

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL, extrasaction='ignore')

        writer.writeheader()
        for record in data:
            # Handle nested objects and arrays by converting to JSON strings
            csv_record = {}
            for key in fieldnames:
                value = record.get(key, "")
                if isinstance(value, (dict, list)):
                    csv_record[key] = json.dumps(value) if value else ""
                else:
                    csv_record[key] = value if value is not None else ""
            writer.writerow(csv_record)

        csv_content = output.getvalue()
        output.close()
        return csv_content

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

    def save_data(self, data: List[Dict], filename: str, output_format: str = "json", schema: Dict = None):
        """Save data in the specified format"""
        from datetime import datetime

        # Generate filename with timestamp if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"data_{output_format}_{timestamp}.{output_format}"

        # Ensure correct file extension
        if not filename.endswith(f".{output_format}"):
            if '.' in filename:
                filename = filename.rsplit('.', 1)[0] + f".{output_format}"
            else:
                filename = f"{filename}.{output_format}"

        if output_format == "json":
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2, default=str)

        elif output_format == "csv":
            csv_content = self.to_csv(data)
            with open(filename, 'w') as f:
                f.write(csv_content)

        elif output_format == "jsonld":
            if schema is None:
                raise ValueError("Schema is required for JSON-LD format")
            jsonld_data = self.to_json_ld(data, schema)
            with open(filename, 'w') as f:
                json.dump(jsonld_data, f, indent=2, default=str)

        else:
            raise ValueError(f"Unsupported format: {output_format}")

        return filename


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