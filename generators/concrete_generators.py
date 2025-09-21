"""
Concrete Generators - Strategy Pattern Implementation
Each generator implements a specific data generation strategy
"""

import random
import string
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List
from .base_generator import DataGenerator


class StringGenerator(DataGenerator):
    """
    Strategy Pattern: Concrete strategy for string generation
    """

    def __init__(self):
        self.common_names = {
            "first_names": [
                "James", "Mary", "John", "Patricia", "Robert", "Jennifer",
                "Michael", "Linda", "William", "Elizabeth", "David", "Barbara"
            ],
            "last_names": [
                "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
                "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez"
            ],
            "companies": [
                "Tech Corp", "Global Systems", "Innovation Ltd", "Digital Solutions",
                "Modern Enterprises", "Future Technologies", "Smart Industries"
            ],
            "cities": [
                "London", "Manchester", "Birmingham", "Leeds", "Glasgow",
                "Sheffield", "Bradford", "Liverpool", "Edinburgh", "Bristol"
            ]
        }

    def generate(self, constraints: Dict = None) -> str:
        """Generate string based on constraints"""
        constraints = constraints or {}

        if "enum" in constraints:
            return random.choice(constraints["enum"])

        if "pattern" in constraints:
            return self._generate_from_pattern(constraints["pattern"])

        data_type = constraints.get("data_type", "random")
        if data_type in self.common_names:
            return random.choice(self.common_names[data_type])

        # Default string generation
        min_length = constraints.get("minLength", 1)
        max_length = constraints.get("maxLength", 50)
        length = random.randint(min_length, max_length)

        if constraints.get("format") == "alphanumeric":
            return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
        else:
            return ''.join(random.choices(string.ascii_letters + ' ', k=length)).strip()

    def validate_constraints(self, constraints: Dict) -> bool:
        """Validate string constraints"""
        if "minLength" in constraints and "maxLength" in constraints:
            return constraints["minLength"] <= constraints["maxLength"]
        return True

    def _generate_from_pattern(self, pattern: str) -> str:
        """Generate string from regex pattern - Template Method Pattern"""
        if pattern == "^[A-Z]{2}[0-9]{6}[A-D]$":
            return self._generate_test_nino()
        elif pattern == "^[0-9]{10,11}$":
            return str(random.randint(10000000000, 99999999999))
        elif pattern == "^[A-Z]{1,2}[0-9]{1,2} [0-9][A-Z]{2}$":  # UK Postcode
            return self._generate_uk_postcode()
        elif pattern == "^[0-9]{2}-[0-9]{2}-[0-9]{2}$":  # Sort code
            return f"{random.randint(10, 99)}-{random.randint(10, 99)}-{random.randint(10, 99)}"
        elif pattern == "^[0-9]{8}$":  # Account number
            return f"{random.randint(10000000, 99999999)}"
        else:
            return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

    def _generate_test_nino(self) -> str:
        """Generate test-only NINO - Template Method Pattern"""
        invalid_prefixes = [
            "BG", "GB", "NK", "KN", "TN", "NT", "ZZ",
            "AA", "AB", "AO", "FY", "NY", "OA", "PO", "OP"
        ]
        invalid_suffixes = ["A", "B", "C", "D"]

        prefix = random.choice(invalid_prefixes)
        number = f"{random.randint(10, 99):02d}{random.randint(10, 99):02d}{random.randint(10, 99):02d}"
        suffix = random.choice(invalid_suffixes)

        return f"{prefix}{number}{suffix}"

    def _generate_uk_postcode(self) -> str:
        """Generate UK postcode"""
        area = random.choice(["M", "B", "L", "LS", "G", "S", "BD", "E", "W", "N"])
        district = random.randint(1, 99)
        unit = random.randint(0, 9)
        letters = ''.join(random.choices("ABDEFGHJLNPQRSTUWXYZ", k=2))
        return f"{area}{district} {unit}{letters}"


class NumberGenerator(DataGenerator):
    """
    Strategy Pattern: Concrete strategy for number generation
    """

    def generate(self, constraints: Dict = None) -> float:
        """Generate number based on constraints"""
        constraints = constraints or {}

        minimum = constraints.get("minimum", 0)
        maximum = constraints.get("maximum", 1000000)
        data_type = constraints.get("data_type", "float")

        if data_type == "integer":
            return random.randint(int(minimum), int(maximum))
        else:
            value = random.uniform(float(minimum), float(maximum))
            decimal_places = constraints.get("decimalPlaces", 2)
            return round(value, decimal_places)

    def validate_constraints(self, constraints: Dict) -> bool:
        """Validate number constraints"""
        if "minimum" in constraints and "maximum" in constraints:
            return constraints["minimum"] <= constraints["maximum"]
        return True


class DateGenerator(DataGenerator):
    """
    Strategy Pattern: Concrete strategy for date generation
    """

    def generate(self, constraints: Dict = None) -> str:
        """Generate date based on constraints"""
        constraints = constraints or {}

        start_date = constraints.get("start", "1970-01-01")
        end_date = constraints.get("end", "2030-12-31")

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        random_date = start + timedelta(days=random.randint(0, (end - start).days))

        if constraints.get("format") == "datetime":
            time_part = f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}:{random.randint(0, 59):02d}"
            return f"{random_date.strftime('%Y-%m-%d')}T{time_part}Z"
        else:
            return random_date.strftime("%Y-%m-%d")

    def validate_constraints(self, constraints: Dict) -> bool:
        """Validate date constraints"""
        if "start" in constraints and "end" in constraints:
            try:
                start = datetime.strptime(constraints["start"], "%Y-%m-%d")
                end = datetime.strptime(constraints["end"], "%Y-%m-%d")
                return start <= end
            except ValueError:
                return False
        return True


class ContactGenerator(DataGenerator):
    """
    Strategy Pattern: Concrete strategy for contact information generation
    """

    def __init__(self):
        self.email_domains = ["gmail.com", "yahoo.com", "hotmail.com", "company.com"]
        self.first_names = ["james", "mary", "john", "patricia", "robert", "jennifer"]
        self.last_names = ["smith", "johnson", "williams", "brown", "jones", "garcia"]

    def generate(self, constraints: Dict = None) -> str:
        """Generate contact information based on type"""
        constraints = constraints or {}
        contact_type = constraints.get("contact_type", "email")

        if contact_type == "email":
            return self._generate_email(constraints)
        elif contact_type == "phone":
            return self._generate_phone(constraints)
        elif contact_type == "url":
            return self._generate_url(constraints)
        else:
            return self._generate_email(constraints)

    def validate_constraints(self, constraints: Dict) -> bool:
        """Validate contact constraints"""
        return True

    def _generate_email(self, constraints: Dict) -> str:
        """Generate email address"""
        name = random.choice(self.first_names)
        surname = random.choice(self.last_names)
        domain = random.choice(self.email_domains)
        return f"{name}.{surname}@{domain}"

    def _generate_phone(self, constraints: Dict) -> str:
        """Generate phone number"""
        format_type = constraints.get("format", "uk")
        if format_type == "uk":
            return f"+44 {random.randint(1000, 9999)} {random.randint(100000, 999999)}"
        else:
            return f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"

    def _generate_url(self, constraints: Dict) -> str:
        """Generate URL"""
        domains = ["example.com", "test.org", "sample.net", "demo.com"]
        paths = ["", "/page", "/api/data", "/user/profile", "/docs"]
        return f"https://{random.choice(domains)}{random.choice(paths)}"


class BooleanGenerator(DataGenerator):
    """
    Strategy Pattern: Concrete strategy for boolean generation
    """

    def generate(self, constraints: Dict = None) -> bool:
        """Generate boolean value"""
        constraints = constraints or {}
        probability = constraints.get("true_probability", 0.5)
        return random.random() < probability

    def validate_constraints(self, constraints: Dict) -> bool:
        """Validate boolean constraints"""
        if "true_probability" in constraints:
            prob = constraints["true_probability"]
            return 0 <= prob <= 1
        return True


class UUIDGenerator(DataGenerator):
    """
    Strategy Pattern: Concrete strategy for UUID generation
    """

    def generate(self, constraints: Dict = None) -> str:
        """Generate UUID"""
        return str(uuid.uuid4())

    def validate_constraints(self, constraints: Dict) -> bool:
        """Validate UUID constraints"""
        return True