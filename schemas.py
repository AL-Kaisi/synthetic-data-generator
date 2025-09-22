#!/usr/bin/env python3
"""
Flexible schema system for synthetic data generation
Supports any JSON Schema format with custom constraints
"""

import json
from typing import Dict, Any, List

class SchemaLibrary:
    """
    A library of predefined schemas for common data types
    Can be extended with custom schemas
    """

    @staticmethod
    def ecommerce_product_schema() -> Dict:
        """E-commerce product schema"""
        return {
            "type": "object",
            "title": "Product",
            "properties": {
                "product_id": {"type": "string"},
                "sku": {"type": "string", "pattern": "^[A-Z]{3}-[0-9]{6}$"},
                "name": {"type": "string", "maxLength": 100},
                "description": {"type": "string", "maxLength": 500},
                "category": {"type": "string", "enum": ["Electronics", "Clothing", "Home", "Sports", "Books", "Food", "Toys"]},
                "subcategory": {"type": "string"},
                "brand": {"type": "string"},
                "price": {"type": "number", "minimum": 0.01, "maximum": 10000.00, "decimalPlaces": 2},
                "discount_percentage": {"type": "integer", "minimum": 0, "maximum": 90},
                "stock_quantity": {"type": "integer", "minimum": 0, "maximum": 1000},
                "weight_kg": {"type": "number", "minimum": 0.01, "maximum": 100.0, "decimalPlaces": 2},
                "dimensions": {
                    "type": "object",
                    "properties": {
                        "length_cm": {"type": "number", "minimum": 1, "maximum": 500},
                        "width_cm": {"type": "number", "minimum": 1, "maximum": 500},
                        "height_cm": {"type": "number", "minimum": 1, "maximum": 500}
                    }
                },
                "color": {"type": "string", "enum": ["Red", "Blue", "Green", "Black", "White", "Yellow", "Purple", "Orange"]},
                "material": {"type": "string"},
                "is_active": {"type": "boolean"},
                "rating": {"type": "number", "minimum": 0, "maximum": 5, "decimalPlaces": 1},
                "review_count": {"type": "integer", "minimum": 0, "maximum": 10000},
                "tags": {"type": "array", "items": {"type": "string"}},
                "created_date": {"type": "string", "start": "2020-01-01", "end": "2024-12-31"},
                "last_modified": {"type": "string"}
            },
            "required": ["product_id", "sku", "name", "price", "category"]
        }

    @staticmethod
    def healthcare_patient_schema() -> Dict:
        """Healthcare patient record schema"""
        return {
            "type": "object",
            "title": "PatientRecord",
            "properties": {
                "patient_id": {"type": "string"},
                "medical_record_number": {"type": "string", "pattern": "^MRN-[0-9]{8}$"},
                "first_name": {"type": "string", "maxLength": 50},
                "last_name": {"type": "string", "maxLength": 50},
                "middle_initial": {"type": "string", "maxLength": 1},
                "date_of_birth": {"type": "string", "start": "1920-01-01", "end": "2024-01-01"},
                "gender": {"type": "string", "enum": ["Male", "Female", "Other", "Prefer not to say"]},
                "blood_type": {"type": "string", "enum": ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]},
                "height_cm": {"type": "integer", "minimum": 50, "maximum": 250},
                "weight_kg": {"type": "number", "minimum": 10, "maximum": 300, "decimalPlaces": 1},
                "bmi": {"type": "number", "minimum": 10, "maximum": 50, "decimalPlaces": 1},
                "allergies": {"type": "array", "items": {"type": "string"}},
                "medications": {"type": "array", "items": {"type": "string"}},
                "chronic_conditions": {"type": "array", "items": {"type": "string"}},
                "emergency_contact_name": {"type": "string"},
                "emergency_contact_phone": {"type": "string"},
                "insurance_provider": {"type": "string"},
                "insurance_policy_number": {"type": "string"},
                "primary_physician": {"type": "string"},
                "last_visit_date": {"type": "string"},
                "next_appointment": {"type": "string"},
                "vaccination_status": {"type": "object"},
                "smoking_status": {"type": "string", "enum": ["Never", "Former", "Current", "Unknown"]},
                "address": {"type": "string"},
                "city": {"type": "string"},
                "state": {"type": "string"},
                "zip_code": {"type": "string", "pattern": "^[0-9]{5}(-[0-9]{4})?$"}
            },
            "required": ["patient_id", "medical_record_number", "first_name", "last_name", "date_of_birth"]
        }

    @staticmethod
    def financial_transaction_schema() -> Dict:
        """Financial transaction schema"""
        return {
            "type": "object",
            "title": "Transaction",
            "properties": {
                "transaction_id": {"type": "string"},
                "account_number": {"type": "string", "pattern": "^[0-9]{10,16}$"},
                "transaction_type": {"type": "string", "enum": ["Debit", "Credit", "Transfer", "Withdrawal", "Deposit", "Payment", "Refund"]},
                "amount": {"type": "number", "minimum": 0.01, "maximum": 1000000.00, "decimalPlaces": 2},
                "currency": {"type": "string", "enum": ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD"]},
                "timestamp": {"type": "string"},
                "merchant_name": {"type": "string"},
                "merchant_category": {"type": "string", "enum": ["Grocery", "Restaurant", "Entertainment", "Travel", "Shopping", "Utilities", "Healthcare", "Education"]},
                "payment_method": {"type": "string", "enum": ["Card", "Cash", "Check", "Wire", "ACH", "Mobile", "Crypto"]},
                "card_last_four": {"type": "string", "pattern": "^[0-9]{4}$"},
                "authorization_code": {"type": "string"},
                "reference_number": {"type": "string"},
                "status": {"type": "string", "enum": ["Pending", "Completed", "Failed", "Cancelled", "Reversed"]},
                "balance_after": {"type": "number", "minimum": 0, "decimalPlaces": 2},
                "fee_amount": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "location": {"type": "string"},
                "ip_address": {"type": "string", "pattern": "^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$"},
                "device_id": {"type": "string"},
                "risk_score": {"type": "integer", "minimum": 0, "maximum": 100},
                "fraud_flag": {"type": "boolean"},
                "notes": {"type": "string", "maxLength": 500}
            },
            "required": ["transaction_id", "account_number", "transaction_type", "amount", "timestamp"]
        }

    @staticmethod
    def education_student_schema() -> Dict:
        """Education student record schema"""
        return {
            "type": "object",
            "title": "StudentRecord",
            "properties": {
                "student_id": {"type": "string"},
                "enrollment_number": {"type": "string", "pattern": "^ENR-[0-9]{8}$"},
                "first_name": {"type": "string", "maxLength": 50},
                "last_name": {"type": "string", "maxLength": 50},
                "date_of_birth": {"type": "string", "start": "1995-01-01", "end": "2010-01-01"},
                "email": {"type": "string"},
                "phone": {"type": "string"},
                "grade_level": {"type": "integer", "minimum": 1, "maximum": 12},
                "gpa": {"type": "number", "minimum": 0.0, "maximum": 4.0, "decimalPlaces": 2},
                "attendance_percentage": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 1},
                "courses": {"type": "array", "items": {"type": "string"}},
                "major": {"type": "string"},
                "minor": {"type": "string"},
                "academic_year": {"type": "string", "pattern": "^[0-9]{4}-[0-9]{4}$"},
                "semester": {"type": "string", "enum": ["Fall", "Spring", "Summer", "Winter"]},
                "enrollment_status": {"type": "string", "enum": ["Full-time", "Part-time", "Graduated", "Withdrawn", "On Leave"]},
                "credits_completed": {"type": "integer", "minimum": 0, "maximum": 200},
                "credits_in_progress": {"type": "integer", "minimum": 0, "maximum": 30},
                "advisor_name": {"type": "string"},
                "financial_aid": {"type": "boolean"},
                "scholarship_amount": {"type": "number", "minimum": 0, "maximum": 50000, "decimalPlaces": 2},
                "campus_residence": {"type": "boolean"},
                "dormitory": {"type": "string"},
                "graduation_date": {"type": "string"},
                "honors": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["student_id", "enrollment_number", "first_name", "last_name", "email"]
        }

    @staticmethod
    def hr_employee_schema() -> Dict:
        """Human Resources employee schema"""
        return {
            "type": "object",
            "title": "Employee",
            "properties": {
                "employee_id": {"type": "string"},
                "badge_number": {"type": "string", "pattern": "^EMP-[0-9]{6}$"},
                "first_name": {"type": "string", "maxLength": 50},
                "last_name": {"type": "string", "maxLength": 50},
                "email": {"type": "string"},
                "phone": {"type": "string"},
                "department": {"type": "string", "enum": ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations", "Legal", "IT"]},
                "position_title": {"type": "string"},
                "manager_id": {"type": "string"},
                "hire_date": {"type": "string", "start": "2000-01-01", "end": "2024-12-31"},
                "employment_type": {"type": "string", "enum": ["Full-time", "Part-time", "Contract", "Intern", "Consultant"]},
                "salary": {"type": "number", "minimum": 20000, "maximum": 500000, "decimalPlaces": 2},
                "bonus_percentage": {"type": "integer", "minimum": 0, "maximum": 50},
                "stock_options": {"type": "integer", "minimum": 0, "maximum": 100000},
                "office_location": {"type": "string"},
                "remote_work": {"type": "boolean"},
                "work_schedule": {"type": "string", "enum": ["9-5", "Flexible", "Shift", "Part-time"]},
                "vacation_days": {"type": "integer", "minimum": 0, "maximum": 30},
                "sick_days_used": {"type": "integer", "minimum": 0, "maximum": 15},
                "performance_rating": {"type": "number", "minimum": 1, "maximum": 5, "decimalPlaces": 1},
                "last_review_date": {"type": "string"},
                "next_review_date": {"type": "string"},
                "certifications": {"type": "array", "items": {"type": "string"}},
                "skills": {"type": "array", "items": {"type": "string"}},
                "emergency_contact": {"type": "string"},
                "is_active": {"type": "boolean"}
            },
            "required": ["employee_id", "first_name", "last_name", "email", "department", "hire_date"]
        }

    @staticmethod
    def iot_sensor_data_schema() -> Dict:
        """IoT sensor data schema"""
        return {
            "type": "object",
            "title": "SensorReading",
            "properties": {
                "reading_id": {"type": "string"},
                "device_id": {"type": "string", "pattern": "^DEV-[A-Z0-9]{8}$"},
                "sensor_type": {"type": "string", "enum": ["Temperature", "Humidity", "Pressure", "Motion", "Light", "Sound", "CO2", "Proximity"]},
                "timestamp": {"type": "string"},
                "value": {"type": "number", "minimum": -100, "maximum": 1000, "decimalPlaces": 2},
                "unit": {"type": "string", "enum": ["Celsius", "Fahrenheit", "Percent", "Pascal", "Lux", "Decibel", "PPM", "Meters"]},
                "location_lat": {"type": "number", "minimum": -90, "maximum": 90, "decimalPlaces": 6},
                "location_lng": {"type": "number", "minimum": -180, "maximum": 180, "decimalPlaces": 6},
                "battery_level": {"type": "integer", "minimum": 0, "maximum": 100},
                "signal_strength": {"type": "integer", "minimum": -120, "maximum": 0},
                "firmware_version": {"type": "string", "pattern": "^[0-9]+\\.[0-9]+\\.[0-9]+$"},
                "calibration_date": {"type": "string"},
                "error_code": {"type": "integer", "minimum": 0, "maximum": 999},
                "status": {"type": "string", "enum": ["Active", "Inactive", "Error", "Maintenance", "Offline"]},
                "anomaly_detected": {"type": "boolean"},
                "threshold_exceeded": {"type": "boolean"},
                "maintenance_required": {"type": "boolean"}
            },
            "required": ["reading_id", "device_id", "sensor_type", "timestamp", "value"]
        }

    @staticmethod
    def social_media_post_schema() -> Dict:
        """Social media post schema"""
        return {
            "type": "object",
            "title": "SocialMediaPost",
            "properties": {
                "post_id": {"type": "string"},
                "user_id": {"type": "string"},
                "username": {"type": "string", "pattern": "^@[a-zA-Z0-9_]{3,15}$"},
                "content": {"type": "string", "maxLength": 280},
                "hashtags": {"type": "array", "items": {"type": "string"}},
                "mentions": {"type": "array", "items": {"type": "string"}},
                "timestamp": {"type": "string"},
                "platform": {"type": "string", "enum": ["Twitter", "Facebook", "Instagram", "LinkedIn", "TikTok", "Reddit"]},
                "post_type": {"type": "string", "enum": ["Text", "Image", "Video", "Link", "Poll", "Story"]},
                "media_urls": {"type": "array", "items": {"type": "string"}},
                "likes_count": {"type": "integer", "minimum": 0, "maximum": 1000000},
                "shares_count": {"type": "integer", "minimum": 0, "maximum": 100000},
                "comments_count": {"type": "integer", "minimum": 0, "maximum": 50000},
                "views_count": {"type": "integer", "minimum": 0, "maximum": 10000000},
                "engagement_rate": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "sentiment": {"type": "string", "enum": ["Positive", "Negative", "Neutral", "Mixed"]},
                "language": {"type": "string"},
                "location": {"type": "string"},
                "is_sponsored": {"type": "boolean"},
                "is_verified": {"type": "boolean"},
                "follower_count": {"type": "integer", "minimum": 0, "maximum": 100000000}
            },
            "required": ["post_id", "user_id", "content", "timestamp", "platform"]
        }

    @staticmethod
    def get_all_schemas() -> Dict[str, Dict]:
        """Get all available schemas"""
        return {
            "ecommerce_product": SchemaLibrary.ecommerce_product_schema(),
            "healthcare_patient": SchemaLibrary.healthcare_patient_schema(),
            "financial_transaction": SchemaLibrary.financial_transaction_schema(),
            "education_student": SchemaLibrary.education_student_schema(),
            "hr_employee": SchemaLibrary.hr_employee_schema(),
            "iot_sensor": SchemaLibrary.iot_sensor_data_schema(),
            "social_media_post": SchemaLibrary.social_media_post_schema()
        }

    @staticmethod
    def custom_schema_from_json(json_schema: Dict) -> Dict:
        """
        Validate and return a custom schema from JSON

        Args:
            json_schema: A valid JSON Schema dictionary

        Returns:
            The validated schema
        """
        required_keys = ["type", "properties"]
        if not all(key in json_schema for key in required_keys):
            raise ValueError(f"Schema must contain: {required_keys}")

        if json_schema["type"] != "object":
            raise ValueError("Schema type must be 'object'")

        return json_schema

    @staticmethod
    def save_schema(schema: Dict, name: str, filepath: str = None) -> str:
        """
        Save a schema to a JSON file

        Args:
            schema: The schema to save
            name: Name for the schema
            filepath: Optional custom filepath

        Returns:
            The filepath where schema was saved
        """
        import os
        from datetime import datetime

        if filepath is None:
            os.makedirs("schemas", exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = f"schemas/{name}_{timestamp}.json"

        with open(filepath, 'w') as f:
            json.dump(schema, f, indent=2)

        return filepath

    @staticmethod
    def load_schema(filepath: str) -> Dict:
        """
        Load a schema from a JSON file

        Args:
            filepath: Path to the schema file

        Returns:
            The loaded schema
        """
        with open(filepath, 'r') as f:
            schema = json.load(f)

        return SchemaLibrary.custom_schema_from_json(schema)


def create_custom_schema_builder():
    """
    Interactive schema builder for creating custom schemas
    """

    class SchemaBuilder:
        def __init__(self):
            self.schema = {
                "type": "object",
                "title": "",
                "properties": {},
                "required": []
            }

        def set_title(self, title: str):
            """Set the schema title"""
            self.schema["title"] = title
            return self

        def add_string_field(self, name: str, **kwargs):
            """Add a string field"""
            field = {"type": "string"}
            if "pattern" in kwargs:
                field["pattern"] = kwargs["pattern"]
            if "maxLength" in kwargs:
                field["maxLength"] = kwargs["maxLength"]
            if "enum" in kwargs:
                field["enum"] = kwargs["enum"]

            self.schema["properties"][name] = field
            return self

        def add_number_field(self, name: str, **kwargs):
            """Add a number field"""
            field = {"type": "number"}
            if "minimum" in kwargs:
                field["minimum"] = kwargs["minimum"]
            if "maximum" in kwargs:
                field["maximum"] = kwargs["maximum"]
            if "decimalPlaces" in kwargs:
                field["decimalPlaces"] = kwargs["decimalPlaces"]

            self.schema["properties"][name] = field
            return self

        def add_integer_field(self, name: str, **kwargs):
            """Add an integer field"""
            field = {"type": "integer"}
            if "minimum" in kwargs:
                field["minimum"] = kwargs["minimum"]
            if "maximum" in kwargs:
                field["maximum"] = kwargs["maximum"]

            self.schema["properties"][name] = field
            return self

        def add_boolean_field(self, name: str):
            """Add a boolean field"""
            self.schema["properties"][name] = {"type": "boolean"}
            return self

        def add_array_field(self, name: str, item_type: str = "string"):
            """Add an array field"""
            self.schema["properties"][name] = {
                "type": "array",
                "items": {"type": item_type}
            }
            return self

        def add_object_field(self, name: str, properties: Dict):
            """Add a nested object field"""
            self.schema["properties"][name] = {
                "type": "object",
                "properties": properties
            }
            return self

        def add_date_field(self, name: str, start: str = None, end: str = None):
            """Add a date field"""
            field = {"type": "string"}
            if start:
                field["start"] = start
            if end:
                field["end"] = end

            self.schema["properties"][name] = field
            return self

        def set_required(self, *field_names):
            """Set required fields"""
            self.schema["required"] = list(field_names)
            return self

        def build(self) -> Dict:
            """Build and return the schema"""
            return self.schema

    return SchemaBuilder()