#!/usr/bin/env python3
"""
Test script to verify Faker integration
"""

from simple_generator import SchemaDataGenerator
import json

def test_faker_fields():
    """Test that Faker is generating better quality data"""

    # Create a test schema with various field types
    test_schema = {
        "type": "object",
        "title": "Faker Test Schema",
        "properties": {
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "email": {"type": "string"},
            "phone": {"type": "string"},
            "address": {"type": "string"},
            "city": {"type": "string"},
            "postcode": {"type": "string"},
            "company": {"type": "string"},
            "job_title": {"type": "string"},
            "website_url": {"type": "string"},
            "username": {"type": "string"},
            "description": {"type": "string", "maxLength": 100},
            "birth_date": {"type": "string"},
            "timestamp": {"type": "string"},
            "department": {"type": "string"},
            "status": {"type": "string"},
            "ip_address": {"type": "string"},
            "color": {"type": "string"},
            "currency": {"type": "string"},
            "amount": {"type": "number", "minimum": 10, "maximum": 1000},
            "quantity": {"type": "integer", "minimum": 1, "maximum": 100},
            "is_active": {"type": "boolean"}
        }
    }

    # Generate sample data
    generator = SchemaDataGenerator()
    data = generator.generate_from_schema(test_schema, 5)

    print("Faker Integration Test Results")
    print("=" * 50)

    # Display sample records
    for i, record in enumerate(data, 1):
        print(f"\nRecord {i}:")
        print("-" * 30)
        for key, value in record.items():
            # Truncate long values for display
            display_value = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
            print(f"  {key:15}: {display_value}")

    # Verify data quality
    print("\nData Quality Check:")
    print("-" * 30)

    quality_checks = {
        "Unique emails": len(set(r.get("email", "") for r in data)) == len([r for r in data if "email" in r]),
        "Valid phone format": all("+" in str(r.get("phone", "+")) or "-" in str(r.get("phone", "-")) for r in data if "phone" in r),
        "Real-looking names": all(r.get("first_name", "").isalpha() for r in data if "first_name" in r),
        "Proper URLs": all(r.get("website_url", "").startswith("http") for r in data if "website_url" in r),
        "Valid dates": all("-" in str(r.get("birth_date", "-")) for r in data if "birth_date" in r),
        "Boolean values": all(isinstance(r.get("is_active"), bool) for r in data if "is_active" in r)
    }

    for check, result in quality_checks.items():
        status = "PASSED" if result else "FAILED"
        print(f"  {check:25}: {status}")

    # Test CSV export with Faker data
    print("\nCSV Export Test:")
    print("-" * 30)
    csv_data = generator.to_csv(data[:2])
    print("First 200 chars of CSV:")
    print(csv_data[:200] + "...")

    print("\nFaker integration successful!")
    return all(quality_checks.values())

if __name__ == "__main__":
    success = test_faker_fields()
    exit(0 if success else 1)