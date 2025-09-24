#!/usr/bin/env python3
"""
Test the improved Faker integration with coherent data generation
"""

from simple_generator import SchemaDataGenerator
import json

def test_coherent_data():
    """Test that generated data is coherent and realistic"""

    # Test schema with various fields
    test_schema = {
        "type": "object",
        "title": "Test Employee",
        "properties": {
            "employee_id": {"type": "string"},
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "email": {"type": "string"},
            "skills": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 3,
                "maxItems": 8
            },
            "certifications": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 1,
                "maxItems": 4
            },
            "courses": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 2,
                "maxItems": 5
            }
        }
    }

    generator = SchemaDataGenerator()
    records = generator.generate_from_schema(test_schema, 5)

    print("Coherent Data Generation Test")
    print("=" * 60)

    all_coherent = True

    for i, record in enumerate(records, 1):
        print(f"\nRecord {i}:")
        print("-" * 40)

        first_name = record.get('first_name', '')
        last_name = record.get('last_name', '')
        email = record.get('email', '')

        print(f"Name: {first_name} {last_name}")
        print(f"Email: {email}")

        # Check if email contains part of the name
        email_coherent = False
        if first_name and last_name and email:
            email_lower = email.lower()
            first_lower = first_name.lower()
            last_lower = last_name.lower()

            if first_lower in email_lower or last_lower in email_lower or first_lower[0] in email_lower:
                email_coherent = True
                print("  ✓ Email matches name")
            else:
                print("  ✗ Email doesn't match name")
                all_coherent = False

        # Check skills quality
        skills = record.get('skills', [])
        if skills:
            print(f"Skills ({len(skills)}):")
            for skill in skills[:5]:  # Show first 5
                print(f"  - {skill}")

            # Check if skills are realistic (not Lorem ipsum)
            realistic_skills = all(
                len(skill) < 30 and not skill.lower().startswith(('lorem', 'ipsum', 'dolor'))
                for skill in skills
            )
            if realistic_skills:
                print("  ✓ Skills are realistic")
            else:
                print("  ✗ Skills contain placeholder text")
                all_coherent = False

        # Check certifications quality
        certs = record.get('certifications', [])
        if certs:
            print(f"Certifications ({len(certs)}):")
            for cert in certs:
                print(f"  - {cert}")

            # Check if certifications are realistic
            realistic_certs = all(
                'AWS' in cert or 'Microsoft' in cert or 'Google' in cert or
                'CompTIA' in cert or 'Cisco' in cert or 'Oracle' in cert or
                'Scrum' in cert or 'PMP' in cert or 'ITIL' in cert or
                'Salesforce' in cert or 'Data' in cert or 'Kubernetes' in cert
                for cert in certs
            )
            if realistic_certs:
                print("  ✓ Certifications are realistic")
            else:
                print("  ✗ Certifications not realistic")
                all_coherent = False

        # Check courses
        courses = record.get('courses', [])
        if courses:
            print(f"Courses ({len(courses)}):")
            for course in courses[:3]:
                print(f"  - {course}")

    print("\n" + "=" * 60)
    if all_coherent:
        print("SUCCESS: All data is coherent and realistic!")
    else:
        print("PARTIAL: Some improvements needed for full coherence")

    return all_coherent

if __name__ == "__main__":
    success = test_coherent_data()
    exit(0 if success else 1)