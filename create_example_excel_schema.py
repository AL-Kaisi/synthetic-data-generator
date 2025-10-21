#!/usr/bin/env python3
"""
Create example Excel schema files with the 3-column format:
- column_name: The name of the column
- values: What values to generate (comma-separated for enums, or examples)
- description: Meaning/description of the column
"""

try:
    import pandas as pd
except ImportError:
    print("pandas is required. Install with: pip install pandas openpyxl")
    exit(1)


def create_employee_schema():
    """Create an employee schema with UK-specific fields including NINO"""

    data = {
        'column_name': [
            'employee_id',
            'first_name',
            'last_name',
            'email',
            'nino',
            'date_of_birth',
            'department',
            'job_title',
            'salary',
            'years_of_service',
            'is_active',
            'skills',
            'office_location'
        ],
        'values': [
            '',  # employee_id - will be auto-generated
            '',  # first_name - will be auto-generated
            '',  # last_name - will be auto-generated
            '',  # email - will be auto-generated
            '',  # nino - will be auto-generated as HMRC NINO
            '',  # date_of_birth - will be auto-generated
            'IT,HR,Finance,Marketing,Sales',  # department - enum values
            '',  # job_title - will be auto-generated
            '',  # salary - will be number between min/max
            '',  # years_of_service - will be integer
            '',  # is_active - boolean
            '',  # skills - array
            'London,Manchester,Birmingham,Edinburgh,Cardiff'  # office_location - enum
        ],
        'description': [
            'unique identifier for employee',
            'employee first name',
            'employee last name',
            'employee email address',
            'UK National Insurance Number',
            'date of birth',
            'department name',
            'job title or position',
            'annual salary amount in GBP',
            'number of years worked at company',
            'employment status - active or not',
            'list of employee skills',
            'office location'
        ]
    }

    df = pd.DataFrame(data)

    # Save to Excel
    output_file = 'custom_schemas/employee_schema_3col.xlsx'
    df.to_excel(output_file, index=False, sheet_name='Schema')
    print(f"✓ Created: {output_file}")
    print(f"  Columns: {list(df.columns)}")
    print(f"  Rows: {len(df)}")

    return output_file


def create_customer_schema():
    """Create a customer schema"""

    data = {
        'column_name': [
            'customer_id',
            'first_name',
            'last_name',
            'email',
            'phone',
            'address',
            'postcode',
            'city',
            'subscription_tier',
            'account_balance',
            'is_active',
            'registration_date'
        ],
        'values': [
            '',
            '',
            '',
            '',
            '',
            '',
            '',
            '',
            'Free,Basic,Premium,Enterprise',  # enum
            '',
            '',
            ''
        ],
        'description': [
            'unique customer identifier',
            'customer first name',
            'customer last name',
            'customer email address',
            'phone number',
            'street address',
            'UK postcode',
            'city name',
            'subscription tier level',
            'current account balance',
            'active status',
            'date of registration'
        ]
    }

    df = pd.DataFrame(data)

    output_file = 'custom_schemas/customer_schema_3col.xlsx'
    df.to_excel(output_file, index=False, sheet_name='Schema')
    print(f"✓ Created: {output_file}")
    print(f"  Columns: {list(df.columns)}")
    print(f"  Rows: {len(df)}")

    return output_file


def create_benefits_claimant_schema():
    """Create a UK benefits claimant schema"""

    data = {
        'column_name': [
            'claim_id',
            'nino',
            'first_name',
            'last_name',
            'date_of_birth',
            'postcode',
            'benefit_type',
            'claim_amount',
            'claim_start_date',
            'claim_status',
            'payment_frequency',
            'has_dependents',
            'number_of_children'
        ],
        'values': [
            '',
            '',  # NINO - will auto-generate HMRC compliant
            '',
            '',
            '',
            '',
            'Universal Credit,Child Benefit,Pension Credit,Jobseeker\'s Allowance,Employment Support Allowance',
            '',
            '',
            'Active,Pending,Suspended,Closed',
            'Weekly,Fortnightly,Monthly',
            '',
            ''
        ],
        'description': [
            'unique claim identifier',
            'National Insurance Number',
            'claimant first name',
            'claimant last name',
            'date of birth',
            'UK postcode',
            'type of benefit claimed',
            'monthly claim amount in GBP',
            'date claim started',
            'current claim status',
            'how often payments are made',
            'has dependent children',
            'number of dependent children'
        ]
    }

    df = pd.DataFrame(data)

    output_file = 'custom_schemas/benefits_claimant_3col.xlsx'
    df.to_excel(output_file, index=False, sheet_name='Schema')
    print(f"✓ Created: {output_file}")
    print(f"  Columns: {list(df.columns)}")
    print(f"  Rows: {len(df)}")

    return output_file


def main():
    """Create all example Excel schemas"""
    print("\n" + "="*70)
    print("Creating Example Excel Schema Files (3-column format)")
    print("="*70)
    print("\nFormat: column_name, values, description")
    print()

    files = []

    try:
        # Create employee schema
        print("1. Employee Schema (with NINO)")
        files.append(create_employee_schema())
        print()

        # Create customer schema
        print("2. Customer Schema")
        files.append(create_customer_schema())
        print()

        # Create benefits claimant schema
        print("3. Benefits Claimant Schema (UK-specific)")
        files.append(create_benefits_claimant_schema())
        print()

        print("="*70)
        print("✓ All Excel schema files created successfully!")
        print("="*70)
        print("\nCreated files:")
        for f in files:
            print(f"  - {f}")

        print("\nYou can now:")
        print("  1. Open these files in Excel to edit them")
        print("  2. Use them directly with the schema parser")
        print("  3. Generate synthetic data from them")

        print("\nExample usage:")
        print("  from schema_parser import SchemaParser")
        print("  parser = SchemaParser()")
        print(f"  schema = parser.parse_schema_file('{files[0]}')")

    except Exception as e:
        print(f"\n❌ Error creating Excel files: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
