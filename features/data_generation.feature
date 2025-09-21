Feature: Synthetic Data Generation
    As a DWP data analyst
    I want to generate synthetic test data based on schemas
    So that I can test applications without using real personal data

    Background:
        Given I have a data generator with DWP safety rules

    Scenario: Generate safe NINO numbers
        Given I have a schema with a NINO field
        When I generate 100 records
        Then all NINO numbers should use invalid prefixes
        And all NINO numbers should follow the correct format
        And no NINO numbers should be potentially real

    Scenario: Generate data from Child Benefit schema
        Given I have the Child Benefit schema
        When I generate 50 records
        Then each record should have all required fields
        And the claim_id field should be unique
        And the weekly_amount should be between 24.00 and 150.00
        And the payment_frequency should be either "Weekly" or "4-Weekly"

    Scenario: Handle missing optional fields
        Given I have a schema with optional fields
        When I generate 100 records
        Then some records should be missing optional fields
        And all records should have required fields
        And no records should have invalid data

    Scenario: Apply business rules for pension data
        Given I have a State Pension schema
        When I generate records for pensioners
        Then all ages should be above state pension age
        And pension amounts should be realistic
        And all dates should be logically consistent

    Scenario: Generate data with custom constraints
        Given I have a schema with numeric constraints
        When I generate data with minimum 100 and maximum 1000
        Then all numeric values should be within the specified range
        And values should follow the specified decimal places

    Scenario: Convert data to JSON-LD format
        Given I have generated synthetic data
        When I convert it to JSON-LD format
        Then the output should have proper @context
        And each record should have @type and @id
        And the data should be semantically structured

    Scenario: Performance requirements
        Given I have a complex schema with 25 fields
        When I generate 1000 records
        Then the generation should complete in under 1 second
        And memory usage should be reasonable
        And the output should be valid JSON

    Scenario: Error handling for invalid schemas
        Given I have an invalid schema
        When I attempt to generate data
        Then I should receive a descriptive error message
        And the system should not crash
        And no partial data should be generated

    Scenario: Domain-specific factory selection
        Given I specify the DWP domain
        When I create a data generator
        Then it should use DWP-specific generators
        And apply DWP business rules
        And ensure NINO safety validation

    Scenario: Chain of responsibility processing
        Given I have a field that needs multiple processing steps
        When the field is processed through the chain
        Then type processing should occur first
        And constraint processing should occur second
        And semantic processing should occur third
        And business rule processing should occur last