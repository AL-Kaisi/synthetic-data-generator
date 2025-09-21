#!/usr/bin/env python3
"""
DWP-style schemas for testing data generation
"""

# Child Benefit Schema
child_benefit_schema = {
    "type": "object",
    "title": "ChildBenefitClaim",
    "properties": {
        "claim_id": {"type": "string", "description": "Unique claim identifier"},
        "nino": {"type": "string", "pattern": "^[A-Z]{2}[0-9]{6}[A-D]$", "description": "National Insurance Number"},
        "claimant_first_name": {"type": "string", "maxLength": 35},
        "claimant_last_name": {"type": "string", "maxLength": 35},
        "claimant_title": {"type": "string", "enum": ["Mr", "Mrs", "Miss", "Ms", "Dr", "Prof"]},
        "date_of_birth": {"type": "string", "start": "1940-01-01", "end": "1990-12-31"},
        "address_line_1": {"type": "string", "maxLength": 50},
        "address_line_2": {"type": "string", "maxLength": 50},
        "city": {"type": "string"},
        "postcode": {"type": "string", "pattern": "^[A-Z]{1,2}[0-9]{1,2} [0-9][A-Z]{2}$"},
        "email": {"type": "string"},
        "phone": {"type": "string"},
        "claim_start_date": {"type": "string", "start": "2020-01-01", "end": "2024-12-31"},
        "number_of_children": {"type": "integer", "minimum": 1, "maximum": 8},
        "eldest_child_premium": {"type": "boolean"},
        "weekly_amount": {"type": "number", "minimum": 24.00, "maximum": 150.00, "decimalPlaces": 2},
        "payment_frequency": {"type": "string", "enum": ["Weekly", "4-Weekly"]},
        "bank_sort_code": {"type": "string", "pattern": "^[0-9]{2}-[0-9]{2}-[0-9]{2}$"},
        "bank_account_number": {"type": "string", "pattern": "^[0-9]{8}$"},
        "claim_status": {"type": "string", "enum": ["Active", "Suspended", "Closed", "Under Review"]},
        "last_updated": {"type": "string"}
    },
    "required": ["claim_id", "nino", "claimant_first_name", "claimant_last_name", "claim_start_date"]
}

# State Pension Schema
state_pension_schema = {
    "type": "object",
    "title": "StatePensionRecord",
    "properties": {
        "pension_id": {"type": "string"},
        "nino": {"type": "string", "pattern": "^[A-Z]{2}[0-9]{6}[A-D]$"},
        "first_name": {"type": "string", "maxLength": 30},
        "last_name": {"type": "string", "maxLength": 30},
        "middle_names": {"type": "string", "maxLength": 50},
        "date_of_birth": {"type": "string", "start": "1930-01-01", "end": "1970-12-31"},
        "gender": {"type": "string", "enum": ["Male", "Female", "Other"]},
        "marital_status": {"type": "string", "enum": ["Single", "Married", "Divorced", "Widowed", "Civil Partnership"]},
        "state_pension_age": {"type": "integer", "minimum": 60, "maximum": 68},
        "qualifying_years": {"type": "integer", "minimum": 0, "maximum": 50},
        "pension_start_date": {"type": "string", "start": "2015-01-01", "end": "2024-12-31"},
        "weekly_pension_amount": {"type": "number", "minimum": 80.00, "maximum": 203.85, "decimalPlaces": 2},
        "annual_pension_amount": {"type": "number", "minimum": 4000.00, "maximum": 11000.00, "decimalPlaces": 2},
        "protected_payment": {"type": "number", "minimum": 0.00, "maximum": 50.00, "decimalPlaces": 2},
        "additional_pension": {"type": "number", "minimum": 0.00, "maximum": 200.00, "decimalPlaces": 2},
        "graduated_retirement_benefit": {"type": "number", "minimum": 0.00, "maximum": 15.00, "decimalPlaces": 2},
        "payment_method": {"type": "string", "enum": ["Bank Transfer", "Post Office Card", "Payable Order"]},
        "bank_sort_code": {"type": "string", "pattern": "^[0-9]{2}-[0-9]{2}-[0-9]{2}$"},
        "bank_account_number": {"type": "string", "pattern": "^[0-9]{8}$"},
        "payment_frequency": {"type": "string", "enum": ["Weekly", "4-Weekly", "13-Weekly"]},
        "pension_credit_entitlement": {"type": "boolean"},
        "housing_benefit_entitlement": {"type": "boolean"},
        "record_status": {"type": "string", "enum": ["Active", "Suspended", "Deferred", "Deceased"]},
        "last_review_date": {"type": "string"},
        "next_review_date": {"type": "string"}
    },
    "required": ["pension_id", "nino", "first_name", "last_name", "date_of_birth", "pension_start_date"]
}

# Universal Credit Schema
universal_credit_schema = {
    "type": "object",
    "title": "UniversalCreditClaim",
    "properties": {
        "uc_claim_id": {"type": "string"},
        "household_id": {"type": "string"},
        "lead_claimant_nino": {"type": "string", "pattern": "^[A-Z]{2}[0-9]{6}[A-D]$"},
        "partner_nino": {"type": "string", "pattern": "^[A-Z]{2}[0-9]{6}[A-D]$"},
        "lead_claimant_first_name": {"type": "string", "maxLength": 35},
        "lead_claimant_last_name": {"type": "string", "maxLength": 35},
        "partner_first_name": {"type": "string", "maxLength": 35},
        "partner_last_name": {"type": "string", "maxLength": 35},
        "claim_date": {"type": "string", "start": "2013-01-01", "end": "2024-12-31"},
        "household_type": {"type": "string", "enum": ["Single", "Couple", "Single Parent", "Couple with Children"]},
        "number_of_children": {"type": "integer", "minimum": 0, "maximum": 10},
        "housing_costs_element": {"type": "number", "minimum": 0.00, "maximum": 2000.00, "decimalPlaces": 2},
        "standard_allowance": {"type": "number", "minimum": 265.00, "maximum": 525.00, "decimalPlaces": 2},
        "child_element": {"type": "number", "minimum": 0.00, "maximum": 1500.00, "decimalPlaces": 2},
        "disability_element": {"type": "number", "minimum": 0.00, "maximum": 500.00, "decimalPlaces": 2},
        "carer_element": {"type": "number", "minimum": 0.00, "maximum": 200.00, "decimalPlaces": 2},
        "total_monthly_award": {"type": "number", "minimum": 0.00, "maximum": 4000.00, "decimalPlaces": 2},
        "earned_income": {"type": "number", "minimum": 0.00, "maximum": 3000.00, "decimalPlaces": 2},
        "unearned_income": {"type": "number", "minimum": 0.00, "maximum": 1000.00, "decimalPlaces": 2},
        "capital": {"type": "number", "minimum": 0.00, "maximum": 16000.00, "decimalPlaces": 2},
        "work_allowance": {"type": "number", "minimum": 0.00, "maximum": 650.00, "decimalPlaces": 2},
        "taper_rate": {"type": "number", "minimum": 55.0, "maximum": 65.0, "decimalPlaces": 1},
        "sanctions_applied": {"type": "boolean"},
        "sanction_amount": {"type": "number", "minimum": 0.00, "maximum": 500.00, "decimalPlaces": 2},
        "address_line_1": {"type": "string", "maxLength": 50},
        "address_line_2": {"type": "string", "maxLength": 50},
        "city": {"type": "string"},
        "postcode": {"type": "string", "pattern": "^[A-Z]{1,2}[0-9]{1,2} [0-9][A-Z]{2}$"},
        "claim_status": {"type": "string", "enum": ["Live", "Closed", "Suspended", "Disallowed"]},
        "assessment_period_start": {"type": "string"},
        "assessment_period_end": {"type": "string"},
        "payment_date": {"type": "string"},
        "created_timestamp": {"type": "string"}
    },
    "required": ["uc_claim_id", "household_id", "lead_claimant_nino", "claim_date", "household_type"]
}

# Employment Support Allowance Schema
esa_schema = {
    "type": "object",
    "title": "ESAClaim",
    "properties": {
        "esa_claim_id": {"type": "string"},
        "nino": {"type": "string", "pattern": "^[A-Z]{2}[0-9]{6}[A-D]$"},
        "claimant_first_name": {"type": "string", "maxLength": 35},
        "claimant_last_name": {"type": "string", "maxLength": 35},
        "date_of_birth": {"type": "string", "start": "1940-01-01", "end": "2000-12-31"},
        "claim_start_date": {"type": "string", "start": "2008-01-01", "end": "2024-12-31"},
        "esa_type": {"type": "string", "enum": ["New Style ESA", "Income-related ESA", "Contribution-based ESA"]},
        "assessment_phase_rate": {"type": "number", "minimum": 61.05, "maximum": 84.80, "decimalPlaces": 2},
        "main_phase_rate": {"type": "number", "minimum": 84.80, "maximum": 120.00, "decimalPlaces": 2},
        "support_component": {"type": "number", "minimum": 0.00, "maximum": 40.60, "decimalPlaces": 2},
        "work_component": {"type": "number", "minimum": 0.00, "maximum": 30.60, "decimalPlaces": 2},
        "medical_condition": {"type": "string", "enum": ["Mental Health", "Physical Disability", "Learning Disability", "Chronic Illness", "Multiple Conditions"]},
        "wca_score": {"type": "integer", "minimum": 0, "maximum": 50},
        "wca_decision": {"type": "string", "enum": ["Fit for Work", "Limited Capability for Work", "Limited Capability for Work and Work-Related Activity"]},
        "work_focused_interview_required": {"type": "boolean"},
        "mandation_status": {"type": "string", "enum": ["Mandated", "Volunteer", "Exempt"]},
        "prognosis": {"type": "string", "enum": ["Less than 6 months", "6-24 months", "More than 2 years", "Indefinite"]},
        "gp_name": {"type": "string", "maxLength": 50},
        "gp_address": {"type": "string", "maxLength": 100},
        "hospital_treatment": {"type": "boolean"},
        "fit_note_end_date": {"type": "string"},
        "next_medical_assessment": {"type": "string"},
        "claim_status": {"type": "string", "enum": ["Assessment Phase", "Main Phase", "Closed", "Suspended", "Appeal"]},
        "payment_amount": {"type": "number", "minimum": 0.00, "maximum": 150.00, "decimalPlaces": 2},
        "payment_frequency": {"type": "string", "enum": ["Weekly", "Fortnightly"]},
        "bank_details_verified": {"type": "boolean"},
        "last_contact_date": {"type": "string"},
        "created_by": {"type": "string", "maxLength": 20}
    },
    "required": ["esa_claim_id", "nino", "claimant_first_name", "claimant_last_name", "claim_start_date", "esa_type"]
}

# Personal Independence Payment Schema
pip_schema = {
    "type": "object",
    "title": "PIPClaim",
    "properties": {
        "pip_claim_id": {"type": "string"},
        "nino": {"type": "string", "pattern": "^[A-Z]{2}[0-9]{6}[A-D]$"},
        "claimant_first_name": {"type": "string", "maxLength": 35},
        "claimant_last_name": {"type": "string", "maxLength": 35},
        "date_of_birth": {"type": "string", "start": "1940-01-01", "end": "2005-12-31"},
        "claim_date": {"type": "string", "start": "2013-01-01", "end": "2024-12-31"},
        "disability_onset_date": {"type": "string", "start": "1980-01-01", "end": "2024-12-31"},
        "daily_living_component": {"type": "string", "enum": ["None", "Standard", "Enhanced"]},
        "mobility_component": {"type": "string", "enum": ["None", "Standard", "Enhanced"]},
        "daily_living_points": {"type": "integer", "minimum": 0, "maximum": 32},
        "mobility_points": {"type": "integer", "minimum": 0, "maximum": 24},
        "daily_living_rate": {"type": "number", "enum": [0.00, 68.10, 101.75], "decimalPlaces": 2},
        "mobility_rate": {"type": "number", "enum": [0.00, 26.90, 71.00], "decimalPlaces": 2},
        "total_weekly_amount": {"type": "number", "minimum": 0.00, "maximum": 172.75, "decimalPlaces": 2},
        "assessment_provider": {"type": "string", "enum": ["Capita", "Independent Assessment Services", "Atos Healthcare"]},
        "assessment_type": {"type": "string", "enum": ["Face to Face", "Paper Based", "Telephone", "Video Call"]},
        "assessment_date": {"type": "string"},
        "assessment_centre": {"type": "string", "maxLength": 50},
        "primary_condition": {"type": "string", "enum": ["Physical Disability", "Mental Health", "Sensory Impairment", "Neurological", "Cognitive"]},
        "secondary_conditions": {"type": "string", "maxLength": 200},
        "medications": {"type": "string", "maxLength": 300},
        "healthcare_professionals": {"type": "string", "maxLength": 200},
        "award_start_date": {"type": "string"},
        "award_end_date": {"type": "string"},
        "review_date": {"type": "string"},
        "light_touch_review": {"type": "boolean"},
        "ongoing_award": {"type": "boolean"},
        "claim_status": {"type": "string", "enum": ["New Claim", "Under Assessment", "Award Made", "Disallowed", "Under Review", "Appeal Lodged"]},
        "decision_maker": {"type": "string", "maxLength": 20},
        "quality_assurance_complete": {"type": "boolean"},
        "created_timestamp": {"type": "string"}
    },
    "required": ["pip_claim_id", "nino", "claimant_first_name", "claimant_last_name", "claim_date"]
}

# All schemas for easy access
dwp_schemas = {
    "child_benefit": child_benefit_schema,
    "state_pension": state_pension_schema,
    "universal_credit": universal_credit_schema,
    "esa": esa_schema,
    "pip": pip_schema
}