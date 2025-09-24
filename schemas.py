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
    def data_pipeline_metadata_schema() -> Dict:
        """Data pipeline execution metadata schema for data engineering"""
        return {
            "type": "object",
            "title": "PipelineExecution",
            "properties": {
                "execution_id": {"type": "string"},
                "pipeline_name": {"type": "string"},
                "pipeline_version": {"type": "string", "pattern": "^v[0-9]+\\.[0-9]+\\.[0-9]+$"},
                "environment": {"type": "string", "enum": ["dev", "staging", "prod"]},
                "trigger_type": {"type": "string", "enum": ["scheduled", "manual", "event-driven", "api"]},
                "start_timestamp": {"type": "string"},
                "end_timestamp": {"type": "string"},
                "duration_seconds": {"type": "integer", "minimum": 0, "maximum": 86400},
                "status": {"type": "string", "enum": ["running", "succeeded", "failed", "cancelled", "retrying"]},
                "exit_code": {"type": "integer", "minimum": 0, "maximum": 255},
                "data_source": {"type": "string", "enum": ["s3", "kafka", "postgres", "mongodb", "api", "sftp", "gcs", "azure-blob"]},
                "data_sink": {"type": "string", "enum": ["redshift", "snowflake", "bigquery", "databricks", "elastic", "s3", "kafka"]},
                "records_processed": {"type": "integer", "minimum": 0, "maximum": 10000000},
                "records_failed": {"type": "integer", "minimum": 0, "maximum": 1000000},
                "bytes_processed": {"type": "integer", "minimum": 0, "maximum": 1000000000000},
                "cpu_usage_percent": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "memory_usage_mb": {"type": "integer", "minimum": 0, "maximum": 32000},
                "cost_usd": {"type": "number", "minimum": 0, "maximum": 10000, "decimalPlaces": 4},
                "data_quality_score": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "sla_met": {"type": "boolean"},
                "error_message": {"type": "string", "maxLength": 1000},
                "retry_count": {"type": "integer", "minimum": 0, "maximum": 5},
                "cluster_id": {"type": "string"},
                "spark_application_id": {"type": "string"},
                "lineage_upstream": {"type": "array", "items": {"type": "string"}},
                "lineage_downstream": {"type": "array", "items": {"type": "string"}},
                "tags": {"type": "array", "items": {"type": "string"}},
                "owner": {"type": "string"},
                "team": {"type": "string"}
            },
            "required": ["execution_id", "pipeline_name", "start_timestamp", "status", "records_processed"]
        }

    @staticmethod
    def ml_model_training_schema() -> Dict:
        """Machine learning model training metadata schema"""
        return {
            "type": "object",
            "title": "MLModelTraining",
            "properties": {
                "training_job_id": {"type": "string"},
                "model_name": {"type": "string"},
                "model_version": {"type": "string", "pattern": "^[0-9]+\\.[0-9]+$"},
                "algorithm": {"type": "string", "enum": ["random-forest", "xgboost", "neural-network", "svm", "linear-regression", "logistic-regression", "decision-tree"]},
                "framework": {"type": "string", "enum": ["scikit-learn", "tensorflow", "pytorch", "xgboost", "h2o", "spark-ml"]},
                "training_start": {"type": "string"},
                "training_end": {"type": "string"},
                "training_duration_minutes": {"type": "integer", "minimum": 1, "maximum": 10080},
                "dataset_size_rows": {"type": "integer", "minimum": 100, "maximum": 100000000},
                "dataset_size_gb": {"type": "number", "minimum": 0.001, "maximum": 10000, "decimalPlaces": 3},
                "feature_count": {"type": "integer", "minimum": 1, "maximum": 10000},
                "train_split": {"type": "number", "minimum": 0.5, "maximum": 0.9, "decimalPlaces": 2},
                "validation_split": {"type": "number", "minimum": 0.05, "maximum": 0.3, "decimalPlaces": 2},
                "test_split": {"type": "number", "minimum": 0.05, "maximum": 0.3, "decimalPlaces": 2},
                "hyperparameters": {"type": "object"},
                "cross_validation_folds": {"type": "integer", "minimum": 3, "maximum": 10},
                "accuracy": {"type": "number", "minimum": 0, "maximum": 1, "decimalPlaces": 4},
                "precision": {"type": "number", "minimum": 0, "maximum": 1, "decimalPlaces": 4},
                "recall": {"type": "number", "minimum": 0, "maximum": 1, "decimalPlaces": 4},
                "f1_score": {"type": "number", "minimum": 0, "maximum": 1, "decimalPlaces": 4},
                "auc_roc": {"type": "number", "minimum": 0, "maximum": 1, "decimalPlaces": 4},
                "rmse": {"type": "number", "minimum": 0, "maximum": 1000, "decimalPlaces": 4},
                "mae": {"type": "number", "minimum": 0, "maximum": 1000, "decimalPlaces": 4},
                "training_loss": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 6},
                "validation_loss": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 6},
                "epochs": {"type": "integer", "minimum": 1, "maximum": 1000},
                "early_stopping": {"type": "boolean"},
                "compute_instance_type": {"type": "string"},
                "gpu_count": {"type": "integer", "minimum": 0, "maximum": 8},
                "memory_peak_gb": {"type": "number", "minimum": 0.1, "maximum": 1000, "decimalPlaces": 2},
                "training_cost_usd": {"type": "number", "minimum": 0.01, "maximum": 10000, "decimalPlaces": 4},
                "model_size_mb": {"type": "number", "minimum": 0.1, "maximum": 10000, "decimalPlaces": 2},
                "feature_importance": {"type": "array", "items": {"type": "object"}},
                "confusion_matrix": {"type": "array", "items": {"type": "array"}},
                "experiment_id": {"type": "string"},
                "mlflow_run_id": {"type": "string"},
                "git_commit": {"type": "string", "pattern": "^[a-f0-9]{40}$"},
                "data_scientist": {"type": "string"},
                "production_ready": {"type": "boolean"}
            },
            "required": ["training_job_id", "model_name", "algorithm", "training_start", "accuracy"]
        }

    @staticmethod
    def distributed_system_metrics_schema() -> Dict:
        """Distributed system monitoring and metrics schema"""
        return {
            "type": "object",
            "title": "SystemMetrics",
            "properties": {
                "metric_id": {"type": "string"},
                "timestamp": {"type": "string"},
                "service_name": {"type": "string"},
                "service_version": {"type": "string", "pattern": "^[0-9]+\\.[0-9]+\\.[0-9]+$"},
                "instance_id": {"type": "string"},
                "cluster_name": {"type": "string"},
                "availability_zone": {"type": "string", "enum": ["us-east-1a", "us-east-1b", "us-west-2a", "us-west-2b", "eu-west-1a", "eu-west-1b"]},
                "environment": {"type": "string", "enum": ["dev", "staging", "prod"]},
                "cpu_utilization": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "memory_utilization": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "disk_utilization": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "network_in_mbps": {"type": "number", "minimum": 0, "maximum": 10000, "decimalPlaces": 2},
                "network_out_mbps": {"type": "number", "minimum": 0, "maximum": 10000, "decimalPlaces": 2},
                "requests_per_second": {"type": "integer", "minimum": 0, "maximum": 100000},
                "response_time_p50": {"type": "number", "minimum": 0, "maximum": 60000, "decimalPlaces": 2},
                "response_time_p95": {"type": "number", "minimum": 0, "maximum": 60000, "decimalPlaces": 2},
                "response_time_p99": {"type": "number", "minimum": 0, "maximum": 60000, "decimalPlaces": 2},
                "error_rate": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 4},
                "thread_count": {"type": "integer", "minimum": 1, "maximum": 10000},
                "connection_pool_active": {"type": "integer", "minimum": 0, "maximum": 1000},
                "connection_pool_idle": {"type": "integer", "minimum": 0, "maximum": 1000},
                "gc_time_ms": {"type": "number", "minimum": 0, "maximum": 10000, "decimalPlaces": 2},
                "heap_memory_mb": {"type": "number", "minimum": 0, "maximum": 32000, "decimalPlaces": 2},
                "cache_hit_rate": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "queue_depth": {"type": "integer", "minimum": 0, "maximum": 100000},
                "active_sessions": {"type": "integer", "minimum": 0, "maximum": 100000},
                "database_connections": {"type": "integer", "minimum": 0, "maximum": 1000},
                "circuit_breaker_state": {"type": "string", "enum": ["closed", "open", "half-open"]},
                "health_check_status": {"type": "string", "enum": ["healthy", "unhealthy", "degraded"]},
                "deployment_version": {"type": "string"},
                "alerts_fired": {"type": "array", "items": {"type": "string"}},
                "slo_compliance": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 4},
                "cost_per_hour": {"type": "number", "minimum": 0.001, "maximum": 1000, "decimalPlaces": 6}
            },
            "required": ["metric_id", "timestamp", "service_name", "instance_id", "cpu_utilization"]
        }

    @staticmethod
    def real_time_analytics_schema() -> Dict:
        """Real-time streaming analytics event schema"""
        return {
            "type": "object",
            "title": "StreamingEvent",
            "properties": {
                "event_id": {"type": "string"},
                "event_type": {"type": "string", "enum": ["click", "view", "purchase", "signup", "login", "logout", "search", "add_to_cart", "checkout"]},
                "timestamp": {"type": "string"},
                "user_id": {"type": "string"},
                "session_id": {"type": "string"},
                "device_id": {"type": "string"},
                "ip_address": {"type": "string", "pattern": "^(?:[0-9]{1,3}\\.){3}[0-9]{1,3}$"},
                "user_agent": {"type": "string"},
                "platform": {"type": "string", "enum": ["web", "mobile_ios", "mobile_android", "tablet", "desktop"]},
                "browser": {"type": "string", "enum": ["chrome", "firefox", "safari", "edge", "other"]},
                "country": {"type": "string"},
                "region": {"type": "string"},
                "city": {"type": "string"},
                "referrer": {"type": "string"},
                "utm_source": {"type": "string"},
                "utm_medium": {"type": "string"},
                "utm_campaign": {"type": "string"},
                "page_url": {"type": "string"},
                "page_title": {"type": "string"},
                "product_id": {"type": "string"},
                "category": {"type": "string"},
                "search_query": {"type": "string"},
                "search_results_count": {"type": "integer", "minimum": 0, "maximum": 10000},
                "scroll_depth": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "time_on_page": {"type": "integer", "minimum": 0, "maximum": 7200},
                "conversion_value": {"type": "number", "minimum": 0, "maximum": 100000, "decimalPlaces": 2},
                "ab_test_variant": {"type": "string"},
                "feature_flags": {"type": "array", "items": {"type": "string"}},
                "custom_properties": {"type": "object"},
                "kafka_partition": {"type": "integer", "minimum": 0, "maximum": 100},
                "kafka_offset": {"type": "integer", "minimum": 0},
                "processing_latency_ms": {"type": "number", "minimum": 0, "maximum": 10000, "decimalPlaces": 2},
                "event_schema_version": {"type": "string", "pattern": "^[0-9]+\\.[0-9]+$"},
                "enrichment_timestamp": {"type": "string"},
                "fraud_score": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "anomaly_detected": {"type": "boolean"},
                "data_quality_issues": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["event_id", "event_type", "timestamp", "user_id", "platform"]
        }

    @staticmethod
    def data_warehouse_table_schema() -> Dict:
        """Data warehouse table metadata and lineage schema"""
        return {
            "type": "object",
            "title": "DataWarehouseTable",
            "properties": {
                "table_id": {"type": "string"},
                "schema_name": {"type": "string"},
                "table_name": {"type": "string"},
                "table_type": {"type": "string", "enum": ["fact", "dimension", "bridge", "staging", "ods", "mart"]},
                "environment": {"type": "string", "enum": ["dev", "staging", "prod"]},
                "warehouse_platform": {"type": "string", "enum": ["snowflake", "redshift", "bigquery", "databricks", "synapse"]},
                "creation_timestamp": {"type": "string"},
                "last_modified": {"type": "string"},
                "last_refreshed": {"type": "string"},
                "row_count": {"type": "integer", "minimum": 0, "maximum": 10000000000},
                "size_gb": {"type": "number", "minimum": 0, "maximum": 100000, "decimalPlaces": 3},
                "column_count": {"type": "integer", "minimum": 1, "maximum": 1000},
                "data_freshness_hours": {"type": "number", "minimum": 0, "maximum": 168, "decimalPlaces": 2},
                "refresh_frequency": {"type": "string", "enum": ["real-time", "hourly", "daily", "weekly", "monthly", "on-demand"]},
                "sla_hours": {"type": "number", "minimum": 0.5, "maximum": 168, "decimalPlaces": 1},
                "sla_met": {"type": "boolean"},
                "data_quality_score": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "null_percentage": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "duplicate_percentage": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "uniqueness_score": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "completeness_score": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "consistency_score": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "source_systems": {"type": "array", "items": {"type": "string"}},
                "upstream_tables": {"type": "array", "items": {"type": "string"}},
                "downstream_tables": {"type": "array", "items": {"type": "string"}},
                "dependent_dashboards": {"type": "array", "items": {"type": "string"}},
                "dependent_reports": {"type": "array", "items": {"type": "string"}},
                "primary_keys": {"type": "array", "items": {"type": "string"}},
                "foreign_keys": {"type": "array", "items": {"type": "string"}},
                "partitioning_strategy": {"type": "string", "enum": ["date", "hash", "range", "none"]},
                "clustering_keys": {"type": "array", "items": {"type": "string"}},
                "compression_ratio": {"type": "number", "minimum": 1, "maximum": 20, "decimalPlaces": 2},
                "storage_cost_monthly": {"type": "number", "minimum": 0, "maximum": 100000, "decimalPlaces": 2},
                "compute_cost_monthly": {"type": "number", "minimum": 0, "maximum": 100000, "decimalPlaces": 2},
                "access_pattern": {"type": "string", "enum": ["high-frequency", "medium-frequency", "low-frequency", "archive"]},
                "business_owner": {"type": "string"},
                "technical_owner": {"type": "string"},
                "data_steward": {"type": "string"},
                "documentation_url": {"type": "string"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "sensitivity_level": {"type": "string", "enum": ["public", "internal", "confidential", "restricted"]},
                "retention_policy_days": {"type": "integer", "minimum": 30, "maximum": 2555}
            },
            "required": ["table_id", "schema_name", "table_name", "table_type", "warehouse_platform", "row_count"]
        }

    @staticmethod
    def cloud_infrastructure_schema() -> Dict:
        """Cloud infrastructure resource monitoring schema"""
        return {
            "type": "object",
            "title": "CloudResource",
            "properties": {
                "resource_id": {"type": "string"},
                "resource_type": {"type": "string", "enum": ["ec2", "rds", "s3", "lambda", "ecs", "eks", "redshift", "elasticsearch"]},
                "cloud_provider": {"type": "string", "enum": ["aws", "azure", "gcp"]},
                "region": {"type": "string"},
                "availability_zone": {"type": "string"},
                "environment": {"type": "string", "enum": ["dev", "staging", "prod"]},
                "timestamp": {"type": "string"},
                "resource_name": {"type": "string"},
                "instance_type": {"type": "string"},
                "instance_size": {"type": "string", "enum": ["nano", "micro", "small", "medium", "large", "xlarge", "2xlarge", "4xlarge", "8xlarge"]},
                "cpu_cores": {"type": "integer", "minimum": 1, "maximum": 96},
                "memory_gb": {"type": "integer", "minimum": 1, "maximum": 768},
                "storage_gb": {"type": "integer", "minimum": 8, "maximum": 64000},
                "network_performance": {"type": "string", "enum": ["low", "moderate", "high", "10-gigabit", "25-gigabit"]},
                "state": {"type": "string", "enum": ["running", "stopped", "terminated", "pending", "stopping"]},
                "uptime_hours": {"type": "number", "minimum": 0, "maximum": 8760, "decimalPlaces": 2},
                "cpu_utilization": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "memory_utilization": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "disk_utilization": {"type": "number", "minimum": 0, "maximum": 100, "decimalPlaces": 2},
                "network_in_gb": {"type": "number", "minimum": 0, "maximum": 10000, "decimalPlaces": 3},
                "network_out_gb": {"type": "number", "minimum": 0, "maximum": 10000, "decimalPlaces": 3},
                "iops": {"type": "integer", "minimum": 0, "maximum": 80000},
                "hourly_cost": {"type": "number", "minimum": 0.001, "maximum": 100, "decimalPlaces": 6},
                "monthly_cost": {"type": "number", "minimum": 0.01, "maximum": 72000, "decimalPlaces": 2},
                "reserved_instance": {"type": "boolean"},
                "spot_instance": {"type": "boolean"},
                "auto_scaling_group": {"type": "string"},
                "load_balancer": {"type": "string"},
                "security_groups": {"type": "array", "items": {"type": "string"}},
                "vpc_id": {"type": "string"},
                "subnet_id": {"type": "string"},
                "key_pair": {"type": "string"},
                "iam_role": {"type": "string"},
                "launch_time": {"type": "string"},
                "last_maintenance": {"type": "string"},
                "backup_enabled": {"type": "boolean"},
                "monitoring_enabled": {"type": "boolean"},
                "encryption_enabled": {"type": "boolean"},
                "compliance_standards": {"type": "array", "items": {"type": "string"}},
                "tags": {"type": "array", "items": {"type": "object"}},
                "owner": {"type": "string"},
                "team": {"type": "string"},
                "project": {"type": "string"},
                "cost_center": {"type": "string"}
            },
            "required": ["resource_id", "resource_type", "cloud_provider", "region", "timestamp", "state"]
        }

    @staticmethod
    def discover_custom_schemas() -> Dict[str, Dict]:
        """
        Automatically discover and load schema files from custom_schemas directory

        Returns:
            Dictionary of discovered schemas
        """
        import os
        import glob

        discovered_schemas = {}
        schema_directories = ["custom_schemas", "schemas"]  # Check both directories

        for schema_dir in schema_directories:
            if not os.path.exists(schema_dir):
                continue

            # Find all JSON files in the directory
            json_files = glob.glob(os.path.join(schema_dir, "*.json"))

            for json_file in json_files:
                try:
                    # Get schema name from filename (without extension)
                    schema_name = os.path.splitext(os.path.basename(json_file))[0]

                    # Skip if already exists (predefined schemas take precedence)
                    if schema_name in discovered_schemas:
                        continue

                    # Load and validate schema
                    schema = SchemaLibrary.load_schema(json_file)
                    discovered_schemas[schema_name] = schema

                except Exception as e:
                    # Silently skip invalid schema files
                    print(f"Warning: Could not load schema file {json_file}: {e}")
                    continue

        return discovered_schemas

    @staticmethod
    def get_all_schemas() -> Dict[str, Dict]:
        """Get all available schemas including auto-discovered ones"""
        # Start with predefined schemas
        schemas = {
            "ecommerce_product": SchemaLibrary.ecommerce_product_schema(),
            "healthcare_patient": SchemaLibrary.healthcare_patient_schema(),
            "financial_transaction": SchemaLibrary.financial_transaction_schema(),
            "education_student": SchemaLibrary.education_student_schema(),
            "hr_employee": SchemaLibrary.hr_employee_schema(),
            "iot_sensor": SchemaLibrary.iot_sensor_data_schema(),
            "social_media_post": SchemaLibrary.social_media_post_schema(),
            "data_pipeline_metadata": SchemaLibrary.data_pipeline_metadata_schema(),
            "ml_model_training": SchemaLibrary.ml_model_training_schema(),
            "distributed_system_metrics": SchemaLibrary.distributed_system_metrics_schema(),
            "real_time_analytics": SchemaLibrary.real_time_analytics_schema(),
            "data_warehouse_table": SchemaLibrary.data_warehouse_table_schema(),
            "cloud_infrastructure": SchemaLibrary.cloud_infrastructure_schema()
        }

        # Import DWP schemas if available
        try:
            from dwp_schemas import dwp_schemas
            schemas.update(dwp_schemas)
        except ImportError:
            pass

        # Auto-discover custom schema files
        custom_schemas = SchemaLibrary.discover_custom_schemas()
        schemas.update(custom_schemas)

        return schemas

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