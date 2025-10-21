-- Employee table schema for data generation
-- This is a sample SQL DDL that will be parsed to generate test data

CREATE TABLE employees (
    employee_id INT NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) NOT NULL,
    nino VARCHAR(9),
    date_of_birth DATE,
    department ENUM('IT', 'HR', 'Finance', 'Marketing', 'Sales', 'Operations'),
    job_title VARCHAR(100),
    salary DECIMAL(10, 2),
    years_of_service INT,
    is_active BOOLEAN DEFAULT TRUE,
    skills TEXT,
    office_location VARCHAR(50),
    hire_date TIMESTAMP,
    PRIMARY KEY (employee_id)
);
