#!/usr/bin/env python3
"""
Test runner for BDD and unit tests
"""

import subprocess
import sys
import os
from pathlib import Path

def install_test_dependencies():
    """Install required test dependencies"""
    dependencies = [
        "pytest>=7.0.0",
        "pytest-bdd>=6.0.0",
        "pytest-html>=3.0.0",
        "pytest-cov>=4.0.0"
    ]

    print("Installing Installing test dependencies...")
    for dep in dependencies:
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", dep],
                         check=True, capture_output=True)
            print(f"Success Installed {dep}")
        except subprocess.CalledProcessError as e:
            print(f"Failed Failed to install {dep}: {e}")
            return False

    return True

def run_bdd_tests():
    """Run BDD tests with pytest"""
    print("\n🧪 Running BDD Tests...")
    print("="*50)

    cmd = [
        sys.executable, "-m", "pytest",
        "features/",
        "-v",
        "--tb=short",
        "--html=test_report.html",
        "--self-contained-html",
        "--cov=generators",
        "--cov=schema_processor",
        "--cov=schema_data_generator",
        "--cov-report=html:htmlcov",
        "--cov-report=term-missing"
    ]

    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode == 0
    except Exception as e:
        print(f"Failed Error running tests: {e}")
        return False

def run_unit_tests():
    """Run unit tests if they exist"""
    test_files = list(Path(".").glob("test_*.py"))
    if not test_files:
        print("Info  No unit test files found (test_*.py)")
        return True

    print("\nRunning Running Unit Tests...")
    print("="*50)

    cmd = [
        sys.executable, "-m", "pytest",
        "-v",
        "--tb=short"
    ] + [str(f) for f in test_files]

    try:
        result = subprocess.run(cmd, check=False)
        return result.returncode == 0
    except Exception as e:
        print(f"Failed Error running unit tests: {e}")
        return False

def run_safety_validation():
    """Run specific safety validation tests"""
    print("\nSafety Running Safety Validation...")
    print("="*50)

    # Create a quick safety test
    safety_test_code = '''
import sys
sys.path.insert(0, ".")
from schema_data_generator import SchemaDataGenerator
from dwp_schemas import dwp_schemas

def test_nino_safety():
    generator = SchemaDataGenerator(domain="dwp")

    # Generate sample NINOs
    test_data = generator.generate_from_schema(dwp_schemas["child_benefit"], 100)

    invalid_prefixes = {
        "BG", "GB", "NK", "KN", "TN", "NT", "ZZ",
        "AA", "AB", "AO", "FY", "NY", "OA", "PO", "OP"
    }

    nino_count = 0
    safe_count = 0

    for record in test_data:
        nino = record.get("nino")
        if nino:
            nino_count += 1
            prefix = nino[:2]
            if prefix in invalid_prefixes:
                safe_count += 1
            else:
                print(f"Warning  Potentially unsafe NINO: {nino}")

    print(f"Success Generated {nino_count} NINOs")
    print(f"Success {safe_count} NINOs use safe prefixes")
    print(f"Success Safety rate: {safe_count/nino_count*100:.1f}%")

    assert safe_count == nino_count, f"Found {nino_count - safe_count} potentially unsafe NINOs"
    print("Safety All NINOs are test-safe!")

if __name__ == "__main__":
    test_nino_safety()
'''

    # Write and run safety test
    with open("temp_safety_test.py", "w") as f:
        f.write(safety_test_code)

    try:
        result = subprocess.run([sys.executable, "temp_safety_test.py"],
                              check=False, capture_output=True, text=True)

        print(result.stdout)
        if result.stderr:
            print("Errors:", result.stderr)

        # Clean up
        os.remove("temp_safety_test.py")

        return result.returncode == 0
    except Exception as e:
        print(f"Failed Error running safety validation: {e}")
        return False

def main():
    """Main test runner"""
    print("Starting Data Generator Test Suite")
    print("="*50)

    # Check if we're in the right directory
    required_files = ["schema_data_generator.py", "dwp_schemas.py"]
    missing_files = [f for f in required_files if not os.path.exists(f)]

    if missing_files:
        print(f"Failed Missing required files: {missing_files}")
        print("Please run this from the project root directory")
        return False

    # Install dependencies
    if not install_test_dependencies():
        print("Failed Failed to install test dependencies")
        return False

    # Run tests
    results = []

    # Safety validation (most important)
    results.append(("Safety Validation", run_safety_validation()))

    # BDD tests
    results.append(("BDD Tests", run_bdd_tests()))

    # Unit tests
    results.append(("Unit Tests", run_unit_tests()))

    # Print results
    print("\n" + "="*50)
    print("TEST RESULTS SUMMARY")
    print("="*50)

    all_passed = True
    for test_name, passed in results:
        status = "Success PASSED" if passed else "Failed FAILED"
        print(f"{test_name:<20}: {status}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\nSuccess All tests passed!")
        print("\nReports Reports generated:")
        print("  • HTML Report: test_report.html")
        print("  • Coverage Report: htmlcov/index.html")
    else:
        print("\nError Some tests failed. Check the output above.")

    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)