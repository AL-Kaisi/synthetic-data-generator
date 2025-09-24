#!/usr/bin/env python3
"""
Performance testing and benchmarking for data generation
"""

import time
import json
import psutil
import os
from datetime import datetime
from typing import Dict, List
from simple_generator import SchemaDataGenerator
from schemas import SchemaLibrary

class PerformanceTester:
    """Test data generation performance across different scenarios"""

    def __init__(self):
        self.generator = SchemaDataGenerator()
        self.results = []

    def measure_memory_usage(self):
        """Get current memory usage in MB"""
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024

    def test_schema_performance(self, schema_name: str, schema: Dict, record_counts: List[int]) -> Dict:
        """Test performance for a specific schema across different record counts"""
        schema_results = {
            "schema_name": schema_name,
            "schema_complexity": len(schema.get("properties", {})),
            "tests": []
        }

        print(f"\nTesting {schema_name} schema...")
        print(f"   Schema has {schema_results['schema_complexity']} fields")

        for count in record_counts:
            print(f"   Generating {count:,} records...", end=" ")

            # Measure memory before
            memory_before = self.measure_memory_usage()

            # Time the generation
            start_time = time.time()
            try:
                data = self.generator.generate_from_schema(schema, count)
                generation_time = time.time() - start_time

                # Time JSON-LD conversion
                jsonld_start = time.time()
                json_ld_data = self.generator.to_json_ld(data, schema)
                jsonld_time = time.time() - jsonld_start

                # Measure memory after
                memory_after = self.measure_memory_usage()
                memory_used = memory_after - memory_before

                # Calculate rates
                records_per_second = count / generation_time if generation_time > 0 else 0

                test_result = {
                    "record_count": count,
                    "generation_time_seconds": round(generation_time, 4),
                    "jsonld_conversion_time_seconds": round(jsonld_time, 4),
                    "total_time_seconds": round(generation_time + jsonld_time, 4),
                    "records_per_second": round(records_per_second, 2),
                    "memory_used_mb": round(memory_used, 2),
                    "data_size_mb": round(len(json.dumps(data)) / 1024 / 1024, 2),
                    "jsonld_size_mb": round(len(json.dumps(json_ld_data)) / 1024 / 1024, 2),
                    "success": True
                }

                print(f"SUCCESS {generation_time:.3f}s ({records_per_second:,.0f} rec/s)")

            except Exception as e:
                test_result = {
                    "record_count": count,
                    "error": str(e),
                    "success": False
                }
                print(f"FAILED: {e}")

            schema_results["tests"].append(test_result)

        return schema_results

    def run_comprehensive_benchmark(self) -> Dict:
        """Run comprehensive performance tests"""
        # Get all available schemas
        all_schemas = SchemaLibrary.get_all_schemas()

        print("Starting comprehensive performance benchmark...")
        print(f"Testing {len(all_schemas)} schemas")
        print(f"System: {psutil.cpu_count()} CPUs, {psutil.virtual_memory().total / 1024**3:.1f}GB RAM")

        # Test different record counts (matching DWP tool capabilities)
        record_counts = [100, 1000, 10000, 50000, 100000]

        benchmark_results = {
            "test_timestamp": datetime.now().isoformat(),
            "system_info": {
                "cpu_count": psutil.cpu_count(),
                "total_memory_gb": round(psutil.virtual_memory().total / 1024**3, 1),
                "python_version": os.sys.version,
                "platform": os.name
            },
            "schemas_tested": [],
            "summary": {}
        }

        # Test each schema
        for schema_name, schema in all_schemas.items():
            schema_results = self.test_schema_performance(schema_name, schema, record_counts)
            benchmark_results["schemas_tested"].append(schema_results)

        # Calculate summary statistics
        benchmark_results["summary"] = self.calculate_summary_stats(benchmark_results["schemas_tested"])

        return benchmark_results

    def calculate_summary_stats(self, schema_results: List[Dict]) -> Dict:
        """Calculate summary statistics across all tests"""
        all_successful_tests = []

        for schema_result in schema_results:
            for test in schema_result["tests"]:
                if test.get("success", False):
                    all_successful_tests.append(test)

        if not all_successful_tests:
            return {"error": "No successful tests to summarize"}

        # Calculate averages and extremes
        generation_times = [t["generation_time_seconds"] for t in all_successful_tests]
        records_per_second = [t["records_per_second"] for t in all_successful_tests]
        memory_usage = [t["memory_used_mb"] for t in all_successful_tests]

        summary = {
            "total_successful_tests": len(all_successful_tests),
            "average_generation_time_seconds": round(sum(generation_times) / len(generation_times), 4),
            "fastest_generation_time_seconds": round(min(generation_times), 4),
            "slowest_generation_time_seconds": round(max(generation_times), 4),
            "average_records_per_second": round(sum(records_per_second) / len(records_per_second), 2),
            "max_records_per_second": round(max(records_per_second), 2),
            "min_records_per_second": round(min(records_per_second), 2),
            "average_memory_usage_mb": round(sum(memory_usage) / len(memory_usage), 2),
            "max_memory_usage_mb": round(max(memory_usage), 2),
            "estimated_time_for_1_million_records": round((sum(generation_times) / len(generation_times)) * (1000000 / (sum([t["record_count"] for t in all_successful_tests]) / len(all_successful_tests))), 2)
        }

        return summary

    def save_results(self, results: Dict, filename: str = None) -> str:
        """Save benchmark results to file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"benchmark_results_{timestamp}.json"

        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)

        return filename

    def print_summary_report(self, results: Dict):
        """Print a formatted summary report"""
        print("\n" + "="*80)
        print("PERFORMANCE BENCHMARK SUMMARY")
        print("="*80)

        summary = results.get("summary", {})
        if "error" in summary:
            print(f"FAILED: {summary['error']}")
            return

        print(f"Total Tests: {summary['total_successful_tests']}")
        print(f"Average Speed: {summary['average_records_per_second']:,.0f} records/second")
        print(f"Max Speed: {summary['max_records_per_second']:,.0f} records/second")
        print(f"Min Speed: {summary['min_records_per_second']:,.0f} records/second")
        print(f"Average Memory: {summary['average_memory_usage_mb']:.1f} MB")
        print(f"Peak Memory: {summary['max_memory_usage_mb']:.1f} MB")
        print(f"Est. time for 1M records: {summary['estimated_time_for_1_million_records']:.1f} seconds")

        print(f"\nSchema Performance (for 1,000 records):")
        for schema_result in results["schemas_tested"]:
            test_1k = next((t for t in schema_result["tests"] if t["record_count"] == 1000 and t.get("success")), None)
            if test_1k:
                print(f"   {schema_result['schema_name']:<15}: {test_1k['records_per_second']:>6,.0f} rec/s  "
                      f"({test_1k['generation_time_seconds']:.3f}s)")

        print(f"\nProjections:")
        avg_speed = summary['average_records_per_second']
        print(f"   10K records: {10000/avg_speed:.1f} seconds")
        print(f"   100K records: {100000/avg_speed:.1f} seconds")
        print(f"   1M records: {1000000/avg_speed:.1f} seconds ({1000000/avg_speed/60:.1f} minutes)")
        print(f"   10M records: {10000000/avg_speed:.1f} seconds ({10000000/avg_speed/3600:.1f} hours)")

def main():
    """Run performance tests"""
    tester = PerformanceTester()

    # Run benchmark
    results = tester.run_comprehensive_benchmark()

    # Save results
    filename = tester.save_results(results)
    print(f"\nResults saved to: {filename}")

    # Print summary
    tester.print_summary_report(results)

if __name__ == "__main__":
    main()