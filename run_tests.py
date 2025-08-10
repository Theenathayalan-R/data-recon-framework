#!/usr/bin/env python3
"""
Test runner script for the Data Reconciliation Framework
"""

import subprocess
import sys
import os

def run_command(cmd, description):
    """Run a command and report results."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {cmd}")
    print(f"{'='*60}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"✅ {description} - PASSED")
        if result.stdout.strip():
            print(result.stdout)
    else:
        print(f"❌ {description} - FAILED")
        if result.stderr.strip():
            print("STDERR:", result.stderr)
        if result.stdout.strip():
            print("STDOUT:", result.stdout)
    
    return result.returncode == 0

def main():
    """Main test runner."""
    print("🚀 Data Reconciliation Framework - Test Suite")
    print("=" * 60)
    
    # Get project root directory (where this script is located)
    project_root = os.path.dirname(os.path.abspath(__file__))
    os.chdir(project_root)
    
    # Set Python path and find Python executable
    src_path = os.path.join(project_root, "src")
    env_cmd = f"PYTHONPATH={src_path}"
    
    # Try to find virtual environment Python, fallback to system Python
    venv_python = os.path.join(project_root, ".venv", "bin", "python")
    if os.path.exists(venv_python):
        python_cmd = venv_python
    else:
        python_cmd = sys.executable
    
    tests_passed = []
    
    # 1. Run pytest
    cmd = f"{env_cmd} {python_cmd} -m pytest tests/ -v --tb=short"
    tests_passed.append(run_command(cmd, "Unit Tests (pytest)"))
    
    # 2. Run local JSON reconciliation test
    local_test_script = '''
import sys
import os

# Add src to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(project_root, "src"))

from reconciliation import ReconciliationFramework
framework = ReconciliationFramework("config/json_recon_config.yaml", connection_type="json")
report = framework.run_reconciliation()
print("Status:", report.overall_status)
print("Records:", str(report.record_count_comparison.source_count), "vs", str(report.record_count_comparison.target_count))
print("Local JSON reconciliation test completed")
'''
    
    # Write to temporary file
    with open("temp_local_test.py", "w") as f:
        f.write(local_test_script)
    
    cmd = f'{env_cmd} {python_cmd} temp_local_test.py'
    tests_passed.append(run_command(cmd, "Local JSON Reconciliation Test"))
    
    # Cleanup
    if os.path.exists("temp_local_test.py"):
        os.remove("temp_local_test.py")
    
    # 3. Configuration validation test
    config_test_script = '''
import sys
import os

# Add src to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(project_root, "src"))

from reconciliation.config import Config
config = Config("config/json_recon_config.yaml")
print("Config loaded with", len(config.column_mappings), "mappings")
print("Key fields:", config.key_fields)
print("Threshold:", config.record_count_threshold)
print("Config validation test completed")
'''
    
    # Write to temporary file
    with open("temp_config_test.py", "w") as f:
        f.write(config_test_script)
    
    cmd = f'{env_cmd} {python_cmd} temp_config_test.py'
    tests_passed.append(run_command(cmd, "Configuration Validation Test"))
    
    # Cleanup
    if os.path.exists("temp_config_test.py"):
        os.remove("temp_config_test.py")
    
    # Summary
    print(f"\n{'='*60}")
    print("📊 TEST SUMMARY")
    print(f"{'='*60}")
    
    total_tests = len(tests_passed)
    passed_tests = sum(tests_passed)
    
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {total_tests - passed_tests}")
    
    if all(tests_passed):
        print("\n🎉 ALL TESTS PASSED! Framework is ready for use.")
        return 0
    else:
        print("\n❌ Some tests failed. Please check the output above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
