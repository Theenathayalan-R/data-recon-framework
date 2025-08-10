#!/bin/bash
# cleanup.sh - Script to clean unwanted files from the project

echo "🧹 Cleaning up unwanted files..."

# Remove Python cache files
find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null
find . -name "*.pyc" -delete 2>/dev/null
find . -name "*.pyo" -delete 2>/dev/null

# Remove build artifacts
rm -rf build/ dist/ *.egg-info/ .pytest_cache/ .tox/ .coverage

# Remove temporary files
rm -f temp_*.py test_local.py test_config.py requirements.txt
rm -f results.json reconciliation_results.json *_results.json
rm -f *.log

# Remove system files
find . -name ".DS_Store" -delete 2>/dev/null
find . -name "Thumbs.db" -delete 2>/dev/null

# Remove any misplaced config files
find src/ -name "*.json" -delete 2>/dev/null
find src/ -name "*.yaml" -delete 2>/dev/null
find src/ -name "*.yml" -delete 2>/dev/null

echo "✅ Cleanup complete!"

# Show current clean status
echo ""
echo "📊 Current project files:"
find . -type f | grep -v .venv | grep -v .git | grep -v __pycache__ | sort
