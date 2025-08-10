from setuptools import setup, find_packages

# Modern Python packaging uses pyproject.toml
# This setup.py is kept for legacy compatibility and to support editable installs
setup(
    name="recon_framework",  # Align with pyproject project.name
    version="1.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    entry_points={
        "console_scripts": [
            "recon-framework=reconciliation.framework:main",
        ],
    },
)
