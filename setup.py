from setuptools import setup, find_packages

setup(
    name="driftdb",
    version="0.1.0",
    description="Workload-drift-aware query optimization for Spark SQL",
    packages=find_packages(exclude=["tests", "tests.*", "examples"]),
    python_requires=">=3.9",
    install_requires=[
        "py4j>=0.10.9",
        "river>=0.21",
        "scikit-learn>=1.3",
        "numpy>=1.24",
        "pyyaml>=6.0",
        "requests>=2.28",
    ],
    extras_require={
        "spark": ["pyspark>=3.4,<4.0"],
    },
    entry_points={
        "console_scripts": [
            "driftdb=driftdb.cli:main",
        ],
    },
)
