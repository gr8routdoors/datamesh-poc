from setuptools import find_packages, setup

setup(
    name="core_data",
    packages=find_packages(exclude=["core_data_tests"]),
    install_requires=[
        "dbt-core",
        "dbt-duckdb",
        "dagster",
        "dagster-cloud",
        "dagster-duckdb",
        "dagster-duckdb-pandas",
        "dagster-dbt",
        "dagster-embedded-elt",         # DLT (the dependencies below came from dlt/requirements.txt)
        "dlt[duckdb,postgres]>=0.4.11", # DLT with destinations
        "sqlalchemy>=1.4",              # DLT SQL DB source
        "psycopg2-binary",              # Sqlalchemy driver for Postgres
    ],
    extras_require={
        "dev": [
            "dagster-webserver", # local Dagster
            "pytest",            # python tests
            # "localstack",        # lets us emulate AWS services (S3)
            # "awscli",            # AWS CLI
            # "awscli-local",      # awslocal CLI (support for localstack)
            # "pandas",            # data analysis libary
            # "pyarrow",           # parquet file viewer (used with pandas)
            # "duckdb"             # local DuckDB
        ]
    },
)
