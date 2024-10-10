import os
import pytest

def pytest_configure(config):
    repos = os.environ.get('SPARK_JARS_REPOSITORIES')
    if repos:
        config.addinivalue_line('spark_options', f"spark.jars.repositories: {repos}")
