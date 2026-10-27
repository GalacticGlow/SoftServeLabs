import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = SparkSession.getActiveSession()

    if session is None:
        session = SparkSession.builder.getOrCreate()

    yield session