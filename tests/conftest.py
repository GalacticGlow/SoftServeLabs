import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    session = SparkSession.getActiveSession()

    if session is None:
        session = SparkSession.builder.getOrCreate()

    yield session