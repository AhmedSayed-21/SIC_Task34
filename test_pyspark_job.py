from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from pyspark_job import clean_data


def create_test_data(spark):
    data = [
        ("Ahmed", 100.0),
        ("Sara", 50.0),
        ("Omar", 200.0),
        ("Mona", 75.0),
        ("Ali", 0.0),
        ("Hassan", -50.0),
        (None, 120.0),
        ("Youssef", 300.0)
    ]

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True)
    ])

    return spark.createDataFrame(data, schema)

import pytest


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("PySpark CI Tests")
        .getOrCreate()
    )

    yield spark
    spark.stop()

def test_valid_records_are_kept(spark):
    df = create_test_data(spark)

    result = clean_data(df)

    assert result.count() == 5

def test_invalid_amounts_are_removed(spark):
    df = create_test_data(spark)

    result = clean_data(df)

    amounts = [row.amount for row in result.collect()]

    assert 0.0 not in amounts
    assert -50.0 not in amounts

def test_null_names_are_removed(spark):
    df = create_test_data(spark)

    result = clean_data(df)

    names = [row.name for row in result.collect()]

    assert None not in names

def test_amount_with_tax_is_calculated_correctly(spark):
    df = create_test_data(spark)

    result = clean_data(df)

    row = result.filter(result.name == "Ahmed").first()

    assert row.amount_with_tax == 120.0
 
