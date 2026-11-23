import pytest
from gold_rules import find_gold_problems
from pyspark.sql import SparkSession

COLUMNS = ["market_segment", "orders", "urgent_orders", "revenue"]
SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").getOrCreate()


def good_rows():
    return [(segment, 10, 2, 100.0) for segment in SEGMENTS]


def test_good_table_passes(spark):
    # Arrange
    gold = spark.createDataFrame(good_rows(), COLUMNS)

    # Act
    problems = find_gold_problems(gold)

    # Assert
    assert problems == []


def test_empty_table_fails(spark):
    # Arrange
    schema = "market_segment string, orders long, urgent_orders long, revenue double"
    gold = spark.createDataFrame([], schema)

    # Act
    problems = find_gold_problems(gold)

    # Assert
    assert problems == ["Table is empty"]


def test_missing_segment_fails(spark):
    # Arrange
    gold = spark.createDataFrame(good_rows()[:4], COLUMNS)

    # Act
    problems = find_gold_problems(gold)

    # Assert
    assert problems == ["Expected 5 market segments, found 4"]


def test_negative_revenue_fails(spark):
    # Arrange
    rows = good_rows()
    rows[0] = ("AUTOMOBILE", 10, 2, -5.0)
    gold = spark.createDataFrame(rows, COLUMNS)

    # Act
    problems = find_gold_problems(gold)

    # Assert
    assert problems == ["1 rows have revenue <= 0"]


def test_more_urgent_than_orders_fails(spark):
    # Arrange
    rows = good_rows()
    rows[0] = ("AUTOMOBILE", 3, 5, 100.0)
    gold = spark.createDataFrame(rows, COLUMNS)

    # Act
    problems = find_gold_problems(gold)

    # Assert
    assert problems == ["1 rows have more urgent orders than orders"]
