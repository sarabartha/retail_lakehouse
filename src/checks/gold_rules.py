"""Table-level quality rules for the gold layer."""

from pyspark.sql import DataFrame

EXPECTED_SEGMENTS = 5


def find_gold_problems(gold: DataFrame) -> list[str]:
    """Check table-level rules on the gold revenue table.

    Args:
        gold: The gold_revenue_by_segment table.

    Returns:
        A list of problems found. An empty list means the table passed.
    """
    if gold.isEmpty():
        return ["Table is empty"]

    problems = []
    segments = gold.select("market_segment").distinct().count()
    if segments != EXPECTED_SEGMENTS:
        problems.append(f"Expected {EXPECTED_SEGMENTS} market segments, found {segments}")

    negative = gold.filter("revenue <= 0").count()
    if negative:
        problems.append(f"{negative} rows have revenue <= 0")

    impossible = gold.filter("urgent_orders > orders").count()
    if impossible:
        problems.append(f"{impossible} rows have more urgent orders than orders")

    return problems
