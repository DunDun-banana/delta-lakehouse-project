"""Deterministic synthetic ``trip_id`` shared by Bronze and the dirty fixture.

NYC TLC files have no trip identifier. ``trip_id`` is the SHA-256 of
``VendorID || PULocationID || DOLocationID || trip_distance || pickup ||
dropoff``. NULL values become empty strings so that a NULL in one position
cannot collide with a NULL in another (the separator keeps positions apart).

Changing this formula changes every ``trip_id`` and breaks the CDC MERGE key
of the existing Silver table.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"

_ID_COLUMNS = ("VendorID", "PULocationID", "DOLocationID", "trip_distance")
_SEPARATOR = "||"


def trip_id_expression() -> Column:
    """Return the SHA-256 ``trip_id`` expression."""

    pickup = F.date_format(F.to_timestamp("tpep_pickup_datetime"), TIMESTAMP_FORMAT)
    dropoff = F.date_format(F.to_timestamp("tpep_dropoff_datetime"), TIMESTAMP_FORMAT)
    identity = F.concat_ws(
        _SEPARATOR,
        *[F.coalesce(F.col(column).cast("string"), F.lit("")) for column in _ID_COLUMNS],
        F.coalesce(pickup, F.lit("")),
        F.coalesce(dropoff, F.lit("")),
    )
    return F.sha2(identity, 256)


def add_trip_id(df: DataFrame) -> DataFrame:
    """Add (or replace) the ``trip_id`` column."""

    return df.withColumn("trip_id", trip_id_expression())
