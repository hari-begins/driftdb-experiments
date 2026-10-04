"""
Loads real dsdgen-generated TPC-DS .dat files into Spark as Parquet tables,
using column definitions parsed straight from the official tpcds-kit DDL
(tools/tpcds.sql) -- not hand-transcribed.
"""
import json
import re

from pyspark.sql.types import (StructType, StructField, IntegerType, LongType,
                                StringType, DateType, DecimalType, DoubleType)

_TYPE_RE = re.compile(r"(\w+)(\((\d+)(,(\d+))?\))?")


def _spark_type(ddl_type: str):
    m = _TYPE_RE.match(ddl_type)
    base = m.group(1).lower()
    if base == "integer":
        return IntegerType()
    if base in ("char", "varchar"):
        return StringType()
    if base == "date":
        return StringType()  # keep as string (YYYY-MM-DD); TPC-DS dates are simple ISO strings
    if base == "decimal":
        precision = int(m.group(3)) if m.group(3) else 10
        scale = int(m.group(5)) if m.group(5) else 0
        return DecimalType(precision, scale)
    if base == "time":
        return StringType()
    return StringType()


def load_schema(ddl_path: str, table: str) -> StructType:
    ddl = open(ddl_path).read()
    m = re.search(r"create table " + table + r"\s*\((.*?)\n\)\s*;", ddl, re.S)
    if not m:
        raise ValueError(f"table {table} not found in {ddl_path}")
    fields = []
    for line in m.group(1).splitlines():
        line = line.strip().rstrip(",")
        if not line or line.lower().startswith("primary key"):
            continue
        parts = line.split()
        colname, coltype = parts[0], parts[1]
        fields.append(StructField(colname, _spark_type(coltype), True))
    return StructType(fields)


def load_dat_table(spark, ddl_path: str, table: str, dat_path: str, sample_fraction=None):
    schema = load_schema(ddl_path, table)
    df = (spark.read
          .option("delimiter", "|")
          .schema(schema)
          .csv(dat_path))
    if sample_fraction is not None:
        df = df.sample(withReplacement=False, fraction=sample_fraction, seed=42)
    return df
