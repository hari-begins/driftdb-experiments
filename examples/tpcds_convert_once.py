"""
examples/tpcds_convert_once.py
=================================
Converts the three TPC-DS tables DriftDB's evaluation harness needs
(store_sales, date_dim, item) from dsdgen's raw pipe-delimited .dat files
into Parquet, once, so every subsequent run of run_tpcds_demo.py can just
register them as external tables instead of re-parsing the raw files.

Usage:
    python3 examples/tpcds_convert_once.py \
        --data-dir /path/to/tpcds_data \
        --ddl /path/to/tpcds-kit/tools/tpcds.sql \
        --out /path/to/tpcds_parquet

See docs/tpcds_methodology.md for how --data-dir and --ddl are produced
(dsdgen's output directory, and tpcds-kit's own DDL file respectively).
"""
import argparse
import os
import sys
import time

os.environ.setdefault("PYSPARK_PYTHON", "python3")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from tpcds_loader import load_dat_table

TABLES = ["store_sales", "date_dim", "item"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True,
                         help="Directory dsdgen wrote its .dat files into.")
    parser.add_argument("--ddl", required=True,
                         help="Path to tpcds-kit/tools/tpcds.sql (the official DDL).")
    parser.add_argument("--out", required=True,
                         help="Output directory for the Parquet tables.")
    args = parser.parse_args()

    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[1]").appName("tpcds-convert")
             .config("spark.driver.memory", "2g")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    t0 = time.time()
    for table in TABLES:
        dat_path = os.path.join(args.data_dir, f"{table}.dat")
        df = load_dat_table(spark, args.ddl, table, dat_path)
        out_path = os.path.join(args.out, table)
        df.write.mode("overwrite").parquet(out_path)
        row_count = spark.read.parquet(out_path).count()
        print(f"{table}: {row_count} rows -> {out_path}")
    print(f"Total conversion time: {time.time() - t0:.1f}s")
    os._exit(0)


if __name__ == "__main__":
    main()
