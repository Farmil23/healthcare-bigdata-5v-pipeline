"""Satu tempat untuk membuat SparkSession (mode lokal)."""
import pandas as pd
from pyspark.sql import SparkSession

from config import SPARK_MASTER, SPARK_DRIVER_MEMORY


def get_spark(name: str = "CDS-5V", memory: str | None = None) -> SparkSession:
    spark = (
        SparkSession.builder.master(SPARK_MASTER)
        .appName(name)
        .config("spark.driver.memory", memory or SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def to_pandas(df) -> pd.DataFrame:
    """Spark DataFrame -> pandas (pengganti toPandas yang bermasalah di Python 3.12 / Windows)."""
    return pd.DataFrame(df.collect(), columns=df.columns)
