"""Створення SparkSession."""
from pyspark import SparkConf
from pyspark.sql import SparkSession

from src.config import APP_NAME, SPARK_CONF, SPARK_MASTER


def create_spark_session() -> SparkSession:
    """Ініціалізує SparkSession; master("local[*]") використовує всі доступні ядра."""
    conf = SparkConf().setAll(list(SPARK_CONF.items()))
    spark = (SparkSession.builder
             .master(SPARK_MASTER)
             .appName(APP_NAME)
             .config(conf=conf)
             .getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    return spark
