"""Аналіз наборів даних IMDb: схеми, кількість рядків, приклади записів, тестовий DataFrame."""
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession

from src.etl_io import read_data
from src.schemas import FILES


def test_dataframe(spark: SparkSession) -> None:
    """Тестовий DataFrame з трьох рядків для перевірки роботи Spark."""
    schema = T.StructType([
        T.StructField('title', T.StringType(), False),
        T.StructField('titleType', T.StringType(), True),
        T.StructField('startYear', T.IntegerType(), True),
        T.StructField('episodes', T.IntegerType(), True),
    ])
    df = spark.createDataFrame([('Friends', 'tvSeries', 1994, 236),
                                ('The Office', 'tvSeries', 2005, 188),
                                ('Inception', 'movie', 2010, None)], schema)
    df.printSchema()
    df.show()


def explore_datasets(spark: SparkSession) -> None:
    """Для кожного файлу: кількість рядків і стовпців, частка NULL у ключових полях, 3 записи."""
    for file_name, schema in FILES.items():
        df = read_data(spark, file_name, schema)
        rows = df.count()
        nulls = df.select([F.round(F.avg(F.col(c).isNull().cast('int')) * 100, 1).alias(c)
                           for c in df.columns]).first().asDict()
        print(f'\n=== {file_name}: {rows:,} рядків, {len(df.columns)} стовпців ===')
        print('Типи:', ', '.join(f'{f.name}:{f.dataType.simpleString()}' for f in schema.fields))
        print('NULL, %:', ', '.join(f'{k}={v}' for k, v in nulls.items() if v))
        df.show(3, truncate=40)
