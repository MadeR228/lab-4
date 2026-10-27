"""Допоміжні функції ETL: E - Extract (read_data) і L - Load (write_data)."""
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from src.config import DATA_DIR, RESULTS_DIR


def read_data(spark_session: SparkSession, file_name: str, schema: T.StructType) -> DataFrame:
    """Універсальна функція для читання TSV-файлів IMDb.

    Обробляє заголовок, роздільник табуляції та замінює '\\N' (позначка відсутнього
    значення в IMDb) на NULL. Лапки вимкнено (quote=''), бо в назвах трапляються
    символи '"', а поля IMDb ніколи не беруться в лапки.
    """
    path = DATA_DIR + file_name
    df = spark_session.read.csv(path,
                                header=True,
                                sep='\t',
                                quote='',
                                nullValue='\\N',
                                schema=schema,
                                mode='PERMISSIVE')
    # Додатковий захист: якщо '\N' залишився в текстовому полі - замінюємо на NULL
    for field in schema.fields:
        if isinstance(field.dataType, T.StringType):
            df = df.withColumn(field.name,
                               F.when(F.col(field.name) == '\\N', None).otherwise(F.col(field.name)))
    return df


def write_data(df: DataFrame, task_name: str, single_file: bool = False) -> str:
    """Записує результат DataFrame у вигляді TSV-файлів (gzip) у каталог результатів.

    single_file=True об'єднує невеликі результати в один файл (coalesce(1)).
    """
    output_dir = RESULTS_DIR + task_name
    print(f'Запис результату у: {output_dir}')
    if single_file:
        df = df.coalesce(1)
    (df.write.csv(output_dir,
                  header=True,
                  sep='\t',
                  mode='overwrite',
                  compression='gzip'))
    return output_dir
