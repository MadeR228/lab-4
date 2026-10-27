"""Налаштування ETL-конвеєра: шляхи до даних і параметри SparkSession."""
import os

# Корінь проєкту (у Docker-контейнері каталог проєкту змонтовано в /app)
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.environ.get('IMDB_DATA_DIR', os.path.join(PROJECT_DIR, 'imdb-data')) + os.sep
RESULTS_DIR = os.environ.get('IMDB_RESULTS_DIR', os.path.join(PROJECT_DIR, 'imdb-results')) + os.sep

# Параметри Spark для локального запуску на одній машині
SPARK_MASTER = 'local[*]'
APP_NAME = 'imdb-etl'
SPARK_CONF = {
    'spark.driver.memory': os.environ.get('SPARK_DRIVER_MEMORY', '6g'),
    'spark.sql.shuffle.partitions': '48',
    'spark.sql.adaptive.enabled': 'true',
    'spark.sql.session.timeZone': 'UTC',
}

# Мінімальна кількість голосів, щоб тайтл вважався «популярним» у завданнях 7 і 8
MIN_VOTES = 10_000
