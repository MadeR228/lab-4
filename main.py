"""Точка входу ETL-конвеєра IMDb.

Приклади запуску (у Docker, див. run_spark.ps1):
    spark-submit main.py --tasks all
    spark-submit main.py --tasks 1,3,7
    spark-submit main.py --explore
"""
import argparse
import time

from src.explore import explore_datasets, test_dataframe
from src.spark_session import create_spark_session
from src.tasks import TASKS, Sources
from src.etl_io import write_data
from src.config import RESULTS_DIR


def parse_args():
    parser = argparse.ArgumentParser(description='ETL-конвеєр IMDb на PySpark')
    parser.add_argument('--tasks', default='all', help="номери завдань через кому або 'all'")
    parser.add_argument('--explore', action='store_true', help='аналіз наборів даних і тестовий DataFrame')
    return parser.parse_args()


def main():
    args = parse_args()
    spark = create_spark_session()
    print('Spark', spark.version, '| master:', spark.sparkContext.master,
          '| ядер:', spark.sparkContext.defaultParallelism)

    if args.explore:
        test_dataframe(spark)
        explore_datasets(spark)
        spark.stop()
        return

    numbers = sorted(TASKS) if args.tasks == 'all' else [int(n) for n in args.tasks.split(',')]
    src = Sources(spark)
    for n in numbers:
        name, transform, single_file = TASKS[n]
        print(f'\n===== Завдання {n}: {name} =====')
        start = time.time()
        result = transform(src)
        result.show(10, truncate=60)
        write_data(result, name, single_file=single_file)
        written = spark.read.csv(f'imdb-results/{name}', header=True, sep='\t').count()
        print(f'Рядків у результаті: {written:,};  час: {time.time() - start:.1f} с')
    spark.stop()


if __name__ == '__main__':
    main()
