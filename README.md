# IMDb ETL з Apache Spark

Лабораторна робота №4 з дисципліни «Методи та засоби інженерії даних та знань»
(Національний університет «Полтавська політехніка імені Юрія Кондратюка»).
ETL-конвеєр на **PySpark** для відкритих наборів даних **IMDb**: Extract (читання TSV з явними схемами),
Transform (фільтрація, join, агрегація, віконні функції), Load (запис результатів у TSV).

## Структура проєкту

```
imdb-etl/
├── main.py              # точка входу: запуск завдань 1-8 або аналізу даних
├── visualize.py         # графіки за результатами (pandas + matplotlib)
├── run_spark.ps1        # запуск у Docker-образі Apache Spark
├── requirements.txt
├── src/
│   ├── config.py        # шляхи до даних і параметри Spark
│   ├── spark_session.py # створення SparkSession
│   ├── schemas.py       # схеми StructType для 7 файлів IMDb
│   ├── etl_io.py        # read_data() / write_data()
│   ├── explore.py       # аналіз наборів даних, тестовий DataFrame
│   └── tasks.py         # трансформації: завдання 1-8
├── figures/             # візуалізація результатів
├── imdb-data/           # вхідні файли *.tsv.gz (не відстежуються git)
└── imdb-results/        # результати Spark (не відстежуються git)
```

## Дані

Сім файлів з офіційного джерела <https://datasets.imdbws.com/> (опис полів:
<https://developer.imdb.com/non-commercial-datasets/>) покласти в каталог `imdb-data/`:
`title.akas`, `title.basics`, `title.crew`, `title.episode`, `title.principals`, `title.ratings`,
`name.basics` (`*.tsv.gz`, разом ≈ 2 ГБ).

## Запуск

Spark запускається в офіційному Docker-образі `spark:4.0.1-python3` (Java 17, Python 3.10), тому на
Windows не потрібні окремі Java, Hadoop і winutils:

```powershell
docker pull spark:4.0.1-python3
.\run_spark.ps1                 # усі завдання
.\run_spark.ps1 -Tasks 1,7      # окремі завдання
.\run_spark.ps1 -Explore        # аналіз наборів даних і тестовий DataFrame
```

Без Docker (потрібна Java 17+): `pip install -r requirements.txt`, потім `spark-submit main.py --tasks all`.

Графіки: `python visualize.py` (після виконання завдань).

## Завдання

| № | Завдання | Файли |
|---|----------|-------|
| 1 | Назви тайтлів, доступні українською (регіон `UA`) | title.akas |
| 2 | Люди, народжені у XIX столітті (1801–1900) | name.basics |
| 3 | Фільми тривалістю понад 2 години | title.basics, title.akas |
| 4 | Актори/акторки, тайтли і персонажі | title.principals, name.basics, title.akas |
| 5 | Кількість adult-тайтлів за регіонами, топ-100 | title.basics, title.akas |
| 6 | Кількість епізодів у серіалах, топ-50 | title.episode, title.akas |
| 7 | 10 найпопулярніших тайтлів кожного десятиліття | title.basics, title.ratings, title.akas |
| 8 | 10 найпопулярніших тайтлів кожного жанру | title.basics, title.ratings, title.akas |

## Робочий процес Git

Гілки `main` (стабільна версія) і `develop` (інтеграція). Кожна частина роботи виконувалася в окремій
feature-гілці від `develop` і вливалася через pull request; `develop` вливається в `main` релізним PR.
