"""T - Transform: завдання 1-8 з трансформації даних IMDb."""
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

from src.config import MIN_VOTES
from src.etl_io import read_data
from src.schemas import (schema_akas, schema_basics, schema_episode, schema_names,
                         schema_principals, schema_ratings)


class Sources:
    """Ледаче завантаження джерел: кожен файл читається один раз і кешується."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._cache = {}

    def _get(self, key, file_name, schema, cache=False):
        if key not in self._cache:
            df = read_data(self.spark, file_name, schema)
            self._cache[key] = df.persist() if cache else df
        return self._cache[key]

    @property
    def akas(self):
        return self._get('akas', 'title.akas.tsv.gz', schema_akas, cache=True)

    @property
    def basics(self):
        return self._get('basics', 'title.basics.tsv.gz', schema_basics, cache=True)

    @property
    def names(self):
        return self._get('names', 'name.basics.tsv.gz', schema_names)

    @property
    def principals(self):
        return self._get('principals', 'title.principals.tsv.gz', schema_principals)

    @property
    def episode(self):
        return self._get('episode', 'title.episode.tsv.gz', schema_episode)

    @property
    def ratings(self):
        return self._get('ratings', 'title.ratings.tsv.gz', schema_ratings, cache=True)

    @property
    def original_titles(self):
        """Оригінальні назви тайтлів (isOriginalTitle = 1), один рядок на titleId."""
        if 'original' not in self._cache:
            w = Window.partitionBy('titleId').orderBy('ordering')
            df = (self.akas
                  .filter(F.col('isOriginalTitle') == 1)
                  .filter(F.col('title').isNotNull())
                  .withColumn('rn', F.row_number().over(w))
                  .filter(F.col('rn') == 1)
                  .select('titleId', 'title'))
            self._cache['original'] = df.persist()
        return self._cache['original']


def task1_ukrainian_titles(src: Sources) -> DataFrame:
    """Завдання 1. Назви фільмів/серіалів тощо, доступні українською (регіон 'UA')."""
    return (src.akas
            .where(F.col('region') == 'UA')
            .select(F.col('titleId'), F.col('title').alias('title_in_Ukrainian')))


def task2_born_19th_century(src: Sources) -> DataFrame:
    """Завдання 2. Імена людей, які народилися у XIX столітті (1801-1900)."""
    return (src.names
            .where(F.col('birthYear').between(1801, 1900))
            .select('primaryName', 'birthYear', 'deathYear', 'primaryProfession')
            .orderBy('birthYear', 'primaryName'))


def task3_long_movies(src: Sources) -> DataFrame:
    """Завдання 3. Назви всіх фільмів, які тривають більше 2 годин."""
    long_movies = (src.basics
                   .filter(F.col('titleType') == 'movie')
                   .filter(F.col('runtimeMinutes') > 120)
                   .select('tconst', 'runtimeMinutes'))
    return (long_movies
            .join(src.original_titles, long_movies['tconst'] == F.col('titleId'), 'inner')
            .select(F.col('title').alias('title_original'),
                    F.col('runtimeMinutes').alias('duration_in_minutes'))
            .orderBy(F.col('duration_in_minutes').desc(), 'title_original'))


def task4_actors_characters(src: Sources) -> DataFrame:
    """Завдання 4. Імена акторів/акторок, назви тайтлів і персонажі, яких вони зіграли."""
    characters = F.from_json(F.col('characters'), T.ArrayType(T.StringType()))
    cast = (src.principals
            .filter(F.col('category').isin('actor', 'actress'))
            .filter(F.col('characters').isNotNull())
            .select('tconst', 'nconst', 'category', F.array_join(characters, ', ').alias('character')))
    return (cast
            .join(src.names.select('nconst', 'primaryName'), on='nconst', how='inner')
            .join(src.original_titles, cast['tconst'] == F.col('titleId'), 'inner')
            .select(F.col('primaryName').alias('name'), F.col('title'), F.col('character'), 'category'))


def task5_adult_by_region(src: Sources) -> DataFrame:
    """Завдання 5. Кількість «дорослих» тайтлів за регіонами, топ-100."""
    adult = src.basics.filter(F.col('isAdult') == 1).select('tconst')
    return (src.akas
            .filter(F.col('region').isNotNull())
            .join(adult, src.akas['titleId'] == adult['tconst'], 'inner')
            .groupBy('region')
            .agg(F.countDistinct('titleId').alias('adult_titles'))
            .orderBy(F.col('adult_titles').desc(), 'region')
            .limit(100))


def task6_episodes_per_series(src: Sources) -> DataFrame:
    """Завдання 6. Кількість епізодів у кожному ТВ-серіалі, топ-50 (з віконною функцією)."""
    joined_series = (src.episode
                     .join(src.original_titles, src.episode['parentTconst'] == F.col('titleId'), 'inner')
                     .select('parentTconst', 'title', 'seasonNumber', 'episodeNumber'))
    window_spec = Window.partitionBy('parentTconst').orderBy(F.col('seasonNumber').desc_nulls_last(),
                                                             F.col('episodeNumber').desc_nulls_last())
    window_all = Window.partitionBy('parentTconst')
    per_series = (joined_series
                  .withColumn('episodes_count', F.count(F.lit(1)).over(window_all))
                  .withColumn('seasons_count', F.max('seasonNumber').over(window_all))
                  .withColumn('rank_by_episode', F.row_number().over(window_spec))
                  .filter(F.col('rank_by_episode') == 1)
                  .select('parentTconst', 'title', 'episodes_count', 'seasons_count'))
    # Топ-50 через orderBy + limit: глобальне вікно без partitionBy зібрало б усі дані в одну партицію
    return (per_series
            .orderBy(F.col('episodes_count').desc(), 'title')
            .limit(50))


def _popular_titles(src: Sources) -> DataFrame:
    """Фільми, ТВ-фільми і серіали (без епізодів і контенту 18+) з рейтингом і >= MIN_VOTES голосів."""
    return (src.basics
            .filter(F.col('titleType').isin('movie', 'tvMovie', 'tvSeries', 'tvMiniSeries'))
            .filter(F.col('isAdult') == 0)
            .join(src.ratings.filter(F.col('numVotes') >= MIN_VOTES), on='tconst', how='inner')
            .join(src.original_titles, F.col('tconst') == F.col('titleId'), 'inner'))


def task7_top10_by_decade(src: Sources) -> DataFrame:
    """Завдання 7. 10 найпопулярніших тайтлів за кожне десятиліття."""
    movies_info = (_popular_titles(src)
                   .withColumn('Decade', (F.floor(F.col('startYear') / 10) * 10).cast(T.IntegerType()))
                   .filter(F.col('Decade').isNotNull())
                   .select('title', 'titleType', 'startYear', 'Decade', 'averageRating', 'numVotes'))
    window_spec_decade = (Window.partitionBy('Decade')
                          .orderBy(F.col('averageRating').desc(), F.col('numVotes').desc()))
    return (movies_info
            .withColumn('rank_in_decade', F.row_number().over(window_spec_decade))
            .filter(F.col('rank_in_decade') <= 10)
            .orderBy(F.col('Decade').desc(), 'rank_in_decade'))


def task8_top10_by_genre(src: Sources) -> DataFrame:
    """Завдання 8. 10 найпопулярніших тайтлів за кожним жанром."""
    by_genre = (_popular_titles(src)
                .filter(F.col('genres').isNotNull())
                .withColumn('genre', F.explode(F.split(F.col('genres'), ',')))
                .select('genre', 'title', 'titleType', 'startYear', 'averageRating', 'numVotes'))
    window_spec_genre = (Window.partitionBy('genre')
                         .orderBy(F.col('averageRating').desc(), F.col('numVotes').desc()))
    return (by_genre
            .withColumn('rank_in_genre', F.row_number().over(window_spec_genre))
            .filter(F.col('rank_in_genre') <= 10)
            .orderBy('genre', 'rank_in_genre'))


TASKS = {
    1: ('task1_ukrainian_titles', task1_ukrainian_titles, False),
    2: ('task2_born_19th_century', task2_born_19th_century, True),
    3: ('task3_long_movies', task3_long_movies, True),
    4: ('task4_actors_characters', task4_actors_characters, False),
    5: ('task5_adult_by_region', task5_adult_by_region, True),
    6: ('task6_top50_series_by_episodes', task6_episodes_per_series, True),
    7: ('task7_top10_by_decade', task7_top10_by_decade, True),
    8: ('task8_top10_by_genre', task8_top10_by_genre, True),
}
