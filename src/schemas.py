"""Схеми (StructType) семи наборів даних IMDb.

Опис полів: https://developer.imdb.com/non-commercial-datasets/
Поля-списки (genres, primaryProfession, knownForTitles, directors, writers)
зберігаються в IMDb як рядки зі значеннями через кому, тому мають тип StringType.
"""
import pyspark.sql.types as T

schema_akas = T.StructType([
    T.StructField('titleId', T.StringType(), False),
    T.StructField('ordering', T.IntegerType(), False),
    T.StructField('title', T.StringType(), True),
    T.StructField('region', T.StringType(), True),
    T.StructField('language', T.StringType(), True),
    T.StructField('types', T.StringType(), True),
    T.StructField('attributes', T.StringType(), True),
    T.StructField('isOriginalTitle', T.IntegerType(), True),
])

schema_basics = T.StructType([
    T.StructField('tconst', T.StringType(), False),
    T.StructField('titleType', T.StringType(), True),
    T.StructField('primaryTitle', T.StringType(), True),
    T.StructField('originalTitle', T.StringType(), True),
    T.StructField('isAdult', T.IntegerType(), True),
    T.StructField('startYear', T.IntegerType(), True),
    T.StructField('endYear', T.IntegerType(), True),
    T.StructField('runtimeMinutes', T.IntegerType(), True),
    T.StructField('genres', T.StringType(), True),
])

schema_names = T.StructType([
    T.StructField('nconst', T.StringType(), False),
    T.StructField('primaryName', T.StringType(), True),
    T.StructField('birthYear', T.IntegerType(), True),
    T.StructField('deathYear', T.IntegerType(), True),
    T.StructField('primaryProfession', T.StringType(), True),
    T.StructField('knownForTitles', T.StringType(), True),
])

schema_principals = T.StructType([
    T.StructField('tconst', T.StringType(), False),
    T.StructField('ordering', T.IntegerType(), False),
    T.StructField('nconst', T.StringType(), False),
    T.StructField('category', T.StringType(), True),
    T.StructField('job', T.StringType(), True),
    T.StructField('characters', T.StringType(), True),
])

schema_episode = T.StructType([
    T.StructField('tconst', T.StringType(), False),
    T.StructField('parentTconst', T.StringType(), False),
    T.StructField('seasonNumber', T.IntegerType(), True),
    T.StructField('episodeNumber', T.IntegerType(), True),
])

schema_crew = T.StructType([
    T.StructField('tconst', T.StringType(), False),
    T.StructField('directors', T.StringType(), True),
    T.StructField('writers', T.StringType(), True),
])

schema_ratings = T.StructType([
    T.StructField('tconst', T.StringType(), False),
    T.StructField('averageRating', T.DoubleType(), True),
    T.StructField('numVotes', T.IntegerType(), True),
])

FILES = {
    'title.akas.tsv.gz': schema_akas,
    'title.basics.tsv.gz': schema_basics,
    'name.basics.tsv.gz': schema_names,
    'title.principals.tsv.gz': schema_principals,
    'title.episode.tsv.gz': schema_episode,
    'title.crew.tsv.gz': schema_crew,
    'title.ratings.tsv.gz': schema_ratings,
}
