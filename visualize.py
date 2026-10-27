"""Візуалізація результатів ETL-конвеєра (запускається поза Spark: pandas + matplotlib).

Читає TSV-файли (gzip) з imdb-results/ і зберігає графіки в figures/.
"""
import glob
import os

import matplotlib.pyplot as plt
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(BASE, 'imdb-results')
FIGURES = os.path.join(BASE, 'figures')
os.makedirs(FIGURES, exist_ok=True)
plt.rcParams['font.family'] = 'DejaVu Sans'


def load(task: str, usecols=None) -> pd.DataFrame:
    """Читає всі part-файли результату завдання в один DataFrame pandas."""
    parts = sorted(glob.glob(os.path.join(RESULTS, task, 'part-*.csv.gz')))
    return pd.concat((pd.read_csv(p, sep='\t', usecols=usecols, quoting=3, keep_default_na=False,
                                  na_values=['']) for p in parts), ignore_index=True)


def save(fig, name):
    fig.tight_layout()
    fig.savefig(os.path.join(FIGURES, name), dpi=120)
    plt.close(fig)
    print('figures/' + name)


def short(text, n=38):
    return text if len(text) <= n else text[:n - 1] + '…'


# Завдання 1: назви українською - кількість і найчастіші назви
t1 = load('task1_ukrainian_titles')
print(f'Завдання 1: {len(t1):,} назв українською, унікальних тайтлів {t1.titleId.nunique():,}')

# Завдання 2: народжені у XIX столітті - розподіл за десятиліттями
t2 = load('task2_born_19th_century', usecols=['primaryName', 'birthYear'])
fig, ax = plt.subplots(figsize=(9, 3.6))
counts = (t2.birthYear // 10 * 10).value_counts().sort_index()
ax.bar(counts.index.astype(str) + 's', counts.values, color='#1f4e79')
ax.set_title(f'Завдання 2. Люди, народжені у 1801-1900 рр., за десятиліттями (усього {len(t2):,})')
ax.set_ylabel('кількість осіб')
ax.tick_params(axis='x', rotation=45)
save(fig, 'task2_birth_decades.png')

# Завдання 3: фільми довші за 2 години - розподіл тривалості
t3 = load('task3_long_movies')
fig, ax = plt.subplots(figsize=(9, 3.6))
ax.hist(t3.duration_in_minutes.clip(upper=300), bins=36, color='#e07b39', edgecolor='white')
ax.set_title(f'Завдання 3. Тривалість фільмів понад 120 хв (усього {len(t3):,}; >300 хв об\'єднано)')
ax.set_xlabel('тривалість, хв')
ax.set_ylabel('кількість фільмів')
save(fig, 'task3_runtime_hist.png')

# Завдання 4: актори - найчастіші персонажі
t4 = load('task4_actors_characters', usecols=['character', 'category'])
top_chars = t4.character.value_counts().head(15)[::-1]
fig, ax = plt.subplots(figsize=(9, 4.6))
ax.barh(top_chars.index, top_chars.values, color='#38761d')
ax.set_title(f'Завдання 4. Найчастіші персонажі ({len(t4):,} ролей акторів і акторок)')
ax.set_xlabel('кількість ролей')
save(fig, 'task4_top_characters.png')
print('Завдання 4:', t4.category.value_counts().to_dict())

# Завдання 5: «дорослі» тайтли за регіонами - топ-20
t5 = load('task5_adult_by_region').head(20)[::-1]
fig, ax = plt.subplots(figsize=(9, 5))
ax.barh(t5.region, t5.adult_titles, color='#674ea7')
ax.set_xscale('log')
ax.set_title('Завдання 5. Кількість adult-тайтлів за регіонами (топ-20, логарифмічна шкала)')
ax.set_xlabel('кількість тайтлів')
save(fig, 'task5_adult_regions.png')

# Завдання 6: серіали з найбільшою кількістю епізодів - топ-20
t6 = load('task6_top50_series_by_episodes').head(20)[::-1]
fig, ax = plt.subplots(figsize=(9, 5.5))
ax.barh([short(t) for t in t6.title], t6.episodes_count, color='#1f4e79')
ax.set_title('Завдання 6. Серіали з найбільшою кількістю епізодів (топ-20)')
ax.set_xlabel('кількість епізодів')
save(fig, 'task6_top_series.png')

# Завдання 7: найкращий тайтл кожного десятиліття
t7 = load('task7_top10_by_decade')
best7 = t7[t7.rank_in_decade == 1].sort_values('Decade')
fig, ax = plt.subplots(figsize=(9, 5))
ax.barh([f"{d}s: {short(t, 34)}" for d, t in zip(best7.Decade, best7.title)], best7.averageRating,
        color='#e07b39')
for y, (r, v) in enumerate(zip(best7.averageRating, best7.numVotes)):
    ax.text(r + 0.05, y, f'{r} ({v:,} гол.)', va='center', fontsize=8)
ax.set_xlim(6, 10.6)
ax.set_title('Завдання 7. Тайтл з найвищим рейтингом у кожному десятилітті')
ax.set_xlabel('середній рейтинг IMDb')
save(fig, 'task7_best_by_decade.png')

# Завдання 8: середній рейтинг топ-10 у кожному жанрі
t8 = load('task8_top10_by_genre')
genre_avg = t8.groupby('genre').averageRating.mean().sort_values()
fig, ax = plt.subplots(figsize=(9, 6))
ax.barh(genre_avg.index, genre_avg.values, color='#38761d')
ax.set_xlim(7, 9.6)
ax.set_title('Завдання 8. Середній рейтинг топ-10 тайтлів за жанрами')
ax.set_xlabel('середній рейтинг IMDb')
save(fig, 'task8_genres.png')
