import sqlite3
from constants.constants import processed_file_dir, processed_database_name


def run_query(db_path, query_string, params=()):

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # execute query string with given parameteres
        cursor.execute(query_string, params)
        # identify if read query
        read_query = query_string.lower().strip().startswith("select")
        if read_query:
            # if read query
            results = cursor.fetchall()
            conn.close()
            return results
        else:
            # if any query that modifies the database
            conn.commit()
            conn.close()
    except sqlite3.Error as e:
        print(f"Error: {e}")
        return None
    
# variables
processed_db = f"{processed_file_dir}/{processed_database_name}"

# create media_ratings view which shows ratings, number of reviews (votes), and genre
query_media_ratings = f"""
CREATE VIEW media_type_ratings AS 
SELECT  
	a.primaryTitle as name, 
	b.averageRating as rating, 
	a.genres as genres, 
	a.titleType as mediaType, 
	b.numVotes as votes
FROM 
	title_basics a 
INNER JOIN 
	title_ratings b 
ON 
	a.tconst=b.tconst
ORDER BY 
	b.averageRating DESC, 
	a.genres
"""
run_query(processed_db, query_media_ratings)

# create view that shows the highest rated movie by genre(s)
query_move_ratings_by_genre = """
CREATE VIEW highest_ratings_genres AS
WITH GenreMaxRating AS (
SELECT  
	a.primaryTitle as name, 
	b.averageRating as rating, 
	a.genres as genres, 
	a.titleType as mediaType, 
	b.numVotes as votes,
	ROW_NUMBER() OVER (PARTITION BY a.genres ORDER BY b.averageRating DESC) as RowRank
FROM 
	title_basics a 
INNER JOIN 
	title_ratings b 
ON 
	a.tconst=b.tconst
WHERE
	b.numVotes > 200 AND
	a.titleType = "movie"
)
SELECT
	name,
	rating,
	genres,
	votes
FROM 
	GenreMaxRating
WHERE
	RowRank = 1
ORDER BY 
	name"""
run_query(processed_db, query_move_ratings_by_genre)

# create view that orders actors in films by number of appearances

query_film_actor_appearances = """
CREATE VIEW film_actor_appearances AS
SELECT
	t3.primaryName,
	COUNT(t3.primaryName) as num_roles
from
	title_basics t1
INNER JOIN
	title_principals t2 ON t1.tconst=t2.tconst
INNER JOIN
	name_basics t3 ON t2.nconst=t3.nconst
WHERE
	t2.category="actor" AND t1.titleType="movie"
GROUP BY
	t3.primaryName
ORDER BY
	num_roles DESC
"""
run_query(processed_db, query_film_actor_appearances)

# create view that orders actors in number of tv appearances
query_tv_actor_appearances = """
CREATE VIEW tv_actor_appearances AS
SELECT
	t3.primaryName,
	COUNT(t3.primaryName) as num_roles
from
	title_basics t1
INNER JOIN
	title_principals t2 ON t1.tconst=t2.tconst
INNER JOIN
	name_basics t3 ON t2.nconst=t3.nconst
WHERE
	t2.category="actor" AND t1.titleType in 
	("tvShort",
	"tvMovie",
	"tvEpisode",
	"tvSeries",
	"tvMiniSeries",
	"tvSpecial")
GROUP BY
	t3.primaryName
ORDER BY
	num_roles DESC
"""
run_query(processed_db, query_tv_actor_appearances)

# create view that orders movies by decade
query_movie_by_decade = """
CREATE VIEW movie_by_decade AS
SELECT 
	primaryTitle AS movie,
	CAST(startYear AS INTEGER) / 10 * 10 as decade_released
FROM 
	title_basics
WHERE
	decade_released < 2025 AND titleType = 'movie'
ORDER BY 
	decade_released DESC
"""
run_query(processed_db, query_movie_by_decade)

# create view that orders tv media by decade
query_tv_by_decade = """
CREATE VIEW tv_by_decade AS
SELECT 
	primaryTitle AS tv_media,
	CAST(startYear AS INTEGER) / 10 * 10 as decade_released
FROM 
	title_basics
WHERE
	decade_released < 2025 AND titleType in 
    ("tvShort",
	"tvMovie",
	"tvEpisode",
	"tvSeries",
	"tvMiniSeries",
	"tvSpecial")
ORDER BY 
	decade_released DESC
"""
run_query(processed_db, query_tv_by_decade)


# create view that lists directors by movie
query_director_by_movie = """
CREATE VIEW directors_by_film AS
SELECT
	t3.primaryName AS director_name,
	t1.primaryTitle AS movie_name
from
	title_basics t1
INNER JOIN
	title_principals t2 ON t1.tconst=t2.tconst
INNER JOIN
	name_basics t3 ON t2.nconst=t3.nconst
WHERE
	t2.category="director" AND t1.titleType="movie"
"""
run_query(processed_db, query_director_by_movie)

# create view that shows TV series with the most seasons
query_tv_seasons_count = """
CREATE VIEW tv_seasons_max AS
WITH rankedSeason AS (
	SELECT
		parentTconst,
		seasonNumber,
		ROW_NUMBER() OVER (PARTITION BY parentTconst ORDER BY seasonNumber DESC) AS RowRank
	FROM
		title_episode
)
SELECT
	T2.primaryTitle AS tv_series,
	CAST(T1.seasonNumber AS NUMERIC) AS season_number
FROM 
	rankedSeason T1
INNER JOIN 
	title_basics T2
ON
	T2.tconst = t1.parentTconst
WHERE
	T1.RowRank = 1 AND
	seasonNumber IS NOT NULL
ORDER BY
	season_number DESC
"""
run_query(processed_db, query_tv_seasons_count)