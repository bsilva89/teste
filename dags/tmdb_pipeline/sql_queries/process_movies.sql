TRUNCATE TABLE `teste-dasa-315722.tmdb.movies`;

INSERT INTO `teste-dasa-315722.tmdb.movies`
WITH now_playing_table AS(
    SELECT DISTINCT * FROM `teste-dasa-315722.tmdb_raw.now_playing_raw` WHERE DATE(_PARTITIONTIME) = DATE('{{ ds }}')
),

upcoming_table AS (
    SELECT DISTINCT * FROM `teste-dasa-315722.tmdb_raw.upcoming_movies_raw` WHERE DATE(_PARTITIONTIME) = DATE('{{ ds }}')
)

SELECT
    COALESCE(now_playing.genre_ids, upcoming.genre_ids) as genre_ids,
    COALESCE(now_playing.id, upcoming.id) AS id,
    COALESCE(now_playing.original_language, upcoming.original_language) AS original_language,
    COALESCE(now_playing.original_title, upcoming.original_title) AS original_title,
    COALESCE(now_playing.popularity, upcoming.popularity) AS popularity,
    COALESCE(now_playing.release_date, upcoming.release_date) AS release_date,
    COALESCE(now_playing.title, upcoming.title) AS title,
    COALESCE(now_playing.vote_average, upcoming.vote_average) AS vote_average,
    COALESCE(now_playing.vote_count, upcoming.vote_count) AS vote_count,
    IF(now_playing.id IS NOT NULL, True, False) AS is_now_playing,
    IF(upcoming.id IS NOT NULL, True, False) AS is_upcoming,
    DATE('{{ ds }}') AS execution_date,
    CURRENT_DATE AS created_at
FROM
    now_playing_table AS now_playing
FULL JOIN
    upcoming_table AS upcoming
ON
    now_playing.id = upcoming.id