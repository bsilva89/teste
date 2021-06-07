CREATE OR REPLACE TABLE
  `teste-dasa-315722.tmdb_raw.now_playing_raw`
  (genre_ids STRING,
  id INT64,
  original_language STRING,
  original_title STRING,
  popularity FLOAT64,
  release_date STRING,
  title STRING,
  vote_average FLOAT64,
  vote_count INT64
  )
PARTITION BY
  _PARTITIONDATE
OPTIONS
  ( partition_expiration_days=7 );

CREATE OR REPLACE TABLE
  `teste-dasa-315722.tmdb_raw.upcoming_movies_raw`
  (genre_ids STRING,
  id INT64,
  original_language STRING,
  original_title STRING,
  popularity FLOAT64,
  release_date STRING,
  title STRING,
  vote_average FLOAT64,
  vote_count INT64
  )
PARTITION BY
  _PARTITIONDATE
OPTIONS
  ( partition_expiration_days=7 );

CREATE OR REPLACE TABLE
  `teste-dasa-315722.tmdb.movies`
  (genre_ids STRING,
  id INT64,
  original_language STRING,
  original_title STRING,
  popularity FLOAT64,
  release_date STRING,
  title STRING,
  vote_average FLOAT64,
  vote_count INT64,
  is_now_playing BOOLEAN,
  is_upcoming BOOLEAN,
  execution_date DATE,
  created_at DATE
  );

CREATE OR REPLACE TABLE
  `teste-dasa-315722.tmdb_raw.director_details_raw`
  (adult BOOLEAN,
  also_known_as STRING,
  biography STRING,
  birthday STRING,
  deathday STRING,
  gender INT64,
  homepage STRING,
  id INT64,
  imdb_id STRING,
  known_for_department STRING,
  name STRING,
  place_of_birth STRING,
  popularity FLOAT64,
  profile_path STRING)
PARTITION BY
  _PARTITIONDATE
OPTIONS
  ( partition_expiration_days=7 );

CREATE OR REPLACE TABLE
  `teste-dasa-315722.tmdb.director_details`
  (adult BOOLEAN,
  also_known_as STRING,
  biography STRING,
  birthday STRING,
  deathday STRING,
  gender INT64,
  homepage STRING,
  id INT64,
  imdb_id STRING,
  known_for_department STRING,
  name STRING,
  place_of_birth STRING,
  popularity FLOAT64,
  profile_path STRING,
  created_at DATE);

CREATE OR REPLACE TABLE
  `teste-dasa-315722.tmdb_raw.movies_x_director_raw`
  (movie_id INT64,
  director_id INT64
  )
PARTITION BY
  _PARTITIONDATE
OPTIONS
  ( partition_expiration_days=7 );

CREATE OR REPLACE TABLE
  `teste-dasa-315722.tmdb_raw.genres_with_counts_raw`
  (id INT64,
  name STRING,
  count INT64
  )
PARTITION BY
  _PARTITIONDATE
OPTIONS
  ( partition_expiration_days=7 );

CREATE OR REPLACE TABLE
  `teste-dasa-315722.tmdb.genres_with_counts`
  (id INT64,
  name STRING,
  count INT64
  );

CREATE OR REPLACE TABLE
  `teste-dasa-315722.tmdb_raw.producers_details_raw`
  (id INT64,
  name STRING
  )
PARTITION BY
  _PARTITIONDATE
OPTIONS
  ( partition_expiration_days=7 );

CREATE OR REPLACE TABLE
  `teste-dasa-315722.tmdb.producers_details`
  (id INT64,
  name STRING
  )

CREATE OR REPLACE TABLE
  `teste-dasa-315722.tmdb_raw.movies_details_raw`
  (budget INT64,
  genres STRING,
  id INT64,
  original_title STRING,
  production_companies STRING,
  production_countries STRING,
  release_date STRING,
  revenue INT64,
  title STRING)
PARTITION BY
  _PARTITIONDATE
OPTIONS
  ( partition_expiration_days=7 );

CREATE OR REPLACE TABLE
  `teste-dasa-315722.tmdb.movies_details`
  (budget INT64,
  genres STRING,
  id INT64,
  original_title STRING,
  production_countries STRING,
  revenue INT64,
  title STRING,
  producer_id INT64,
  director_id INT64,
  budget_split INT64,
  revenue_split INT64,
  profit_split INT64,
  release_date DATE);