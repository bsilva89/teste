BEGIN
TRUNCATE TABLE `teste-dasa-315722.tmdb.producers_details`;
TRUNCATE TABLE `teste-dasa-315722.tmdb.director_details`;
TRUNCATE TABLE `teste-dasa-315722.tmdb.genres_with_counts`;

INSERT INTO `teste-dasa-315722.tmdb.producers_details` 
SELECT DISTINCT * 
FROM `teste-dasa-315722.tmdb_raw.producers_details_raw` 
WHERE DATE(_PARTITIONTIME) = CURRENT_DATE;

INSERT INTO `teste-dasa-315722.tmdb.genres_with_counts` 
SELECT DISTINCT * 
FROM `teste-dasa-315722.tmdb_raw.genres_with_counts_raw` 
WHERE DATE(_PARTITIONTIME) = CURRENT_DATE;

INSERT INTO `teste-dasa-315722.tmdb.director_details` 
SELECT DISTINCT *, CURRENT_DATE AS created_at
FROM `teste-dasa-315722.tmdb_raw.director_details_raw` 
WHERE DATE(_PARTITIONTIME) = CURRENT_DATE;
END