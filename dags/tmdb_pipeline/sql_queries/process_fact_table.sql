BEGIN
TRUNCATE TABLE `teste-dasa-315722.tmdb.movies_details`;

INSERT INTO `teste-dasa-315722.tmdb.movies_details`
WITH movies_details_raw AS (
    SELECT DISTINCT
        * EXCEPT(production_companies),
        REPLACE(production_companies, 'None', "'None'") AS production_companies
    FROM 
        `teste-dasa-315722.tmdb_raw.movies_details_raw`
    WHERE 
        DATE(_PARTITIONTIME) = CURRENT_DATE),

string_array_table AS (
    SELECT
        *, 
        JSON_EXTRACT_ARRAY(production_companies, '$') AS string_array
    FROM 
        movies_details_raw),

unnested_table AS (
SELECT 
    * EXCEPT(production_companies, string_array)
FROM 
    string_array_table,
    string_array_table.string_array AS producer_json),

unnested_and_filtered_table AS (
SELECT 
    * EXCEPT(producer_json),
    CAST(SPLIT(SPLIT(producer_json, ':')[OFFSET(1)], ',')[OFFSET(0)] AS INT64) AS producer_id
FROM unnested_table),

split_budget_and_revenues_table AS (
SELECT
    * EXCEPT(movie_id),
    CAST(budget/COUNT(*)  OVER (PARTITION BY id) AS INT64) AS budget_split,
    CAST(revenue/COUNT(*)  OVER (PARTITION BY id) AS INT64) AS revenue_split,
FROM  
    unnested_and_filtered_table a 
FULL JOIN
    (SELECT * FROM `teste-dasa-315722.tmdb_raw.movies_x_director_raw` WHERE DATE(_PARTITIONTIME) = CURRENT_DATE) b
ON
    CAST(a.id AS INT64) = CAST(b.movie_id AS INT64)
where id IS NOT NULL)

SELECT 
    * EXCEPT(release_date),
    revenue_split-budget_split as profit_split,
    DATE(release_date) as release_date
FROM
    split_budget_and_revenues_table;
END