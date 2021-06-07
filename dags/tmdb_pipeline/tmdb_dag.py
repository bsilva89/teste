"""
GOAL: Get TMDB data from movies, dump to BQ and update tables in use.\n
EXECUTION: Everyday 12PM (UTC)\n
SOURCES: TMDB API API\n
SINKS: BigQuery\n
COMMON PROBLEMS: -
"""

from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

project_id = 'teste-dasa-315722'
start_date = datetime(2021, 6, 6)
schedule_interval = "0 12 * * *"

default_args = {
    'owner': 'data', 
    'start_date': start_date, 
    'email': 'data@gmail.com',
    'email_on_failure': True, 
    'email_on_retry': False, 
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'project_id': project_id
}

def get_api_data():
    #imports
    import tmdbsimple as tmdb
    import pandas_gbq
    import pandas as pd
    from dateutil.relativedelta import relativedelta
    import datetime
    import requests
    from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
    from airflow.models import Variable

    #set configs
    project_id = 'teste-dasa-315722'
    gpc_hook = GoogleCloudBaseHook(gcp_conn_id='google_cloud_default', delegate_to=None)
    credentials = gpc_hook._get_credentials()
    pandas_gbq.context.credentials = credentials
    pandas_gbq.context.project = project_id

    DEFAUT_LANGUAGE = 'en-US'
    API_KEY = Variable.get("KEY_TMDB_API")

    tmdb.API_KEY = API_KEY

    url_tmdb_genre = 'https://api.themoviedb.org/3/genre/movie/list'

    movie_columns_list = ['genre_ids', 'id', 'original_language', 
                            'original_title', 'popularity', 'release_date', 'title', 'vote_average', 'vote_count']
    movie_details_columns_list = ['budget', 'genres','id', 'original_title','production_companies',
                                'production_countries', 'release_date', 'revenue', 'title']
                
    #get now_playing movies
    movies = tmdb.Movies()
    movies.now_playing(language = DEFAUT_LANGUAGE)

    df_now_playing = pd.DataFrame(movies.results)
    df_now_playing = df_now_playing[movie_columns_list]

    pandas_gbq.to_gbq(df_now_playing, destination_table = 'tmdb_raw.now_playing_raw', project_id = 'teste-dasa-315722',
        if_exists = 'append', progress_bar = None)
    
    #get upcoming movies
    movies.upcoming(language = DEFAUT_LANGUAGE)

    df_upcoming = pd.DataFrame(movies.results)
    df_upcoming = df_upcoming[movie_columns_list]
    
    #sink to BQ
    pandas_gbq.to_gbq(df_upcoming, destination_table = 'tmdb_raw.upcoming_movies_raw', 
        project_id = 'teste-dasa-315722', if_exists = 'append', progress_bar = None)

    df_movies = pd.concat([df_now_playing,df_upcoming])
    movies_id_list = list(df_movies.id.unique())

    ### PRODUCERS ###
    def get_producers_list(movies_id_list):
        producers_list = []
        for movie_id in movies_id_list:
            producers = tmdb.Movies(movie_id).info(language = DEFAUT_LANGUAGE)['production_companies']
            for producer in producers:
                producers_list.append({'id': producer['id'], 'name': producer['name']})
        return producers_list

    #get producers list from movies
    producers_list = get_producers_list(movies_id_list)
    unique_producers_list = [i for n, i in enumerate(producers_list) if i not in producers_list[n + 1:]]

    #sink to BQ
    #upload producers list
    df_unique_producers_list = pd.DataFrame(unique_producers_list)
    pandas_gbq.to_gbq(df_unique_producers_list, destination_table = 'tmdb_raw.producers_details_raw', project_id = 'teste-dasa-315722',
                    if_exists = 'append', progress_bar = None)
    
    #get date threshold for producers data
    producers_historic_limit_date = datetime.datetime.now() - relativedelta(years=9)
    min_date = producers_historic_limit_date.isoformat()[0:4]

    #movies_x_producers_list
    discover = tmdb.Discover()
    movies_x_producers_list = []
    for producer_id in df_unique_producers_list.id.to_list():
        
        total_pages = discover.movie(language = DEFAUT_LANGUAGE, primary_release_date_gte = min_date, with_companies = producer_id).get('total_pages')
        for each_page in range(1, total_pages+1):
            pagination_info_list = discover.movie(language = DEFAUT_LANGUAGE, primary_release_date_gte = min_date, with_companies = producer_id, page = each_page)['results']
            for movie in pagination_info_list:
                movies_x_producers_list.append({'movie_id': movie['id'], 'producer_id': producer_id})

    #movies_x_producers_list to df_movies_x_producers
    df_movies_x_producers = pd.DataFrame(movies_x_producers_list)

    ### DIRECTORS ###
    #get directors id list from movies_id
    def get_directors_id_to_list(movies_id_list):
        directors_id_list = []
        for movie_id in movies_id_list:
            movie_crew = tmdb.Movies(movie_id).credits(language = DEFAUT_LANGUAGE).get('crew')
            for role in movie_crew:
                if role['job'] == 'Director': directors_id_list.append(role.get('id', None)) 
                
        return directors_id_list

    directors_id_list = get_directors_id_to_list(movies_id_list)

    #retrieving directors details
    def get_directors_info(directors_id_list):
        directors_info_list = []
        movies_x_directors_list = []
        
        for director_id in directors_id_list:
            directors_info_list.append(tmdb.People(director_id).info(language = DEFAUT_LANGUAGE)) #director details
            for movie_from_director in tmdb.People(director_id).movie_credits(language = DEFAUT_LANGUAGE).get('crew'): #list of the movies a person is involved
                if movie_from_director['job'] == 'Director': 
                    movies_x_directors_list.append({'movie_id': movie_from_director['id'], 'director_id': director_id})
        return {'directors_info_list': directors_info_list, 'movies_x_directors_list': movies_x_directors_list}

    directors_dict = get_directors_info(directors_id_list)
    directors_info_list = directors_dict.get('directors_info_list')
    movies_x_directors_list = directors_dict.get('movies_x_directors_list')

    #dump to BQ - df_directors_detailed_info
    df_directors_detailed_info = pd.DataFrame(directors_info_list)
    pandas_gbq.to_gbq(df_directors_detailed_info, destination_table = 'tmdb_raw.director_details_raw', project_id = 'teste-dasa-315722',
                    if_exists = 'append', progress_bar = None)

    #dump to BQ - df_movies_x_directors
    df_movies_x_directors = pd.DataFrame(movies_x_directors_list)
    pandas_gbq.to_gbq(df_movies_x_directors, destination_table = 'tmdb_raw.movies_x_director_raw', project_id = 'teste-dasa-315722',
                    if_exists = 'append', progress_bar = None)

    ### GENRE LIST ###

    #getting genres list vs count
    payload = {'api_key': API_KEY, 'language': 'en-US'}
    r = requests.get(url_tmdb_genre, params=payload)

    genres_list = r.json().get('genres')

    #defining threshold datetime for genre movies
    genre_historic_limit_date = datetime.datetime.now() - relativedelta(years=4)
    genre_historic_min_date = genre_historic_limit_date.isoformat()[0:4]
    genre_historic_max_date = datetime.datetime.now().isoformat()[0:4]

    years_list = range(int(genre_historic_min_date), int(genre_historic_max_date)+1)

    for idx, genre in enumerate(genres_list):
        count = 0
        for year in years_list:
            count += discover.movie(language = DEFAUT_LANGUAGE, year = year, with_genres = genre.get('id')).get('total_results')
        genres_list[idx]['count'] = count

    #dump to BQ - df_genres_list
    # df_genres_list = pd.DataFrame(genres_list)
    # pandas_gbq.to_gbq(df_genres_list, destination_table = 'tmdb_raw.genres_with_counts_raw', project_id = 'teste-dasa-315722',
    #                 if_exists = 'append', progress_bar = None)
    
    # ### MOVIE DETAILS ### 
    # #get detailed info from movies list
    # df_merge = pd.concat([df_movies_x_producers.movie_id,df_movies_x_directors.movie_id])
    # df_merge.drop_duplicates(inplace=True)

    # movies_info_list = []
    # for movie_id in list(df_merge):
    #     try:
    #         movies_info_list.append(tmdb.Movies(movie_id).info(language = DEFAUT_LANGUAGE))
    #     except:
    #         print(f'Cannot get info from movie: {movie_id}')
    
    # #dump to BQ - df_movies_info
    # df_movies_info = pd.DataFrame(movies_info_list)
    # df_movies_info = df_movies_info[movie_details_columns_list]
    # pandas_gbq.to_gbq(df_movies_info, destination_table = 'tmdb_raw.movies_details_raw', project_id = 'teste-dasa-315722',
    #                 if_exists = 'append', progress_bar = None)

with DAG('tmdb_pipeline', default_args=default_args, schedule_interval= schedule_interval) as dag:
    dag.doc_md = __doc__
    
    get_api_data = PythonVirtualenvOperator( 
        task_id='get_api_data', 
        python_callable=get_api_data,
        requirements = ["tmdbsimple", "requests", "pandas_gbq"],
        system_site_packages= True,
        python_version='3'
        #op_kwargs = {}
    )
    
    process_movies = BigQueryOperator(
        task_id = 'process_movies',
        sql = "{% include 'sql_queries/process_movies.sql' %}",
        allow_large_results = True,
        use_legacy_sql = False
    )

    process_producers_genres_directors = BigQueryOperator(
        task_id = 'process_producers_genres_directors',
        sql = "{% include 'sql_queries/process_producers_genres_directors.sql' %}",
        allow_large_results = True,
        use_legacy_sql = False
    )

    process_fact_table = BigQueryOperator(
        task_id = 'process_fact_table',
        sql = "{% include 'sql_queries/process_fact_table.sql' %}",
        allow_large_results = True,
        use_legacy_sql = False
    )
    
    get_api_data >> [process_movies, process_producers_genres_directors]
    process_movies >> process_fact_table