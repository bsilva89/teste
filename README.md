## Overview
Para a execução do projeto foi escolhido o Airflow como orquestrador de tarefas/ETL, o Big Query como banco de dados (mesmo não sendo relacional) por conta do free tier e o PowerBI como data viz, devido a facilidade de transportar o dataset.

## Arquivos
- Airflow DAG em /dag
- Imagens da modelagem BQ e PBI em /images
- PBIX na pasta raíz
- Consultas SQL utilizadas para a criação das tabelas e processamento entre raw e camada de utilização em /dags/tmdb_pipeline/sql_queries/

## Premissas
- Para a execução é necessário ter uma conta na origem dos dados para a criação da chave da API.
- Necessário ter um ambiente Airflow e o PowerBI desktop instalado.
- É necessário para o armazenamento ter uma conta no GCP e acesso de criação de datasets e manipulador de dados no BigQuery.
- Necessário ter um service account com os privilégios acima e ter este configurado como a connection default a ser utilizada no Airflow para acessos aos serviços GCP.
- Necessário definir uma variável do ambiente Airflow com o nome KEY_TMDB_API e valor igual a chave da API do TMDB.
- configurar a pasta de trabalho do airflow como sendo a pasta /dags

## Execução
1. Criar as tabelas no GCP utilizando a query create_tables.sql
2. Redefinir os caminhos das tabelas no arquivo de DAG e arquivos SQL direcionando para as tabelas criadas.
2. Iniciar o Airflow e habilitar a dag para ser executada. Executar manualmente caso necessário.
3. Utilizar as tabelas contidas no dataset tmdb como fonte de dados no PowerBI assim como exemplificado na figura do projeto dimensional.

## Modelagem

- **PBI**: Modelagem entre tabelas

  ![DAGs](https://github.com/bsilva89/teste/blob/master/images/relalacionamentos%20PBI.JPG)

- **Big Query**: Divisão entre tabelas raw e processadas

  ![BQ](https://github.com/bsilva89/teste/blob/master/images/tabelas%20BQ.JPG)
