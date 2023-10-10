# This is an overview of my carsome project.
The goal of this project is to scrape data from 'www.carsome.com' and store it in a local Mysql database 
before performing ETL processes such as deleting null values, data imputation to change some columns that contain Thai language into English, 
adding dimension columns, and mapping the brand of the car with the origin countries for analytic usecases.

Furthermore, requests some  API customer data in order to run a scenario in which a person is offered a deal for buying a 2nd-handed used car.


![project_overview](https://github.com/phakawatfong/carSome_Project/blob/main/pictures/carsome_project_overview.png)

## Carsome Dashboard !!!
[https://lookerstudio.google.com/reporting/406b1b02-f9dd-42ec-a0ce-2096ca9e5c81]
![project_dashboard](https://github.com/phakawatfong/carSome_Project/blob/main/pictures/carsome-dashboard.png)

## Build DockerImages and Startup the Airflow with Docker Container. (first time only)

```
docker compose up --build
```

## Startup docker each time you want to work on a project

```
docker compose up
```

## fetch docker-compose.yml files

```
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.1/docker-compose.yaml'
```

## prepare environment for Airflow Docker


```
mkdir -p ./dags ./logs ./plugins ./config ./env_conf ./dags/script ./import_data ./output
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

### Build Docker Images and startup the Airflow with Docker (first time ).

```
docker compose up --build
```

## Command to go into Postgres shell

***docker exec <container_id> -it psql -U <user_name>***
***docker exec <container_name> -it psql -U <user_name>***

```
docker exec -it de_car_proj-postgres-1 psql -U airflow
```


## Postgres command
ref : [https://www.commandprompt.com/education/how-to-show-tables-in-postgresql/]

List database

```
\l;
```

Access database

***\c <database_name>;***
```
\c carsome_db;
```

List table

```
\dt;
```

List columns of specific table
***\d <table_name>***

```
\d carsome_scraped
```

Prepare new database for the project on Postgres through command line

Note that, the DDL command has to be in ***UPPER CASE***

```
CREATE DATABASE carsome_db;
```



## Working with Google Cloud Platform (GCP)

### create custom-role to access GoogleCloudStorage
- storage.buckets.create
- storage.objects.create
- storage.objects.delete

### create custom-role to get data from GoogleCloudStorage and Load to Bigquery
-   bigquery.jobs.create
-   bigquery.tables.create
-   bigquery.tables.get
-   bigquery.tables.update
-   bigquery.tables.updateData
-   storage.objects.get
