# This is an overview of my carsome project.
The goal of this project is to scrape data from 'www.carsome.com' and store it in a local Mysql database 
before performing ETL processes such as deleting null values, data imputation to change some columns that contain Thai language into English, 
adding dimension columns, and mapping the brand of the car with the origin countries for analytic usecases.

Furthermore, requests some  API customer data in order to run a scenario in which a person is offered a deal for buying a 2nd-handed used car.


<img width="959" alt="image" src="https://github.com/phakawatfong/carSome_Project/assets/105853659/3b4c4536-03f7-4c8e-acaa-24db0703f685">



## fetch docker-compose.yml files

```
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.1/docker-compose.yaml'
```

## prepare environment for Airflow Docker


```
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```


## Command to go into Postgres shell

***docker exec <container_id> -it psql -U <user_name>***

```
docker exec 66c93e922289  -it psql -U airflow
```


## PSQL command

list database

```
\l
```