# Guide

### Data
Add the data from [Kaggle](https://www.kaggle.com/datasets/Cornell-University/arxiv/) and put it into 
data folder with the name ```arxiv-metadata-oai-snapshot.json```

### Ports
* Webserver: 8091 (Airflow GUI)
* Airflow DB: 8092 (used by Airflow)
* Data DB: 8093 (for business data)
* Graph DB: 7474 (Neo4j)

### Configuration
For the webserver to work you need to configure a connection for the database...<br>
To do this go to ```Admin > Connections``` and add a new Postgres connection. <br>
The server needs to be connected to the data database!
* Connection id: <strong>postgres-data</strong>
* Connection type: <strong>Postgres</strong>
* Host: <strong>postgres-data</strong>
* Schema: <strong>data_db</strong>
* Login: <strong>data_user</strong>
* Password: <strong>data_password</strong>

### Usernames and passwords
* Airflow
  * UN: airflow
  * PW: airflow

### Running the project
Use ```docker-compose up --build -d``` in root directory to run the project

### NB!
* The data file is 3+ GB, so make sure that your Docker limits facilitate this

### Max sizes of attributes

id: 16
submitter: 64
authors: 36662
title: 382
comments: 1150
journal-ref: 557
doi: 153
report-no: 479
categories: 125
license: 51
abstract: 6091
versions: 187
update_date: 10
authors_parsed: 2832
