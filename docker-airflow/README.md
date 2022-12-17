#How to use Airflow with docker and docker-compose

##Prerequisites
Install docker and docker-compose. If you cannot run the following commands due to permission exception, try to add sudo before those commands. 

##Build the image
```bash
docker-compose build
```
##Initialize the Airflow services
```bash
docker-compose up airflow-init
```
##Access to Airflow web UI
Go to any internet browser and direct to localhost or the hostname of your hosting machine. 
Login to the Airflow with default account airflow/airflow

##Shut down the containers
```
docker-compose down
```
To stop and delete containers, volumes, database data, images, run
```
docker-compose down --volumes --rmi all
```
Or
```
docker-compose down --volumes --remove-orphans
```