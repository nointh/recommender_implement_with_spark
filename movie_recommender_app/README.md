
## To run the docker flask application, run these command
```
docker build -t apache-flask .
```

```
docker-compose up -d
```
To stop and delete containers, volumes, database data, images, run
```
docker-compose down --volumes --rmi all
```
Or
```
docker-compose down --volumes --remove-orphans
```
## To run flask app in debug mode

```
flask run
```