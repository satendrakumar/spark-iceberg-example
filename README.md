# spark-iceberg-example


### Gravitino RESTCatalog server setup 
```shell
git clone git@github.com:datastrato/gravitino.git
cd gravitino
./gradlew clean assembleDistribution -x test
cd distribution
cd gravitino-iceberg-rest-server
bin/gravitino-iceberg-rest-server.sh start
# Logs
tail -f logs/*
```

### Load data into iceberg using Spark
```shell
sbt "runMain com.techmonad.spark.IcebergDataGenerator"
```

### Start query service:
```shell
sbt "runMain com.techmonad.http.HTTPService"
```

### Run query:
```shell
curl --location 'http://localhost:8001/api/customer?from=2020-05-15&to=2020-06-01'
```


