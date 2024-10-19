# spark-iceberg-example

### Gravitino RESTCatalog server setup
[iceberg-rest-service-doc](https://github.com/apache/gravitino/blob/main/docs/iceberg-rest-service.md)
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
##### Add aws jar to libraries from [here](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws-bundle/1.6.1)


### Load data into iceberg using Spark
if you don't have sbt installed then [Install from here](https://www.scala-sbt.org/1.x/docs/Installing-sbt-on-Mac.html)

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


