# blog-post-kafka-connect

## development
### 1. build uber-jar containing all dependencies

```
$ ./gradlew clean shadowJar
``` 

### 2. build docker image

```
$ docker-compose build
```

### 3. create output directory and run docker-compose
Create a directory which where the csv files created by the sink connector will be stored
```
$ mkdir output
```
Note: if you use another name than `output` you have to change the `file.output.dir` value in `sink_connector_config.properties`
and rebuild the docker image (`docker-compose build connect`)

The following command starts Apache Kafka and a Kafka connect standalone process with both connectors
``` 
$ docker-compose up --remove-orphans 
```
A lot of logs will be shown. After sometime lines like
```
[2023-03-12 15:18:17,626] INFO [airquality_sink_connector|task-0] Put Done - 500 records have been written (de.inovex.airquality.connector.sink.AirQualitySinkTask:98)
```
should appear. You should then find some csv data in the output directory
```
$ ls output

sensordata_2023-03-12T1500.csv  sensordata_2023-03-12T1600.csv
```
