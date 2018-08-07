# Example Spark Streaming app writing into Kudu 

Example app where Kudu a table is written by a *ForeachWriter*.

*Beware*: errors are not handled yet. It is just a rapid prototype app. 

## Prerequisite

Kudu table must be created for example via impala-shell, like:

```
CREATE TABLE test_table (value STRING PRIMARY KEY, count INT) STORED AS KUDU;
```

## Starting Netcat

```
nc -lk 9999
```


## Starting spark app

You can start the Spark app for example in local mode via maven exec plugin, like (`<kuduMaster>` must be replaced):

```
mvn exec:java -Dexec.classpathScope="compile" -pl core -Dexec.mainClass="com.example.SparkStreamingIntoKuduExample" -Dexec.args="local localhost 9999 <kuduMaster> impala::default.test_table"
```
