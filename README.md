# rrd4j-cassandra

Rrd4j Backend implementation for [Cassandra](https://github.com/apache/cassandra) by the [Datastax](https://github.com/datastax/java-driver) driver.

## How to use
In order to use the backed you need to create a new instance of the RrdDatastaxBackendFactry.
You will need to provide the datastax session as argument to the constructor.

The factory will create the keyspace "rrd4j" if it is not already created
It will also create the table rrd in the keyspace where all the the backend data will be stored.    
  