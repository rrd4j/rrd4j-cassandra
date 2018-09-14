# rrd4j-cassandra [![Build Status](https://travis-ci.org/rrd4j/rrd4j-cassandra.svg?branch=master)](https://travis-ci.org/rrd4j/rrd4j-cassandra)

Rrd4j Backend implementation for [Cassandra](https://github.com/apache/cassandra) by the [Datastax](https://github.com/datastax/java-driver) driver.

## How to use
In order to use the backed you need to create a new instance of the RrdDatastaxBackendFactry.
You will need to provide the datastax session as argument to the constructor.

The factory will create the keyspace "rrd4j" if it is not already created
It will also create the table rrd in the keyspace where all the the backend data will be stored.

#### Note on large archives 
Please note that if you configure many archive in rrd
you might need to increase the `commitlog_segment_size_in_mb` in cassandra configuration.
Otherwise you will get an IllegalArgumentException complaining: `Mutation of xxxMiB is too large for the maximum size of 16.000MiB`
as the size of the RrdDatastaxBackend.buffer stored will be grater the the half of a cassandra segment.
For further information see [datastax](https://support.datastax.com/hc/en-us/articles/207267063-Mutation-of-x-bytes-is-too-large-for-the-maxiumum-size-of-y-)   
  
    
  