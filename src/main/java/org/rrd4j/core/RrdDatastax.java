package org.rrd4j.core;

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;


import java.nio.ByteBuffer;

/*@Table(keyspace = "rrd4j", name = "rrd",
        readConsistency = "QUORUM",
        writeConsistency = "QUORUM"
        )*/
@CqlName("rrd")
@Entity(defaultKeyspace = "rrd4j")
public class RrdDatastax {
    @PartitionKey
    private String path;

    private ByteBuffer rrd;

    public RrdDatastax() {
    }

    public String getPath() {
        return path;
    }

    public RrdDatastax setPath(String path) {
        this.path = path;
        return this;
    }

    public ByteBuffer getRrd() {
        return rrd;
    }

    public RrdDatastax setRrd(ByteBuffer rrd) {
        this.rrd = rrd;
        return this;
    }

}
