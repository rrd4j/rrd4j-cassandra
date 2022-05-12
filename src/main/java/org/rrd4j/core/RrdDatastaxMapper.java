package org.rrd4j.core;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;

@Mapper
public interface RrdDatastaxMapper {
    @DaoFactory
    RrdDatastaxDao rrdDao(@DaoKeyspace CqlIdentifier keyspace);
}
