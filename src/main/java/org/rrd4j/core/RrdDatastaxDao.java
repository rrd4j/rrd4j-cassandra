package org.rrd4j.core;


import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.mapper.annotations.*;

@Dao
public interface RrdDatastaxDao {
    @Select
    RrdDatastax findByPath(String path);

    @Select
    PagingIterable<RrdDatastax> all();

      @Insert
      void create(RrdDatastax product);

      @Update
      void update(RrdDatastax product);

      @Delete
      void delete(RrdDatastax product);
}
