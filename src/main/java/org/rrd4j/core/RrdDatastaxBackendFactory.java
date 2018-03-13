package org.rrd4j.core;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

/**
 * {@link RrdBackendFactory} that uses
 * <a href="http://docs.datastax.com/en/developer/java-driver/3.4/manual/object_mapper/using/">Datastax mapping java driver</a>
 * to read data. Construct a Mapper {@link Mapper} object and pass it via the constructor.
 *
 * @author <a href="mailto:kasperf@asergo.com">Kasper Fock</a>
 */
@SuppressWarnings("HardCodedStringLiteral")
public class RrdDatastaxBackendFactory extends RrdBackendFactory {
    private Session session;
    private MappingManager manager;
    private Mapper<RrdDatastax> mapper;


    private final String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS rrd4j WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 }";
    private final String createTable = "CREATE TABLE IF NOT EXISTS rrd4j.rrd (path text primary key, rrd blob)";

    static public RrdDatastaxBackendFactory findOrCreate(Session session){
        try {
            return (RrdDatastaxBackendFactory) RrdBackendFactory.getFactory("DATASTAX");
        } catch (IllegalArgumentException e) {
            return new RrdDatastaxBackendFactory(session);
        }
    }
    /**
     * <p>Constructor for RrdDatastaxBackendFactory.</p>
     *
     * @param session a {@link Session} object.
     */
    public RrdDatastaxBackendFactory(Session session) {
        this.session = session;
        ResultSet rs = session.execute(createKeyspace);
        if(!rs.wasApplied()){
            Logger.getLogger("RrdDatastaxBackendFactory").warning("Failed to create Keyspace for RRD backend");
        }
        ResultSet tableCreated = session.execute(createTable);
        if(!tableCreated.wasApplied()){
            Logger.getLogger("RrdDatastaxBackendFactory").warning("RRD table not created in cassandra");
        }
        manager = new MappingManager(session);
        mapper = manager.mapper(RrdDatastax.class,"rrd4j");
        RrdBackendFactory.registerAndSetAsDefaultFactory(this);
    }

    /**
     * {@inheritDoc}
     * Creates new RrdDatastaxBackend object for the given id (path).
     */
    protected RrdBackend open(String path, boolean readOnly) throws IOException {
        return new RrdDatastaxBackend(path, mapper);
    }

    /**
     * @return all rrdDatastax objects .
     */
    public List<RrdDatastax> all() {
        ResultSet results = session.execute("SELECT * FROM rrd4j.rrd");
        return mapper.map(results).all();
    }

    /**
     * <p>delete the path</p>
     * @param path to delete.
     */
    public void delete(String path) {
        mapper.delete(path);
    }

    /**
     * {@inheritDoc}
     *
     * Checks if the RRD with the given id (path) already exists in the database.
     */
    protected boolean exists(String path) throws IOException {
        return mapper.get(path) != null;
    }

    /** {@inheritDoc} */
    protected boolean shouldValidateHeader(String path) {
        return false;
    }

    /**
     * <p>getName.</p>
     * @return The {@link String} "DATASTAX".
     */
    public String getName() {
        return "DATASTAX";
    }

}