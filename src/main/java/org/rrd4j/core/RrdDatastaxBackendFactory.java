package org.rrd4j.core;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
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
@RrdBackendAnnotation(name = "DATASTAX", shouldValidateHeader = false)
public class RrdDatastaxBackendFactory extends RrdBackendFactory {
    private Session session;
    private MappingManager manager;
    private Mapper<RrdDatastax> mapper;


    private final String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS rrd4j WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 }";
    private final String createTable = "CREATE TABLE IF NOT EXISTS rrd4j.rrd (path text primary key, rrd blob)";

    static public RrdDatastaxBackendFactory findOrCreate(Session session) {
        try {
            RrdBackendFactory fact = RrdBackendFactory.getDefaultFactory();
            if (fact.name.equals("DATASTAX")) {
                return (RrdDatastaxBackendFactory) fact;
            }
            return new RrdDatastaxBackendFactory(session);
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
        if (!rs.wasApplied()) {
            Logger.getLogger("RrdDatastaxBackendFactory").warning("Failed to create Keyspace for RRD backend");
        }
        ResultSet tableCreated = session.execute(createTable);
        if (!tableCreated.wasApplied()) {
            Logger.getLogger("RrdDatastaxBackendFactory").warning("RRD table not created in cassandra");
        }
        manager = new MappingManager(session);
        mapper = manager.mapper(RrdDatastax.class, "rrd4j");
//        RrdBackendFactory.registerFactory(this);
        RrdBackendFactory.setActiveFactories(this);
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
     * @return all rrdDatastax objects .
     */
    public List<String> allPaths() {
        ResultSet results = session.execute("SELECT path FROM rrd4j.rrd");
        Iterator<Row> it = results.iterator();
        List<String> paths = new ArrayList<String>();
        while (it.hasNext()){
            paths.add(it.next().getString("path"));
        }
        return paths;
    }

    public boolean movePath(String from, String to){
        RrdDatastax f = mapper.get(from);
        mapper.save(new RrdDatastax().setPath(to).setRrd(f.getRrd()));
        mapper.delete(from);
        return true;
    }

    /**
     * <p>delete the path</p>
     *
     * @param path to delete.
     */
    public void delete(String path) {
        mapper.delete(path);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Checks if the RRD with the given id (path) already exists in the database.
     */
    protected boolean exists(String path) throws IOException {
        return mapper.get(path) != null;
    }

    public boolean canStore(URI uri) {
        if (!"datastax".equals(uri.getScheme())) {
            return false;
        } else {
            return !uri.getPath().isEmpty();
        }
    }

}
