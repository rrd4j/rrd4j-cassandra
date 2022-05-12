package org.rrd4j.core;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;


import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

/**
 * {@link RrdBackendFactory} that uses
 * <a href="http://docs.datastax.com/en/developer/java-driver/3.4/manual/object_mapper/using/">Datastax mapping java driver</a>
 * to read data. Construct a Mapper object and pass it via the constructor.
 *
 * @author <a href="mailto:kasperf@asergo.com">Kasper Fock</a>
 */
@SuppressWarnings("HardCodedStringLiteral")
@RrdBackendAnnotation(name = "DATASTAX", shouldValidateHeader = false)
public class RrdDatastaxBackendFactory extends RrdBackendFactory {
    private CqlSession session;
    private RrdDatastaxDao dao;



    private final SimpleStatement createKeyspace = SimpleStatement.newInstance("CREATE KEYSPACE IF NOT EXISTS rrd4j WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 }");
    private final SimpleStatement createTable = SimpleStatement.newInstance("CREATE TABLE IF NOT EXISTS rrd4j.rrd (path text primary key, rrd blob)");

    static public RrdDatastaxBackendFactory findOrCreate(CqlSession session) {
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
    public RrdDatastaxBackendFactory(CqlSession session) {
        this.session = session;
        ResultSet rs = session.execute(createKeyspace, GenericType.of(ResultSet.class));
        if (!rs.wasApplied()) {
            Logger.getLogger("RrdDatastaxBackendFactory").warning("Failed to create Keyspace for RRD backend");
        }
        ResultSet tableCreated = session.execute(createTable,GenericType.of(ResultSet.class));
        if (!tableCreated.wasApplied()) {
            Logger.getLogger("RrdDatastaxBackendFactory").warning("RRD table not created in cassandra");
        }
        RrdDatastaxMapper mapper = new RrdDatastaxMapperBuilder(session).build();
        dao = mapper.rrdDao(CqlIdentifier.fromCql("rrd4j"));
        RrdBackendFactory.setActiveFactories(this);
    }

    /**
     * {@inheritDoc}
     * Creates new RrdDatastaxBackend object for the given id (path).
     */
    protected RrdBackend open(String path, boolean readOnly) throws IOException {
        return new RrdDatastaxBackend(path, this.dao, readOnly);
    }

    /**
     * @return all rrdDatastax objects .
     */
    public List<RrdDatastax> all() {
        return this.dao.all().all();
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
        RrdDatastax f = dao.findByPath(from);
        dao.create(new RrdDatastax().setPath(to).setRrd(f.getRrd()));
        this.dao.delete(f);
        return true;
    }

    /**
     * <p>delete the path</p>
     *
     * @param path to delete.
     */
    public void delete(String path) {
        RrdDatastax d = this.dao.findByPath(path);
        this.dao.delete(d);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Checks if the RRD with the given id (path) already exists in the database.
     */
    protected boolean exists(String path) throws IOException {
        return this.dao.findByPath(path) != null;
    }

    public boolean canStore(URI uri) {
        if (!"datastax".equals(uri.getScheme())) {
            return false;
        } else {
            return !uri.getPath().isEmpty();
        }
    }

}
