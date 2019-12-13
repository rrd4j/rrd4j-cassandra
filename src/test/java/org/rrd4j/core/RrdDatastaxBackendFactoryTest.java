package org.rrd4j.core;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.rrd4j.ConsolFun.AVERAGE;
import static org.rrd4j.DsType.GAUGE;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RrdDatastaxBackendFactoryTest {

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Before
    public void startDb() {
        try {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.DEFAULT_CASSANDRA_YML_FILE, testFolder.newFolder().getAbsolutePath(), 20000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void test1_Backend() throws IOException {
        RrdDatastaxBackendFactory fact = RrdDatastaxBackendFactory.findOrCreate(EmbeddedCassandraServerHelper.getSession());
        Assert.assertEquals("Not Expected to find any sources", 0, fact.all().size());
        RrdBackend be = fact.open("test", false);
        be.setLength(10);
        be.writeDouble(0, 1.2);
        be.close();
        Assert.assertEquals("Expected newly created source to be found", 1, fact.all().size());
        Assert.assertTrue("Expected newly created source to be found", fact.exists("test"));
        RrdBackend ber = fact.open("test", true);
        Assert.assertEquals("Expected to have found the value from backend", 1.2, ber.readDouble(0), 0);
    }

    @Test
    public void test2_Values() throws IOException {
        RrdDatastaxBackendFactory fact = RrdDatastaxBackendFactory.findOrCreate(EmbeddedCassandraServerHelper.getSession());
        Assert.assertEquals("Not Expected to find one source", 1, fact.all().size());
        Assert.assertEquals("Not Expected to find one source", 1, fact.allPaths().size());
        for (int i = 1; i < 11; i++) {
            RrdBackend be = fact.open("test", false);
            be.setLength(20);
            be.writeDouble(i - 1, 1.5 * i);
            be.close();
        }
        List<String> expected = new ArrayList<String>();
        expected.add("test");
        Assert.assertEquals("Expected newly created source to be found", 1, fact.allPaths().size());
        Assert.assertEquals("Expected newly created source to be found", expected, fact.allPaths());
        Assert.assertTrue("Expected newly created source to be found", fact.exists("test"));
        RrdBackend ber = fact.open("test", true);
        Assert.assertEquals("Expected to have found the value from backend", 15, ber.readDouble(9), 0);
    }


    @Test
    public void test3_RrdDef() throws IOException, URISyntaxException {
        RrdDatastaxBackendFactory factory = RrdDatastaxBackendFactory.findOrCreate(EmbeddedCassandraServerHelper.getSession());
        RrdBackendFactory.setActiveFactories(factory);
        URI dbUri = factory.getUri("testid@temperature");
//                    URI dbUri = new URI("datastax", "testid@temperature", "/testid@temperature", "", "testrrd");
        long now = Util.normalize(Util.getTimestamp(new Date()), 300);
        try (RrdDb db = RrdDb.getBuilder().setRrdDef(getDef(dbUri)).build()) {
            db.createSample(now).setValue("short", 1.0).update();
        }
        try (RrdDb db = RrdDb.getBuilder().setBackendFactory(factory).setPath("testid@temperature").build()) {
            Assert.assertEquals(now, db.getLastArchiveUpdateTime());
        }
    } @Test
    public void test4_all() throws IOException, URISyntaxException {
        RrdDatastaxBackendFactory fact = RrdDatastaxBackendFactory.findOrCreate(EmbeddedCassandraServerHelper.getSession());
        Assert.assertEquals("Expected two sources", 2, fact.allPaths().size());
        Assert.assertTrue("Expected to find test path", fact.allPaths().contains("test"));
        fact.movePath("test", "testb");
        Assert.assertTrue("Expected to find testb path", fact.allPaths().contains("testb"));
        Assert.assertFalse("Expected to not find test path", fact.allPaths().contains("test"));
    }


    private RrdDef getDef(URI uri) {
//            RrdDef rrdDef = new RrdDef(uri, Util.getTimestamp(2010, 4, 1) - 1, 300);
        RrdDef rrdDef = new RrdDef(uri, Util.getTimestamp(2010, 4, 1) - 1, 300);
        rrdDef.setVersion(2);
        rrdDef.addDatasource("short", GAUGE, 600, 0, Double.NaN);
        rrdDef.addArchive(AVERAGE, 0.5, 1, 600);
        return rrdDef;
    }

    @Test
    public void test5_DeleteSource() throws IOException {
        RrdDatastaxBackendFactory fact = RrdDatastaxBackendFactory.findOrCreate(EmbeddedCassandraServerHelper.getSession());
        Assert.assertTrue("Expected source to be found", fact.exists("testb"));
        fact.delete("testb");
        Assert.assertFalse("Expected source to be gone", fact.exists("testb"));
    }
}
