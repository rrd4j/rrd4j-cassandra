package org.rrd4j.core;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

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
    public void testBackend() throws IOException {
        RrdDatastaxBackendFactory fact = RrdDatastaxBackendFactory.findOrCreate(EmbeddedCassandraServerHelper.getSession());
        Assert.assertEquals("Not Expected to find any sources", 0, fact.all().size());
        RrdBackend be = fact.open("test", false);
        be.setLength(10);
        be.writeDouble(0, 1.2);
        be.close();
        Assert.assertEquals("Expected newly created source to be found", 1, fact.all().size());
        Assert.assertTrue("Expected newly created source to be found", fact.exists("test"));
        RrdBackend ber = fact.open("test",true);
        Assert.assertEquals("Expected to have found the value from backend", 1.2, ber.readDouble(0),0);
    }

    @Test
        public void testDeleteSource() throws IOException {
        RrdDatastaxBackendFactory fact = RrdDatastaxBackendFactory.findOrCreate(EmbeddedCassandraServerHelper.getSession());
        Assert.assertTrue("Expected newly created source to be found", fact.exists("test"));
        fact.delete("test");
        Assert.assertFalse("Expected newly deleted source to be gone", fact.exists("test"));
    }
}