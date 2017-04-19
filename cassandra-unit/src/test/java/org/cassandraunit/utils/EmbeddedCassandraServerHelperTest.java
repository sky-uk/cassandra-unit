package org.cassandraunit.utils;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

/**
 * UnitTest for EmbeddedCassandra with random port. Because Cassandra basically can only be started once per JVM, this test is
 * disabled, and should be manually enabled for single tests only. (CassandraDaemon#deactivate is a bad joke. There may be some
 * workaround with surefire-fork or classloaders or whatever, but one shouldnt invest too much in a workaround for a broken
 * external functionality)
 * 
 * @author Markus Kull
 */
@Ignore("Cassandra can only be started once. If you want to run this test, then enable it and run only this test")
public class EmbeddedCassandraServerHelperTest {

    @Test
    public void shouldStartupOnRandomFreePort() throws Exception {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE);
        int nativePort = EmbeddedCassandraServerHelper.getNativeTransportPort();
        int rpcPort = EmbeddedCassandraServerHelper.getRpcPort();
        Assert.assertTrue(nativePort > 0);
        Assert.assertTrue(rpcPort > 0);
        Assert.assertTrue(rpcPort != 9171); // may seldomly fail if system chooses exactly port 9171 ...
        testIfTheEmbeddedCassandraServerIsUpOnHost("127.0.0.1", rpcPort);
    }

    @Test
    public void shouldStartupOnGivenTemporaryDirectory() throws Exception {
        //given
        String tmpDir = UUID.randomUUID().toString();

        List<String> sourceConfig = IOUtils.readLines(
                getClass().getResourceAsStream("/" + EmbeddedCassandraServerHelper.CASSANDRA_CONCURRENT_YML_FILE));


        //when
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_CONCURRENT_YML_FILE, tmpDir);

        //then
        File destFile = new File(tmpDir + EmbeddedCassandraServerHelper.CASSANDRA_CONCURRENT_YML_FILE);

        int nativePort = EmbeddedCassandraServerHelper.getNativeTransportPort();
        testIfTheEmbeddedCassandraServerIsUpOnHost("127.0.0.1", nativePort);
    }

    private void testIfTheEmbeddedCassandraServerIsUpOnHost(String host, int port) {
        com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder()
                .addContactPoints(host)
                .withPort(port)
                .build();


        try {
            Session session = cluster.connect();

            assertThat(session.getState().getConnectedHosts().size(),is(1));
            KeyspaceMetadata system = session.getCluster().getMetadata().getKeyspace("system");
            assertThat(system.getTables().size(),not(0));

        } finally {
            cluster.close();
        }
    }

    private class TestConfig {
//        private String
    }
    /**
     * hints_directory
     * data_file_directories
     * commitlog_directory
     * cdc_raw_directory
     * saved_caches_directory
     */
}
