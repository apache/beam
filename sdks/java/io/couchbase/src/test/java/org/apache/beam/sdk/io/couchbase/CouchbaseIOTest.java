package org.apache.beam.sdk.io.couchbase;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.testing.TestPipeline;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.couchbase.mock.Bucket;
import com.couchbase.mock.BucketConfiguration;
import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.client.MockClient;

@RunWith(JUnit4.class)
public class CouchbaseIOTest implements Serializable {

    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    private static CouchbaseMock cb;
    private static MockClient client;

    protected static void createMock(@NotNull String name, @NotNull String password) throws Exception {
        BucketConfiguration bucketConfiguration = new BucketConfiguration();
        bucketConfiguration.numNodes = 10;
        bucketConfiguration.numReplicas = 3;
        bucketConfiguration.name = name;
        bucketConfiguration.type = Bucket.BucketType.COUCHBASE;
        bucketConfiguration.password = password;
        ArrayList<BucketConfiguration> configList = new ArrayList<>();
        configList.add(bucketConfiguration);
        cb = new CouchbaseMock(0, configList);
        cb.start();
        cb.waitForStartup();
    }

    protected void createClients() throws Exception {
        client = new MockClient(new InetSocketAddress("localhost", 0));
        cb.startHarakiriMonitor("localhost:" + client.getPort(), false);
        client.negotiate();

        List<URI> uriList = new ArrayList<URI>();
        uriList.add(new URI("http", null, "localhost", cb.getHttpPort(), "/pools", "", ""));
    }

    @BeforeClass
    public static void startCouchbase() throws Exception {
        createMock("mock", "pwd");
    }

    @AfterClass
    public static void stopCouchbase() {
    }

    @Test
    public void testRead() {
        insertData();

    }

    private void insertData() {
    }
}
