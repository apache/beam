package org.apache.beam.sdk.io.elasticsearch;

import static java.net.InetAddress.getByName;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * Manipulates test data used by the {@link ElasticsearchIO}
 * integration tests.
 *
 * <p>This is independent from the tests so that for read tests it can be run separately after data
 * store creation rather than every time (which can be more fragile.)
 */
public class ElasticsearchTestDataSet {

  public static final String ES_INDEX = "beam";
  public static final String ES_TYPE = "test";
  public static final long NUM_DOCS = 60000;
  private static String writeIndex = ES_INDEX + org.joda.time.Instant.now().getMillis();

  /**
   * Use this to create the index for reading before IT read tests.
   *
   * <p>To invoke this class, you can use this command line from elasticsearch io module directory:
   *
   * <pre>
   * mvn test-compile exec:java \
   * -Dexec.mainClass=org.apache.beam.sdk.io.elasticsearch.ElasticsearchTestDataSet \
   *   -Dexec.args="--elasticsearchServer=1.2.3.4 \
   *  --elasticsearchHttpPort=9200 \
   *  --elasticsearchTcpPort=9300" \
   *   -Dexec.classpathScope=test
   *   </pre>
   *
   * @param args Please pass options from ElasticsearchTestOptions used for connection to
   *     Elasticsearch as shown above.
   */
  public static void main(String[] args) throws Exception {
    PipelineOptionsFactory.register(ElasticsearchTestOptions.class);
    ElasticsearchTestOptions options =
        PipelineOptionsFactory.fromArgs(args).as(ElasticsearchTestOptions.class);

    createAndPopulateIndex(getClient(options), ReadOrWrite.READ);
  }

  private static void createAndPopulateIndex(TransportClient client, ReadOrWrite rOw)
      throws Exception {
    // automatically creates the index and insert docs
    ElasticSearchIOTestUtils.insertTestDocuments(
        (rOw == ReadOrWrite.READ) ? ES_INDEX : writeIndex, ES_TYPE, NUM_DOCS, client);
  }

  public static TransportClient getClient(ElasticsearchTestOptions options) throws Exception {
    TransportClient client =
        TransportClient.builder()
            .build()
            .addTransportAddress(
                new InetSocketTransportAddress(
                    getByName(options.getElasticsearchServer()),
                    Integer.valueOf(options.getElasticsearchTcpPort())));
    return client;
  }

  public static ElasticsearchIO.ConnectionConfiguration getConnectionConfiguration(
      ElasticsearchTestOptions options, ReadOrWrite rOw) {
    ElasticsearchIO.ConnectionConfiguration connectionConfiguration =
        ElasticsearchIO.ConnectionConfiguration.create(
            new String[] {
              "http://"
                  + options.getElasticsearchServer()
                  + ":"
                  + options.getElasticsearchHttpPort()
            },
            (rOw == ReadOrWrite.READ) ? ES_INDEX : writeIndex,
            ES_TYPE);
    return connectionConfiguration;
  };

  public static void deleteIndex(TransportClient client, ReadOrWrite rOw) throws Exception {
    ElasticSearchIOTestUtils.deleteIndex((rOw == ReadOrWrite.READ) ? ES_INDEX : writeIndex, client);
  }

  /** Enum that tells whether we use the index for reading or for writing. */
  public enum ReadOrWrite {
    READ,
    WRITE
  }
}
