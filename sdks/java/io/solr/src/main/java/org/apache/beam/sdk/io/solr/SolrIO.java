package org.apache.beam.sdk.io.solr;


import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;

/**
 * Transforms for reading and writing data from/to Solr.
 *
 * <h3>Reading from Solr</h3>
 *
 * <p>{@link SolrIO#read SolrIO.read()} returns a bounded
 * {@link PCollection PCollection&lt;SolrDocument&gt;} representing Solr documents.
 *
 * <p>To configure the {@link SolrIO#read}, you have to provide a connection configuration
 * containing the Zookeeper address of the Solr cluster. The following example
 * illustrates options for configuring the source:
 *
 * <pre>{@code
 *
 * pipeline.apply(SolrIO.read().withConnectionConfiguration(
 *    SolrIO.ConnectionConfiguration.create("127.0.0.1:9983", "my-collection")
 * )
 *
 * }</pre>
 *
 * <p>You can specify a query on the {@code read()} using {@code withQuery()}.
 *
 * <h3>Writing to Solr</h3>
 *
 * <p>To write documents to Solr, use
 * {@link SolrIO#write SolrIO.write()}, which writes Solr documents from a
 * {@link PCollection PCollection&lt;SolrInputDocument&gt;} (which can be bounded or unbounded).
 *
 * <p>To configure {@link SolrIO#write SolrIO.write()}, similar to the read, you
 * have to provide a connection configuration. For instance:
 *
 * <pre>{@code
 *
 *  pipeline
 *    .apply(...)
 *    .apply(SolrIO.write().withConnectionConfiguration(
 *       SolrIO.ConnectionConfiguration.create("127.0.0.1:9983", "my-collection")
 *    )
 *
 * }</pre>
 *
 * <p>Optionally, you can provide {@code withBatchSize()}
 * to specify the size of the write batch in number of documents.
 */
@Experimental
public class SolrIO {

  public static Read read() {
    return new AutoValue_SolrIO_Read.Builder().setBatchSize(100L).build();
  }

  public static Write write() {
    return new AutoValue_SolrIO_Write.Builder().setMaxBatchSize(1000L).build();
  }

  private SolrIO() {
  }

  /** A POJO describing a connection configuration to Solr. */
  @AutoValue public abstract static class ConnectionConfiguration implements Serializable {

    //TODO add user name password
    abstract String getZkHost();

    abstract String getCollection();

    abstract Builder builder();

    @AutoValue.Builder abstract static class Builder {

      abstract Builder setZkHost(String zkHost);

      abstract Builder setCollection(String collection);

      abstract ConnectionConfiguration build();
    }

    /**
     * Creates a new Solr connection configuration.
     *
     * @param zkHost host of zookeeper
     * @param collection the collection toward which the requests will be issued
     * @return the connection configuration object
     * @throws IOException when it fails to connect to Solr
     */
    public static ConnectionConfiguration create(String zkHost, String collection)
        throws IOException {
      checkArgument(zkHost != null,
          "ConnectionConfiguration.create(zkHost, collection) "
              + "called with null address");
      checkArgument(collection != null,
          "ConnectionConfiguration.create(zkHost, collection) "
              + "called with null collectioin");
      ConnectionConfiguration connectionConfiguration = new AutoValue_SolrIO_ConnectionConfiguration
          .Builder().setZkHost(zkHost).setCollection(collection).build();
      return connectionConfiguration;
    }

    private void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("zkHost", getZkHost()));
      builder.add(DisplayData.item("collection", getCollection()));
    }

    CloudSolrClient createClient() throws MalformedURLException {
      CloudSolrClient solrClient = new CloudSolrClient.Builder().withZkHost(getZkHost()).build();
      solrClient.setDefaultCollection(getCollection());
      return solrClient;
    }

    HttpSolrClient createClient(String shardUrl) {
      HttpSolrClient solrClient = new HttpSolrClient.Builder(shardUrl).build();
      return solrClient;
    }
  }

  /** A {@link PTransform} reading data from Solr. */
  @AutoValue public abstract static class Read
      extends PTransform<PBegin, PCollection<SolrDocument>> {

    private static final long MAX_BATCH_SIZE = 10000L;

    @Nullable abstract ConnectionConfiguration getConnectionConfiguration();

    @Nullable abstract String getQuery();

    abstract long getBatchSize();

    abstract Builder builder();

    @AutoValue.Builder abstract static class Builder {

      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setQuery(String query);

      abstract Builder setBatchSize(long batchSize);

      abstract Read build();
    }

    /**
     * Provide the Solr connection configuration object.
     *
     * @param connectionConfiguration the Solr {@link ConnectionConfiguration} object
     * @return the {@link Read} with connection configuration set
     */
    public Read withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "SolrIO.read()"
          + ".withConnectionConfiguration(configuration) called with null configuration");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide a query used while reading from Solr.
     *
     * @param query the query. See <a
     *     href="https://cwiki.apache.org/confluence/display/solr/The+Standard+Query+Parser">
     *              Solr Query
     *     </a>
     * @return the {@link Read} object with query set
     */
    public Read withQuery(String query) {
      checkArgument(!Strings.isNullOrEmpty(query),
          "SolrIO.read().withQuery(query) called" + " with null or empty query");
      return builder().setQuery(query).build();
    }

    /**
     * Provide a size for the cursor read. See <a
     * href="https://cwiki.apache.org/confluence/display/solr/Pagination+of+Results">
     * cursor API</a> Default is 100. Maximum is 10 000. If documents are small, increasing batch
     * size might improve read performance. If documents are big, you might need to decrease
     * batchSize
     *
     * @param batchSize number of documents read in each scroll read
     * @return the {@link Read} with batch size set
     */
    public Read withBatchSize(long batchSize) {
      checkArgument(batchSize > 0, "SolrIO.read().withBatchSize(batchSize) "
          + "called with a negative or equal to 0 value: %s", batchSize);
      checkArgument(batchSize <= MAX_BATCH_SIZE,
          "SolrIO.read().withBatchSize(batchSize) "
              + "called with a too large value (over %s): %s",
          MAX_BATCH_SIZE, batchSize);
      return builder().setBatchSize(batchSize).build();
    }

    @Override public PCollection<SolrDocument> expand(PBegin input) {
      return input.apply(org.apache.beam.sdk.io.Read
          .from(new BoundedSolrSource(this, null)));
    }

    @Override public void validate(PipelineOptions options) {
      checkState(getConnectionConfiguration() != null,
          "SolrIO.read() requires a connection configuration"
              + " to be set via withConnectionConfiguration(configuration)");
    }

    @Override public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("query", getQuery()));
      getConnectionConfiguration().populateDisplayData(builder);
    }
  }

  /** A {@link BoundedSource} reading from Solr. */
  @VisibleForTesting static class BoundedSolrSource extends BoundedSource<SolrDocument> {

    private final SolrIO.Read spec;
    // replica is the info of the shard where the source will read the documents
    @Nullable private final Replica replica;

    BoundedSolrSource(Read spec, @Nullable Replica replica) {
      this.spec = spec;
      this.replica = replica;
    }

    @Override public List<? extends BoundedSource<SolrDocument>> split(long desiredBundleSizeBytes,
        PipelineOptions options) throws Exception {
      List<BoundedSolrSource> sources = new ArrayList<>();
      int numShard;
      try (CloudSolrClient client = spec.getConnectionConfiguration().createClient()) {
        // connect to zk cluster
        client.connect();
        ClusterState clusterState = client.getZkStateReader().getClusterState();
        DocCollection docCollection = clusterState
            .getCollection(spec.getConnectionConfiguration().getCollection());
        numShard = docCollection.getSlices().size();
        for (Slice slice : docCollection.getSlices()) {
          sources.add(new BoundedSolrSource(spec, slice.getLeader()));
        }
      }
      checkArgument(sources.size() == numShard, "Not enough leaders were found");
      return sources;
    }

    @Override public long getEstimatedSizeBytes(PipelineOptions options) throws IOException {
      if (replica != null) {
        return getEstimatedSizeOfShard(replica);
      } else {
        return getEstimatedSizeOfCollection();
      }
    }

    private long getEstimatedSizeOfShard(Replica replica) throws IOException {
      try (HttpSolrClient solrClient = spec.getConnectionConfiguration()
          .createClient(replica.getBaseUrl())) {
        CoreAdminRequest req = new CoreAdminRequest();
        req.setAction(CoreAdminParams.CoreAdminAction.STATUS);
        req.setIndexInfoNeeded(true);
        CoreAdminResponse response;
        try {
          response = req.process(solrClient);
        } catch (SolrServerException e) {
          throw new IOException("Can not get core status from " + replica, e);
        }
        NamedList<Object> coreStatus = response.getCoreStatus(replica.getCoreName());
        NamedList<Object> indexStats = (NamedList<Object>) coreStatus.get("index");
        return (long) indexStats.get("sizeInBytes");
      }
    }

    private long getEstimatedSizeOfCollection() throws IOException {
      long sizeInBytes = 0;
      ConnectionConfiguration config = spec.getConnectionConfiguration();
      try (CloudSolrClient solrClient = config.createClient()) {
        solrClient.connect();
        ClusterState clusterState = solrClient.getZkStateReader().getClusterState();
        DocCollection docCollection = clusterState.getCollection(config.getCollection());
        for (Slice slice : docCollection.getSlices()) {
          Replica replica = slice.getLeader();
          sizeInBytes += getEstimatedSizeOfShard(replica);
        }
      }
      return sizeInBytes;
    }

    @Override public void populateDisplayData(DisplayData.Builder builder) {
      spec.populateDisplayData(builder);
      if (replica != null) {
        builder.addIfNotNull(DisplayData.item("shardUrl", replica.getCoreUrl()));
      }
    }

    @Override public BoundedReader<SolrDocument> createReader(PipelineOptions options)
        throws IOException {
      return new BoundedSolrReader(this);
    }

    @Override public void validate() {
      spec.validate(null);
    }

    @Override public Coder<SolrDocument> getDefaultOutputCoder() {
      return SolrCoder.of();
    }
  }

  /**
   * A {@link Coder} that encodes {@link SolrDocument SolrDocument}.
   */
  public static class SolrCoder extends Coder<SolrDocument> {

    private static final SolrCoder INSTANCE = new SolrCoder();

    public static SolrCoder of() {
      return INSTANCE;
    }

    @Override public void encode(SolrDocument value, OutputStream outStream)
        throws CoderException, IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      JavaBinCodec codec = new JavaBinCodec();
      codec.marshal(value, baos);

      byte[] bytes = baos.toByteArray();
      VarInt.encode(bytes.length, outStream);
      outStream.write(bytes);
    }

    @Override public SolrDocument decode(InputStream inStream) throws CoderException, IOException {
      DataInputStream in = new DataInputStream(inStream);

      int len = VarInt.decodeInt(in);
      if (len < 0) {
        throw new CoderException("Invalid encoded SolrDocument length: " + len);
      }
      byte[] bytes = new byte[len];
      in.readFully(bytes);

      JavaBinCodec codec = new JavaBinCodec();
      return (SolrDocument) codec.unmarshal(new ByteArrayInputStream(bytes));
    }

    @Override public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override public void verifyDeterministic() throws NonDeterministicException {

    }
  }

  private static class BoundedSolrReader extends BoundedSource.BoundedReader<SolrDocument> {

    private final BoundedSolrSource source;

    private SolrClient solrClient;
    private SolrDocument current;
    private String cursorMark;
    private Iterator<SolrDocument> batchIterator;
    private boolean done;

    private BoundedSolrReader(BoundedSolrSource source) {
      this.source = source;
      this.cursorMark = CursorMarkParams.CURSOR_MARK_START;
    }

    @Override public boolean start() throws IOException {
      if (source.replica != null) {
        solrClient = source.spec.getConnectionConfiguration()
            .createClient(source.replica.getCoreUrl());
      } else {
        solrClient = source.spec.getConnectionConfiguration().createClient();
      }

      SolrQuery solrParams = getQueryParams(source);
      try {
        QueryResponse response = solrClient.query(solrParams);
        updateCursorMark(response);
        return readNextBatchAndReturnFirstDocument(response);
      } catch (SolrServerException e) {
        throw new IOException(e);
      }
    }

    private SolrQuery getQueryParams(BoundedSolrSource source) {
      String query = source.spec.getQuery();
      if (query == null) {
        query = "*:*";
      }
      SolrQuery solrQuery = new SolrQuery(query);
      solrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark);
      solrQuery.addSort("id", SolrQuery.ORDER.asc);
      if (source.replica != null) {
        solrQuery.setDistrib(false);
      }
      return solrQuery;
    }

    private void updateCursorMark(QueryResponse response) {
      if (cursorMark.equals(response.getNextCursorMark())) {
        done = true;
      }
      cursorMark = response.getNextCursorMark();
    }

    @Override public boolean advance() throws IOException {
      if (batchIterator.hasNext()) {
        current = batchIterator.next();
        return true;
      } else {
        SolrQuery solrQuery = getQueryParams(source);
        try {
          QueryResponse response = solrClient.query(solrQuery);
          updateCursorMark(response);
          return readNextBatchAndReturnFirstDocument(response);
        } catch (SolrServerException e) {
          throw new IOException(e);
        }
      }
    }

    private boolean readNextBatchAndReturnFirstDocument(QueryResponse response) {
      if (response.getResults().isEmpty() || done) {
        current = null;
        batchIterator = null;
        return false;
      }

      batchIterator = response.getResults().iterator();
      current = batchIterator.next();
      return true;
    }

    @Override public SolrDocument getCurrent() throws NoSuchElementException {
      if (current == null) {
        throw new NoSuchElementException();
      }
      return current;
    }

    @Override public void close() throws IOException {
      solrClient.close();
    }

    @Override public BoundedSource<SolrDocument> getCurrentSource() {
      return source;
    }
  }

  /** A {@link PTransform} writing data to Solr. */
  @AutoValue public abstract static class Write
      extends PTransform<PCollection<SolrInputDocument>, PDone> {

    @Nullable abstract ConnectionConfiguration getConnectionConfiguration();

    abstract long getMaxBatchSize();

    abstract Builder builder();

    @AutoValue.Builder abstract static class Builder {

      abstract Builder setConnectionConfiguration(ConnectionConfiguration connectionConfiguration);

      abstract Builder setMaxBatchSize(long maxBatchSize);

      abstract Write build();
    }

    /**
     * Provide the Solr connection configuration object.
     *
     * @param connectionConfiguration the Solr {@link ConnectionConfiguration} object
     * @return the {@link Write} with connection configuration set
     */
    public Write withConnectionConfiguration(ConnectionConfiguration connectionConfiguration) {
      checkArgument(connectionConfiguration != null, "SolrIO.write()"
          + ".withConnectionConfiguration(configuration) called with null configuration");
      return builder().setConnectionConfiguration(connectionConfiguration).build();
    }

    /**
     * Provide a maximum size in number of documents for the batch. Depending on the
     * execution engine, size of bundles may vary, this sets the maximum size. Change this if you
     * need to have smaller batch.
     *
     * @param batchSize maximum batch size in number of documents
     * @return the {@link Write} with connection batch size set
     */
    public Write withMaxBatchSize(long batchSize) {
      checkArgument(batchSize > 0,
          "SolrIO.write()" + ".withMaxBatchSize(batchSize) called with incorrect <= 0 value");
      return builder().setMaxBatchSize(batchSize).build();
    }

    @Override public void validate(PipelineOptions options) {
      checkState(getConnectionConfiguration() != null,
          "SolrIO.write() requires a connection configuration"
              + " to be set via withConnectionConfiguration(configuration)");
    }

    @Override public PDone expand(PCollection<SolrInputDocument> input) {
      input.apply(ParDo.of(new WriteFn(this)));
      return PDone.in(input.getPipeline());
    }

    @VisibleForTesting static class WriteFn extends DoFn<SolrInputDocument, Void> {

      private final Write spec;

      private transient CloudSolrClient solrClient;
      private Collection<SolrInputDocument> batch;

      WriteFn(Write spec) {
        this.spec = spec;
      }

      @Setup public void createClient() throws Exception {
        solrClient = spec.getConnectionConfiguration().createClient();
        solrClient.connect();
      }

      @StartBundle public void startBundle(StartBundleContext context) throws Exception {
        batch = new ArrayList<>();
      }

      @ProcessElement public void processElement(ProcessContext context) throws Exception {
        SolrInputDocument document = context.element();
        batch.add(document);
        if (batch.size() >= spec.getMaxBatchSize()) {
          flushBatch();
        }
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext context) throws Exception {
        flushBatch();
      }

      private void flushBatch() throws IOException {
        if (batch.isEmpty()) {
          return;
        }
        try {
          solrClient.add(spec.getConnectionConfiguration().getCollection(), batch);
        } catch (SolrServerException e) {
          throw new IOException("Error writing to Solr", e);
        } finally {
          batch.clear();
        }
      }

      @Teardown
      public void closeClient() throws Exception {
        if (solrClient != null) {
          solrClient.close();
        }
      }
    }
  }
}
