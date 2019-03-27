/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.couchbase;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write data on Couchbase.
 *
 * <h3>Reading from Couchbase</h3>
 *
 * <p>{@link CouchbaseIO} provides a source to read data and returns a bounded set of JsonDocument.
 * The {@link JsonDocument} is the JSON form of Couchbase document.
 *
 * <p>The following example illustrates various options for configuring the IO:
 *
 * <pre>{@code
 *  pipeline.apply(
 *             CouchbaseIO.read()
 *                 .withHosts(Arrays.asList("host1", "host2"))
 *                 .withHttpPort(8091) // Optional
 *                 .withCarrierPort(11210) // Optional
 *                 .withBucket("bucket1")
 *                 .withPassword("pwd")) // Bucket-level password
 *
 *  }</pre>
 */
public class CouchbaseIO {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseIO.class);

  private CouchbaseIO() {}

  /**
   * Provide a {@link Read} {@link PTransform} to read data from a Couchbase database. Here some
   * default options are provided.
   */
  public static Read read() {
    return new AutoValue_CouchbaseIO_Read.Builder().build();
  }

  /** A {@link PTransform} to read data from Couchbase. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<JsonDocument>> {
    @Nullable
    abstract List<String> hosts();

    @Nullable
    abstract Integer httpPort();

    @Nullable
    abstract Integer carrierPort();

    @Nullable
    abstract String bucket();

    @Nullable
    abstract String password();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setHosts(List<String> hosts);

      abstract Builder setHttpPort(Integer port);

      abstract Builder setCarrierPort(Integer port);

      abstract Builder setBucket(String bucket);

      abstract Builder setPassword(String password);

      abstract Read build();
    }

    /**
     * Define a list of ip to the cluster nodes.
     *
     * @param hosts list of ip address
     * @return a {@link PTransform} reading data from Couchbase
     */
    public Read withHosts(List<String> hosts) {
      checkArgument(hosts != null, "hosts can not be null");
      checkArgument(!hosts.isEmpty(), "hosts can not be empty");
      return builder().setHosts(hosts).build();
    }

    /**
     * Define the http port connecting to Couchbase.
     *
     * @param port the http port
     * @return a {@link PTransform} reading data from Couchbase
     */
    public Read withHttpPort(int port) {
      checkArgument(port > 0, "httpPort must be > 0, but was: %s", port);
      return builder().setHttpPort(port).build();
    }

    /**
     * Define the carrier port connecting to Couchbase.
     *
     * @param port the carrier port
     * @return a {@link PTransform} reading data from Couchbase
     */
    public Read withCarrierPort(int port) {
      checkArgument(port > 0, "carrierPort must be > 0, but was: %s", port);
      return builder().setCarrierPort(port).build();
    }

    /**
     * Define the name of bucket.
     *
     * @param bucket the bucket name
     * @return a {@link PTransform} reading data from Couchbase
     */
    public Read withBucket(String bucket) {
      checkArgument(bucket != null, "bucket can not be null");
      return builder().setBucket(bucket).build();
    }

    /**
     * Define the bucket-level password to the target bucket.
     *
     * @param password password
     * @return a {@link PTransform} reading data from Couchbase
     */
    public Read withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      return builder().setPassword(password).build();
    }

    @Override
    public PCollection<JsonDocument> expand(PBegin input) {
      checkArgument((hosts() != null), "WithHosts()is required");
      checkArgument(bucket() != null, "withBucket() is required");

      CouchbaseSource source = new CouchbaseSource(this);
      PCollection<JsonDocument> result = input.apply(org.apache.beam.sdk.io.Read.from(source));
      // Disconnect the client from Couchbase
      source.close();
      return result;
    }
  }

  @VisibleForTesting
  static class CouchbaseSource extends BoundedSource<JsonDocument> {

    private final Read spec;
    private int itemCount;
    private final int lowerBound; // Lower bound of key range (included)
    private final int upperBound; // Upper bound of key range (excluded)
    private Cluster cluster;
    private Bucket bucket;

    CouchbaseSource(Read spec) {
      this(spec, null, null, 0, 0);
    }

    CouchbaseSource(Read spec, Cluster cluster, Bucket bucket, int lb, int ub) {
      this.spec = spec;
      this.cluster = cluster;
      this.bucket = bucket;
      this.lowerBound = lb;
      this.upperBound = ub;
    }

    private Bucket getBucket() {
      if (cluster == null) {
        DefaultCouchbaseEnvironment.Builder builder = DefaultCouchbaseEnvironment.builder();
        if (spec.httpPort() != null) {
          builder.bootstrapHttpDirectPort(spec.httpPort());
        }
        if (spec.carrierPort() != null) {
          builder.bootstrapCarrierDirectPort(spec.carrierPort());
        }
        cluster = CouchbaseCluster.create(builder.build(), spec.hosts());
      }
      if (bucket == null) {
        // For Couchbase Server, in the previous version than 5.0, the passwordless bucket can be
        // supported.
        // But after version 5.0, the newly created user should have a username equal to bucket name
        // and a password.
        // For more information, please go to
        // https://docs.couchbase.com/java-sdk/2.7/sdk-authentication-overview.html#legacy-connection-code
        bucket =
            spec.password() == null
                ? cluster.openBucket(spec.bucket())
                : cluster.openBucket(spec.bucket(), spec.password());
      }
      return bucket;
    }

    @Override
    public Coder<JsonDocument> getOutputCoder() {
      return SerializableCoder.of(JsonDocument.class);
    }

    @Override
    public List<? extends BoundedSource<JsonDocument>> split(
        long desiredBundleSize, PipelineOptions options) {
      // If the desiredBundleSize equals to 0, it means that there will be only one bundle of data
      // to be read.
      int totalBundle = desiredBundleSize == 0 ? 1 : (int) Math.ceil(itemCount / desiredBundleSize);
      List<CouchbaseSource> sources = new ArrayList<>(totalBundle);
      for (int i = 0, offset = 0; i < totalBundle; i++) {
        int lowerBound = offset;
        int upperBound = offset += (int) desiredBundleSize;
        if (i == totalBundle - 1) {
          upperBound = itemCount;
        }
        sources.add(new CouchbaseSource(spec, cluster, bucket, lowerBound, upperBound));
      }
      LOG.debug(String.format("The original source is split to %d sources.", sources.size()));
      return sources;
    }

    /**
     * Attention: The idea is to divide the {@link BoundedSource} by the number of documents. So we
     * do not estimate the total data size in bytes. In fact, we just fetch the total number of keys
     * to the target bucket.
     *
     * @param options Pipeline options
     * @return the number of keys to the bucket
     * @throws Exception throw {@link CouchbaseIOException} when querying Couchbase
     */
    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      String query = String.format("SELECT RAW COUNT(META().id) FROM `%s`", getBucket().name());
      LOG.debug(query);
      N1qlQueryResult result = getBucket().query(N1qlQuery.simple(query));
      if (!result.finalSuccess()) {
        throw new CouchbaseIOException(result.errors().get(0).getString("msg"));
      }
      itemCount =
          Integer.valueOf(
              new String(result.allRows().get(0).byteValue(), Charset.defaultCharset()));
      return itemCount;
    }

    @Override
    public BoundedReader<JsonDocument> createReader(PipelineOptions options) {
      return new CouchbaseReader(this);
    }

    void close() {
      if (cluster != null) {
        cluster.disconnect();
      }
      if (bucket != null) {
        bucket.close();
      }
    }
  }

  private static class CouchbaseReader extends BoundedSource.BoundedReader<JsonDocument> {

    private final CouchbaseSource source;
    private Iterator<N1qlQueryRow> keyIterator;
    private String currentKey;

    CouchbaseReader(CouchbaseSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      // Fetch the keys between the range
      String query =
          String.format(
              "SELECT RAW META().id FROM `%s` OFFSET %d LIMIT %d",
              source.spec.bucket(), source.lowerBound, source.upperBound - source.lowerBound);
      LOG.debug(
          String.format(
              "Couchbase reader [%d, %d): %s", source.lowerBound, source.upperBound, query));
      N1qlQueryResult result = source.bucket.query(N1qlQuery.simple(query));
      if (!result.finalSuccess()) {
        throw new CouchbaseIOException(result.errors().get(0).getString("msg"));
      }
      List<N1qlQueryRow> keys = result.allRows();
      keyIterator = keys.iterator();
      return advance();
    }

    @Override
    public boolean advance() {
      if (keyIterator.hasNext()) {
        currentKey = new String(keyIterator.next().byteValue(), Charset.defaultCharset());
        // Need to remove the replicated quotes around the key (""key"" => "key").
        // The type of key is limited to String.
        currentKey = currentKey.substring(1, currentKey.length() - 1);
        return true;
      }
      return false;
    }

    @Override
    public JsonDocument getCurrent() throws NoSuchElementException {
      return source.bucket.get(currentKey);
    }

    @Override
    public void close() {
      // Delegate the disconnection of Couchbase to the method "expand" of CouchbaseSource,
      // because the Couchbase client instance is shared by all the threads.
    }

    @Override
    public BoundedSource<JsonDocument> getCurrentSource() {
      return source;
    }
  }
}
