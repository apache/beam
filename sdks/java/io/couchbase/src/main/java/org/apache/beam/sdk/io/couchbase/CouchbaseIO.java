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
import com.couchbase.client.java.env.CouchbaseEnvironment;
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
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Couchbase IO. */
public class CouchbaseIO {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseIO.class);

  private CouchbaseIO() {}

  /** Provide a {@link Read} {@link PTransform} to read data from a Cassandra database. */
  public static Read read() {
    return new AutoValue_CouchbaseIO_Read.Builder().build();
  }

  /** Read the data and generate a PCollection. */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection> {
    @Nullable
    abstract List<String> hosts();

    @Nullable
    abstract Integer httpPort();

    @Nullable
    abstract Integer carrierPort();

    @Nullable
    abstract String bucket();

    @Nullable
    abstract Class entity();

    @Nullable
    abstract Coder coder();

    @Nullable
    abstract String username();

    @Nullable
    abstract String password();

    @Nullable
    abstract Integer minNumberOfSplits();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setHosts(List<String> hosts);

      abstract Builder setHttpPort(Integer port);

      abstract Builder setCarrierPort(Integer port);

      abstract Builder setBucket(String bucket);

      abstract Builder setEntity(Class entity);

      abstract Builder setCoder(Coder coder);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract Builder setMinNumberOfSplits(Integer minNumberOfSplits);

      abstract Read build();
    }

    public Read withHosts(List<String> hosts) {
      checkArgument(hosts != null, "hosts can not be null");
      checkArgument(!hosts.isEmpty(), "hosts can not be empty");
      return builder().setHosts(hosts).build();
    }

    public Read withHttpPort(int port) {
      checkArgument(port > 0, "httpPort must be > 0, but was: %s", port);
      return builder().setHttpPort(port).build();
    }

    public Read withCarrierPort(int port) {
      checkArgument(port > 0, "carrierPort must be > 0, but was: %s", port);
      return builder().setCarrierPort(port).build();
    }

    public Read withBucket(String bucket) {
      checkArgument(bucket != null, "bucket can not be null");
      return builder().setBucket(bucket).build();
    }

    public Read withEntity(Class entity) {
      checkArgument(entity != null, "entity can not be null");
      return builder().setEntity(entity).build();
    }

    public Read withCoder(Coder coder) {
      checkArgument(coder != null, "coder can not be null");
      return builder().setCoder(coder).build();
    }

    public Read withUsername(String username) {
      checkArgument(username != null, "username can not be null");
      return builder().setUsername(username).build();
    }

    public Read withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      return builder().setPassword(password).build();
    }

    /**
     * It's possible that system.size_estimates isn't populated or that the number of splits
     * computed by Beam is still to low for Cassandra to handle it. This setting allows to enforce a
     * minimum number of splits in case Beam cannot compute it correctly.
     */
    public Read withMinNumberOfSplits(Integer minNumberOfSplits) {
      checkArgument(minNumberOfSplits != null, "minNumberOfSplits can not be null");
      checkArgument(minNumberOfSplits > 0, "minNumberOfSplits must be greater than 0");
      return builder().setMinNumberOfSplits(minNumberOfSplits).build();
    }

    @Override
    public PCollection expand(PBegin input) {
      checkArgument(
          (hosts() != null && httpPort() != null && carrierPort() != null),
          "WithHosts(), withHttpPort() and withCarrierPort() are required");
      checkArgument(bucket() != null, "withBucket() is required");
      checkArgument(entity() != null, "withEntity() is required");
      checkArgument(coder() != null, "withCoder() is required");

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

    @Override
    public Coder<JsonDocument> getOutputCoder() {
      return spec.coder();
    }

    @Override
    public List<? extends BoundedSource<JsonDocument>> split(
        long desiredBundleSize, PipelineOptions options) throws Exception {
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
      return sources;
    }

    private void connectToCouchbase() {
      CouchbaseEnvironment env =
          DefaultCouchbaseEnvironment.builder()
              .bootstrapHttpDirectPort(spec.httpPort())
              .bootstrapCarrierDirectPort(spec.carrierPort())
              .build();
      if (this.cluster == null) {
        this.cluster =
            CouchbaseCluster.create(env, spec.hosts())
                .authenticate(spec.username(), spec.password());
      }
      if (this.bucket == null) {
        this.bucket = cluster.openBucket(spec.bucket());
      }
    }

    /**
     * The idea is to divide the source by the number of documents. So here we try to fetch the
     * total number of keys to the bucket
     *
     * @param options Pipeline options
     * @return The number of keys to the bucket
     * @throws Exception
     */
    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      // TODO WARNING: More than 1 Couchbase Environments found (3), this can have severe impact on
      // performance and stability. Reuse environments!
      connectToCouchbase();
      itemCount = bucket.bucketManager().info().raw().getObject("basicStats").getInt("itemCount");
      return itemCount;
    }

    @Override
    public BoundedReader<JsonDocument> createReader(PipelineOptions options) throws IOException {
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
      if (LOG.isDebugEnabled()) {
        LOG.debug("Start the Couchbase reader");
      }
      // Fetch the keys inside the range
      N1qlQueryResult result =
          source.bucket.query(
              N1qlQuery.simple(
                  String.format(
                      "SELECT RAW META().id FROM `%s` OFFSET %d LIMIT %d",
                      source.spec.bucket(),
                      source.lowerBound,
                      source.upperBound - source.lowerBound)));
      if (!result.finalSuccess()) {
        throw new CouchbaseIOException(result.errors().get(0).getString("msg"));
      }
      List<N1qlQueryRow> keys = result.allRows();
      keyIterator = keys.iterator();
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (keyIterator.hasNext()) {
        currentKey = new String(keyIterator.next().byteValue(), Charset.defaultCharset());
        // Need to remove the replicated quotes around the key (""key"" => "key").
        // The type of key is limited to String according to Couchbase.
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
    public void close() throws IOException {
      // Delegate the disconnection to the method "expand" of CouchbaseSource,
      // because the client instance is shared by all the thread.
    }

    @Override
    public BoundedSource<JsonDocument> getCurrentSource() {
      return source;
    }
  }
}
