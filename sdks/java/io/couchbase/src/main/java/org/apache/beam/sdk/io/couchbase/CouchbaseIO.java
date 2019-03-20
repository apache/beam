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

import java.io.IOException;
import java.util.Collections;
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

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.google.auto.value.AutoValue;

/** Couchbase IO. */
public class CouchbaseIO {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseIO.class);

  private CouchbaseIO() {}

  /** Provide a {@link Read} {@link PTransform} to read data from a Cassandra database. */
  public static Read read() {
    return new AutoValue_CouchbaseIO_Read.Builder().build();
  }

  /**
   * Read class.
   *
   * @param
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection> {
    @Nullable
    abstract List<String> hosts();

    @Nullable
    abstract Integer port();

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
    abstract String localDc();

    @Nullable
    abstract String consistencyLevel();

    @Nullable
    abstract String where();

    @Nullable
    abstract Integer minNumberOfSplits();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setHosts(List<String> hosts);

      abstract Builder setPort(Integer port);

      abstract Builder setBucket(String bucket);

      abstract Builder setEntity(Class entity);

      abstract Builder setCoder(Coder coder);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract Builder setLocalDc(String localDc);

      abstract Builder setConsistencyLevel(String consistencyLevel);

      abstract Builder setWhere(String where);

      abstract Builder setMinNumberOfSplits(Integer minNumberOfSplits);

      abstract Read build();
    }

    public Read withHosts(List<String> hosts) {
      checkArgument(hosts != null, "hosts can not be null");
      checkArgument(!hosts.isEmpty(), "hosts can not be empty");
      return builder().setHosts(hosts).build();
    }

    public Read withPort(int port) {
      checkArgument(port > 0, "port must be > 0, but was: %s", port);
      return builder().setPort(port).build();
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

    /** Specify the local DC used for the load balancing. */
    public Read withLocalDc(String localDc) {
      checkArgument(localDc != null, "localDc can not be null");
      return builder().setLocalDc(localDc).build();
    }

    public Read withConsistencyLevel(String consistencyLevel) {
      checkArgument(consistencyLevel != null, "consistencyLevel can not be null");
      return builder().setConsistencyLevel(consistencyLevel).build();
    }

    public Read withWhere(String where) {
      checkArgument(where != null, "where can not be null");
      return builder().setWhere(where).build();
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
      checkArgument((hosts() != null && port() != null), "WithHosts() and withPort() are required");
      checkArgument(bucket() != null, "withBucket() is required");
      checkArgument(entity() != null, "withEntity() is required");
      checkArgument(coder() != null, "withCoder() is required");

      return input.apply(org.apache.beam.sdk.io.Read.from(new CouchbaseSource(this, null)));
    }
  }

  @VisibleForTesting
  static class CouchbaseSource extends BoundedSource<com.couchbase.client.java.query.N1qlQueryRow> {

    final Read spec;
    final String query;

    CouchbaseSource(Read spec, String query) {
      this.spec = spec;
      this.query = query;
    }

    @Override
    public Coder<com.couchbase.client.java.query.N1qlQueryRow> getOutputCoder() {
      return spec.coder();
    }

    @Override
    public List<? extends BoundedSource<com.couchbase.client.java.query.N1qlQueryRow>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      // Couchbase does not support query splitting, so just use the query all.
      String query = String.format("SELECT * FROM %s", spec.bucket());
      return Collections.singletonList(
              new CouchbaseIO.CouchbaseSource(spec, query));
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0;
    }

    @Override
    public BoundedReader<com.couchbase.client.java.query.N1qlQueryRow> createReader(PipelineOptions options) throws IOException {
      return new CouchbaseReader(this);
    }
  }

  private static class CouchbaseReader extends BoundedSource.BoundedReader<N1qlQueryRow> {

    private final CouchbaseIO.CouchbaseSource source;
    private Cluster cluster;
    private Bucket bucket;
    private Iterator<N1qlQueryRow> iterator;
    private N1qlQueryRow current;

    CouchbaseReader(CouchbaseSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Start the Couchbase reader");
      }
      cluster = CouchbaseCluster.create(source.spec.hosts())
                                .authenticate(source.spec.username(), source.spec.password());
      bucket = cluster.openBucket(source.spec.bucket());
      N1qlQueryResult result = bucket.query(N1qlQuery.parameterized(source.query, JsonObject.empty()));
      iterator = result.iterator();
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      }
      return false;
    }

    @Override
    public N1qlQueryRow getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public void close() throws IOException {
      if (cluster != null) {
        cluster.disconnect();
      }
      if (bucket != null) {
        bucket.close();
      }
    }

    @Override
    public BoundedSource<N1qlQueryRow> getCurrentSource() {
      return source;
    }
  }
}
