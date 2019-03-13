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

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.List;
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

/**
 * Couchbase IO.
 */
public class CouchbaseIO {

  private static final Logger LOG = LoggerFactory.getLogger(CouchbaseIO.class);

  private CouchbaseIO() {}

  /** Provide a {@link Read} {@link PTransform} to read data from a Cassandra database. */
  public static <T> Read<T> read() {
    return new AutoValue_CouchbaseIO_Read.Builder<T>().build();
  }

  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable
    abstract List<String> hosts();

    @Nullable
    abstract Integer port();

    @Nullable
    abstract String bucket();

    @Nullable
    abstract Class<T> entity();

    @Nullable
    abstract Coder<T> coder();

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

    abstract Builder<T> builder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setHosts(List<String> hosts);

      abstract Builder<T> setPort(Integer port);

      abstract Builder<T> setBucket(String bucket);

      abstract Builder<T> setEntity(Class<T> entity);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setUsername(String username);

      abstract Builder<T> setPassword(String password);

      abstract Builder<T> setLocalDc(String localDc);

      abstract Builder<T> setConsistencyLevel(String consistencyLevel);

      abstract Builder<T> setWhere(String where);

      abstract Builder<T> setMinNumberOfSplits(Integer minNumberOfSplits);

      abstract Read<T> build();
    }

    public Read<T> withHosts(List<String> hosts) {
      checkArgument(hosts != null, "hosts can not be null");
      checkArgument(!hosts.isEmpty(), "hosts can not be empty");
      return builder().setHosts(hosts).build();
    }

    public Read<T> withPort(int port) {
      checkArgument(port > 0, "port must be > 0, but was: %s", port);
      return builder().setPort(port).build();
    }

    public Read<T> withBucket(String bucket) {
      checkArgument(bucket != null, "bucket can not be null");
      return builder().setBucket(bucket).build();
    }

    public Read<T> withEntity(Class<T> entity) {
      checkArgument(entity != null, "entity can not be null");
      return builder().setEntity(entity).build();
    }

    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "coder can not be null");
      return builder().setCoder(coder).build();
    }

    public Read<T> withUsername(String username) {
      checkArgument(username != null, "username can not be null");
      return builder().setUsername(username).build();
    }

    public Read<T> withPassword(String password) {
      checkArgument(password != null, "password can not be null");
      return builder().setPassword(password).build();
    }

    /** Specify the local DC used for the load balancing. */
    public Read<T> withLocalDc(String localDc) {
      checkArgument(localDc != null, "localDc can not be null");
      return builder().setLocalDc(localDc).build();
    }

    public Read<T> withConsistencyLevel(String consistencyLevel) {
      checkArgument(consistencyLevel != null, "consistencyLevel can not be null");
      return builder().setConsistencyLevel(consistencyLevel).build();
    }

    public Read<T> withWhere(String where) {
      checkArgument(where != null, "where can not be null");
      return builder().setWhere(where).build();
    }

    /**
     * It's possible that system.size_estimates isn't populated or that the number of splits
     * computed by Beam is still to low for Cassandra to handle it. This setting allows to enforce a
     * minimum number of splits in case Beam cannot compute it correctly.
     */
    public Read<T> withMinNumberOfSplits(Integer minNumberOfSplits) {
      checkArgument(minNumberOfSplits != null, "minNumberOfSplits can not be null");
      checkArgument(minNumberOfSplits > 0, "minNumberOfSplits must be greater than 0");
      return builder().setMinNumberOfSplits(minNumberOfSplits).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      checkArgument((hosts() != null && port() != null), "WithHosts() and withPort() are required");
      checkArgument(bucket() != null, "withBucket() is required");
      checkArgument(entity() != null, "withEntity() is required");
      checkArgument(coder() != null, "withCoder() is required");

      return input.apply(org.apache.beam.sdk.io.Read.from(new CouchbaseSource<>(this, null)));
    }
  }

  @VisibleForTesting
  static class CouchbaseSource<T> extends BoundedSource<T> {

    final Read<T> spec;
    final List<String> splitQueries;

    CouchbaseSource(Read<T> spec, List<String> splitQueries) {
      this.spec = spec;
      this.splitQueries = splitQueries;
    }

    @Override
    public Coder<T> getOutputCoder() {
      return spec.coder();
    }

    @Override
    public List<? extends BoundedSource<T>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return null;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0;
    }

    @Override
    public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
      return null;
    }
  }
}
