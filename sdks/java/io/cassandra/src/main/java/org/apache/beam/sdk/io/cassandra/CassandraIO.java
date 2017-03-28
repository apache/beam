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
package org.apache.beam.sdk.io.cassandra;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;

import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IO to read from Apache Cassandra.
 *
 * <h3>Reading from Apache Cassandra</h3>
 *
 * <p>{@code CassandraIO} provides a source to read and returns a bounded collection of
 * entities as {@code PCollection<Entity>}. An entity is built by Cassandra mapper
 * ({@link com.datastax.driver.mapping.EntityMapper}) based on a
 * POJO containing annotations (as described {@see <a href="http://docs.datastax
 * .com/en/developer/java-driver/2.1/manual/object_mapper/creating/">http://docs.datastax
 * .com/en/developer/java-driver/2.1/manual/object_mapper/creating/</a>}).
 *
 * <p>The following example illustrates various options for configuring the IO:
 *
 * <pre>{@code
 * pipeline.apply(CassandraIO.<Person>read()
 *     .withHosts(Arrays.asList("host1", "host2"))
 *     .withPort(9042)
 *     .withKeyspace("beam")
 *     .withTable("Person")
 *     .withEntity(Person.class)
 *     .withCoder(SerializableCoder.of(Person.class))
 *     // above options are the minimum set, returns PCollection<Person>
 *
 * }</pre>
 */
public class CassandraIO {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraIO.class);

  private CassandraIO() {}

  public static <T> Read<T> read() {
    return new AutoValue_CassandraIO_Read.Builder<T>().build();
  }

  /**
   * A {@link PTransform} to read data from Apache Cassandra. See {@link CassandraIO} for more
   * information on usage and configuration.
   */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    @Nullable abstract List<String> hosts();
    @Nullable abstract Integer port();
    @Nullable abstract String keyspace();
    @Nullable abstract String table();
    @Nullable abstract Class<T> entity();
    @Nullable abstract Coder<T> coder();
    @Nullable abstract CassandraService<T> cassandraService();
    abstract Builder<T> builder();

    public Read<T> withHosts(List<String> hosts) {
      checkArgument(hosts != null, "CassandraIO.read().withHosts(hosts) called with null hosts");
      checkArgument(hosts.isEmpty(), "CassandraIO.read().withHosts(hosts) called with empty "
          + "hosts list");
      return builder().setHosts(hosts).build();
    }

    public Read<T> withPort(int port) {
      checkArgument(port > 0, "CassandraIO.read().withPort(port) called with invalid port "
          + "number (%d)", port);
      return builder().setPort(port).build();
    }

    public Read<T> withKeyspace(String keyspace) {
      checkArgument(keyspace != null, "CassandraIO.read().withKeyspace(keyspace) called with "
          + "null keyspace");
      return builder().setKeyspace(keyspace).build();
    }

    public Read<T> withTable(String table) {
      checkArgument(table != null, "CassandraIO.read().withTable(table) called with null table");
      return builder().setTable(table).build();
    }

    public Read<T> withEntity(Class<T> entity) {
      checkArgument(entity != null, "CassandraIO.read().withEntity(entity) called with null "
          + "entity");
      return builder().setEntity(entity).build();
    }

    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "CassandraIO.read().withCoder(coder) called with null coder");
      return builder().setCoder(coder).build();
    }

    public Read<T> withCassandraService(CassandraService<T> cassandraService) {
      checkArgument(cassandraService != null, "CassandraIO.read().withCassandraService(service)"
          + " called with null service");
      return builder().setCassandraService(cassandraService).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      return input.apply(org.apache.beam.sdk.io.Read.from(
          new CassandraSource<T>(this, null)));
    }

    @Override
    public void validate(PBegin input) {
      checkState(hosts() != null || cassandraService() != null,
          "CassandraIO.read() requires a list of hosts to be set via withHosts(hosts) or a "
              + "Cassandra service to be set via withCassandraService(service)");
      checkState(port() != null || cassandraService() != null, "CassandraIO.read() requires a "
          + "valid port number to be set via withPort(port) or a Cassandra service to be set via "
          + "withCassandraService(service)");
      checkState(keyspace() != null, "CassandraIO.read() requires a keyspace to be set via "
          + "withKeyspace(keyspace)");
      checkState(table() != null, "CassandraIO.read() requires a table to be set via "
          + "withTable(table)");
      checkState(entity() != null, "CassandraIO.read() requires an entity to be set via "
          + "withEntity(entity)");
      checkState(coder() != null, "CassandraIO.read() requires a coder to be set via "
          + "withCoder(coder)");
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setHosts(List<String> hosts);
      abstract Builder<T> setPort(Integer port);
      abstract Builder<T> setKeyspace(String keyspace);
      abstract Builder<T> setTable(String table);
      abstract Builder<T> setEntity(Class<T> entity);
      abstract Builder<T> setCoder(Coder<T> coder);
      abstract Builder<T> setCassandraService(CassandraService<T> cassandraService);
      abstract Read<T> build();
    }

    /**
     * Helper function to either get a fake/mock Cassandra service provided by
     * {@link #withCassandraService(CassandraService)} or creates and returns an implementation
     * of a concrete Cassandra service dealing with a Cassandra instance.
     */
    @VisibleForTesting
    CassandraService<T> getCassandraService(PipelineOptions pipelineOptions) {
      if (cassandraService() != null) {
        return cassandraService();
      }
      return new CassandraServiceImpl<>();
    }

  }

  @VisibleForTesting
  static class CassandraSource<T> extends BoundedSource<T> {

    protected final Read<T> spec;
    protected final String splitQuery;

    CassandraSource(Read<T> spec,
                    String splitQuery) {
      this.spec = spec;
      this.splitQuery = splitQuery;
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return spec.coder();
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public BoundedReader<T> createReader(PipelineOptions pipelineOptions) {
      return spec.getCassandraService(pipelineOptions).createReader(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws Exception {
      return spec.getCassandraService(pipelineOptions).getEstimatedSizeBytes(spec);
    }

    @Override
    public List<BoundedSource<T>> split(long desiredBundleSizeBytes,
                                                   PipelineOptions pipelineOptions) {
      return spec.getCassandraService(pipelineOptions)
          .split(spec, desiredBundleSizeBytes);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      if (spec.hosts() != null) {
        builder.add(DisplayData.item("hosts", spec.hosts().toString()));
      }
      if (spec.port() != null) {
        builder.add(DisplayData.item("port", spec.port()));
      }
      builder.addIfNotNull(DisplayData.item("keyspace", spec.keyspace()));
      builder.addIfNotNull(DisplayData.item("table", spec.table()));
    }
  }

}
