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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An IO to read from Apache Cassandra.
 *
 * <h3>Reading from Apache Cassandra</h3>
 *
 * <p>{@code CassandraIO} provides a source to read and returns a bounded collection of
 * entities as {@code PCollection<Entity>}. An entity is built by Cassandra mapper
 * ({@code com.datastax.driver.mapping.EntityMapper}) based on a
 * POJO containing annotations (as described http://docs.datastax
 * .com/en/developer/java-driver/2.1/manual/object_mapper/creating/").
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
 *
 * <h3>Writing to Apache Cassandra</h3>
 *
 * <p>{@code CassandraIO} provides a sink to write a collection of entities to Apache Cassandra.
 *
 * <p>The following example illustrates various options for configuring the IO write:
 *
 * <pre>{@code
 * pipeline
 *    .apply(...) // provides a PCollection<Person> where Person is an entity
 *    .apply(CassandraIO.<Person>write()
 *        .withHosts(Arrays.asList("host1", "host2"))
 *        .withPort(9042)
 *        .withKeyspace("beam")
 *        .withEntity(Person.class));
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class CassandraIO {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraIO.class);

  private CassandraIO() {}

  /**
   * Provide a {@link Read} {@link PTransform} to read data from a Cassandra database.
   */
  public static <T> Read<T> read() {
    return new AutoValue_CassandraIO_Read.Builder<T>().build();
  }

  /**
   * Provide a {@link Write} {@link PTransform} to write data to a Cassandra database.
   */
  public static <T> Write<T> write() {
    return new AutoValue_CassandraIO_Write.Builder<T>().build();
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
    @Nullable abstract String username();
    @Nullable abstract String password();
    @Nullable abstract String localDc();
    @Nullable abstract String consistencyLevel();
    @Nullable abstract CassandraService<T> cassandraService();
    abstract Builder<T> builder();

    /**
     * Specify the hosts of the Apache Cassandra instances.
     */
    public Read<T> withHosts(List<String> hosts) {
      checkArgument(hosts != null, "CassandraIO.read().withHosts(hosts) called with null hosts");
      checkArgument(!hosts.isEmpty(), "CassandraIO.read().withHosts(hosts) called with empty "
          + "hosts list");
      return builder().setHosts(hosts).build();
    }

    /**
     * Specify the port number of the Apache Cassandra instances.
     */
    public Read<T> withPort(int port) {
      checkArgument(port > 0, "CassandraIO.read().withPort(port) called with invalid port "
          + "number (%d)", port);
      return builder().setPort(port).build();
    }

    /**
     * Specify the Cassandra keyspace where to read data.
     */
    public Read<T> withKeyspace(String keyspace) {
      checkArgument(keyspace != null, "CassandraIO.read().withKeyspace(keyspace) called with "
          + "null keyspace");
      return builder().setKeyspace(keyspace).build();
    }

    /**
     * Specify the Cassandra table where to read data.
     */
    public Read<T> withTable(String table) {
      checkArgument(table != null, "CassandraIO.read().withTable(table) called with null table");
      return builder().setTable(table).build();
    }

    /**
     * Specify the entity class (annotated POJO). The {@link CassandraIO} will read the data and
     * convert the data as entity instances. The {@link PCollection} resulting from the read will
     * contains entity elements.
     */
    public Read<T> withEntity(Class<T> entity) {
      checkArgument(entity != null, "CassandraIO.read().withEntity(entity) called with null "
          + "entity");
      return builder().setEntity(entity).build();
    }

    /**
     * Specify the {@link Coder} used to serialize the entity in the {@link PCollection}.
     */
    public Read<T> withCoder(Coder<T> coder) {
      checkArgument(coder != null, "CassandraIO.read().withCoder(coder) called with null coder");
      return builder().setCoder(coder).build();
    }

    /**
     * Specify the username for authentication.
     */
    public Read<T> withUsername(String username) {
      checkArgument(username != null, "CassandraIO.read().withUsername(username) called with "
          + "null username");
      return builder().setUsername(username).build();
    }

    /**
     * Specify the password for authentication.
     */
    public Read<T> withPassword(String password) {
      checkArgument(password != null, "CassandraIO.read().withPassword(password) called with "
          + "null password");
      return builder().setPassword(password).build();
    }

    /**
     * Specify the local DC used for the load balancing.
     */
    public Read<T> withLocalDc(String localDc) {
      checkArgument(localDc != null, "CassandraIO.read().withLocalDc(localDc) called with null "
          + "localDc");
      return builder().setLocalDc(localDc).build();
    }

    public Read<T> withConsistencyLevel(String consistencyLevel) {
      checkArgument(consistencyLevel != null, "CassandraIO.read().withConsistencyLevel"
          + "(consistencyLevel) called with null consistencyLevel");
      return builder().setConsistencyLevel(consistencyLevel).build();
    }

    /**
     * Specify an instance of {@link CassandraService} used to connect and read from Cassandra
     * database.
     */
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
    public void validate(PipelineOptions pipelineOptions) {
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
      abstract Builder<T> setUsername(String username);
      abstract Builder<T> setPassword(String password);
      abstract Builder<T> setLocalDc(String localDc);
      abstract Builder<T> setConsistencyLevel(String consistencyLevel);
      abstract Builder<T> setCassandraService(CassandraService<T> cassandraService);
      abstract Read<T> build();
    }

    /**
     * Helper function to either get a fake/mock Cassandra service provided by
     * {@link #withCassandraService(CassandraService)} or creates and returns an implementation
     * of a concrete Cassandra service dealing with a Cassandra instance.
     */
    @VisibleForTesting
    CassandraService<T> getCassandraService() {
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
    public Coder<T> getOutputCoder() {
      return spec.coder();
    }

    @Override
    public void validate() {
      spec.validate(null);
    }

    @Override
    public BoundedReader<T> createReader(PipelineOptions pipelineOptions) {
      return spec.getCassandraService().createReader(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions pipelineOptions) throws Exception {
      return spec.getCassandraService().getEstimatedSizeBytes(spec);
    }

    @Override
    public List<BoundedSource<T>> split(long desiredBundleSizeBytes,
                                                   PipelineOptions pipelineOptions) {
      return spec.getCassandraService()
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
      builder.addIfNotNull(DisplayData.item("username", spec.username()));
      builder.addIfNotNull(DisplayData.item("localDc", spec.localDc()));
      builder.addIfNotNull(DisplayData.item("consistencyLevel", spec.consistencyLevel()));
    }
  }

  /**
   * A {@link PTransform} to write into Apache Cassandra. See {@link CassandraIO} for details on
   * usage and configuration.
   */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

    @Nullable abstract List<String> hosts();
    @Nullable abstract Integer port();
    @Nullable abstract String keyspace();
    @Nullable abstract Class<T> entity();
    @Nullable abstract String username();
    @Nullable abstract String password();
    @Nullable abstract String localDc();
    @Nullable abstract String consistencyLevel();
    @Nullable abstract CassandraService<T> cassandraService();
    abstract Builder<T> builder();

    /**
     * Specify the Cassandra instance hosts where to write data.
     */
    public Write<T> withHosts(List<String> hosts) {
      checkArgument(hosts != null, "CassandraIO.write().withHosts(hosts) called with null hosts");
      checkArgument(!hosts.isEmpty(), "CassandraIO.write().withHosts(hosts) called with empty "
          + "hosts list");
      return builder().setHosts(hosts).build();
    }

    /**
     * Specify the Cassandra instance port number where to write data.
     */
    public Write<T> withPort(int port) {
      checkArgument(port > 0, "CassandraIO.write().withPort(port) called with invalid port "
          + "number (%d)", port);
      return builder().setPort(port).build();
    }

    /**
     * Specify the Cassandra keyspace where to write data.
     */
    public Write<T> withKeyspace(String keyspace) {
      checkArgument(keyspace != null, "CassandraIO.write().withKeyspace(keyspace) called with "
          + "null keyspace");
      return builder().setKeyspace(keyspace).build();
    }

    /**
     * Specify the entity class in the input {@link PCollection}. The {@link CassandraIO} will
     * map this entity to the Cassandra table thanks to the annotations.
     */
    public Write<T> withEntity(Class<T> entity) {
      checkArgument(entity != null, "CassandraIO.write().withEntity(entity) called with null "
          + "entity");
      return builder().setEntity(entity).build();
    }

    /**
     * Specify the username used for authentication.
     */
    public Write<T> withUsername(String username) {
      checkArgument(username != null, "CassandraIO.write().withUsername(username) called with "
          + "null username");
      return builder().setUsername(username).build();
    }

    /**
     * Specify the password used for authentication.
     */
    public Write<T> withPassword(String password) {
      checkArgument(password != null, "CassandraIO.write().withPassword(password) called with "
          + "null password");
      return builder().setPassword(password).build();
    }

    /**
     * Specify the local DC used by the load balancing policy.
     */
    public Write<T> withLocalDc(String localDc) {
      checkArgument(localDc != null, "CassandraIO.write().withLocalDc(localDc) called with null"
          + " localDc");
      return builder().setLocalDc(localDc).build();
    }

    public Write<T> withConsistencyLevel(String consistencyLevel) {
      checkArgument(consistencyLevel != null, "CassandraIO.write().withConsistencyLevel"
          + "(consistencyLevel) called with null consistencyLevel");
      return builder().setConsistencyLevel(consistencyLevel).build();
    }

    /**
     * Specify the {@link CassandraService} used to connect and write into the Cassandra database.
     */
    public Write<T> withCassandraService(CassandraService<T> cassandraService) {
      checkArgument(cassandraService != null, "CassandraIO.write().withCassandraService"
          + "(service) called with null service");
      return builder().setCassandraService(cassandraService).build();
    }

    @Override
    public void validate(PipelineOptions pipelineOptions) {
      checkState(hosts() != null || cassandraService() != null,
          "CassandraIO.write() requires a list of hosts to be set via withHosts(hosts) or a "
              + "Cassandra service to be set via withCassandraService(service)");
      checkState(port() != null || cassandraService() != null, "CassandraIO.write() requires a "
          + "valid port number to be set via withPort(port) or a Cassandra service to be set via "
          + "withCassandraService(service)");
      checkState(keyspace() != null, "CassandraIO.write() requires a keyspace to be set via "
          + "withKeyspace(keyspace)");
      checkState(entity() != null, "CassandraIO.write() requires an entity to be set via "
          + "withEntity(entity)");
    }

    @Override
    public PDone expand(PCollection<T> input) {
      input.apply(ParDo.of(new WriteFn<T>(this)));
      return PDone.in(input.getPipeline());
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setHosts(List<String> hosts);
      abstract Builder<T> setPort(Integer port);
      abstract Builder<T> setKeyspace(String keyspace);
      abstract Builder<T> setEntity(Class<T> entity);
      abstract Builder<T> setUsername(String username);
      abstract Builder<T> setPassword(String password);
      abstract Builder<T> setLocalDc(String localDc);
      abstract Builder<T> setConsistencyLevel(String consistencyLevel);
      abstract Builder<T> setCassandraService(CassandraService<T> cassandraService);
      abstract Write<T> build();
    }

    /**
     * Helper function to either get a fake/mock Cassandra service provided by
     * {@link #withCassandraService(CassandraService)} or creates and returns an implementation
     * of a concrete Cassandra service dealing with a Cassandra instance.
     */
    @VisibleForTesting
    CassandraService<T> getCassandraService() {
      if (cassandraService() != null) {
        return cassandraService();
      }
      return new CassandraServiceImpl<>();
    }

  }

  private static class WriteFn<T> extends DoFn<T, Void> {

    private final Write<T> spec;
    private CassandraService.Writer writer;

    public WriteFn(Write<T> spec) {
      this.spec = spec;
    }

    @Setup
    public void setup() throws Exception {
      writer = spec.getCassandraService().createWriter(spec);
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {
      T entity = processContext.element();
      writer.write(entity);
    }

    @Teardown
    public void teardown() throws Exception {
      writer.close();
      writer = null;
    }

  }

}
