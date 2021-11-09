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
package org.apache.beam.sdk.io.neo4j;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.TransactionWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a Beam IO to read from, and write data to, Neo4j.
 *
 * <p>
 *
 * <p>
 *
 * <h3>Driver configuration</h3>
 *
 * <p>To read from or write to Neo4j you have to provide a {@link DriverConfiguration} using<br>
 * 1. {@link DriverConfiguration#create()} (which must be {@link Serializable});<br>
 * 2. or {@link DriverConfiguration#create(String, String, String)} (URL, username and password).
 *
 * <p>If you have trouble connecting to a Neo4j Aura database please try to disable a few security
 * algorithms in your JVM. This makes sure that the right one is picked to connect:
 *
 * <p>
 *
 * <pre>{@code
 * Security.setProperty(
 *       "jdk.tls.disabledAlgorithms",
 *       "SSLv3, RC4, DES, MD5withRSA, DH keySize < 1024, EC keySize < 224, 3DES_EDE_CBC, anon, NULL");
 * }</pre>
 *
 * <p>
 *
 * <p>
 *
 * <p>To execute this code on GCP Dataflow you can create a class which extends {@link
 * JvmInitializer} and implement the {@link JvmInitializer#onStartup()} method. You need to annotate
 * this new class with {@link com.google.auto.service.AutoService}
 *
 * <pre>{@code
 * @AutoService(value = JvmInitializer.class)
 * }</pre>
 *
 * <p>
 *
 * <h3>Reading from Neo4j</h3>
 *
 * <p>{@link Neo4jIO#readAll()} source returns a bounded collection of {@code OuptutT} as a {@code
 * PCollection<OutputT>}. OutputT is the type returned by the provided {@link RowMapper}. It accepts
 * parameters as input in the form of {@code ParameterT} as a {@code PCollection<ParameterT>}
 *
 * <p>The following example reads ages to return the IDs of Person nodes. It runs a Cypher query for
 * each provided age.
 *
 * <p>The {@link ParametersMapper} maps input values to each execution of the Cypher statement.
 * These parameter values are stored in a {@link Map} (mapping String to Object). The map is cleared
 * after each execution.
 *
 * <p>The {@link RowMapper} converts output Neo4j {@link Record} values to the output of the source.
 *
 * <pre>{@code
 * pipeline
 *   .apply(Create.of(40, 50, 60))
 *   .apply(Neo4jIO.<Integer, String>readAll()
 *     .withDriverConfiguration(Neo4jIO.DriverConfiguration.create("neo4j://localhost:7687", "neo4j", "password"))
 *     .withCypher("MATCH(n:Person) WHERE n.age = $age RETURN n.id")
 *     .withReadTransaction()
 *     .withCoder(StringUtf8Coder.of())
 *     .withParametersMapper((age, rowMap) -> rowMap.put("age", age))
 *     .withRowMapper( record -> return record.get(0).asString() )
 *   );
 * }</pre>
 *
 * <h3>Writing to Neo4j</h3>
 *
 * <p>Neo4j sink supports writing data to a graph. It writes a {@link PCollection} to the graph by
 * collecting a batch of rows after which all rows in this batch are written together to Neo4j. This
 * is done using the {@link WriteUnwind} sink transform.
 *
 * <p>Like the source, to configure the sink, you have to provide a {@link DriverConfiguration}.
 *
 * <p>In the following example we'll merge a collection of {@link org.apache.beam.sdk.values.Row}
 * into Person nodes. Since this is a Sink it has no output and as such no RowMapper is needed. The
 * rows are being used as a container for the parameters of the Cypher statement. The used Cypher in
 * question needs to be an UNWIND statement. Like in the read case, the {@link ParametersMapper}
 * maps values to a {@link Map<String, Object>}. The difference is that the resulting Map is stored
 * in a {@link List} (containing maps) which in turn is stored in another Map under the name
 * provided by the {@link WriteUnwind#withUnwindMapName(String)} method. All of this is handled
 * automatically. You do need to provide the unwind map name so that you can reference that in the
 * UNWIND statement.
 *
 * <p>
 *
 * <p>For example:
 *
 * <pre>{@code
 * pipeline
 *   .apply(...)
 *   .apply(Neo4jIO.<Row>writeUnwind()
 *      .withDriverConfiguration(Neo4jIO.DriverConfiguration.create("neo4j://localhost:7687", "neo4j", "password"))
 *      .withUnwindMapName("rows")
 *      .withCypher("UNWIND $rows AS row MERGE(n:Person { id : row.id } ) SET n.firstName = row.first, n.lastName = row.last")
 *      .withParametersMapper( (row, map ) -> {
 *        map.put("id", row.getString("id));
 *        map.put("first", row.getString("firstName"));
 *        map.put("last", row.getString("lastName"));
 *      })
 *    );
 * }</pre>
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class Neo4jIO {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jIO.class);

  /**
   * Read all rows using a Neo4j Cypher query.
   *
   * @param <ParameterT> Type of the data representing query parameters.
   * @param <OutputT> Type of the data to be read.
   */
  public static <ParameterT, OutputT> ReadAll<ParameterT, OutputT> readAll() {
    return new AutoValue_Neo4jIO_ReadAll.Builder<ParameterT, OutputT>()
        .setFetchSize(ValueProvider.StaticValueProvider.of(Config.defaultConfig().fetchSize()))
        .build();
  }

  /**
   * Write all rows using a Neo4j Cypher UNWIND cypher statement. This sets a default batch batch
   * size of 5000.
   *
   * @param <ParameterT> Type of the data representing query parameters.
   */
  public static <ParameterT> WriteUnwind<ParameterT> writeUnwind() {
    return new AutoValue_Neo4jIO_WriteUnwind.Builder<ParameterT>()
        .setBatchSize(ValueProvider.StaticValueProvider.of(5000L))
        .build();
  }

  private static <ParameterT, OutputT> PCollection<OutputT> getOutputPCollection(
      PCollection<ParameterT> input, DoFn<ParameterT, OutputT> writeFn, Coder<OutputT> coder) {
    PCollection<OutputT> output = input.apply(ParDo.of(writeFn)).setCoder(coder);

    try {
      TypeDescriptor<OutputT> typeDesc = coder.getEncodedTypeDescriptor();
      SchemaRegistry registry = input.getPipeline().getSchemaRegistry();
      Schema schema = registry.getSchema(typeDesc);
      output.setSchema(
          schema,
          typeDesc,
          registry.getToRowFunction(typeDesc),
          registry.getFromRowFunction(typeDesc));
    } catch (NoSuchSchemaException e) {
      // ignore
    }
    return output;
  }

  /**
   * An interface used by {@link ReadAll} for converting each row of a Neo4j {@link Result} record
   * {@link Record} into an element of the resulting {@link PCollection}.
   */
  @FunctionalInterface
  public interface RowMapper<T> extends Serializable {
    T mapRow(Record record) throws Exception;
  }

  /**
   * An interface used by {@link ReadAll} for converting input parameter data to a parameters map
   * which can be used by your Neo4j cypher query.
   */
  @FunctionalInterface
  public interface ParametersMapper<ParameterT> extends Serializable {
    /**
     * Simply map an input record to a parameters map.
     *
     * @param input the input read
     * @param parametersMap the parameters map to update in this llabmda
     */
    void mapParameters(ParameterT input, Map<String, Object> parametersMap);
  }

  /** This describes all the information needed to create a Neo4j {@link Session}. */
  @AutoValue
  public abstract static class DriverConfiguration implements Serializable {
    public static DriverConfiguration create() {
      return new AutoValue_Neo4jIO_DriverConfiguration.Builder().build();
    }

    public static DriverConfiguration create(String url, String username, String password) {
      checkArgument(url != null, "url can not be null");
      checkArgument(username != null, "username can not be null");
      checkArgument(password != null, "password can not be null");
      return new AutoValue_Neo4jIO_DriverConfiguration.Builder()
          .build()
          .withUrl(url)
          .withUsername(username)
          .withPassword(password);
    }

    abstract @Nullable ValueProvider<String> getUrl();

    abstract @Nullable ValueProvider<List<String>> getUrls();

    abstract @Nullable ValueProvider<String> getUsername();

    abstract @Nullable ValueProvider<String> getPassword();

    abstract @Nullable ValueProvider<Boolean> getEncryption();

    abstract @Nullable ValueProvider<Long> getConnectionLivenessCheckTimeoutMs();

    abstract @Nullable ValueProvider<Long> getMaxConnectionLifetimeMs();

    abstract @Nullable ValueProvider<Integer> getMaxConnectionPoolSize();

    abstract @Nullable ValueProvider<Long> getConnectionAcquisitionTimeoutMs();

    abstract @Nullable ValueProvider<Long> getConnectionTimeoutMs();

    abstract @Nullable ValueProvider<Long> getMaxTransactionRetryTimeMs();

    abstract @Nullable ValueProvider<Boolean> getRouting();

    abstract Builder builder();

    // URL
    public DriverConfiguration withUrl(String url) {
      return withUrl(ValueProvider.StaticValueProvider.of(url));
    }

    public DriverConfiguration withUrl(ValueProvider<String> url) {
      Preconditions.checkArgument(
          url != null, "a neo4j connection URL can not be empty or null", url);
      Preconditions.checkArgument(
          StringUtils.isNotEmpty(url.get()),
          "a neo4j connection URL can not be empty or null",
          url);
      return builder().setUrl(url).build();
    }

    // URLS
    public DriverConfiguration withUrls(List<String> urls) {
      return withUrls(ValueProvider.StaticValueProvider.of(urls));
    }

    public DriverConfiguration withUrls(ValueProvider<List<String>> urls) {
      Preconditions.checkArgument(
          urls != null, "a list of neo4j connection URLs can not be empty or null", urls);
      Preconditions.checkArgument(
          urls.get() != null && !urls.get().isEmpty(),
          "a neo4j connection URL can not be empty or null",
          urls);
      return builder().setUrls(urls).build();
    }

    // Encryption
    public DriverConfiguration withEncryption() {
      return builder().setEncryption(ValueProvider.StaticValueProvider.of(Boolean.TRUE)).build();
    }

    public DriverConfiguration withoutEncryption() {
      return builder().setEncryption(ValueProvider.StaticValueProvider.of(Boolean.FALSE)).build();
    }

    // Connection Liveness Check Timout
    public DriverConfiguration withConnectionLivenessCheckTimeoutMs(
        long connectionLivenessCheckTimeoutMs) {
      return withConnectionLivenessCheckTimeoutMs(
          ValueProvider.StaticValueProvider.of(connectionLivenessCheckTimeoutMs));
    }

    public DriverConfiguration withConnectionLivenessCheckTimeoutMs(
        ValueProvider<Long> connectionLivenessCheckTimeoutMs) {
      return builder()
          .setConnectionLivenessCheckTimeoutMs(connectionLivenessCheckTimeoutMs)
          .build();
    }

    // Maximum Connection Lifetime
    public DriverConfiguration withMaxConnectionLifetimeMs(long maxConnectionLifetimeMs) {
      return withMaxConnectionLifetimeMs(
          ValueProvider.StaticValueProvider.of(maxConnectionLifetimeMs));
    }

    public DriverConfiguration withMaxConnectionLifetimeMs(
        ValueProvider<Long> maxConnectionLifetimeMs) {
      return builder().setMaxConnectionLifetimeMs(maxConnectionLifetimeMs).build();
    }

    // Maximum Connection pool size
    public DriverConfiguration withMaxConnectionPoolSize(int maxConnectionPoolSize) {
      return withMaxConnectionPoolSize(ValueProvider.StaticValueProvider.of(maxConnectionPoolSize));
    }

    public DriverConfiguration withMaxConnectionPoolSize(
        ValueProvider<Integer> maxConnectionPoolSize) {
      return builder().setMaxConnectionPoolSize(maxConnectionPoolSize).build();
    }

    // Connection Acq Timeout
    public DriverConfiguration withConnectionAcquisitionTimeoutMs(
        long connectionAcquisitionTimeoutMs) {
      return withConnectionAcquisitionTimeoutMs(
          ValueProvider.StaticValueProvider.of(connectionAcquisitionTimeoutMs));
    }

    public DriverConfiguration withConnectionAcquisitionTimeoutMs(
        ValueProvider<Long> connectionAcquisitionTimeoutMs) {
      return builder().setConnectionAcquisitionTimeoutMs(connectionAcquisitionTimeoutMs).build();
    }

    // Connection Timeout
    public DriverConfiguration withConnectionTimeoutMs(long connectionTimeoutMs) {
      return withConnectionTimeoutMs(ValueProvider.StaticValueProvider.of(connectionTimeoutMs));
    }

    public DriverConfiguration withConnectionTimeoutMs(ValueProvider<Long> connectionTimeoutMs) {
      return builder().setConnectionTimeoutMs(connectionTimeoutMs).build();
    }

    // Maximum Transaction Retry Time
    public DriverConfiguration withMaxTransactionRetryTimeMs(long maxTransactionRetryTimeMs) {
      return withMaxTransactionRetryTimeMs(
          ValueProvider.StaticValueProvider.of(maxTransactionRetryTimeMs));
    }

    public DriverConfiguration withMaxTransactionRetryTimeMs(
        ValueProvider<Long> maxTransactionRetryTimeMs) {
      return builder().setMaxTransactionRetryTimeMs(maxTransactionRetryTimeMs).build();
    }

    public DriverConfiguration withUsername(String username) {
      return withUsername(ValueProvider.StaticValueProvider.of(username));
    }

    public DriverConfiguration withUsername(ValueProvider<String> username) {
      Preconditions.checkArgument(username != null, "neo4j username can not be null", username);
      Preconditions.checkArgument(
          username.get() != null, "neo4j username can not be null", username);
      return builder().setUsername(username).build();
    }

    public DriverConfiguration withPassword(String password) {
      return withPassword(ValueProvider.StaticValueProvider.of(password));
    }

    public DriverConfiguration withPassword(ValueProvider<String> password) {
      Preconditions.checkArgument(password != null, "neo4j password can not be null", password);
      Preconditions.checkArgument(
          password.get() != null, "neo4j password can not be null", password);
      return builder().setPassword(password).build();
    }

    // Encryption
    public DriverConfiguration withRouting() {
      return builder().setRouting(ValueProvider.StaticValueProvider.of(Boolean.TRUE)).build();
    }

    public DriverConfiguration withoutRouting() {
      return builder().setRouting(ValueProvider.StaticValueProvider.of(Boolean.FALSE)).build();
    }

    void populateDisplayData(DisplayData.Builder builder) {
      builder.addIfNotNull(DisplayData.item("neo4j-url", getUrl()));
      builder.addIfNotNull(DisplayData.item("neo4j-username", getUsername()));
      builder.addIfNotNull(
          DisplayData.item(
              "neo4j-password", getPassword() != null ? "<provided>" : "<not-provided>"));
      builder.addIfNotNull(
          DisplayData.item(
              "neo4j-encryption", getEncryption() != null ? "<provided>" : "<not-provided>"));
    }

    Driver buildDriver() {
      // Create the Neo4j Driver
      // The default is: have the driver make the determination
      //
      Config.ConfigBuilder configBuilder = Config.builder();

      if (getEncryption() != null && getEncryption().get() != null) {
        if (getEncryption().get()) {
          configBuilder =
              Config.builder()
                  .withEncryption()
                  .withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
        } else {
          configBuilder = Config.builder().withoutEncryption();
        }
      }

      // physical layer
      if (getConnectionLivenessCheckTimeoutMs() != null
          && getConnectionLivenessCheckTimeoutMs().get() != null
          && getConnectionLivenessCheckTimeoutMs().get() > 0) {
        configBuilder =
            configBuilder.withConnectionLivenessCheckTimeout(
                getConnectionLivenessCheckTimeoutMs().get(), TimeUnit.MILLISECONDS);
      }
      if (getMaxConnectionLifetimeMs() != null
          && getMaxConnectionLifetimeMs().get() != null
          && getMaxConnectionLifetimeMs().get() > 0) {
        configBuilder =
            configBuilder.withMaxConnectionLifetime(
                getMaxConnectionLifetimeMs().get(), TimeUnit.MILLISECONDS);
      }
      if (getMaxConnectionPoolSize() != null && getMaxConnectionPoolSize().get() > 0) {
        configBuilder = configBuilder.withMaxConnectionPoolSize(getMaxConnectionPoolSize().get());
      }
      if (getConnectionAcquisitionTimeoutMs() != null
          && getConnectionAcquisitionTimeoutMs().get() != null
          && getConnectionAcquisitionTimeoutMs().get() > 0) {
        configBuilder =
            configBuilder.withConnectionAcquisitionTimeout(
                getConnectionAcquisitionTimeoutMs().get(), TimeUnit.MILLISECONDS);
      }
      if (getConnectionTimeoutMs() != null
          && getConnectionTimeoutMs().get() != null
          && getConnectionTimeoutMs().get() > 0) {
        configBuilder =
            configBuilder.withConnectionTimeout(
                getConnectionTimeoutMs().get(), TimeUnit.MILLISECONDS);
      }
      if (getMaxTransactionRetryTimeMs() != null
          && getMaxTransactionRetryTimeMs().get() != null
          && getMaxTransactionRetryTimeMs().get() > 0) {
        configBuilder =
            configBuilder.withMaxTransactionRetryTime(
                getMaxTransactionRetryTimeMs().get(), TimeUnit.MILLISECONDS);
      }

      // Set sane Logging level
      //
      configBuilder = configBuilder.withLogging(Logging.javaUtilLogging(Level.WARNING));

      // Now we have the configuration for the driver
      //
      Config config = configBuilder.build();

      // Get the list of the URI to connect with
      //
      List<URI> uris = new ArrayList<>();
      if (getUrl() != null && getUrl().get() != null) {
        try {
          uris.add(new URI(getUrl().get()));
        } catch (URISyntaxException e) {
          throw new RuntimeException("Error creating URI from URL '" + getUrl().get() + "'", e);
        }
      }
      if (getUrls() != null && getUrls().get() != null) {
        List<String> urls = getUrls().get();
        for (String url : urls) {
          try {
            uris.add(new URI(url));
          } catch (URISyntaxException e) {
            throw new RuntimeException(
                "Error creating URI '"
                    + getUrl().get()
                    + "' from a list of "
                    + urls.size()
                    + " URLs",
                e);
          }
        }
      }

      checkArgument(
          getUsername() != null && getUsername().get() != null,
          "please provide a username to connect to Neo4j");
      checkArgument(
          getPassword() != null && getPassword().get() != null,
          "please provide a password to connect to Neo4j");

      Driver driver;
      if (getRouting() != null && getRouting().get() != null && getRouting().get()) {
        driver =
            GraphDatabase.routingDriver(
                uris, AuthTokens.basic(getUsername().get(), getPassword().get()), config);
      } else {
        // Just take the first URI that was provided
        driver =
            GraphDatabase.driver(
                uris.get(0), AuthTokens.basic(getUsername().get(), getPassword().get()), config);
      }

      // Now we create a
      return driver;
    }

    /**
     * The Builder class below is not visible. We use it to service the "with" methods below the
     * Builder class.
     */
    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setUrl(ValueProvider<String> url);

      abstract Builder setUrls(ValueProvider<List<String>> url);

      abstract Builder setUsername(ValueProvider<String> username);

      abstract Builder setPassword(ValueProvider<String> password);

      abstract Builder setEncryption(ValueProvider<Boolean> encryption);

      abstract Builder setConnectionLivenessCheckTimeoutMs(
          ValueProvider<Long> connectionLivenessCheckTimeoutMs);

      abstract Builder setMaxConnectionLifetimeMs(ValueProvider<Long> maxConnectionLifetimeMs);

      abstract Builder setMaxConnectionPoolSize(ValueProvider<Integer> maxConnectionPoolSize);

      abstract Builder setConnectionAcquisitionTimeoutMs(
          ValueProvider<Long> connectionAcquisitionTimeoutMs);

      abstract Builder setConnectionTimeoutMs(ValueProvider<Long> connectionTimeoutMs);

      abstract Builder setMaxTransactionRetryTimeMs(ValueProvider<Long> maxTransactionRetryTimeMs);

      abstract Builder setRouting(ValueProvider<Boolean> routing);

      abstract DriverConfiguration build();
    }
  }

  /** This is the class which handles the work behind the {@link #readAll} method. */
  @AutoValue
  public abstract static class ReadAll<ParameterT, OutputT>
      extends PTransform<PCollection<ParameterT>, PCollection<OutputT>> {

    abstract @Nullable SerializableFunction<Void, Driver> getDriverProviderFn();

    abstract @Nullable ValueProvider<String> getDatabase();

    abstract @Nullable ValueProvider<String> getCypher();

    abstract @Nullable ValueProvider<Boolean> getWriteTransaction();

    abstract @Nullable ValueProvider<Long> getTransactionTimeoutMs();

    abstract @Nullable RowMapper<OutputT> getRowMapper();

    abstract @Nullable ParametersMapper<ParameterT> getParametersMapper();

    abstract @Nullable Coder<OutputT> getCoder();

    abstract @Nullable ValueProvider<Long> getFetchSize();

    abstract @Nullable ValueProvider<Boolean> getLogCypher();

    abstract Builder<ParameterT, OutputT> toBuilder();

    // Driver configuration
    public ReadAll<ParameterT, OutputT> withDriverConfiguration(DriverConfiguration config) {
      return toBuilder()
          .setDriverProviderFn(new DriverProviderFromDriverConfiguration(config))
          .build();
    }

    // Cypher
    public ReadAll<ParameterT, OutputT> withCypher(String cypher) {
      checkArgument(
          cypher != null, "Neo4jIO.readAll().withCypher(query) called with null cypher query");
      return withCypher(ValueProvider.StaticValueProvider.of(cypher));
    }

    public ReadAll<ParameterT, OutputT> withCypher(ValueProvider<String> cypher) {
      checkArgument(cypher != null, "Neo4jIO.readAll().withCypher(cypher) called with null cypher");
      return toBuilder().setCypher(cypher).build();
    }

    // Transaction timeout
    public ReadAll<ParameterT, OutputT> withTransactionTimeoutMs(long timeout) {
      checkArgument(
          timeout > 0, "Neo4jIO.readAll().withTransactionTimeOutMs(timeout) called with timeout<=0");
      return withTransactionTimeoutMs(ValueProvider.StaticValueProvider.of(timeout));
    }

    public ReadAll<ParameterT, OutputT> withTransactionTimeoutMs(ValueProvider<Long> timeout) {
      checkArgument(
          timeout != null && timeout.get() > 0,
          "Neo4jIO.readAll().withTransactionTimeOutMs(timeout) called with timeout<=0");
      return toBuilder().setTransactionTimeoutMs(timeout).build();
    }

    // Database
    public ReadAll<ParameterT, OutputT> withDatabase(String database) {
      checkArgument(
          database != null, "Neo4jIO.readAll().withDatabase(database) called with null database");
      return withDatabase(ValueProvider.StaticValueProvider.of(database));
    }

    public ReadAll<ParameterT, OutputT> withDatabase(ValueProvider<String> database) {
      checkArgument(
          database != null, "Neo4jIO.readAll().withDatabase(database) called with null database");
      return toBuilder().setDatabase(database).build();
    }

    // Fetch size
    public ReadAll<ParameterT, OutputT> withFetchSize(long fetchSize) {
      checkArgument(fetchSize > 0, "Neo4jIO.readAll().withFetchSize(query) called with fetchSize<=0");
      return withFetchSize(ValueProvider.StaticValueProvider.of(fetchSize));
    }

    public ReadAll<ParameterT, OutputT> withFetchSize(ValueProvider<Long> fetchSize) {
      checkArgument(
          fetchSize != null && fetchSize.get() >= 0,
          "Neo4jIO.readAll().withFetchSize(query) called with fetchSize<=0");
      return toBuilder().setFetchSize(fetchSize).build();
    }

    // Row mapper
    public ReadAll<ParameterT, OutputT> withRowMapper(RowMapper<OutputT> rowMapper) {
      checkArgument(
          rowMapper != null, "Neo4jIO.readAll().withRowMapper(rowMapper) called with null rowMapper");
      return toBuilder().setRowMapper(rowMapper).build();
    }

    // Parameters mapper
    public ReadAll<ParameterT, OutputT> withParametersMapper(
        ParametersMapper<ParameterT> parametersMapper) {
      checkArgument(
          parametersMapper != null,
          "Neo4jIO.readAll().withParametersMapper(parametersMapper) called with null parametersMapper");
      return toBuilder().setParametersMapper(parametersMapper).build();
    }

    // Coder
    public ReadAll<ParameterT, OutputT> withCoder(Coder<OutputT> coder) {
      checkArgument(coder != null, "Neo4jIO.readAll().withCoder(coder) called with null coder");
      return toBuilder().setCoder(coder).build();
    }

    // Read/Write transaction
    public ReadAll<ParameterT, OutputT> withReadTransaction() {
      return toBuilder()
          .setWriteTransaction(ValueProvider.StaticValueProvider.of(Boolean.FALSE))
          .build();
    }

    public ReadAll<ParameterT, OutputT> withWriteTransaction() {
      return toBuilder()
          .setWriteTransaction(ValueProvider.StaticValueProvider.of(Boolean.TRUE))
          .build();
    }

    // Log cypher statements
    public ReadAll<ParameterT, OutputT> withoutCypherLogging() {
      return toBuilder().setLogCypher(ValueProvider.StaticValueProvider.of(Boolean.FALSE)).build();
    }

    public ReadAll<ParameterT, OutputT> withCypherLogging() {
      return toBuilder().setLogCypher(ValueProvider.StaticValueProvider.of(Boolean.TRUE)).build();
    }

    @Override
    public PCollection<OutputT> expand(PCollection<ParameterT> input) {

      checkArgument(
          getCypher() != null && getCypher().get() != null,
          "please provide a cypher statement to execute");

      long fetchSize;
      if (getFetchSize() == null || getFetchSize().get() <= 0) {
        fetchSize = 0;
      } else {
        fetchSize = getFetchSize().get();
      }

      String databaseName = null;
      if (getDatabase() != null) {
        databaseName = getDatabase().get();
      }

      boolean writeTransaction = false;
      if (getWriteTransaction() != null) {
        writeTransaction = getWriteTransaction().get();
      }

      long transactionTimeOutMs;
      if (getTransactionTimeoutMs() == null || getTransactionTimeoutMs().get() < 0) {
        transactionTimeOutMs = -1;
      } else {
        transactionTimeOutMs = getTransactionTimeoutMs().get();
      }

      boolean logCypher = false;
      if (getLogCypher() != null) {
        logCypher = getLogCypher().get();
      }

      ReadFn<ParameterT, OutputT> readFn =
          new ReadFn<>(
              getDriverProviderFn(),
              getCypher().get(),
              getRowMapper(),
              getParametersMapper(),
              fetchSize,
              databaseName,
              writeTransaction,
              transactionTimeOutMs,
              logCypher);

      return getOutputPCollection(input, readFn, getCoder());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("cypher", getCypher()));
      builder.add(DisplayData.item("rowMapper", getRowMapper().getClass().getName()));
      builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
      if (getDriverProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDriverProviderFn()).populateDisplayData(builder);
      }
    }

    @AutoValue.Builder
    abstract static class Builder<ParameterT, OutputT> {
      abstract Builder<ParameterT, OutputT> setDriverProviderFn(
          SerializableFunction<Void, Driver> driverProviderFn);

      abstract Builder<ParameterT, OutputT> setCypher(ValueProvider<String> cypher);

      abstract Builder<ParameterT, OutputT> setDatabase(ValueProvider<String> cypher);

      abstract Builder<ParameterT, OutputT> setWriteTransaction(
          ValueProvider<Boolean> writeTransaction);

      abstract Builder<ParameterT, OutputT> setTransactionTimeoutMs(
          ValueProvider<Long> transactionTimeoutMs);

      abstract Builder<ParameterT, OutputT> setRowMapper(RowMapper<OutputT> rowMapper);

      abstract Builder<ParameterT, OutputT> setParametersMapper(
          ParametersMapper<ParameterT> parameterMapper);

      abstract Builder<ParameterT, OutputT> setCoder(Coder<OutputT> coder);

      abstract Builder<ParameterT, OutputT> setFetchSize(ValueProvider<Long> fetchSize);

      abstract Builder<ParameterT, OutputT> setLogCypher(ValueProvider<Boolean> logCypher);

      abstract ReadAll<ParameterT, OutputT> build();
    }
  }

  /** A {@link DoFn} to execute a Cypher query to read from Neo4j. */
  private static class ReadWriteFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    protected final SerializableFunction<Void, Driver> driverProviderFn;
    protected final String databaseName;
    protected final long fetchSize;
    protected final long transactionTimeoutMs;

    protected transient Driver driver;
    protected transient Session session;
    protected transient TransactionConfig transactionConfig;

    protected Map<String, Object> parametersMap;

    private ReadWriteFn(
        SerializableFunction<Void, Driver> driverProviderFn,
        String databaseName,
        long fetchSize,
        long transactionTimeoutMs) {
      this.driverProviderFn = driverProviderFn;
      this.databaseName = databaseName;
      this.fetchSize = fetchSize;
      this.transactionTimeoutMs = transactionTimeoutMs;

      parametersMap = new HashMap<>();
    }

    /**
     * Delay the creation of driver and session until we actually have data to do something with.
     */
    @Setup
    public void setup() {}

    protected void buildDriverAndSession() {
      if (driver == null && session == null && transactionConfig == null) {
        driver = driverProviderFn.apply(null);

        SessionConfig.Builder builder = SessionConfig.builder();
        if (databaseName != null) {
          builder = builder.withDatabase(databaseName);
        }
        builder = builder.withFetchSize(fetchSize);
        session = driver.session(builder.build());

        TransactionConfig.Builder configBuilder = TransactionConfig.builder();
        if (transactionTimeoutMs > 0) {
          configBuilder = configBuilder.withTimeout(Duration.ofMillis(transactionTimeoutMs));
        }
        transactionConfig = configBuilder.build();
      }
    }

    @FinishBundle
    public void finishBundle() {
      cleanUpDriverSession();
    }

    @Override
    protected void finalize() {
      cleanUpDriverSession();
    }

    protected void cleanUpDriverSession() {
      if (session != null) {
        try {
          session.close();
        } finally {
          session = null;
        }
      }
      if (driver != null) {
        try {
          driver.close();
        } finally {
          driver = null;
        }
      }
    }

    protected String getParametersString(Map<String, Object> parametersMap) {
      StringBuilder parametersString = new StringBuilder();
      parametersMap
          .keySet()
          .forEach(
              key -> {
                if (parametersString.length() > 0) {
                  parametersString.append(',');
                }
                parametersString.append(key).append('=');
                Object value = parametersMap.get(key);
                if (value == null) {
                  parametersString.append("<null>");
                } else {
                  parametersString.append(value);
                }
              });
      return parametersString.toString();
    }
  }

  /** A {@link DoFn} to execute a Cypher query to read from Neo4j. */
  private static class ReadFn<ParameterT, OutputT> extends ReadWriteFn<ParameterT, OutputT> {
    private final String cypher;
    private final RowMapper<OutputT> rowMapper;
    private final ParametersMapper<ParameterT> parametersMapper;
    private final boolean writeTransaction;
    private final boolean logCypher;

    private ReadFn(
        SerializableFunction<Void, Driver> driverProviderFn,
        String cypher,
        RowMapper<OutputT> rowMapper,
        ParametersMapper<ParameterT> parametersMapper,
        long fetchSize,
        String databaseName,
        boolean writeTransaction,
        long transactionTimeoutMs,
        boolean logCypher) {
      super(driverProviderFn, databaseName, fetchSize, transactionTimeoutMs);
      this.cypher = cypher;
      this.rowMapper = rowMapper;
      this.parametersMapper = parametersMapper;
      this.writeTransaction = writeTransaction;
      this.logCypher = logCypher;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      // Some initialization needs to take place as late as possible.
      // If this method is not called (no input) then we don't need to create a session
      //
      if (session == null) {
        buildDriverAndSession();
      }

      // Map the input data to the parameters map...
      //
      ParameterT parameters = context.element();
      if (parametersMapper != null) {
        parametersMapper.mapParameters(parameters, parametersMap);
      }

      executeReadCypherStatement(context);
    }

    private void executeReadCypherStatement(final ProcessContext processContext) {
      // The transaction.
      //
      TransactionWork<Void> transactionWork =
          transaction -> {
            Result result = transaction.run(cypher, parametersMap);
            while (result.hasNext()) {
              Record record = result.next();
              try {
                OutputT outputT = rowMapper.mapRow(record);
                processContext.output(outputT);
              } catch (Exception e) {
                throw new RuntimeException("error mapping Neo4j record to row", e);
              }
            }

            // No specific Neo4j transaction output beyond what goes to the output
            return null;
          };

      if (logCypher) {
        String parametersString = getParametersString(parametersMap);

        String readWrite = writeTransaction ? "write" : "read";
        LOG.info(
            "Starting a "
                + readWrite
                + " transaction for cypher: "
                + cypher
                + ", parameters: "
                + parametersString);
      }

      // There are 2 ways to do a transaction on Neo4j: read or write
      //
      if (writeTransaction) {
        session.writeTransaction(transactionWork, transactionConfig);
      } else {
        session.readTransaction(transactionWork, transactionConfig);
      }

      // Now we need to clear out the parameters map
      //
      parametersMap.clear();
    }
  }

  /**
   * Wraps a {@link DriverConfiguration} to provide a {@link Driver}.
   *
   * <p>At most a single {@link Driver} instance will be constructed during pipeline execution for
   * each unique {@link DriverConfiguration} within the pipeline.
   */
  public static class DriverProviderFromDriverConfiguration
      implements SerializableFunction<Void, Driver>, HasDisplayData {
    private static final ConcurrentHashMap<DriverConfiguration, Driver> instances =
        new ConcurrentHashMap<>();
    private final DriverConfiguration config;

    private DriverProviderFromDriverConfiguration(DriverConfiguration config) {
      this.config = config;
    }

    public static SerializableFunction<Void, Driver> of(DriverConfiguration config) {
      return new DriverProviderFromDriverConfiguration(config);
    }

    @Override
    public Driver apply(Void input) {
      return config.buildDriver();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      config.populateDisplayData(builder);
    }
  }

  /** This is the class which handles the work behind the {@link #writeUnwind()} method. */
  @AutoValue
  public abstract static class WriteUnwind<ParameterT>
      extends PTransform<PCollection<ParameterT>, PDone> {

    abstract @Nullable SerializableFunction<Void, Driver> getDriverProviderFn();

    abstract @Nullable ValueProvider<String> getDatabase();

    abstract @Nullable ValueProvider<String> getCypher();

    abstract @Nullable ValueProvider<String> getUnwindMapName();

    abstract @Nullable ValueProvider<Long> getTransactionTimeoutMs();

    abstract @Nullable ParametersMapper<ParameterT> getParametersMapper();

    abstract @Nullable ValueProvider<Long> getBatchSize();

    abstract @Nullable ValueProvider<Boolean> getLogCypher();

    abstract Builder<ParameterT> toBuilder();

    // Driver configuration
    public WriteUnwind<ParameterT> withDriverConfiguration(DriverConfiguration config) {
      return toBuilder()
          .setDriverProviderFn(new DriverProviderFromDriverConfiguration(config))
          .build();
    }

    // Cypher
    public WriteUnwind<ParameterT> withCypher(String cypher) {
      checkArgument(
          cypher != null, "Neo4jIO.writeUnwind().withCypher(query) called with null cypher query");
      return withCypher(ValueProvider.StaticValueProvider.of(cypher));
    }

    public WriteUnwind<ParameterT> withCypher(ValueProvider<String> cypher) {
      checkArgument(
          cypher != null, "Neo4jIO.writeUnwind().withCypher(cypher) called with null cypher");
      return toBuilder().setCypher(cypher).build();
    }

    // UnwindMapName
    public WriteUnwind<ParameterT> withUnwindMapName(String mapName) {
      checkArgument(
          mapName != null,
          "Neo4jIO.writeUnwind().withUnwindMapName(query) called with null mapName");
      return withUnwindMapName(ValueProvider.StaticValueProvider.of(mapName));
    }

    public WriteUnwind<ParameterT> withUnwindMapName(ValueProvider<String> mapName) {
      checkArgument(
          mapName != null,
          "Neo4jIO.writeUnwind().withUnwindMapName(cypher) called with null mapName");
      return toBuilder().setUnwindMapName(mapName).build();
    }

    // Transaction timeout
    public WriteUnwind<ParameterT> withTransactionTimeoutMs(long timeout) {
      checkArgument(
          timeout > 0,
          "Neo4jIO.writeUnwind().withTransactionTimeOutMs(timeout) called with timeout<=0");
      return withTransactionTimeoutMs(ValueProvider.StaticValueProvider.of(timeout));
    }

    public WriteUnwind<ParameterT> withTransactionTimeoutMs(ValueProvider<Long> timeout) {
      checkArgument(
          timeout != null && timeout.get() > 0,
          "Neo4jIO.writeUnwind().withTransactionTimeOutMs(timeout) called with timeout<=0");
      return toBuilder().setTransactionTimeoutMs(timeout).build();
    }

    // Database
    public WriteUnwind<ParameterT> withDatabase(String database) {
      checkArgument(
          database != null,
          "Neo4jIO.writeUnwind().withDatabase(database) called with null database");
      return withDatabase(ValueProvider.StaticValueProvider.of(database));
    }

    public WriteUnwind<ParameterT> withDatabase(ValueProvider<String> database) {
      checkArgument(
          database != null,
          "Neo4jIO.writeUnwind().withDatabase(database) called with null database");
      return toBuilder().setDatabase(database).build();
    }

    // Batch size
    public WriteUnwind<ParameterT> withBatchSize(long batchSize) {
      checkArgument(
          batchSize > 0, "Neo4jIO.writeUnwind().withFetchSize(query) called with batchSize<=0");
      return withBatchSize(ValueProvider.StaticValueProvider.of(batchSize));
    }

    public WriteUnwind<ParameterT> withBatchSize(ValueProvider<Long> batchSize) {
      checkArgument(
          batchSize != null && batchSize.get() >= 0,
          "Neo4jIO.readAll().withBatchSize(query) called with batchSize<=0");
      return toBuilder().setBatchSize(batchSize).build();
    }

    // Parameters mapper
    public WriteUnwind<ParameterT> withParametersMapper(
        ParametersMapper<ParameterT> parametersMapper) {
      checkArgument(
          parametersMapper != null,
          "Neo4jIO.readAll().withParametersMapper(parametersMapper) called with null parametersMapper");
      return toBuilder().setParametersMapper(parametersMapper).build();
    }

    // Logging
    public WriteUnwind<ParameterT> withCypherLogging() {
      return toBuilder().setLogCypher(ValueProvider.StaticValueProvider.of(Boolean.TRUE)).build();
    }

    @Override
    public PDone expand(PCollection<ParameterT> input) {

      checkArgument(
          getCypher() != null && getCypher().get() != null,
          "please provide an unwind cypher statement to execute");
      checkArgument(
          getUnwindMapName() != null && getUnwindMapName().get() != null,
          "please provide an unwind map name");

      String databaseName = null;
      if (getDatabase() != null) {
        databaseName = getDatabase().get();
      }

      String unwindMapName = null;
      if (getUnwindMapName() != null) {
        unwindMapName = getUnwindMapName().get();
      }

      long transactionTimeOutMs;
      if (getTransactionTimeoutMs() == null || getTransactionTimeoutMs().get() < 0) {
        transactionTimeOutMs = -1;
      } else {
        transactionTimeOutMs = getTransactionTimeoutMs().get();
      }

      long batchSize;
      if (getBatchSize() == null || getBatchSize().get() <= 0) {
        batchSize = 1;
      } else {
        batchSize = getBatchSize().get();
      }

      boolean logCypher = false;
      if (getLogCypher() != null) {
        logCypher = getLogCypher().get();
      }

      WriteUnwindFn<ParameterT> writeFn =
          new WriteUnwindFn<>(
              getDriverProviderFn(),
              getCypher().get(),
              getParametersMapper(),
              -1,
              databaseName,
              transactionTimeOutMs,
              batchSize,
              logCypher,
              unwindMapName);

      input.apply(ParDo.of(writeFn));

      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("cypher", getCypher()));
      if (getDriverProviderFn() instanceof HasDisplayData) {
        ((HasDisplayData) getDriverProviderFn()).populateDisplayData(builder);
      }
    }

    @AutoValue.Builder
    abstract static class Builder<ParameterT> {
      abstract Builder<ParameterT> setDriverProviderFn(
          SerializableFunction<Void, Driver> driverProviderFn);

      abstract Builder<ParameterT> setCypher(ValueProvider<String> cypher);

      abstract Builder<ParameterT> setUnwindMapName(ValueProvider<String> unwindMapName);

      abstract Builder<ParameterT> setDatabase(ValueProvider<String> database);

      abstract Builder<ParameterT> setTransactionTimeoutMs(
          ValueProvider<Long> transactionTimeoutMs);

      abstract Builder<ParameterT> setParametersMapper(
          ParametersMapper<ParameterT> parameterMapper);

      abstract Builder<ParameterT> setBatchSize(ValueProvider<Long> batchSize);

      abstract Builder<ParameterT> setLogCypher(ValueProvider<Boolean> logCypher);

      abstract WriteUnwind<ParameterT> build();
    }
  }

  /** A {@link DoFn} to execute a Cypher query to read from Neo4j. */
  private static class WriteUnwindFn<ParameterT> extends ReadWriteFn<ParameterT, Void> {

    private final String cypher;
    private final ParametersMapper<ParameterT> parametersMapper;
    private final boolean logCypher;
    private final long batchSize;
    private final String unwindMapName;

    private long elementsInput;
    private boolean loggingDone;
    private List<Map<String, Object>> unwindList;

    private WriteUnwindFn(
        SerializableFunction<Void, Driver> driverProviderFn,
        String cypher,
        ParametersMapper<ParameterT> parametersMapper,
        long fetchSize,
        String databaseName,
        long transactionTimeoutMs,
        long batchSize,
        boolean logCypher,
        String unwindMapName) {
      super(driverProviderFn, databaseName, fetchSize, transactionTimeoutMs);
      this.cypher = cypher;
      this.parametersMapper = parametersMapper;
      this.logCypher = logCypher;
      this.batchSize = batchSize;
      this.unwindMapName = unwindMapName;

      unwindList = new ArrayList<>();

      elementsInput = 0;
      loggingDone = false;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      if (session == null) {
        buildDriverAndSession();
      }

      // Map the input data to the parameters map...
      //
      ParameterT parameters = context.element();
      if (parametersMapper != null) {
        // Every row gets a new Map<String,Object> entry in unwindList
        //
        Map<String, Object> rowMap = new HashMap<>();
        parametersMapper.mapParameters(parameters, rowMap);
        unwindList.add(rowMap);
      }
      elementsInput++;

      if (elementsInput >= batchSize) {
        // Execute the cypher query with the collected parameters map
        //
        executeCypherUnwindStatement();
      }
    }

    private void executeCypherUnwindStatement() {
      // Wait until the very last moment to connect to Neo4j
      //
      if (session == null) {
        buildDriverAndSession();
      }

      // In case of errors and no actual input read (error in mapper) we don't have input
      // So we don't want to execute any cypher in this case.  There's no need to generate even more
      // errors
      //
      if (elementsInput == 0) {
        return;
      }

      // The transaction.
      //
      TransactionWork<Void> transactionWork =
          transaction -> {
            // Add the list to the overall parameters map
            //
            parametersMap.clear();
            parametersMap.put(unwindMapName, unwindList);

            Result result = transaction.run(cypher, parametersMap);
            while (result.hasNext()) {
              // This just consumes any output but the function basically has no output
              // To be revisited based on requirements.
              //
              result.next();
            }
            return null;
          };

      if (logCypher && !loggingDone) {
        String parametersString = getParametersString(parametersMap);
        LOG.info(
            "Starting a write transaction for unwind statement cypher: "
                + cypher
                + ", parameters: "
                + parametersString);
        loggingDone = true;
      }

      try {
        session.writeTransaction(transactionWork, transactionConfig);
      } catch (Exception e) {
        throw new RuntimeException(
            "Error writing " + unwindList.size() + " rows to Neo4j with Cypher: " + cypher, e);
      }
      LOG.info("Write transaction for unwind finished.");

      // Now we need to reset the number of elements read and the parameters map
      //
      unwindList.clear();
      elementsInput = 0;
    }

    @FinishBundle
    @Override
    public void finishBundle() {
      executeCypherUnwindStatement();
    }
  }
}
