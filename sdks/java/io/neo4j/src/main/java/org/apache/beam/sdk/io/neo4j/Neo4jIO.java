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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
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
 * {@link DriverConfiguration#create()} or {@link DriverConfiguration#create(String, String,
 * String)} (URL, username and password). Note that subclasses of DriverConfiguration must also be
 * {@link Serializable}). <br>
 * At the level of the Neo4j driver configuration you can specify a Neo4j {@link Config} object with
 * {@link DriverConfiguration#withConfig(Config)}. This way you can configure the Neo4j driver
 * characteristics. Likewise, you can control the characteristics of Neo4j sessions by optionally
 * passing a {@link SessionConfig} object to {@link ReadAll} or {@link WriteUnwind}. For example,
 * the session configuration will allow you to target a specific database or set a fetch size.
 * Finally, in even rarer cases you might need to configure the various aspects of Neo4j
 * transactions, for example their timeout. You can do this with a Neo4j {@link TransactionConfig}
 * object.
 *
 * <p>
 *
 * <p>
 *
 * <h3>Neo4j Aura</h3>
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
 * <p>The mapping {@link SerializableFunction} maps input values to each execution of the Cypher
 * statement. In the function simply return a map containing the parameters you want to set.
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
 *     .withParametersFunction( age -> Collections.singletonMap( "age", age ))
 *     .withRowMapper( record -> return record.get(0).asString() )
 *   );
 * }</pre>
 *
 * <h3>Writing to Neo4j</h3>
 *
 * <p>The Neo4j {@link WriteUnwind} transform supports writing data to a graph. It writes a {@link
 * PCollection} to the graph by collecting a batch of elements after which all elements in the batch
 * are written together to Neo4j.
 *
 * <p>Like the source, to configure this sink, you have to provide a {@link DriverConfiguration}.
 *
 * <p>In the following example we'll merge a collection of {@link org.apache.beam.sdk.values.Row}
 * into Person nodes. Since this is a Sink it has no output and as such no RowMapper is needed. The
 * rows are being used as a container for the parameters of the Cypher statement. The used Cypher in
 * question needs to be an UNWIND statement. Like in the read case, the parameters {@link
 * SerializableFunction} converts parameter values to a {@link Map}. The difference here is that the
 * resulting Map is stored in a {@link List} (containing maps) which in turn is stored in another
 * Map under the name provided by the {@link WriteUnwind#withUnwindMapName(String)} method. All of
 * this is handled automatically. You do need to provide the unwind map name so that you can
 * reference that in the UNWIND statement.
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
 *      .withParametersFunction( row -> ImmutableMap.of(
 *        "id", row.getString("id),
 *        "first", row.getString("firstName")
 *        "last", row.getString("lastName")))
 *    );
 * }</pre>
 */
public class Neo4jIO {

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jIO.class);

  /**
   * Read all rows using a Neo4j Cypher query.
   *
   * @param <ParameterT> Type of the data representing query parameters.
   * @param <OutputT> Type of the data to be read.
   */
  public static <ParameterT, OutputT> ReadAll<ParameterT, OutputT> readAll() {
    return new AutoValue_Neo4jIO_ReadAll.Builder<ParameterT, OutputT>().build();
  }

  /**
   * Write all rows using a Neo4j Cypher UNWIND cypher statement. This sets a default batch size of
   * 5000.
   *
   * @param <ParameterT> Type of the data representing query parameters.
   */
  public static <ParameterT> WriteUnwind<ParameterT> writeUnwind() {
    return new AutoValue_Neo4jIO_WriteUnwind.Builder<ParameterT>()
        .setBatchSize(ValueProvider.StaticValueProvider.of(5000L))
        .build();
  }

  private static <ParameterT, OutputT> PCollection<OutputT> getOutputPCollection(
      PCollection<ParameterT> input,
      DoFn<ParameterT, OutputT> writeFn,
      @Nullable Coder<OutputT> coder) {
    PCollection<OutputT> output = input.apply(ParDo.of(writeFn));
    if (coder != null) {
      output.setCoder(coder);
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
   * A convenience method to clarify the way {@link ValueProvider} works to the static code checker
   * framework for {@link Nullable} values.
   *
   * @param valueProvider
   * @param <T>
   * @return The provided value or null if none was specified.
   */
  private static <T> T getProvidedValue(@Nullable ValueProvider<T> valueProvider) {
    if (valueProvider == null) {
      return (T) null;
    }
    return valueProvider.get();
  }

  /** This describes all the information needed to create a Neo4j {@link Session}. */
  @AutoValue
  public abstract static class DriverConfiguration implements Serializable {
    public static DriverConfiguration create() {
      return new AutoValue_Neo4jIO_DriverConfiguration.Builder()
          .build()
          .withDefaultConfig(true)
          .withConfig(Config.defaultConfig());
    }

    public static DriverConfiguration create(String url, String username, String password) {
      checkArgument(url != null, "url can not be null");
      checkArgument(username != null, "username can not be null");
      checkArgument(password != null, "password can not be null");
      return new AutoValue_Neo4jIO_DriverConfiguration.Builder()
          .build()
          .withDefaultConfig(true)
          .withConfig(Config.defaultConfig())
          .withUrl(url)
          .withUsername(username)
          .withPassword(password);
    }

    abstract @Nullable ValueProvider<String> getUrl();

    abstract @Nullable ValueProvider<List<String>> getUrls();

    abstract @Nullable ValueProvider<String> getUsername();

    abstract @Nullable ValueProvider<String> getPassword();

    abstract @Nullable Config getConfig();

    abstract @Nullable ValueProvider<Boolean> getHasDefaultConfig();

    abstract Builder builder();

    public DriverConfiguration withUrl(String url) {
      return withUrl(ValueProvider.StaticValueProvider.of(url));
    }

    public DriverConfiguration withUrl(ValueProvider<String> url) {
      checkArgument(url != null, "a neo4j connection URL can not be empty or null", url);
      checkArgument(
          StringUtils.isNotEmpty(url.get()),
          "a neo4j connection URL can not be empty or null",
          url);
      return builder().setUrl(url).build();
    }

    public DriverConfiguration withUrls(List<String> urls) {
      return withUrls(ValueProvider.StaticValueProvider.of(urls));
    }

    public DriverConfiguration withUrls(ValueProvider<List<String>> urls) {
      checkArgument(urls != null, "a list of neo4j connection URLs can not be empty or null", urls);
      Preconditions.checkArgument(
          urls.get() != null && !urls.get().isEmpty(),
          "a neo4j connection URL can not be empty or null",
          urls);
      return builder().setUrls(urls).build();
    }

    public DriverConfiguration withConfig(Config config) {
      return builder().setConfig(config).build();
    }

    public DriverConfiguration withUsername(String username) {
      return withUsername(ValueProvider.StaticValueProvider.of(username));
    }

    public DriverConfiguration withUsername(ValueProvider<String> username) {
      checkArgument(username != null, "neo4j username can not be null", username);
      checkArgument(username.get() != null, "neo4j username can not be null", username);
      return builder().setUsername(username).build();
    }

    public DriverConfiguration withPassword(String password) {
      return withPassword(ValueProvider.StaticValueProvider.of(password));
    }

    public DriverConfiguration withPassword(ValueProvider<String> password) {
      checkArgument(password != null, "neo4j password can not be null", password);
      checkArgument(password.get() != null, "neo4j password can not be null", password);
      return builder().setPassword(password).build();
    }

    public DriverConfiguration withDefaultConfig(boolean useDefault) {
      return withDefaultConfig(ValueProvider.StaticValueProvider.of(useDefault));
    }

    public DriverConfiguration withDefaultConfig(ValueProvider<Boolean> useDefault) {
      checkArgument(
          useDefault != null, "withDefaultConfig parameter useDefault can not be null", useDefault);
      checkArgument(
          useDefault.get() != null,
          "withDefaultConfig parameter useDefault can not be null",
          useDefault);
      return builder().setHasDefaultConfig(useDefault).build();
    }

    void populateDisplayData(DisplayData.Builder builder) {
      builder.addIfNotNull(DisplayData.item("neo4j-url", getUrl()));
      builder.addIfNotNull(DisplayData.item("neo4j-username", getUsername()));
      builder.addIfNotNull(
          DisplayData.item(
              "neo4j-password", getPassword() != null ? "<provided>" : "<not-provided>"));
    }

    Driver buildDriver() {
      // Create the Neo4j Driver
      // This uses the provided Neo4j configuration along with URLs, username and password
      //
      Config config = getConfig();
      if (config == null) {
        throw new RuntimeException("please provide a neo4j config");
      }
      // We're trying to work around a subtle serialisation bug in the Neo4j Java driver.
      // The fix is work in progress.  For now, we harden our code to avoid
      // wild goose chases.
      //
      Boolean hasDefaultConfig = getProvidedValue(getHasDefaultConfig());
      if (hasDefaultConfig != null && hasDefaultConfig) {
        config = Config.defaultConfig();
      }

      // Get the list of the URI to connect with
      //
      List<URI> uris = new ArrayList<>();
      String url = getProvidedValue(getUrl());
      if (url != null) {
        try {
          uris.add(new URI(url));
        } catch (URISyntaxException e) {
          throw new RuntimeException("Error creating URI from URL '" + url + "'", e);
        }
      }
      List<String> providedUrls = getProvidedValue(getUrls());
      if (providedUrls != null) {
        for (String providedUrl : providedUrls) {
          try {
            uris.add(new URI(providedUrl));
          } catch (URISyntaxException e) {
            throw new RuntimeException(
                "Error creating URI '"
                    + providedUrl
                    + "' from a list of "
                    + providedUrls.size()
                    + " URLs",
                e);
          }
        }
      }

      // A specific routing driver can be used to connect to specific clustered configurations.
      // Often we don't need it because the Java driver automatically can figure this out
      // automatically. To keep things simple we use the routing driver in case we have more
      // than one URL specified.  This is an exceptional case.
      //
      Driver driver;
      AuthToken authTokens =
          getAuthToken(getProvidedValue(getUsername()), getProvidedValue(getPassword()));
      if (uris.size() > 1) {
        driver = GraphDatabase.routingDriver(uris, authTokens, config);
      } else {
        // Just take the first URI that was provided
        driver = GraphDatabase.driver(uris.get(0), authTokens, config);
      }

      return driver;
    }

    /**
     * Certain embedded scenarios and so on actually allow for having no authentication at all.
     *
     * @param username The username if one is needed
     * @param password The password if one is needed
     * @return The AuthToken
     */
    protected AuthToken getAuthToken(String username, String password) {
      if (username != null && password != null) {
        return AuthTokens.basic(username, password);
      } else {
        return AuthTokens.none();
      }
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

      abstract Builder setConfig(Config config);

      abstract Builder setHasDefaultConfig(ValueProvider<Boolean> useDefault);

      abstract DriverConfiguration build();
    }
  }

  /** This is the class which handles the work behind the {@link #readAll} method. */
  @AutoValue
  public abstract static class ReadAll<ParameterT, OutputT>
      extends PTransform<PCollection<ParameterT>, PCollection<OutputT>> {

    abstract @Nullable SerializableFunction<Void, Driver> getDriverProviderFn();

    abstract @Nullable SessionConfig getSessionConfig();

    abstract @Nullable TransactionConfig getTransactionConfig();

    abstract @Nullable ValueProvider<String> getCypher();

    abstract @Nullable ValueProvider<Boolean> getWriteTransaction();

    abstract @Nullable RowMapper<OutputT> getRowMapper();

    abstract @Nullable SerializableFunction<ParameterT, Map<String, Object>>
        getParametersFunction();

    abstract @Nullable Coder<OutputT> getCoder();

    abstract @Nullable ValueProvider<Boolean> getLogCypher();

    abstract Builder<ParameterT, OutputT> toBuilder();

    public ReadAll<ParameterT, OutputT> withDriverConfiguration(DriverConfiguration config) {
      return toBuilder()
          .setDriverProviderFn(new DriverProviderFromDriverConfiguration(config))
          .build();
    }

    public ReadAll<ParameterT, OutputT> withCypher(String cypher) {
      checkArgument(cypher != null, "Neo4jIO.readAll().withCypher(cypher) called with null cypher");
      return withCypher(ValueProvider.StaticValueProvider.of(cypher));
    }

    public ReadAll<ParameterT, OutputT> withCypher(ValueProvider<String> cypher) {
      checkArgument(cypher != null, "Neo4jIO.readAll().withCypher(cypher) called with null cypher");
      return toBuilder().setCypher(cypher).build();
    }

    public ReadAll<ParameterT, OutputT> withSessionConfig(SessionConfig sessionConfig) {
      checkArgument(
          sessionConfig != null,
          "Neo4jIO.readAll().withSessionConfig(sessionConfig) called with null sessionConfig");
      return toBuilder().setSessionConfig(sessionConfig).build();
    }

    public ReadAll<ParameterT, OutputT> withTransactionConfig(TransactionConfig transactionConfig) {
      checkArgument(
          transactionConfig != null,
          "Neo4jIO.readAll().withTransactionConfig(transactionConfig) called with null transactionConfig");
      return toBuilder().setTransactionConfig(transactionConfig).build();
    }

    public ReadAll<ParameterT, OutputT> withRowMapper(RowMapper<OutputT> rowMapper) {
      checkArgument(
          rowMapper != null,
          "Neo4jIO.readAll().withRowMapper(rowMapper) called with null rowMapper");
      return toBuilder().setRowMapper(rowMapper).build();
    }

    public ReadAll<ParameterT, OutputT> withParametersFunction(
        SerializableFunction<ParameterT, Map<String, Object>> parametersFunction) {
      checkArgument(
          parametersFunction != null,
          "Neo4jIO.readAll().withParametersFunction(parametersFunction) called with null parametersFunction");
      return toBuilder().setParametersFunction(parametersFunction).build();
    }

    public ReadAll<ParameterT, OutputT> withCoder(Coder<OutputT> coder) {
      checkArgument(coder != null, "Neo4jIO.readAll().withCoder(coder) called with null coder");
      return toBuilder().setCoder(coder).build();
    }

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

    public ReadAll<ParameterT, OutputT> withCypherLogging() {
      return toBuilder().setLogCypher(ValueProvider.StaticValueProvider.of(Boolean.TRUE)).build();
    }

    @Override
    public PCollection<OutputT> expand(PCollection<ParameterT> input) {

      final SerializableFunction<Void, Driver> driverProviderFn = getDriverProviderFn();
      final RowMapper<OutputT> rowMapper = getRowMapper();
      SerializableFunction<ParameterT, Map<String, Object>> parametersFunction =
          getParametersFunction();

      final String cypher = getProvidedValue(getCypher());
      checkArgument(cypher != null, "please provide a cypher statement to execute");

      SessionConfig sessionConfig = getSessionConfig();
      if (sessionConfig == null) {
        // Create a default session configuration as recommended by Neo4j
        //
        sessionConfig = SessionConfig.defaultConfig();
      }

      TransactionConfig transactionConfig = getTransactionConfig();
      if (transactionConfig == null) {
        transactionConfig = TransactionConfig.empty();
      }

      Boolean writeTransaction = getProvidedValue(getWriteTransaction());
      if (writeTransaction == null) {
        writeTransaction = Boolean.FALSE;
      }

      Boolean logCypher = getProvidedValue(getLogCypher());
      if (logCypher == null) {
        logCypher = Boolean.FALSE;
      }

      if (driverProviderFn == null) {
        throw new RuntimeException("please provide a driver provider");
      }
      if (rowMapper == null) {
        throw new RuntimeException("please provide a row mapper");
      }
      if (parametersFunction == null) {
        parametersFunction = t -> Collections.emptyMap();
      }

      ReadFn<ParameterT, OutputT> readFn =
          new ReadFn<>(
              driverProviderFn,
              sessionConfig,
              transactionConfig,
              cypher,
              rowMapper,
              parametersFunction,
              writeTransaction,
              logCypher);

      return getOutputPCollection(input, readFn, getCoder());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      String cypher = getProvidedValue(getCypher());
      if (cypher == null) {
        cypher = "";
      }
      builder.add(DisplayData.item("cypher", cypher));
      SerializableFunction<Void, Driver> driverProviderFn = getDriverProviderFn();
      if (driverProviderFn != null) {
        if (driverProviderFn instanceof HasDisplayData) {
          ((HasDisplayData) driverProviderFn).populateDisplayData(builder);
        }
      }
    }

    @AutoValue.Builder
    abstract static class Builder<ParameterT, OutputT> {
      abstract Builder<ParameterT, OutputT> setDriverProviderFn(
          SerializableFunction<Void, Driver> driverProviderFn);

      abstract Builder<ParameterT, OutputT> setCypher(ValueProvider<String> cypher);

      abstract Builder<ParameterT, OutputT> setSessionConfig(SessionConfig sessionConfig);

      abstract Builder<ParameterT, OutputT> setTransactionConfig(
          TransactionConfig transactionConfig);

      abstract Builder<ParameterT, OutputT> setWriteTransaction(
          ValueProvider<Boolean> writeTransaction);

      abstract Builder<ParameterT, OutputT> setRowMapper(RowMapper<OutputT> rowMapper);

      abstract Builder<ParameterT, OutputT> setParametersFunction(
          SerializableFunction<ParameterT, Map<String, Object>> parametersFunction);

      abstract Builder<ParameterT, OutputT> setCoder(Coder<OutputT> coder);

      abstract Builder<ParameterT, OutputT> setLogCypher(ValueProvider<Boolean> logCypher);

      abstract ReadAll<ParameterT, OutputT> build();
    }
  }

  /** A {@link DoFn} to execute a Cypher query to read from Neo4j. */
  private static class ReadWriteFn<ParameterT, OutputT> extends DoFn<ParameterT, OutputT> {
    protected static class DriverSession {
      public @Nullable Driver driver;
      public @Nullable Session session;
      public @NonNull AtomicBoolean closed;

      protected DriverSession(Driver driver, Session session) {
        this.driver = driver;
        this.session = session;
        this.closed = new AtomicBoolean(false);
      }

      private DriverSession() {
        this.driver = null;
        this.session = null;
        this.closed = new AtomicBoolean(true);
      }

      protected static @NonNull DriverSession emptyClosed() {
        return new DriverSession();
      }
    }

    protected final @NonNull SerializableFunction<Void, Driver> driverProviderFn;
    protected final @NonNull SessionConfig sessionConfig;
    protected final @NonNull TransactionConfig transactionConfig;

    protected transient @NonNull DriverSession driverSession;

    protected ReadWriteFn(
        @NonNull SerializableFunction<Void, Driver> driverProviderFn,
        @NonNull SessionConfig sessionConfig,
        @NonNull TransactionConfig transactionConfig) {
      this.driverProviderFn = driverProviderFn;
      this.sessionConfig = sessionConfig;
      this.transactionConfig = transactionConfig;
      this.driverSession = DriverSession.emptyClosed();
    }

    /**
     * Delay the creation of driver and session until we actually have data to do something with.
     */
    @Setup
    public void setup() {}

    protected @NonNull Driver createDriver() {
      Driver driver = driverProviderFn.apply(null);
      if (driver == null) {
        throw new RuntimeException("null driver given by driver provider");
      }
      return driver;
    }

    protected @Initialized @NonNull DriverSession buildDriverSession() {
      @NonNull Driver driver = createDriver();
      @NonNull Session session = driver.session(sessionConfig);
      return new DriverSession(driver, session);
    }

    @StartBundle
    public void startBundle() {
      if (driverSession == null || driverSession.closed.get()) {
        driverSession = buildDriverSession();
      }
    }

    @FinishBundle
    public void finishBundle() {
      cleanUpDriverSession();
    }

    @Teardown
    public void tearDown() {
      cleanUpDriverSession();
    }

    protected void cleanUpDriverSession() {
      if (!driverSession.closed.get()) {
        try {
          if (driverSession.session != null) {
            driverSession.session.close();
          }
          if (driverSession.driver != null) {
            driverSession.driver.close();
          }
        } finally {
          driverSession.closed.set(true);
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
    protected final @NonNull String cypher;
    protected final @NonNull RowMapper<OutputT> rowMapper;
    protected final @Nullable SerializableFunction<ParameterT, Map<String, Object>>
        parametersFunction;

    private final boolean writeTransaction;
    private final boolean logCypher;

    private ReadFn(
        @NonNull SerializableFunction<Void, Driver> driverProviderFn,
        @NonNull SessionConfig sessionConfig,
        @NonNull TransactionConfig transactionConfig,
        @NonNull String cypher,
        @NonNull RowMapper<OutputT> rowMapper,
        @Nullable SerializableFunction<ParameterT, Map<String, Object>> parametersFunction,
        boolean writeTransaction,
        boolean logCypher) {
      super(driverProviderFn, sessionConfig, transactionConfig);
      this.cypher = cypher;
      this.rowMapper = rowMapper;
      this.parametersFunction = parametersFunction;
      this.writeTransaction = writeTransaction;
      this.logCypher = logCypher;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      // Map the input data to the parameters map...
      //
      ParameterT parameters = context.element();
      final Map<String, Object> parametersMap;
      if (parametersFunction != null) {
        parametersMap = parametersFunction.apply(parameters);
      } else {
        parametersMap = Collections.emptyMap();
      }
      executeReadCypherStatement(context, parametersMap);
    }

    private void executeReadCypherStatement(
        final ProcessContext processContext, Map<String, Object> parametersMap) {
      // The actual "reading" work needs to happen in a transaction.
      // We could actually read and write here depending on the type of transaction
      // we picked.  As long as the Cypher statement returns values it's fine.
      //
      TransactionWork<Long> transactionWork =
          transaction -> {
            long count = 0L;
            Result result = transaction.run(cypher, parametersMap);
            while (result.hasNext()) {
              try {
                processContext.output(rowMapper.mapRow(result.next()));
              } catch (Exception e) {
                throw new RuntimeException("Error mapping Neo4J result", e);
              }
              count++;
            }
            return count;
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
      // It's important that the right type is selected, especially in clustered configurations.
      //
      if (driverSession.session == null) {
        throw new RuntimeException("neo4j session was not initialized correctly");
      } else {
        final Long count;
        if (writeTransaction) {
          count = driverSession.session.writeTransaction(transactionWork, transactionConfig);
        } else {
          count = driverSession.session.readTransaction(transactionWork, transactionConfig);
        }
        LOG.debug("Retrieved " + count + " elements from Neo4J");
      }
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

    abstract @Nullable ValueProvider<SessionConfig> getSessionConfig();

    abstract @Nullable ValueProvider<String> getCypher();

    abstract @Nullable ValueProvider<String> getUnwindMapName();

    abstract @Nullable ValueProvider<TransactionConfig> getTransactionConfig();

    abstract @Nullable SerializableFunction<ParameterT, Map<String, Object>>
        getParametersFunction();

    abstract @Nullable ValueProvider<Long> getBatchSize();

    abstract @Nullable ValueProvider<Boolean> getLogCypher();

    abstract Builder<ParameterT> toBuilder();

    public WriteUnwind<ParameterT> withDriverConfiguration(DriverConfiguration config) {
      return toBuilder()
          .setDriverProviderFn(new DriverProviderFromDriverConfiguration(config))
          .build();
    }

    public WriteUnwind<ParameterT> withCypher(String cypher) {
      checkArgument(
          cypher != null, "Neo4jIO.writeUnwind().withCypher(cypher) called with null cypher");
      return withCypher(ValueProvider.StaticValueProvider.of(cypher));
    }

    public WriteUnwind<ParameterT> withCypher(ValueProvider<String> cypher) {
      checkArgument(
          cypher != null, "Neo4jIO.writeUnwind().withCypher(cypher) called with null cypher");
      return toBuilder().setCypher(cypher).build();
    }

    public WriteUnwind<ParameterT> withUnwindMapName(String mapName) {
      checkArgument(
          mapName != null,
          "Neo4jIO.writeUnwind().withUnwindMapName(mapName) called with null mapName");
      return withUnwindMapName(ValueProvider.StaticValueProvider.of(mapName));
    }

    public WriteUnwind<ParameterT> withUnwindMapName(ValueProvider<String> mapName) {
      checkArgument(
          mapName != null,
          "Neo4jIO.writeUnwind().withUnwindMapName(mapName) called with null mapName");
      return toBuilder().setUnwindMapName(mapName).build();
    }

    public WriteUnwind<ParameterT> withTransactionConfig(TransactionConfig transactionConfig) {
      checkArgument(
          transactionConfig != null,
          "Neo4jIO.writeUnwind().withTransactionConfig(transactionConfig) called with null transactionConfig");
      return withTransactionConfig(ValueProvider.StaticValueProvider.of(transactionConfig));
    }

    public WriteUnwind<ParameterT> withTransactionConfig(
        ValueProvider<TransactionConfig> transactionConfig) {
      checkArgument(
          transactionConfig != null,
          "Neo4jIO.writeUnwind().withTransactionConfig(transactionConfig) called with null transactionConfig");
      return toBuilder().setTransactionConfig(transactionConfig).build();
    }

    public WriteUnwind<ParameterT> withSessionConfig(SessionConfig sessionConfig) {
      checkArgument(
          sessionConfig != null,
          "Neo4jIO.writeUnwind().withSessionConfig(sessionConfig) called with null sessionConfig");
      return withSessionConfig(ValueProvider.StaticValueProvider.of(sessionConfig));
    }

    public WriteUnwind<ParameterT> withSessionConfig(ValueProvider<SessionConfig> sessionConfig) {
      checkArgument(
          sessionConfig != null,
          "Neo4jIO.writeUnwind().withSessionConfig(sessionConfig) called with null sessionConfig");
      return toBuilder().setSessionConfig(sessionConfig).build();
    }

    // Batch size
    public WriteUnwind<ParameterT> withBatchSize(long batchSize) {
      checkArgument(
          batchSize > 0, "Neo4jIO.writeUnwind().withBatchSize(batchSize) called with batchSize<=0");
      return withBatchSize(ValueProvider.StaticValueProvider.of(batchSize));
    }

    public WriteUnwind<ParameterT> withBatchSize(ValueProvider<Long> batchSize) {
      checkArgument(
          batchSize != null && batchSize.get() >= 0,
          "Neo4jIO.readAll().withBatchSize(batchSize) called with batchSize<=0");
      return toBuilder().setBatchSize(batchSize).build();
    }

    public WriteUnwind<ParameterT> withParametersFunction(
        SerializableFunction<ParameterT, Map<String, Object>> parametersFunction) {
      checkArgument(
          parametersFunction != null,
          "Neo4jIO.readAll().withParametersFunction(parametersFunction) called with null parametersFunction");
      return toBuilder().setParametersFunction(parametersFunction).build();
    }

    public WriteUnwind<ParameterT> withCypherLogging() {
      return toBuilder().setLogCypher(ValueProvider.StaticValueProvider.of(Boolean.TRUE)).build();
    }

    @Override
    public PDone expand(PCollection<ParameterT> input) {

      final SerializableFunction<Void, Driver> driverProviderFn = getDriverProviderFn();
      final SerializableFunction<ParameterT, Map<String, Object>> parametersFunction =
          getParametersFunction();
      SessionConfig sessionConfig = getProvidedValue(getSessionConfig());
      if (sessionConfig == null) {
        sessionConfig = SessionConfig.defaultConfig();
      }
      TransactionConfig transactionConfig = getProvidedValue(getTransactionConfig());
      if (transactionConfig == null) {
        transactionConfig = TransactionConfig.empty();
      }
      final String cypher = getProvidedValue(getCypher());
      checkArgument(cypher != null, "please provide an unwind cypher statement to execute");
      final String unwindMapName = getProvidedValue(getUnwindMapName());
      checkArgument(unwindMapName != null, "please provide an unwind map name");

      Long batchSize = getProvidedValue(getBatchSize());
      if (batchSize == null || batchSize <= 0) {
        batchSize = 5000L;
      }

      Boolean logCypher = getProvidedValue(getLogCypher());
      if (logCypher == null) {
        logCypher = Boolean.FALSE;
      }

      if (driverProviderFn == null) {
        throw new RuntimeException("please provide a driver provider");
      }
      if (parametersFunction == null) {
        throw new RuntimeException("please provide a parameters function");
      }
      WriteUnwindFn<ParameterT> writeFn =
          new WriteUnwindFn<>(
              driverProviderFn,
              sessionConfig,
              transactionConfig,
              cypher,
              parametersFunction,
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

      final SerializableFunction<Void, Driver> driverProviderFn = getDriverProviderFn();
      if (driverProviderFn != null) {
        if (driverProviderFn instanceof HasDisplayData) {
          ((HasDisplayData) driverProviderFn).populateDisplayData(builder);
        }
      }
    }

    @AutoValue.Builder
    abstract static class Builder<ParameterT> {
      abstract Builder<ParameterT> setDriverProviderFn(
          SerializableFunction<Void, Driver> driverProviderFn);

      abstract Builder<ParameterT> setSessionConfig(ValueProvider<SessionConfig> sessionConfig);

      abstract Builder<ParameterT> setTransactionConfig(
          ValueProvider<TransactionConfig> transactionConfig);

      abstract Builder<ParameterT> setCypher(ValueProvider<String> cypher);

      abstract Builder<ParameterT> setUnwindMapName(ValueProvider<String> unwindMapName);

      abstract Builder<ParameterT> setParametersFunction(
          SerializableFunction<ParameterT, Map<String, Object>> parametersFunction);

      abstract Builder<ParameterT> setBatchSize(ValueProvider<Long> batchSize);

      abstract Builder<ParameterT> setLogCypher(ValueProvider<Boolean> logCypher);

      abstract WriteUnwind<ParameterT> build();
    }
  }

  /** A {@link DoFn} to execute a Cypher query to read from Neo4j. */
  private static class WriteUnwindFn<ParameterT> extends ReadWriteFn<ParameterT, Void> {

    private final @NonNull String cypher;
    private final @Nullable SerializableFunction<ParameterT, Map<String, Object>>
        parametersFunction;
    private final boolean logCypher;
    private final long batchSize;
    private final @NonNull String unwindMapName;

    private long elementsInput;
    private boolean loggingDone;
    private List<Map<String, Object>> unwindList;

    private WriteUnwindFn(
        @NonNull SerializableFunction<Void, Driver> driverProviderFn,
        @NonNull SessionConfig sessionConfig,
        @NonNull TransactionConfig transactionConfig,
        @NonNull String cypher,
        @Nullable SerializableFunction<ParameterT, Map<String, Object>> parametersFunction,
        long batchSize,
        boolean logCypher,
        String unwindMapName) {
      super(driverProviderFn, sessionConfig, transactionConfig);
      this.cypher = cypher;
      this.parametersFunction = parametersFunction;
      this.logCypher = logCypher;
      this.batchSize = batchSize;
      this.unwindMapName = unwindMapName;

      unwindList = new ArrayList<>();

      elementsInput = 0;
      loggingDone = false;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      // Map the input data to the parameters map...
      //
      ParameterT parameters = context.element();
      if (parametersFunction != null) {
        // Every input element creates a new Map<String,Object> entry in unwindList
        //
        unwindList.add(parametersFunction.apply(parameters));
      } else {
        // Someone is writing a bunch of static or procedurally generated values to Neo4j
        unwindList.add(Collections.emptyMap());
      }
      elementsInput++;

      if (elementsInput >= batchSize) {
        // Execute the cypher query with the collected parameters map
        //
        executeCypherUnwindStatement();
      }
    }

    private void executeCypherUnwindStatement() {
      // In case of errors and no actual input read (error in mapper) we don't have input
      // So we don't want to execute any cypher in this case.  There's no need to generate even more
      // errors
      //
      if (elementsInput == 0) {
        return;
      }

      // Add the accumulated list to the overall parameters map
      // It contains a single parameter to unwind
      //
      final Map<String, Object> parametersMap = new HashMap<>();
      parametersMap.put(unwindMapName, unwindList);

      // Every "write" transaction writes a batch of elements to Neo4j.
      // The changes to the database are automatically committed.
      //
      TransactionWork<Void> transactionWork =
          transaction -> {
            transaction.run(cypher, parametersMap).consume();
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

      if (driverSession.session == null) {
        throw new RuntimeException("neo4j session was not initialized correctly");
      } else {
        try {
          driverSession.session.writeTransaction(transactionWork, transactionConfig);
        } catch (Exception e) {
          throw new RuntimeException(
              "Error writing " + unwindList.size() + " rows to Neo4j with Cypher: " + cypher, e);
        }
      }

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
