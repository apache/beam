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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.ReactiveCollection;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.query.ReactiveQueryResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * IO to read and write on <a href="https://www.couchbase.com/">Couchbase</a>.
 *
 * <p>Supports Couchbase Server 8.0 and later, using the Couchbase Java SDK 3.x. Use a {@code
 * couchbases://} connection string to connect over TLS.
 *
 * <h3>Writing to Couchbase</h3>
 *
 * <p>{@link CouchbaseIO#write()} upserts each element of a {@link PCollection} as a JSON document
 * into a collection. You provide a {@link ConnectionConfiguration}, a function that returns the
 * document ID for an element, and a function that returns its content as a {@link JsonObject}:
 *
 * <pre>{@code
 * pipeline
 *     .apply(...)
 *     .apply(CouchbaseIO.<Airline>write()
 *         .withConnectionConfiguration(
 *             CouchbaseIO.ConnectionConfiguration.create("couchbase://host")
 *                 .withUsername(username)
 *                 .withPassword(password)
 *                 .withBucket("travel")
 *                 .withScope("inventory")
 *                 .withCollection("airline"))
 *         .withIdFn(airline -> airline.getId())
 *         .withContentFn(airline -> JsonObject.create().put("name", airline.getName())));
 * }</pre>
 *
 * <p>Documents are written with UPSERT, so a document that already exists is overwritten. If a
 * bundle fails, the runner retries it; documents written by the failed attempt are overwritten with
 * the same content, so retries do not produce duplicates. Writes are not exactly-once: a document
 * may be written more than once, but the end state is the same as writing it once.
 *
 * <p>The user needs the Data Writer role on the target collection.
 *
 * <p>Elements are buffered and written in batches of {@value #DEFAULT_BATCH_SIZE} (see {@link
 * Write#withBatchSize}), with at most {@value #DEFAULT_MAX_CONCURRENCY} upserts in flight per batch
 * (see {@link Write#withMaxConcurrency}).
 *
 * <p>The password is serialized with the pipeline. Read it from a secret manager when building the
 * pipeline rather than hardcoding it.
 */
public class CouchbaseIO {

  static final int DEFAULT_BATCH_SIZE = 500;
  static final int DEFAULT_MAX_CONCURRENCY = 32;

  private CouchbaseIO() {}

  /**
   * Returns a transform that runs a SQL++ query and emits each result row as a {@link JsonObject}.
   * Use {@link Read#withParseFn} to emit another type.
   */
  public static Read<JsonObject> read() {
    return new AutoValue_CouchbaseIO_Read.Builder<JsonObject>()
        .setScanConsistency(QueryScanConsistency.NOT_BOUNDED)
        .setParseFn(SerializableFunctions.identity())
        .setCoder(JsonObjectCoder.of())
        .setRedistribute(false)
        .build();
  }

  /**
   * Returns a transform that runs a SQL++ query and converts each result row to a {@link Row} with
   * the given schema. Fields not in the schema are ignored. A field missing from the row is null if
   * the schema declares it nullable; otherwise the read fails. A value that doesn't match its field
   * type also fails the read, with an error naming the field.
   *
   * <p>Throws if the schema has a field type that can't be read from JSON, such as {@code BYTES}.
   */
  public static Read<Row> readRows(Schema schema) {
    return read().withParseFn(new JsonToRowFn(schema)).withCoder(RowCoder.of(schema));
  }

  /**
   * Returns a transform that upserts each element as a JSON document. Defaults: batch size {@value
   * #DEFAULT_BATCH_SIZE}, max concurrency {@value #DEFAULT_MAX_CONCURRENCY}.
   */
  public static <T> Write<T> write() {
    return new AutoValue_CouchbaseIO_Write.Builder<T>()
        .setBatchSize(DEFAULT_BATCH_SIZE)
        .setMaxConcurrency(DEFAULT_MAX_CONCURRENCY)
        .build();
  }

  @AutoValue
  public abstract static class ConnectionConfiguration implements Serializable {
    abstract String connectionString();

    abstract @Nullable String username();

    abstract @Nullable String password();

    abstract @Nullable String bucket();

    abstract String scope(); // default "_default"

    abstract String collection(); // default "_default"

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setConnectionString(String connectionString);

      abstract Builder setUsername(String username);

      abstract Builder setPassword(String password);

      abstract Builder setBucket(String bucket);

      abstract Builder setScope(String scope);

      abstract Builder setCollection(String collection);

      abstract ConnectionConfiguration build();
    }

    public static ConnectionConfiguration create(String connectionString) {
      return new AutoValue_CouchbaseIO_ConnectionConfiguration.Builder()
          .setConnectionString(connectionString)
          .setScope("_default")
          .setCollection("_default")
          .build();
    }

    /** Sets the username used to authenticate to the cluster. */
    public ConnectionConfiguration withUsername(String username) {
      return toBuilder().setUsername(username).build();
    }

    /** Sets the password used to authenticate to the cluster. */
    public ConnectionConfiguration withPassword(String password) {
      return toBuilder().setPassword(password).build();
    }

    /** Sets the bucket to read from or write to. */
    public ConnectionConfiguration withBucket(String bucket) {
      return toBuilder().setBucket(bucket).build();
    }

    /** Sets the scope within the bucket. Defaults to {@code _default}. */
    public ConnectionConfiguration withScope(String scope) {
      return toBuilder().setScope(scope).build();
    }

    /** Sets the collection within the scope. Defaults to {@code _default}. */
    public ConnectionConfiguration withCollection(String collection) {
      return toBuilder().setCollection(collection).build();
    }

    void populateDisplayData(DisplayData.Builder builder) {
      builder.add(DisplayData.item("connectionString", connectionString()));
      builder.addIfNotNull(DisplayData.item("username", username()));
      builder.addIfNotNull(DisplayData.item("bucket", bucket()));
      builder.add(DisplayData.item("scope", scope()));
      builder.add(DisplayData.item("collection", collection()));
    }

    // Hand-written so the password is never logged; AutoValue's generated toString includes it.
    @Override
    public final String toString() {
      return MoreObjects.toStringHelper(this)
          .add("connectionString", connectionString())
          .add("username", username())
          .add("bucket", bucket())
          .add("scope", scope())
          .add("collection", collection())
          .toString();
    }

    Cluster connect() {
      String user = checkStateNotNull(username(), "username is required");
      String pass = checkStateNotNull(password(), "password is required");
      return Cluster.connect(connectionString(), ClusterOptions.clusterOptions(user, pass));
    }
  }

  /** A {@link PTransform} that runs a SQL++ query and emits one element per result row. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    abstract @Nullable ConnectionConfiguration connectionConfiguration();

    abstract @Nullable String query();

    abstract @Nullable JsonObject namedParameters();

    abstract @Nullable JsonArray positionalParameters();

    abstract QueryScanConsistency scanConsistency();

    abstract SerializableFunction<JsonObject, T> parseFn();

    abstract @Nullable Coder<T> coder();

    abstract boolean redistribute();

    abstract @Nullable Integer redistributeNumKeys();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setConnectionConfiguration(@Nullable ConnectionConfiguration config);

      abstract Builder<T> setQuery(@Nullable String query);

      abstract Builder<T> setNamedParameters(@Nullable JsonObject namedParameters);

      abstract Builder<T> setPositionalParameters(@Nullable JsonArray positionalParameters);

      abstract Builder<T> setScanConsistency(QueryScanConsistency scanConsistency);

      abstract Builder<T> setParseFn(SerializableFunction<JsonObject, T> parseFn);

      abstract Builder<T> setCoder(@Nullable Coder<T> coder);

      abstract Builder<T> setRedistribute(boolean redistribute);

      abstract Builder<T> setRedistributeNumKeys(@Nullable Integer redistributeNumKeys);

      abstract Read<T> build();
    }

    /**
     * Sets the cluster and credentials. If a bucket is set, the query runs in the configured scope,
     * so it can name collections without qualifying them; otherwise it runs at cluster level. The
     * configured collection is not used: the query names what it reads.
     */
    public Read<T> withConnectionConfiguration(ConnectionConfiguration config) {
      return toBuilder().setConnectionConfiguration(config).build();
    }

    /**
     * Sets the SQL++ query. Include {@code META().id} in the projection if the document ID is
     * needed.
     */
    public Read<T> withQuery(String query) {
      return toBuilder().setQuery(query).build();
    }

    /** Adds a value for the named parameter {@code $name} in the query. */
    public Read<T> withNamedParameter(String name, Object value) {
      JsonObject current = namedParameters();
      JsonObject parameters =
          current == null ? JsonObject.create() : JsonObject.from(current.toMap());
      return toBuilder().setNamedParameters(parameters.put(name, value)).build();
    }

    /** Sets the values for the positional parameters {@code $1}, {@code $2}, ... in the query. */
    public Read<T> withPositionalParameters(Object... values) {
      return toBuilder().setPositionalParameters(JsonArray.from(values)).build();
    }

    /**
     * Sets the scan consistency. Defaults to {@link QueryScanConsistency#NOT_BOUNDED}; use {@link
     * QueryScanConsistency#REQUEST_PLUS} when the read must see all prior mutations.
     */
    public Read<T> withScanConsistency(QueryScanConsistency scanConsistency) {
      return toBuilder().setScanConsistency(scanConsistency).build();
    }

    /**
     * Sets the function that converts each result row to the output type. It runs inside the read,
     * before any redistribute. Resets any coder set with {@link #withCoder}, because that coder was
     * for the previous output type.
     */
    public <OutputT> Read<OutputT> withParseFn(SerializableFunction<JsonObject, OutputT> parseFn) {
      return new AutoValue_CouchbaseIO_Read.Builder<OutputT>()
          .setConnectionConfiguration(connectionConfiguration())
          .setQuery(query())
          .setNamedParameters(namedParameters())
          .setPositionalParameters(positionalParameters())
          .setScanConsistency(scanConsistency())
          .setParseFn(parseFn)
          .setRedistribute(redistribute())
          .setRedistributeNumKeys(redistributeNumKeys())
          .build();
    }

    /**
     * Sets the output coder. Required when it can't be inferred from {@link #withParseFn}, for
     * example when the parsing function is a lambda.
     */
    public Read<T> withCoder(Coder<T> coder) {
      return toBuilder().setCoder(coder).build();
    }

    /**
     * Redistributes the output after parsing, so the shuffle carries the parsed type.
     * Redistributing the default {@link JsonObject} output is costly; use {@link #withParseFn}
     * first.
     */
    public Read<T> withRedistribute() {
      return toBuilder().setRedistribute(true).build();
    }

    /** Sets the number of keys to redistribute into. Requires {@link #withRedistribute()}. */
    public Read<T> withRedistributeNumKeys(int redistributeNumKeys) {
      checkArgument(
          redistributeNumKeys > 0, "redistributeNumKeys must be > 0, got %s", redistributeNumKeys);
      return toBuilder().setRedistributeNumKeys(redistributeNumKeys).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      ConnectionConfiguration config =
          checkArgumentNotNull(
              connectionConfiguration(), "withConnectionConfiguration() is required");
      checkArgument(query() != null, "withQuery() is required");
      checkArgument(
          config.username() != null, "ConnectionConfiguration.withUsername() is required");
      checkArgument(
          config.password() != null, "ConnectionConfiguration.withPassword() is required");
      checkArgument(
          namedParameters() == null || positionalParameters() == null,
          "withNamedParameter() and withPositionalParameters() cannot be combined");
      checkArgument(
          redistributeNumKeys() == null || redistribute(),
          "withRedistributeNumKeys() requires withRedistribute()");

      PCollection<T> output =
          input
              .apply("Impulse", Impulse.create())
              .apply("Query", ParDo.of(new ReadFn<>(this)))
              .setCoder(outputCoder(input.getPipeline().getCoderRegistry()));
      if (redistribute()) {
        output =
            output.apply(
                "Redistribute",
                Redistribute.<T>arbitrarily().withNumBuckets(redistributeNumKeys()));
      }
      return output;
    }

    private Coder<T> outputCoder(CoderRegistry registry) {
      Coder<T> coder = coder();
      if (coder != null) {
        return coder;
      }
      try {
        return registry.getCoder(TypeDescriptors.outputOf(parseFn()));
      } catch (CannotProvideCoderException e) {
        throw new IllegalArgumentException(
            "Unable to infer a coder for the output of withParseFn(); set one with withCoder()", e);
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      ConnectionConfiguration config = connectionConfiguration();
      if (config != null) {
        config.populateDisplayData(builder);
      }
      builder.addIfNotNull(DisplayData.item("query", query()));
      builder.add(DisplayData.item("scanConsistency", scanConsistency().name()));
      builder.add(DisplayData.item("redistribute", redistribute()));
      builder.addIfNotNull(DisplayData.item("redistributeNumKeys", redistributeNumKeys()));
    }
  }

  // Runs the whole query in one call: a SQL++ query can't be split by the client. A retried
  // bundle runs it again from the start.
  static class ReadFn<T> extends DoFn<byte[], T> {
    private final ConnectionConfiguration config;
    private final String query;
    private final @Nullable JsonObject namedParameters;
    private final @Nullable JsonArray positionalParameters;
    private final QueryScanConsistency scanConsistency;
    private final SerializableFunction<JsonObject, T> parseFn;

    private transient @MonotonicNonNull Cluster cluster;

    ReadFn(Read<T> spec) {
      this.config = checkStateNotNull(spec.connectionConfiguration());
      this.query = checkStateNotNull(spec.query());
      this.namedParameters = spec.namedParameters();
      this.positionalParameters = spec.positionalParameters();
      this.scanConsistency = spec.scanConsistency();
      this.parseFn = spec.parseFn();
    }

    @Setup
    public void setup() {
      cluster = config.connect();
    }

    @ProcessElement
    public void processElement(OutputReceiver<T> out) {
      Cluster connected = checkStateNotNull(cluster);
      QueryOptions options = QueryOptions.queryOptions().scanConsistency(scanConsistency);
      if (namedParameters != null) {
        options.parameters(namedParameters);
      } else if (positionalParameters != null) {
        options.parameters(positionalParameters);
      }
      String bucket = config.bucket();
      Mono<ReactiveQueryResult> result =
          bucket == null
              ? connected.reactive().query(query, options)
              : connected.bucket(bucket).scope(config.scope()).reactive().query(query, options);
      // toStream() pulls rows with backpressure on this thread, so the full result set is never
      // buffered; closing it cancels the query if parsing fails part way.
      try (Stream<JsonObject> rows =
          result.flatMapMany(ReactiveQueryResult::rowsAsObject).toStream()) {
        rows.forEach(row -> out.output(parseFn.apply(row)));
      }
    }

    @Teardown
    public void teardown() {
      if (cluster != null) {
        cluster.disconnect();
      }
    }
  }

  static class JsonToRowFn implements SerializableFunction<JsonObject, Row> {
    private final RowJsonDeserializer deserializer;

    // ObjectMapper isn't serializable, so each worker builds its own.
    private transient @MonotonicNonNull ObjectMapper mapper;

    JsonToRowFn(Schema schema) {
      this.deserializer = RowJsonDeserializer.forSchema(schema);
    }

    @Override
    public Row apply(JsonObject json) {
      ObjectMapper m = mapper;
      if (m == null) {
        m = RowJsonUtils.newObjectMapperWith(deserializer);
        mapper = m;
      }
      return RowJsonUtils.jsonToRow(m, json.toString());
    }
  }

  /** A {@link PTransform} that upserts elements into a Couchbase collection. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {
    abstract @Nullable ConnectionConfiguration connectionConfiguration();

    abstract @Nullable SerializableFunction<T, String> idFn();

    abstract @Nullable SerializableFunction<T, JsonObject> contentFn();

    abstract int batchSize();

    abstract int maxConcurrency();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setConnectionConfiguration(ConnectionConfiguration config);

      abstract Builder<T> setIdFn(SerializableFunction<T, String> idFn);

      abstract Builder<T> setContentFn(SerializableFunction<T, JsonObject> contentFn);

      abstract Builder<T> setBatchSize(int batchSize);

      abstract Builder<T> setMaxConcurrency(int maxConcurrency);

      abstract Write<T> build();
    }

    /** Sets the cluster, credentials and target collection. */
    public Write<T> withConnectionConfiguration(ConnectionConfiguration config) {
      return toBuilder().setConnectionConfiguration(config).build();
    }

    /** Sets the function that returns the document ID for an element. */
    public Write<T> withIdFn(SerializableFunction<T, String> idFn) {
      return toBuilder().setIdFn(idFn).build();
    }

    /**
     * Sets the function that returns the document content for an element. It must return a new
     * {@link JsonObject} on each call, because the object is buffered and written later.
     */
    public Write<T> withContentFn(SerializableFunction<T, JsonObject> contentFn) {
      return toBuilder().setContentFn(contentFn).build();
    }

    /** Sets the number of documents buffered before they are written. Must be positive. */
    public Write<T> withBatchSize(int batchSize) {
      checkArgument(batchSize > 0, "batchSize must be > 0, got %s", batchSize);
      return toBuilder().setBatchSize(batchSize).build();
    }

    /** Sets the maximum number of in-flight upserts per batch. Must be positive. */
    public Write<T> withMaxConcurrency(int maxConcurrency) {
      checkArgument(maxConcurrency > 0, "maxConcurrency must be > 0, got %s", maxConcurrency);
      return toBuilder().setMaxConcurrency(maxConcurrency).build();
    }

    @Override
    public PDone expand(PCollection<T> input) {
      ConnectionConfiguration config =
          checkArgumentNotNull(
              connectionConfiguration(), "withConnectionConfiguration() is required");
      checkArgument(idFn() != null, "withIdFn() is required");
      checkArgument(contentFn() != null, "withContentFn() is required");
      checkArgument(config.bucket() != null, "ConnectionConfiguration.withBucket() is required");
      checkArgument(
          config.username() != null, "ConnectionConfiguration.withUsername() is required");
      checkArgument(
          config.password() != null, "ConnectionConfiguration.withPassword() is required");
      input.apply(ParDo.of(new WriteFn<>(this)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      ConnectionConfiguration config = connectionConfiguration();
      if (config != null) {
        config.populateDisplayData(builder);
      }
      builder.add(DisplayData.item("batchSize", batchSize()));
      builder.add(DisplayData.item("maxConcurrency", maxConcurrency()));
    }
  }

  static class WriteFn<T> extends DoFn<T, Void> {
    private final ConnectionConfiguration config;
    private final SerializableFunction<T, String> idFn;
    private final SerializableFunction<T, JsonObject> contentFn;
    private final int batchSize;
    private final int maxConcurrency;

    private static final Duration BUCKET_READY_TIMEOUT = Duration.ofSeconds(30);

    private transient @MonotonicNonNull Cluster cluster;
    private transient @MonotonicNonNull ReactiveCollection collection;
    private transient @MonotonicNonNull List<KV<String, JsonObject>> batch;

    WriteFn(Write<T> spec) {
      this.config = checkStateNotNull(spec.connectionConfiguration());
      this.idFn = checkStateNotNull(spec.idFn());
      this.contentFn = checkStateNotNull(spec.contentFn());
      this.batchSize = spec.batchSize();
      this.maxConcurrency = spec.maxConcurrency();
    }

    @Setup
    public void setup() {
      Cluster connected = config.connect();
      cluster = connected;
      Bucket bucket = connected.bucket(checkStateNotNull(config.bucket()));
      // Opening a bucket is asynchronous; without this, the first upserts can time out with
      // BUCKET_OPEN_IN_PROGRESS on a freshly started worker.
      bucket.waitUntilReady(BUCKET_READY_TIMEOUT);
      collection = bucket.scope(config.scope()).collection(config.collection()).reactive();
    }

    @StartBundle
    public void startBundle() {
      batch = new ArrayList<>(batchSize);
    }

    @ProcessElement
    public void processElement(@Element T element) {
      List<KV<String, JsonObject>> buffer = checkStateNotNull(batch);
      buffer.add(KV.of(idFn.apply(element), contentFn.apply(element)));
      if (buffer.size() >= batchSize) {
        flush(buffer);
      }
    }

    @FinishBundle
    public void finishBundle() {
      flush(checkStateNotNull(batch));
    }

    @Teardown
    public void teardown() {
      if (cluster != null) {
        cluster.disconnect();
      }
    }

    // The first failed upsert fails the bundle and the runner retries it. Upsert makes the
    // retry idempotent: documents already written are overwritten with the same content.
    private void flush(List<KV<String, JsonObject>> buffer) {
      if (buffer.isEmpty()) {
        return;
      }
      ReactiveCollection target = checkStateNotNull(collection);
      Flux.fromIterable(buffer)
          .flatMap(kv -> target.upsert(kv.getKey(), kv.getValue()), maxConcurrency)
          .then()
          .block();
      buffer.clear();
    }
  }
}
