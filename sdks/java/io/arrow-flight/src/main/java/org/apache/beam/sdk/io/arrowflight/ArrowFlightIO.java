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
package org.apache.beam.sdk.io.arrowflight;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.arrow.ArrowConversion;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IO to read and write data using <a href="https://arrow.apache.org/docs/format/Flight.html">Apache
 * Arrow Flight</a>.
 *
 * <p>Arrow Flight is a high-performance RPC framework for transferring Arrow-formatted data over
 * gRPC. It enables 10-50x faster data transfer compared to JDBC/ODBC by avoiding
 * serialization/deserialization overhead.
 *
 * <h3>Reading from an Arrow Flight server</h3>
 *
 * <p>{@link ArrowFlightIO#read()} returns a bounded {@link PCollection} of {@link Row} elements.
 * Each row is converted from Arrow record batches using the existing {@link ArrowConversion}
 * extension.
 *
 * <pre>{@code
 * PCollection<Row> rows = pipeline.apply(
 *     ArrowFlightIO.read()
 *         .withHost("localhost")
 *         .withPort(47470)
 *         .withCommand("SELECT * FROM my_table"));
 * }</pre>
 *
 * <h3>Writing to an Arrow Flight server</h3>
 *
 * <p>{@link ArrowFlightIO#write()} accepts a {@link PCollection} of {@link Row} elements and
 * streams them to a Flight server using {@code doPut}.
 *
 * <pre>{@code
 * rows.apply(
 *     ArrowFlightIO.write()
 *         .withHost("localhost")
 *         .withPort(47470)
 *         .withDescriptor("my_table")
 *         .withBatchSize(1024));
 * }</pre>
 */
public class ArrowFlightIO {

  private static final Logger LOG = LoggerFactory.getLogger(ArrowFlightIO.class);

  private ArrowFlightIO() {}

  private static byte[] copyToken(byte[] token) {
    return Arrays.copyOf(checkNotNull(token, "token"), token.length);
  }

  private static CallOption[] callOptions(byte @Nullable [] token) {
    if (token == null) {
      return new CallOption[0];
    }
    FlightCallHeaders headers = new FlightCallHeaders();
    headers.insert("authorization", "Bearer " + new String(token, StandardCharsets.UTF_8));
    return new CallOption[] {new HeaderCallOption(headers)};
  }

  private static void validateWriteSchema(Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      switch (field.getType().getTypeName()) {
        case BYTE:
        case INT16:
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
        case STRING:
        case BOOLEAN:
        case BYTES:
        case DATETIME:
          break;
        default:
          throw new IllegalArgumentException(
              String.format(
                  "ArrowFlightIO.write() does not support Beam type '%s' for field '%s'.",
                  field.getType().getTypeName(), field.getName()));
      }
    }
  }

  public static Read read() {
    return new AutoValue_ArrowFlightIO_Read.Builder().setPort(47470).setUseTls(false).build();
  }

  public static Write write() {
    return new AutoValue_ArrowFlightIO_Write.Builder()
        .setPort(47470)
        .setUseTls(false)
        .setBatchSize(1024)
        .build();
  }

  /**
   * Creates a {@link FlightClient} from the given connection parameters.
   *
   * <p>The client uses a {@link RootAllocator} for Arrow memory management and connects to the
   * specified host and port using either plaintext or TLS.
   */
  static FlightClient createClient(
      BufferAllocator allocator, String host, int port, boolean useTls) {
    Location location;
    if (useTls) {
      location = Location.forGrpcTls(host, port);
    } else {
      location = Location.forGrpcInsecure(host, port);
    }
    return FlightClient.builder(allocator, location).build();
  }

  /** A serializable wrapper around Flight endpoint information for use in BoundedSource splits. */
  static class SerializableEndpoint implements Serializable {
    private static final long serialVersionUID = 1L;

    private final byte[] ticketBytes;
    private final @Nullable String host;
    private final int port;

    SerializableEndpoint(byte[] ticketBytes, @Nullable String host, int port) {
      this.ticketBytes = ticketBytes;
      this.host = host;
      this.port = port;
    }

    static SerializableEndpoint fromFlightEndpoint(
        FlightEndpoint endpoint, String defaultHost, int defaultPort) {
      byte[] ticket = endpoint.getTicket().getBytes();
      List<Location> locations = endpoint.getLocations();
      if (locations != null && !locations.isEmpty()) {
        URI uri = locations.get(0).getUri();
        return new SerializableEndpoint(ticket, uri.getHost(), uri.getPort());
      }
      return new SerializableEndpoint(ticket, defaultHost, defaultPort);
    }

    Ticket getTicket() {
      return new Ticket(ticketBytes);
    }

    String getHost(String defaultHost) {
      return host != null ? host : defaultHost;
    }

    int getPort(int defaultPort) {
      return port > 0 ? port : defaultPort;
    }
  }

  // ======================== READ ========================

  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<Row>> {

    abstract @Nullable String host();

    abstract int port();

    abstract boolean useTls();

    abstract @Nullable String command();

    @SuppressWarnings("mutable")
    abstract byte @Nullable [] token();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setHost(String host);

      abstract Builder setPort(int port);

      abstract Builder setUseTls(boolean useTls);

      abstract Builder setCommand(String command);

      abstract Builder setToken(byte[] token);

      abstract Read build();
    }

    /** Sets the Flight server host. */
    public Read withHost(String host) {
      return builder().setHost(host).build();
    }

    /** Sets the Flight server port. */
    public Read withPort(int port) {
      return builder().setPort(port).build();
    }

    /** Enables TLS for the connection. */
    public Read withUseTls(boolean useTls) {
      return builder().setUseTls(useTls).build();
    }

    /** Sets the command (e.g., a SQL query or table name) to request from the Flight server. */
    public Read withCommand(String command) {
      return builder().setCommand(command).build();
    }

    /** Sets a bearer token for authentication. */
    public Read withToken(byte[] token) {
      return builder().setToken(copyToken(token)).build();
    }

    @Override
    public PCollection<Row> expand(PBegin input) {
      checkArgument(host() != null, "withHost() is required");
      checkArgument(command() != null, "withCommand() is required");

      Schema beamSchema;
      try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
          FlightClient client =
              createClient(allocator, checkNotNull(host(), "host"), port(), useTls())) {
        FlightInfo info =
            client.getInfo(
                FlightDescriptor.command(
                    checkNotNull(command(), "command").getBytes(StandardCharsets.UTF_8)),
                callOptions());
        beamSchema = ArrowConversion.ArrowSchemaTranslator.toBeamSchema(info.getSchema());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while fetching Flight schema", e);
      }

      return input
          .apply(org.apache.beam.sdk.io.Read.from(new FlightBoundedSource(this, beamSchema)))
          .setRowSchema(beamSchema);
    }

    CallOption[] callOptions() {
      return ArrowFlightIO.callOptions(token());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("host", host()));
      builder.add(DisplayData.item("port", port()));
      builder.add(DisplayData.item("useTls", useTls()));
      builder.addIfNotNull(DisplayData.item("command", command()));
    }
  }

  /** A {@link BoundedSource} that reads rows from Arrow Flight endpoints. */
  static class FlightBoundedSource extends BoundedSource<Row> {
    private final Read spec;
    private final Schema beamSchema;
    private final @Nullable SerializableEndpoint endpoint;

    FlightBoundedSource(Read spec, Schema beamSchema) {
      this(spec, beamSchema, null);
    }

    FlightBoundedSource(Read spec, Schema beamSchema, @Nullable SerializableEndpoint endpoint) {
      this.spec = spec;
      this.beamSchema = beamSchema;
      this.endpoint = endpoint;
    }

    @Override
    public List<? extends BoundedSource<Row>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      if (endpoint != null) {
        return Collections.singletonList(this);
      }

      List<BoundedSource<Row>> sources = new ArrayList<>();
      try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
          FlightClient client =
              createClient(
                  allocator, checkNotNull(spec.host(), "host"), spec.port(), spec.useTls())) {
        FlightInfo info =
            client.getInfo(
                FlightDescriptor.command(
                    checkNotNull(spec.command(), "command").getBytes(StandardCharsets.UTF_8)),
                spec.callOptions());
        for (FlightEndpoint fe : info.getEndpoints()) {
          SerializableEndpoint se =
              SerializableEndpoint.fromFlightEndpoint(
                  fe, checkNotNull(spec.host(), "host"), spec.port());
          sources.add(new FlightBoundedSource(spec, beamSchema, se));
        }
      }

      if (sources.isEmpty()) {
        sources.add(this);
      }
      return sources;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      if (endpoint != null) {
        return -1;
      }
      try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
          FlightClient client =
              createClient(
                  allocator, checkNotNull(spec.host(), "host"), spec.port(), spec.useTls())) {
        FlightInfo info =
            client.getInfo(
                FlightDescriptor.command(
                    checkNotNull(spec.command(), "command").getBytes(StandardCharsets.UTF_8)),
                spec.callOptions());
        return info.getBytes();
      }
    }

    @Override
    public BoundedReader<Row> createReader(PipelineOptions options) {
      return new FlightBoundedReader(this);
    }

    @Override
    public void validate() {
      checkArgument(spec.host() != null, "host is required");
      checkArgument(spec.command() != null, "command is required");
    }

    @Override
    public Coder<Row> getOutputCoder() {
      return RowCoder.of(beamSchema);
    }
  }

  /** Reader that streams Arrow record batches from a Flight endpoint and emits Beam Rows. */
  @SuppressWarnings("initialization.fields.uninitialized")
  static class FlightBoundedReader extends BoundedSource.BoundedReader<Row> {
    private static final Counter RECORDS_READ = Metrics.counter(ArrowFlightIO.class, "recordsRead");

    private final FlightBoundedSource source;
    private transient BufferAllocator allocator;
    private transient FlightClient client;
    private transient FlightStream stream;
    private transient Iterator<Row> currentBatchIterator;
    private transient Row current;

    FlightBoundedReader(FlightBoundedSource source) {
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      allocator = new RootAllocator(Long.MAX_VALUE);
      Read spec = source.spec;

      String hostName = checkNotNull(spec.host(), "host");
      if (source.endpoint != null) {
        String host = source.endpoint.getHost(hostName);
        int port = source.endpoint.getPort(spec.port());
        client = createClient(allocator, host, port, spec.useTls());
        stream = client.getStream(source.endpoint.getTicket(), spec.callOptions());
      } else {
        client = createClient(allocator, hostName, spec.port(), spec.useTls());
        FlightInfo info =
            client.getInfo(
                FlightDescriptor.command(
                    checkNotNull(spec.command(), "command").getBytes(StandardCharsets.UTF_8)),
                spec.callOptions());
        List<FlightEndpoint> endpoints = info.getEndpoints();
        if (endpoints.isEmpty()) {
          return false;
        }
        stream = client.getStream(endpoints.get(0).getTicket(), spec.callOptions());
      }

      currentBatchIterator = Collections.emptyIterator();
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      while (true) {
        if (currentBatchIterator.hasNext()) {
          current = currentBatchIterator.next();
          RECORDS_READ.inc();
          return true;
        }
        if (stream.next()) {
          VectorSchemaRoot root = stream.getRoot();
          if (root.getRowCount() > 0) {
            Iterator<Row> lazyIterator =
                ArrowConversion.rowsFromRecordBatch(source.beamSchema, root);
            List<Row> materializedRows = new ArrayList<>();
            while (lazyIterator.hasNext()) {
              Row lazyRow = lazyIterator.next();
              materializedRows.add(
                  Row.withSchema(source.beamSchema).addValues(lazyRow.getValues()).build());
            }
            currentBatchIterator = materializedRows.iterator();
          }
        } else {
          return false;
        }
      }
    }

    @Override
    public Row getCurrent() {
      return current;
    }

    @Override
    public void close() throws IOException {
      try {
        if (stream != null) {
          stream.close();
        }
      } catch (Exception e) {
        LOG.warn("Error closing FlightStream", e);
      }
      try {
        if (client != null) {
          client.close();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Interrupted closing FlightClient", e);
      }
      try {
        if (allocator != null) {
          allocator.close();
        }
      } catch (Exception e) {
        LOG.warn("Error closing BufferAllocator", e);
      }
    }

    @Override
    public BoundedSource<Row> getCurrentSource() {
      return source;
    }
  }

  // ======================== WRITE ========================

  @AutoValue
  public abstract static class Write extends PTransform<PCollection<Row>, PDone> {

    abstract @Nullable String host();

    abstract int port();

    abstract boolean useTls();

    abstract @Nullable String descriptor();

    abstract int batchSize();

    @SuppressWarnings("mutable")
    abstract byte @Nullable [] token();

    abstract Builder builder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setHost(String host);

      abstract Builder setPort(int port);

      abstract Builder setUseTls(boolean useTls);

      abstract Builder setDescriptor(String descriptor);

      abstract Builder setBatchSize(int batchSize);

      abstract Builder setToken(byte[] token);

      abstract Write build();
    }

    /** Sets the Flight server host. */
    public Write withHost(String host) {
      return builder().setHost(host).build();
    }

    /** Sets the Flight server port. */
    public Write withPort(int port) {
      return builder().setPort(port).build();
    }

    /** Enables TLS for the connection. */
    public Write withUseTls(boolean useTls) {
      return builder().setUseTls(useTls).build();
    }

    /** Sets the Flight descriptor (table name or path) for the write target. */
    public Write withDescriptor(String descriptor) {
      return builder().setDescriptor(descriptor).build();
    }

    /** Sets the batch size for writing. Rows are buffered and flushed in batches. */
    public Write withBatchSize(int batchSize) {
      checkArgument(batchSize > 0, "batchSize must be positive");
      return builder().setBatchSize(batchSize).build();
    }

    /** Sets a bearer token for authentication. */
    public Write withToken(byte[] token) {
      return builder().setToken(copyToken(token)).build();
    }

    CallOption[] callOptions() {
      return ArrowFlightIO.callOptions(token());
    }

    @Override
    public PDone expand(PCollection<Row> input) {
      checkArgument(host() != null, "withHost() is required");
      checkArgument(descriptor() != null, "withDescriptor() is required");
      Schema inputSchema = checkNotNull(input.getSchema(), "input schema");
      validateWriteSchema(inputSchema);

      input.apply(ParDo.of(new FlightWriteFn(this, inputSchema)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.addIfNotNull(DisplayData.item("host", host()));
      builder.add(DisplayData.item("port", port()));
      builder.add(DisplayData.item("useTls", useTls()));
      builder.addIfNotNull(DisplayData.item("descriptor", descriptor()));
      builder.add(DisplayData.item("batchSize", batchSize()));
    }
  }

  /** DoFn that buffers Beam Rows and streams them as Arrow record batches to a Flight server. */
  @SuppressWarnings("initialization.fields.uninitialized")
  static class FlightWriteFn extends DoFn<Row, Void> {
    private static final Counter RECORDS_WRITTEN =
        Metrics.counter(ArrowFlightIO.class, "recordsWritten");
    private static final Counter BATCHES_WRITTEN =
        Metrics.counter(ArrowFlightIO.class, "batchesWritten");

    private final Write spec;
    private final Schema beamSchema;
    private transient @Nullable BufferAllocator allocator;
    private transient @Nullable FlightClient client;
    private transient FlightClient.@Nullable ClientStreamListener listener;
    private transient @Nullable VectorSchemaRoot root;
    private transient org.apache.arrow.vector.types.pojo.Schema arrowSchema;
    private transient List<Row> batch;

    FlightWriteFn(Write spec, Schema beamSchema) {
      this.spec = spec;
      this.beamSchema = beamSchema;
    }

    @StartBundle
    public void startBundle() {
      batch = new ArrayList<>();
    }

    @ProcessElement
    public void processElement(@Element Row row) {
      checkArgument(
          row.getSchema().equivalent(beamSchema),
          "ArrowFlightIO.write() requires all rows to use the same schema.");
      batch.add(row);
      if (batch.size() >= spec.batchSize()) {
        flush();
      }
    }

    @FinishBundle
    public void finishBundle() {
      RuntimeException failure = null;
      try {
        flush();
      } catch (RuntimeException e) {
        failure = e;
      }

      try {
        closeConnection();
      } catch (RuntimeException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }

      if (failure != null) {
        throw failure;
      }
    }

    @Teardown
    public void teardown() {
      try {
        closeConnection();
      } catch (RuntimeException e) {
        LOG.warn("Error closing Flight write connection during teardown", e);
      }
    }

    private void ensureConnection() {
      if (client == null) {
        BufferAllocator currentAllocator = new RootAllocator(Long.MAX_VALUE);
        allocator = currentAllocator;
        FlightClient currentClient =
            createClient(
                currentAllocator, checkNotNull(spec.host(), "host"), spec.port(), spec.useTls());
        client = currentClient;

        List<Field> arrowFields = new ArrayList<>();
        for (Schema.Field beamField : beamSchema.getFields()) {
          arrowFields.add(toArrowField(beamField));
        }
        arrowSchema = new org.apache.arrow.vector.types.pojo.Schema(arrowFields);
        VectorSchemaRoot currentRoot = VectorSchemaRoot.create(arrowSchema, currentAllocator);
        root = currentRoot;

        FlightDescriptor descriptor =
            FlightDescriptor.path(checkNotNull(spec.descriptor(), "descriptor"));
        listener =
            currentClient.startPut(
                descriptor, currentRoot, new AsyncPutListener(), spec.callOptions());
      }
    }

    private Field toArrowField(Schema.Field beamField) {
      ArrowType arrowType = beamTypeToArrowType(beamField.getType());
      FieldType fieldType =
          beamField.getType().getNullable()
              ? FieldType.nullable(arrowType)
              : FieldType.notNullable(arrowType);
      return new Field(beamField.getName(), fieldType, Collections.emptyList());
    }

    private ArrowType beamTypeToArrowType(Schema.FieldType beamType) {
      switch (beamType.getTypeName()) {
        case BYTE:
          return new ArrowType.Int(8, true);
        case INT16:
          return new ArrowType.Int(16, true);
        case INT32:
          return new ArrowType.Int(32, true);
        case INT64:
          return new ArrowType.Int(64, true);
        case FLOAT:
          return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        case DOUBLE:
          return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        case STRING:
          return ArrowType.Utf8.INSTANCE;
        case BOOLEAN:
          return ArrowType.Bool.INSTANCE;
        case BYTES:
          return ArrowType.Binary.INSTANCE;
        case DATETIME:
          return new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
        default:
          throw new IllegalArgumentException(
              "Unsupported Beam type for ArrowFlightIO.write(): " + beamType.getTypeName());
      }
    }

    @SuppressWarnings("nullness")
    private void flush() {
      if (batch == null || batch.isEmpty()) {
        return;
      }
      ensureConnection();

      for (int colIdx = 0; colIdx < beamSchema.getFieldCount(); colIdx++) {
        FieldVector vector = root.getVector(colIdx);
        vector.allocateNew();
        Schema.Field field = beamSchema.getField(colIdx);
        for (int rowIdx = 0; rowIdx < batch.size(); rowIdx++) {
          Object value = batch.get(rowIdx).getValue(colIdx);
          if (value == null) {
            vector.setNull(rowIdx);
          } else {
            setVectorValue(vector, rowIdx, value, field.getType());
          }
        }
        vector.setValueCount(batch.size());
      }
      root.setRowCount(batch.size());

      listener.putNext();
      RECORDS_WRITTEN.inc(batch.size());
      BATCHES_WRITTEN.inc();
      root.clear();
      batch.clear();
    }

    @SuppressWarnings("nullness")
    private void setVectorValue(
        FieldVector vector, int index, Object value, Schema.FieldType type) {
      switch (type.getTypeName()) {
        case BYTE:
          ((TinyIntVector) vector).setSafe(index, ((Number) value).byteValue());
          break;
        case INT16:
          ((SmallIntVector) vector).setSafe(index, ((Number) value).shortValue());
          break;
        case INT32:
          ((IntVector) vector).setSafe(index, ((Number) value).intValue());
          break;
        case INT64:
          ((BigIntVector) vector).setSafe(index, ((Number) value).longValue());
          break;
        case FLOAT:
          ((Float4Vector) vector).setSafe(index, ((Number) value).floatValue());
          break;
        case DOUBLE:
          ((Float8Vector) vector).setSafe(index, ((Number) value).doubleValue());
          break;
        case BOOLEAN:
          ((BitVector) vector).setSafe(index, ((Boolean) value) ? 1 : 0);
          break;
        case STRING:
          ((VarCharVector) vector)
              .setSafe(index, value.toString().getBytes(StandardCharsets.UTF_8));
          break;
        case BYTES:
          ((VarBinaryVector) vector).setSafe(index, (byte[]) value);
          break;
        case DATETIME:
          long millis;
          if (value instanceof org.joda.time.ReadableInstant) {
            millis = ((org.joda.time.ReadableInstant) value).getMillis();
          } else {
            millis = ((Number) value).longValue();
          }
          ((TimeStampMilliTZVector) vector).setSafe(index, millis);
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported Beam type for ArrowFlightIO.write(): " + type.getTypeName());
      }
    }

    private void closeConnection() {
      RuntimeException failure = null;
      FlightClient.ClientStreamListener currentListener = listener;
      listener = null;
      try {
        if (currentListener != null) {
          currentListener.completed();
          currentListener.getResult();
        }
      } catch (RuntimeException e) {
        failure = e;
      }

      VectorSchemaRoot currentRoot = root;
      root = null;
      try {
        if (currentRoot != null) {
          currentRoot.close();
        }
      } catch (Exception e) {
        if (failure == null) {
          failure = new RuntimeException("Error closing VectorSchemaRoot", e);
        } else {
          failure.addSuppressed(e);
        }
      }

      FlightClient currentClient = client;
      client = null;
      try {
        if (currentClient != null) {
          currentClient.close();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        RuntimeException closeFailure = new RuntimeException("Interrupted closing FlightClient", e);
        if (failure == null) {
          failure = closeFailure;
        } else {
          failure.addSuppressed(closeFailure);
        }
      }

      BufferAllocator currentAllocator = allocator;
      allocator = null;
      try {
        if (currentAllocator != null) {
          currentAllocator.close();
        }
      } catch (Exception e) {
        if (failure == null) {
          failure = new RuntimeException("Error closing BufferAllocator", e);
        } else {
          failure.addSuppressed(e);
        }
      }

      if (failure != null) {
        throw failure;
      }
    }
  }
}
