/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io.bigtable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.bigtable.v1.Mutation;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.VarLongCoder;
import com.google.cloud.dataflow.sdk.io.Sink.WriteOperation;
import com.google.cloud.dataflow.sdk.io.Sink.Writer;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.DataflowReleaseInfo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.Nullable;

/**
 * A bounded sink for Google Cloud Bigtable.
 *
 * <p>For more information, see the online documentation at
 * <a href="https://cloud.google.com/bigtable/">Google Cloud Bigtable</a>.
 *
 * <h3>Writing to Cloud Bigtable</h3>
 *
 * <p>The Bigtable sink executes a set of row mutations on a single table. It takes as input a
 * {@link PCollection PCollection&lt;KV&lt;ByteString, Iterable&lt;Mutation&gt;&gt;&gt;}, where the
 * {@link ByteString} is the key of the row being mutated, and each {@link Mutation} represents an
 * idempotent transformation to that row.
 *
 * <p>To configure a Cloud Bigtable sink, you must supply a table id and a {@link BigtableOptions}
 * or builder configured with the project and other information necessary to identify the
 * Bigtable cluster, for example:
 *
 * <pre>{@code
 * BigtableOptions.Builder optionsBuilder =
 *     new BigtableOptions.Builder()
 *         .setProjectId("project")
 *         .setClusterId("cluster")
 *         .setZoneId("zone");
 *
 * PCollection<KV<ByteString, Iterable<Mutation>>> data = ...;
 *
 * data.apply("write",
 *     BigtableIO.write()
 *         .withBigtableOptions(optionsBuilder)
 *         .withTableId("table"));
 * }</pre>
 *
 * <h3>Experimental</h3>
 *
 * <p>This connector for Cloud Bigtable is considered experimental and may break or receive
 * backwards-incompatible changes in future versions of the Cloud Dataflow SDK. Cloud Bigtable is
 * in Beta, and thus it may introduce breaking changes in future revisions of its service or APIs.
 *
 * <h3>Permissions</h3>
 *
 * <p>Permission requirements depend on the {@link PipelineRunner} that is used to execute the
 * Dataflow job. Please refer to the documentation of corresponding
 * {@link PipelineRunner PipelineRunners} for more details.
 */
@Experimental
public class BigtableIO {
  private static final Logger logger = LoggerFactory.getLogger(BigtableIO.class);

  /**
   * Creates an uninitialized {@link BigtableIO.Write}. Before use, the {@code Write} must be
   * initialized with a
   * {@link BigtableIO.Write#withBigtableOptions(BigtableOptions) BigtableOptions} that specifies
   * the destination Cloud Bigtable cluster, and a {@link BigtableIO.Write#withTableId table} that
   * specifies which table to write.
   */
  @Experimental
  public static Write write() {
    return new Write(null, "", null);
  }

  /**
   * A {@link PTransform} that writes to Google Cloud Bigtable. See the class-level Javadoc on
   * {@link BigtableIO} for more information.
   *
   * @see BigtableIO
   */
  @Experimental
  public static class Write
      extends PTransform<PCollection<KV<ByteString, Iterable<Mutation>>>, PDone> {
    /**
     * Used to define the Cloud Bigtable cluster and any options for the networking layer.
     * Cannot actually be {@code null} at validation time, but may start out {@code null} while
     * source is being built.
     */
    @Nullable private final BigtableOptions options;
    private final String tableId;
    @Nullable private final BigtableService bigtableService;

    private Write(
        @Nullable BigtableOptions options,
        String tableId,
        @Nullable BigtableService bigtableService) {
      this.options = options;
      this.tableId = checkNotNull(tableId, "tableId");
      this.bigtableService = bigtableService;
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write to the Cloud Bigtable cluster
     * indicated by the given options, and using any other specified customizations.
     *
     * <p>Does not modify this object.
     */
    public Write withBigtableOptions(BigtableOptions options) {
      checkNotNull(options, "options");
      return withBigtableOptions(options.toBuilder());
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write to the Cloud Bigtable cluster
     * indicated by the given options, and using any other specified customizations.
     *
     * <p>Clones the given {@link BigtableOptions.Builder} is cloned so that any further changes
     * will have no effect on the returned {@link BigtableIO.Write}.
     *
     * <p>Does not modify this object.
     */
    public Write withBigtableOptions(BigtableOptions.Builder optionsBuilder) {
      checkNotNull(optionsBuilder, "optionsBuilder");
      // TODO: is there a better way to clone a Builder? Want it to be immune from user changes.
      BigtableOptions.Builder clonedBuilder = optionsBuilder.build().toBuilder();
      BigtableOptions optionsWithAgent = clonedBuilder.setUserAgent(getUserAgent()).build();
      return new Write(optionsWithAgent, tableId, bigtableService);
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write to the specified table.
     *
     * <p>Does not modify this object.
     */
    public Write withTableId(String tableId) {
      checkNotNull(tableId, "tableId");
      return new Write(options, tableId, bigtableService);
    }

    /**
     * Returns the Google Cloud Bigtable cluster being written to, and other parameters.
     */
    public BigtableOptions getBigtableOptions() {
      return options;
    }

    /**
     * Returns the table being written to.
     */
    public String getTableId() {
      return tableId;
    }

    @Override
    public PDone apply(PCollection<KV<ByteString, Iterable<Mutation>>> input) {
      Sink sink = new Sink(tableId, getBigtableService());
      return input.apply(com.google.cloud.dataflow.sdk.io.Write.to(sink));
    }

    @Override
    public void validate(PCollection<KV<ByteString, Iterable<Mutation>>> input) {
      checkArgument(options != null, "BigtableOptions not specified");
      checkArgument(!tableId.isEmpty(), "Table ID not specified");
      try {
        checkArgument(
            getBigtableService().tableExists(tableId), "Table %s does not exist", tableId);
      } catch (IOException e) {
        logger.warn("Error checking whether table {} exists; proceeding.", tableId, e);
      }
    }

    /**
     * Returns a new {@link BigtableIO.Write} that will write using the given Cloud Bigtable
     * service implementation.
     *
     * <p>This is used for testing.
     *
     * <p>Does not modify this object.
     */
    Write withBigtableService(BigtableService bigtableService) {
      checkNotNull(bigtableService, "bigtableService");
      return new Write(options, tableId, bigtableService);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(Write.class)
          .add("options", options)
          .add("tableId", tableId)
          .toString();
    }

    /**
     * Helper function that either returns the mock Bigtable service supplied by
     * {@link #withBigtableService} or creates and returns an implementation that talks to
     * {@code Cloud Bigtable}.
     */
    private BigtableService getBigtableService() {
      if (bigtableService != null) {
        return bigtableService;
      }
      return new BigtableServiceImpl(options);
    }
  }

  private static class Sink
      extends com.google.cloud.dataflow.sdk.io.Sink<KV<ByteString, Iterable<Mutation>>> {

    public Sink(String tableId, BigtableService bigtableService) {
      this.tableId = checkNotNull(tableId, "tableId");
      this.bigtableService = checkNotNull(bigtableService, "bigtableService");
    }

    public String getTableId() {
      return tableId;
    }

    public BigtableService getBigtableService() {
      return bigtableService;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(Sink.class)
          .add("bigtableService", bigtableService)
          .add("tableId", tableId)
          .toString();
    }

    ///////////////////////////////////////////////////////////////////////////////
    private final String tableId;
    private final BigtableService bigtableService;

    @Override
    public WriteOperation<KV<ByteString, Iterable<Mutation>>, Long> createWriteOperation(
        PipelineOptions options) {
      return new BigtableWriteOperation(this);
    }

    /** Does nothing, as it is redundant with {@link Write#validate}. */
    @Override
    public void validate(PipelineOptions options) {}
  }

  private static class BigtableWriteOperation
      extends WriteOperation<KV<ByteString, Iterable<Mutation>>, Long> {
    private final Sink sink;

    public BigtableWriteOperation(Sink sink) {
      this.sink = sink;
    }

    @Override
    public Writer<KV<ByteString, Iterable<Mutation>>, Long> createWriter(PipelineOptions options)
        throws Exception {
      return new BigtableWriter(this);
    }

    @Override
    public void initialize(PipelineOptions options) {}

    @Override
    public void finalize(Iterable<Long> writerResults, PipelineOptions options) {
      long count = 0;
      for (Long value : writerResults) {
        value += count;
      }
      logger.debug("Wrote {} elements to BigtableIO.Sink {}", sink);
    }

    @Override
    public Sink getSink() {
      return sink;
    }

    @Override
    public Coder<Long> getWriterResultCoder() {
      return VarLongCoder.of();
    }
  }

  private static class BigtableWriter extends Writer<KV<ByteString, Iterable<Mutation>>, Long> {
    private final BigtableWriteOperation writeOperation;
    private final Sink sink;
    private BigtableService.Writer bigtableWriter;
    private long recordsWritten;
    private final ConcurrentLinkedQueue<BigtableWriteException> failures;

    public BigtableWriter(BigtableWriteOperation writeOperation) {
      this.writeOperation = writeOperation;
      this.sink = writeOperation.getSink();
      this.failures = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void open(String uId) throws Exception {
      bigtableWriter = sink.getBigtableService().openForWriting(sink.getTableId());
      recordsWritten = 0;
    }

    /**
     * If any write has asynchronously failed, fail the bundle with a useful error.
     */
    private void checkForFailures() throws IOException {
      // Note that this function is never called by multiple threads and is the only place that
      // we remove from failures, so this code is safe.
      if (failures.isEmpty()) {
        return;
      }

      StringBuilder logEntry = new StringBuilder();
      int i = 0;
      for (; i < 10 && !failures.isEmpty(); ++i) {
        BigtableWriteException exc = failures.remove();
        logEntry.append("\n").append(exc.getMessage());
        if (exc.getCause() != null) {
          logEntry.append(": ").append(exc.getCause().getMessage());
        }
      }
      String message =
          String.format(
              "At least %d errors occurred writing to Bigtable. First %d errors: %s",
              i + failures.size(),
              i,
              logEntry.toString());
      logger.error(message);
      throw new IOException(message);
    }

    @Override
    public void write(KV<ByteString, Iterable<Mutation>> rowMutations) throws Exception {
      checkForFailures();
      Futures.addCallback(
          bigtableWriter.writeRecord(rowMutations), new WriteExceptionCallback(rowMutations));
      ++recordsWritten;
    }

    @Override
    public Long close() throws Exception {
      bigtableWriter.close();
      bigtableWriter = null;
      checkForFailures();
      logger.info("Wrote {} records", recordsWritten);
      return recordsWritten;
    }

    @Override
    public WriteOperation<KV<ByteString, Iterable<Mutation>>, Long> getWriteOperation() {
      return writeOperation;
    }

    private class WriteExceptionCallback implements FutureCallback<Empty> {
      private final KV<ByteString, Iterable<Mutation>> value;

      public WriteExceptionCallback(KV<ByteString, Iterable<Mutation>> value) {
        this.value = value;
      }

      @Override
      public void onFailure(Throwable cause) {
        failures.add(new BigtableWriteException(value, cause));
      }

      @Override
      public void onSuccess(Empty produced) {}
    }
  }

  /**
   * An exception that puts information about the failed record being written in its message.
   */
  static class BigtableWriteException extends IOException {
    public BigtableWriteException(KV<ByteString, Iterable<Mutation>> record, Throwable cause) {
      super(
          String.format(
              "Error mutating row %s with mutations %s",
              record.getKey().toStringUtf8(),
              record.getValue()),
          cause);
    }
  }

  /**
   * A helper function to produce a Cloud Bigtable user agent string.
   */
  private static String getUserAgent() {
    String javaVersion = System.getProperty("java.specification.version");
    DataflowReleaseInfo info = DataflowReleaseInfo.getReleaseInfo();
    return String.format(
        "%s/%s (%s); %s",
        info.getName(),
        info.getVersion(),
        javaVersion,
        "0.2.3" /* TODO get Bigtable client version directly from jar. */);
  }
}
