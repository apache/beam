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
package org.apache.beam.sdk.io.gcp.firestore;

import static java.util.Objects.requireNonNull;

import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.WriteResult;
import com.google.rpc.Status;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1WriteFn.BatchWriteFnWithDeadLetterQueue;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1WriteFn.BatchWriteFnWithSummary;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@link FirestoreV1} provides an API which provides lifecycle managed {@link PTransform}s for <a
 * target="_blank" rel="noopener noreferrer"
 * href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1">Cloud Firestore
 * v1 API</a>.
 *
 * <p>This class is part of the Firestore Connector DSL and should be accessed via {@link
 * FirestoreIO#v1()}.
 *
 * <p>All {@link PTransform}s provided by this API use {@link
 * org.apache.beam.sdk.extensions.gcp.options.GcpOptions GcpOptions} on {@link
 * org.apache.beam.sdk.options.PipelineOptions PipelineOptions} for credentials access and projectId
 * resolution. As such, the lifecycle of gRPC clients and project information is scoped to the
 * bundle level, not the worker level.
 *
 * <p>
 *
 * <h3>Operations</h3>
 *
 * <h4>Write</h4>
 *
 * To write a {@link PCollection} to Cloud Firestore use {@link FirestoreV1#write()}, picking the
 * behavior of the writer.
 *
 * <p>Writes use Cloud Firestore's BatchWrite api which provides fine grained write semantics.
 *
 * <p>The default behavior is to fail a bundle if any single write fails with a non-retryable error.
 *
 * <pre>{@code
 * PCollection<Write> writes = ...;
 * PCollection<WriteSummary> sink = writes
 *     .apply(FirestoreIO.v1().write().batchWrite().build());
 * }</pre>
 *
 * Alternatively, if you'd rather output write failures to a Dead Letter Queue add {@link
 * BatchWriteWithSummary.Builder#withDeadLetterQueue() withDeadLetterQueue} when building your
 * writer.
 *
 * <pre>{@code
 * PCollection<Write> writes = ...;
 * PCollection<WriteFailure> writeFailures = writes
 *     .apply(FirestoreIO.v1().write().batchWrite().withDeadLetterQueue().build());
 * }</pre>
 *
 * <h3>Permissions</h3>
 *
 * Permission requirements depend on the {@code PipelineRunner} that is used to execute the
 * pipeline. Please refer to the documentation of corresponding {@code PipelineRunner}s for more
 * details.
 *
 * <p>Please see <a target="_blank" rel="noopener noreferrer"
 * href="https://cloud.google.com/firestore/docs/security/iam#roles">Security for server client
 * libraries > Roles </a>for security and permission related information specific to Cloud
 * Firestore.
 *
 * <p>Optionally, Cloud Firestore V1 Emulator, running locally, could be used for testing purposes
 * by providing the host port information via {@link FirestoreOptions#setEmulatorHost(String)}. In
 * such a case, all the Cloud Firestore API calls are directed to the Emulator.
 *
 * @see FirestoreIO#v1()
 * @see org.apache.beam.sdk.PipelineRunner
 * @see org.apache.beam.sdk.options.PipelineOptions
 * @see org.apache.beam.sdk.extensions.gcp.options.GcpOptions
 * @see <a target="_blank" rel="noopener noreferrer"
 *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1">Cloud
 *     Firestore v1 API</a>
 */
@Immutable
public final class FirestoreV1 {
  static final FirestoreV1 INSTANCE = new FirestoreV1();

  private FirestoreV1() {}

  /**
   * The class returned by this method provides the ability to create {@link PTransform PTransforms}
   * for write operations available in the Firestore V1 API provided by {@link
   * com.google.cloud.firestore.v1.stub.FirestoreStub FirestoreStub}.
   *
   * <p>This method is part of the Firestore Connector DSL and should be accessed via {@link
   * FirestoreIO#v1()}.
   *
   * <p>
   *
   * @return Type safe builder factory for write operations.
   * @see FirestoreIO#v1()
   */
  public Write write() {
    return Write.INSTANCE;
  }

  /**
   * Type safe builder factory for write operations.
   *
   * <p>This class is part of the Firestore Connector DSL and should be accessed via {@link #write()
   * FirestoreIO.v1().write()}.
   *
   * <p>
   *
   * <p>This class provides access to a set of type safe builders for supported write operations
   * available in the Firestore V1 API accessed through {@link
   * com.google.cloud.firestore.v1.stub.FirestoreStub FirestoreStub}. Each builder allows
   * configuration before creating an immutable instance which can be used in your pipeline.
   *
   * @see FirestoreIO#v1()
   * @see #write()
   */
  @Experimental(Kind.SOURCE_SINK)
  @Immutable
  public static final class Write {
    private static final Write INSTANCE = new Write();

    private Write() {}

    /**
     * Factory method to create a new type safe builder for {@link com.google.firestore.v1.Write}
     * operations.
     *
     * <p>By default, when an error is encountered while trying to write to Cloud Firestore a {@link
     * FailedWritesException} will be thrown. If you would like a failed write to not result in a
     * {@link FailedWritesException}, you can instead use {@link BatchWriteWithDeadLetterQueue}
     * which will output any failed write. {@link BatchWriteWithDeadLetterQueue} can be used by
     * including {@link BatchWriteWithSummary.Builder#withDeadLetterQueue()} when constructing the
     * write handler.
     *
     * <p>This method is part of the Firestore Connector DSL and should be accessed via {@link
     * FirestoreIO#v1()}.
     *
     * <p>All request quality-of-service for the built {@link BatchWriteWithSummary} PTransform is
     * scoped to the worker and configured based on the {@link RpcQosOptions} specified via this
     * builder.
     *
     * @return A new type safe builder providing configuration for processing of {@link
     *     com.google.firestore.v1.Write}s
     * @see FirestoreIO#v1()
     * @see BatchWriteWithSummary
     * @see BatchWriteRequest
     * @see com.google.firestore.v1.BatchWriteResponse
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.BatchWrite">google.firestore.v1.Firestore.BatchWrite</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchWriteRequest">google.firestore.v1.BatchWriteRequest</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchWriteResponse">google.firestore.v1.BatchWriteResponse</a>
     */
    public BatchWriteWithSummary.Builder batchWrite() {
      return new BatchWriteWithSummary.Builder();
    }
  }

  /**
   * Concrete class representing a {@link PTransform}{@code <}{@link PCollection}{@code <}{@link
   * com.google.firestore.v1.Write}{@code >, }{@link PDone}{@code >} which will write to Firestore.
   *
   * <p>If an error is encountered while trying to write to Cloud Firestore a {@link
   * FailedWritesException} will be thrown. If you would like a failed write to not result in a
   * {@link FailedWritesException}, you can instead use {@link BatchWriteWithDeadLetterQueue} which
   * will instead output any failed write. {@link BatchWriteWithDeadLetterQueue } can be used by
   * including {@link Builder#withDeadLetterQueue()} when constructing the write handler.
   *
   * <p>This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
   * {@link FirestoreIO#v1()}{@code .}{@link FirestoreV1#write() write()}{@code .}{@link
   * FirestoreV1.Write#batchWrite() batchWrite()}.
   *
   * <p>All request quality-of-service for an instance of this PTransform is scoped to the worker
   * and configured via {@link Builder#withRpcQosOptions(RpcQosOptions)}.
   *
   * <p>Writes performed against Firestore will be ordered and grouped to maximize throughput while
   * maintaining a high request success rate. Batch sizes will be determined by the QOS layer.
   *
   * @see FirestoreIO#v1()
   * @see FirestoreV1#write()
   * @see FirestoreV1.Write#batchWrite()
   * @see BatchWriteWithSummary.Builder
   * @see BatchWriteWithDeadLetterQueue
   * @see BatchWriteRequest
   * @see com.google.firestore.v1.BatchWriteResponse
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.BatchWrite">google.firestore.v1.Firestore.BatchWrite</a>
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchWriteRequest">google.firestore.v1.BatchWriteRequest</a>
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchWriteResponse">google.firestore.v1.BatchWriteResponse</a>
   */
  public static final class BatchWriteWithSummary
      extends Transform<
          PCollection<com.google.firestore.v1.Write>,
          PCollection<WriteSuccessSummary>,
          BatchWriteWithSummary,
          BatchWriteWithSummary.Builder> {

    private BatchWriteWithSummary(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions);
    }

    @Override
    public PCollection<WriteSuccessSummary> expand(
        PCollection<com.google.firestore.v1.Write> input) {
      return input.apply(
          "batchWrite",
          ParDo.of(
              new BatchWriteFnWithSummary(
                  clock,
                  firestoreStatefulComponentFactory,
                  rpcQosOptions,
                  CounterFactory.DEFAULT)));
    }

    @Override
    public Builder toBuilder() {
      return new Builder(clock, firestoreStatefulComponentFactory, rpcQosOptions);
    }

    /**
     * A type safe builder for {@link BatchWriteWithSummary} allowing configuration and
     * instantiation.
     *
     * <p>This class is part of the Firestore Connector DSL, it has a type safe builder accessible
     * via {@link FirestoreIO#v1()}{@code .}{@link FirestoreV1#write() write()}{@code .}{@link
     * FirestoreV1.Write#batchWrite() batchWrite()}.
     *
     * <p>
     *
     * @see FirestoreIO#v1()
     * @see FirestoreV1#write()
     * @see FirestoreV1.Write#batchWrite()
     * @see BatchWriteWithSummary
     * @see BatchWriteRequest
     * @see com.google.firestore.v1.BatchWriteResponse
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.BatchWrite">google.firestore.v1.Firestore.BatchWrite</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchWriteRequest">google.firestore.v1.BatchWriteRequest</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchWriteResponse">google.firestore.v1.BatchWriteResponse</a>
     */
    public static final class Builder
        extends Transform.Builder<
            PCollection<com.google.firestore.v1.Write>,
            PCollection<WriteSuccessSummary>,
            BatchWriteWithSummary,
            BatchWriteWithSummary.Builder> {

      private Builder() {
        super();
      }

      private Builder(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions) {
        super(clock, firestoreStatefulComponentFactory, rpcQosOptions);
      }

      public BatchWriteWithDeadLetterQueue.Builder withDeadLetterQueue() {
        return new BatchWriteWithDeadLetterQueue.Builder(
            clock, firestoreStatefulComponentFactory, rpcQosOptions);
      }

      @Override
      public BatchWriteWithSummary build() {
        return genericBuild();
      }

      @Override
      BatchWriteWithSummary buildSafe(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions) {
        return new BatchWriteWithSummary(clock, firestoreStatefulComponentFactory, rpcQosOptions);
      }
    }
  }

  /**
   * Concrete class representing a {@link PTransform}{@code <}{@link PCollection}{@code <}{@link
   * com.google.firestore.v1.Write}{@code >, }{@link PCollection}{@code <}{@link WriteFailure}{@code
   * >} which will write to Firestore. {@link WriteFailure}s output by this {@code PTransform} are
   * those writes which were not able to be applied to Cloud Firestore.
   *
   * <p>Use this BatchWrite when you do not want a failed write to error an entire bundle.
   *
   * <p>This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
   * {@link FirestoreIO#v1()}{@code .}{@link FirestoreV1#write() write()}{@code .}{@link
   * FirestoreV1.Write#batchWrite() batchWrite()}{@code .}{@link
   * BatchWriteWithSummary.Builder#withDeadLetterQueue() withDeadLetterQueue()}.
   *
   * <p>All request quality-of-service for an instance of this PTransform is scoped to the worker
   * and configured via {@link Builder#withRpcQosOptions(RpcQosOptions)}.
   *
   * <p>Writes performed against Firestore will be ordered and grouped to maximize throughput while
   * maintaining a high request success rate. Batch sizes will be determined by the QOS layer.
   *
   * @see FirestoreIO#v1()
   * @see FirestoreV1#write()
   * @see FirestoreV1.Write#batchWrite()
   * @see BatchWriteWithSummary.Builder
   * @see BatchWriteWithSummary.Builder#withDeadLetterQueue()
   * @see BatchWriteRequest
   * @see com.google.firestore.v1.BatchWriteResponse
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.BatchWrite">google.firestore.v1.Firestore.BatchWrite</a>
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchWriteRequest">google.firestore.v1.BatchWriteRequest</a>
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchWriteResponse">google.firestore.v1.BatchWriteResponse</a>
   */
  public static final class BatchWriteWithDeadLetterQueue
      extends Transform<
          PCollection<com.google.firestore.v1.Write>,
          PCollection<WriteFailure>,
          BatchWriteWithDeadLetterQueue,
          BatchWriteWithDeadLetterQueue.Builder> {

    private BatchWriteWithDeadLetterQueue(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions);
    }

    @Override
    public PCollection<WriteFailure> expand(PCollection<com.google.firestore.v1.Write> input) {
      return input.apply(
          "batchWrite",
          ParDo.of(
              new BatchWriteFnWithDeadLetterQueue(
                  clock,
                  firestoreStatefulComponentFactory,
                  rpcQosOptions,
                  CounterFactory.DEFAULT)));
    }

    @Override
    public Builder toBuilder() {
      return new Builder(clock, firestoreStatefulComponentFactory, rpcQosOptions);
    }

    /**
     * A type safe builder for {@link BatchWriteWithDeadLetterQueue} allowing configuration and
     * instantiation.
     *
     * <p>This class is part of the Firestore Connector DSL, it has a type safe builder accessible
     * via {@link FirestoreIO#v1()}{@code .}{@link FirestoreV1#write() write()}{@code .}{@link
     * FirestoreV1.Write#batchWrite() batchWrite()}.
     *
     * <p>
     *
     * @see FirestoreIO#v1()
     * @see FirestoreV1#write()
     * @see FirestoreV1.Write#batchWrite()
     * @see BatchWriteWithSummary
     * @see BatchWriteRequest
     * @see com.google.firestore.v1.BatchWriteResponse
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.BatchWrite">google.firestore.v1.Firestore.BatchWrite</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchWriteRequest">google.firestore.v1.BatchWriteRequest</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchWriteResponse">google.firestore.v1.BatchWriteResponse</a>
     */
    public static final class Builder
        extends Transform.Builder<
            PCollection<com.google.firestore.v1.Write>,
            PCollection<WriteFailure>,
            BatchWriteWithDeadLetterQueue,
            BatchWriteWithDeadLetterQueue.Builder> {

      private Builder() {
        super();
      }

      private Builder(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions) {
        super(clock, firestoreStatefulComponentFactory, rpcQosOptions);
      }

      @Override
      public BatchWriteWithDeadLetterQueue build() {
        return genericBuild();
      }

      @Override
      BatchWriteWithDeadLetterQueue buildSafe(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions) {
        return new BatchWriteWithDeadLetterQueue(
            clock, firestoreStatefulComponentFactory, rpcQosOptions);
      }
    }
  }

  /**
   * Summary object produced when a number of writes are successfully written to Firestore in a
   * single BatchWrite.
   */
  @Immutable
  public static final class WriteSuccessSummary implements Serializable {
    private final int numWrites;
    private final long numBytes;

    public WriteSuccessSummary(int numWrites, long numBytes) {
      this.numWrites = numWrites;
      this.numBytes = numBytes;
    }

    public int getNumWrites() {
      return numWrites;
    }

    public long getNumBytes() {
      return numBytes;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof WriteSuccessSummary)) {
        return false;
      }
      WriteSuccessSummary that = (WriteSuccessSummary) o;
      return numWrites == that.numWrites && numBytes == that.numBytes;
    }

    @Override
    public int hashCode() {
      return Objects.hash(numWrites, numBytes);
    }

    @Override
    public String toString() {
      return "WriteSummary{" + "numWrites=" + numWrites + ", numBytes=" + numBytes + '}';
    }
  }

  /**
   * Failure details for an attempted {@link com.google.firestore.v1.Write}. When a {@link
   * com.google.firestore.v1.Write} is unable to be applied an instance of this class will be
   * created with the details of the failure.
   *
   * <p>Included data:
   *
   * <ul>
   *   <li>The original {@link com.google.firestore.v1.Write}
   *   <li>The {@link WriteResult} returned by the Cloud Firestore API
   *   <li>The {@link Status} returned by the Cloud Firestore API (often {@link Status#getMessage()}
   *       will provide details of why the write was unsuccessful
   * </ul>
   */
  @Immutable
  public static final class WriteFailure implements Serializable {
    private final com.google.firestore.v1.Write write;
    private final WriteResult writeResult;
    private final Status status;

    public WriteFailure(
        com.google.firestore.v1.Write write, WriteResult writeResult, Status status) {
      this.write = write;
      this.writeResult = writeResult;
      this.status = status;
    }

    public com.google.firestore.v1.Write getWrite() {
      return write;
    }

    public WriteResult getWriteResult() {
      return writeResult;
    }

    public Status getStatus() {
      return status;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof WriteFailure)) {
        return false;
      }
      WriteFailure that = (WriteFailure) o;
      return write.equals(that.write)
          && writeResult.equals(that.writeResult)
          && status.equals(that.status);
    }

    @Override
    public int hashCode() {
      return Objects.hash(write, writeResult, status);
    }
  }

  /**
   * Exception that is thrown if one or more {@link com.google.firestore.v1.Write}s is unsuccessful
   * with a non-retryable status code.
   */
  public static class FailedWritesException extends RuntimeException {
    private final List<WriteFailure> writeFailures;

    public FailedWritesException(List<WriteFailure> writeFailures) {
      super(String.format("Not-retryable status code(s) for %d writes", writeFailures.size()));
      this.writeFailures = writeFailures;
    }

    /** This list of {@link WriteFailure}s detailing which writes failed and for what reason. */
    public List<WriteFailure> getWriteFailures() {
      return writeFailures;
    }
  }

  /**
   * Our base PTransform class for Firestore V1 API related functions.
   *
   * <p>
   *
   * @param <InT> The type of the previous stage of the pipeline, usually a {@link PCollection} of a
   *     request type from {@link com.google.firestore.v1}
   * @param <OutT> The type returned from the RPC operation (usually a response class from {@link
   *     com.google.firestore.v1})
   * @param <TrfmT> The type of this transform used to bind this type and the corresponding type
   *     safe {@link BldrT} together
   * @param <BldrT> The type of the type safe builder which is used to build and instance of {@link
   *     TrfmT}
   */
  private abstract static class Transform<
          InT extends PInput,
          OutT extends POutput,
          TrfmT extends Transform<InT, OutT, TrfmT, BldrT>,
          BldrT extends Transform.Builder<InT, OutT, TrfmT, BldrT>>
      extends PTransform<InT, OutT> implements HasDisplayData {
    final JodaClock clock;
    final FirestoreStatefulComponentFactory firestoreStatefulComponentFactory;
    final RpcQosOptions rpcQosOptions;

    Transform(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions) {
      this.clock = clock;
      this.firestoreStatefulComponentFactory = firestoreStatefulComponentFactory;
      this.rpcQosOptions = rpcQosOptions;
    }

    @Override
    public final void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
    }

    /**
     * Create a new {@link BldrT Builder} from the current instance.
     *
     * @return a new instance of a {@link BldrT Builder} initialized to the current state of this
     *     instance
     */
    public abstract BldrT toBuilder();

    /**
     * Our base type safe builder for a {@link FirestoreV1.Transform}
     *
     * <p>This type safe builder provides a user (and semver) friendly way to expose optional
     * parameters to those users that wish to configure them. Additionally, we are able to add and
     * deprecate individual parameters as may be needed.
     *
     * @param <InT> The type of the previous stage of the pipeline, usually a {@link PCollection} of
     *     a request type from {@link com.google.firestore.v1}
     * @param <OutT> The type returned from the RPC operation (usually a response class from {@link
     *     com.google.firestore.v1})
     * @param <TrfmT> The type of this transform used to bind this type and the corresponding type
     *     safe {@link BldrT} together
     * @param <BldrT> The type of the type safe builder which is used to build and instance of
     *     {@link TrfmT}
     */
    abstract static class Builder<
        InT extends PInput,
        OutT extends POutput,
        TrfmT extends Transform<InT, OutT, TrfmT, BldrT>,
        BldrT extends Transform.Builder<InT, OutT, TrfmT, BldrT>> {
      JodaClock clock;
      FirestoreStatefulComponentFactory firestoreStatefulComponentFactory;
      RpcQosOptions rpcQosOptions;

      Builder() {
        clock = JodaClock.DEFAULT;
        firestoreStatefulComponentFactory = FirestoreStatefulComponentFactory.INSTANCE;
        rpcQosOptions = RpcQosOptions.defaultOptions();
      }

      private Builder(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions) {
        this.clock = clock;
        this.firestoreStatefulComponentFactory = firestoreStatefulComponentFactory;
        this.rpcQosOptions = rpcQosOptions;
      }

      /**
       * Convenience method to take care of hiding the unchecked cast warning from the compiler.
       * This cast is safe because we are always an instance of {@link BldrT} as the only way to get
       * an instance of {@link FirestoreV1.Transform.Builder} is for it to conform to {@code Bldr}'s
       * constraints.
       *
       * @return Down cast this
       */
      @SuppressWarnings({"unchecked", "RedundantSuppression"})
      private BldrT self() {
        return (BldrT) this;
      }

      /**
       * Create a new instance of {@link TrfmT Transform} from the current builder state.
       *
       * @return a new instance of {@link TrfmT Transform} from the current builder state.
       */
      public abstract TrfmT build();

      /**
       * Provide a central location for the validation before ultimately constructing a transformer.
       *
       * <p>While this method looks to purely be duplication (given that each implementation of
       * {@link #build()}) simply delegates to this method, the build method carries with it the
       * concrete class rather than the generic type information. Having the concrete class
       * available to users is advantageous to reduce the necessity of reading the complex type
       * information and instead presenting them with a concrete class name.
       *
       * <p>Comparing the type of the builder at the use site of each method:
       *
       * <table>
       *   <tr>
       *     <th>{@code build()}</th>
       *     <th>{@code genericBuild()}</th>
       *   </tr>
       *   <tr>
       *     <td><pre>{@code FirestoreV1.BatchGetDocuments.Builder<In>}</pre></td>
       *     <td><pre>{@code FirestoreV1.Transform.Builder<
       *            In extends PInput,
       *            Out extends POutput,
       *            Trfm extends FirestoreV1.Transform<In, Out, Trfm, Bldr>,
       *            Bldr extends FirestoreV1.Transform.Builder<In, Out, Trfm, Bldr>
       *          >}</pre></td>
       *   </tr>
       * </table>
       *
       * While this type information is important for our implementation, it is less important for
       * the users using our implementation.
       */
      final TrfmT genericBuild() {
        return buildSafe(
            requireNonNull(clock, "clock must be non null"),
            requireNonNull(firestoreStatefulComponentFactory, "firestoreFactory must be non null"),
            requireNonNull(rpcQosOptions, "rpcQosOptions must be non null"));
      }

      abstract TrfmT buildSafe(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions);

      /**
       * Specify the {@link RpcQosOptions} that will be used when bootstrapping the QOS of each
       * running instance of the {@link TrfmT Transform} created by this builder.
       *
       * <p><i>NOTE</i> This method behaves as set, mutating the value in this builder instance.
       *
       * @param rpcQosOptions The QOS Options to use when bootstrapping and running the built {@link
       *     TrfmT Transform}.
       * @return this builder
       * @see RpcQosOptions
       * @see RpcQosOptions#defaultOptions()
       * @see RpcQosOptions#newBuilder()
       */
      public final BldrT withRpcQosOptions(RpcQosOptions rpcQosOptions) {
        requireNonNull(rpcQosOptions, "rpcQosOptions must be non null");
        this.rpcQosOptions = rpcQosOptions;
        return self();
      }
    }
  }
}
