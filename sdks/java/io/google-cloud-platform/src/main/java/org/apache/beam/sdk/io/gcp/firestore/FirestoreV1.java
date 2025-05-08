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

import com.google.firestore.v1.BatchGetDocumentsRequest;
import com.google.firestore.v1.BatchGetDocumentsResponse;
import com.google.firestore.v1.BatchWriteRequest;
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.ListCollectionIdsRequest;
import com.google.firestore.v1.ListCollectionIdsResponse;
import com.google.firestore.v1.ListDocumentsRequest;
import com.google.firestore.v1.ListDocumentsResponse;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.PartitionQueryResponse;
import com.google.firestore.v1.RunQueryRequest;
import com.google.firestore.v1.RunQueryResponse;
import com.google.firestore.v1.StructuredQuery;
import com.google.firestore.v1.StructuredQuery.FieldReference;
import com.google.firestore.v1.StructuredQuery.Projection;
import com.google.firestore.v1.Value;
import com.google.firestore.v1.WriteResult;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Status;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.concurrent.Immutable;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.BatchGetDocumentsFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.ListCollectionIdsFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.ListDocumentsFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.PartitionQueryFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.PartitionQueryPair;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1ReadFn.RunQueryFn;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1WriteFn.BatchWriteFnWithDeadLetterQueue;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1WriteFn.BatchWriteFnWithSummary;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

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
 * <h4>Read</h4>
 *
 * <p>The currently supported read operations and their execution behavior are as follows:
 *
 * <table>
 *   <tbody>
 *     <tr>
 *       <th>RPC</th>
 *       <th>Execution Behavior</th>
 *       <th>Example Usage</th>
 *     </tr>
 *     <tr>
 *       <td>{@link PartitionQuery}</td>
 *       <td>Parallel Streaming</td>
 *       <td>
 * <pre>
 * PCollection<{@link PartitionQueryRequest}> partitionQueryRequests = ...;
 * PCollection<{@link RunQueryRequest}> runQueryRequests = partitionQueryRequests
 *     .apply(FirestoreIO.v1().read().{@link Read#partitionQuery() partitionQuery()}.build());
 * PCollection<{@link RunQueryResponse}> runQueryResponses = runQueryRequests
 *     .apply(FirestoreIO.v1().read().{@link Read#runQuery() runQuery()}.build());
 * </pre>
 *       </td>
 *     </tr>
 *     <tr>
 *       <td>{@link RunQuery}</td>
 *       <td>Sequential Streaming</td>
 *       <td>
 * <pre>
 * PCollection<{@link RunQueryRequest}> runQueryRequests = ...;
 * PCollection<{@link RunQueryResponse}> runQueryResponses = runQueryRequests
 *     .apply(FirestoreIO.v1().read().{@link Read#runQuery() runQuery()}.build());
 * </pre>
 *       </td>
 *     </tr>
 *     <tr>
 *       <td>{@link BatchGetDocuments}</td>
 *       <td>Sequential Streaming</td>
 *       <td>
 * <pre>
 * PCollection<{@link BatchGetDocumentsRequest}> batchGetDocumentsRequests = ...;
 * PCollection<{@link BatchGetDocumentsResponse}> batchGetDocumentsResponses = batchGetDocumentsRequests
 *     .apply(FirestoreIO.v1().read().{@link Read#batchGetDocuments() batchGetDocuments()}.build());
 * </pre>
 *       </td>
 *     </tr>
 *     <tr>
 *       <td>{@link ListCollectionIds}</td>
 *       <td>Sequential Paginated</td>
 *       <td>
 * <pre>
 * PCollection<{@link ListCollectionIdsRequest}> listCollectionIdsRequests = ...;
 * PCollection<{@link ListCollectionIdsResponse}> listCollectionIdsResponses = listCollectionIdsRequests
 *     .apply(FirestoreIO.v1().read().{@link Read#listCollectionIds() listCollectionIds()}.build());
 * </pre>
 *       </td>
 *     </tr>
 *     <tr>
 *       <td>{@link ListDocuments}</td>
 *       <td>Sequential Paginated</td>
 *       <td>
 * <pre>
 * PCollection<{@link ListDocumentsRequest}> listDocumentsRequests = ...;
 * PCollection<{@link ListDocumentsResponse}> listDocumentsResponses = listDocumentsRequests
 *     .apply(FirestoreIO.v1().read().{@link Read#listDocuments() listDocuments()}.build());
 * </pre>
 *       </td>
 *     </tr>
 *   </tbody>
 * </table>
 *
 * <p>PartitionQuery should be preferred over other options if at all possible, becuase it has the
 * ability to parallelize execution of multiple queries for specific sub-ranges of the full results.
 * When choosing the value to set for {@link PartitionQueryRequest.Builder#setPartitionCount(long)},
 * ensure you are picking a value this makes sense for your data set and your max number of workers.
 * <i>If you find that a partition query is taking a unexpectedly long time, try increasing the
 * number of partitions.</i> Depending on how large your dataset is increasing as much as 10x can
 * significantly reduce total partition query wall time.
 *
 * <p>You should only ever use ListDocuments if the use of <a target="_blank" rel="noopener
 * noreferrer"
 * href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.ListDocumentsRequest">{@code
 * show_missing}</a> is needed to access a document. RunQuery and PartitionQuery will always be
 * faster if the use of {@code show_missing} is not needed.
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
 * <pre>
 * PCollection<{@link com.google.firestore.v1.Write}> writes = ...;
 * PCollection<{@link WriteSuccessSummary}> sink = writes
 *     .apply(FirestoreIO.v1().write().{@link Write#batchWrite() batchWrite()}.build());
 * </pre>
 *
 * Alternatively, if you'd rather output write failures to a Dead Letter Queue add {@link
 * BatchWriteWithSummary.Builder#withDeadLetterQueue() withDeadLetterQueue} when building your
 * writer.
 *
 * <pre>
 * PCollection<{@link com.google.firestore.v1.Write}> writes = ...;
 * PCollection<{@link WriteFailure}> writeFailures = writes
 *     .apply(FirestoreIO.v1().write().{@link Write#batchWrite() batchWrite()}.withDeadLetterQueue().build());
 * </pre>
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
   * for read operations available in the Firestore V1 API provided by {@link
   * com.google.cloud.firestore.v1.stub.FirestoreStub FirestoreStub}.
   *
   * <p>This method is part of the Firestore Connector DSL and should be accessed via {@link
   * FirestoreIO#v1()}.
   *
   * <p>
   *
   * @return Type safe builder factory for read operations.
   * @see FirestoreIO#v1()
   */
  public Read read() {
    return Read.INSTANCE;
  }

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
   * Type safe builder factory for read operations.
   *
   * <p>This class is part of the Firestore Connector DSL and should be accessed via {@link #read()
   * FirestoreIO.v1().read()}.
   *
   * <p>This class provides access to a set of type safe builders for read operations available in
   * the Firestore V1 API accessed through {@link com.google.cloud.firestore.v1.stub.FirestoreStub
   * FirestoreStub}. Each builder allows configuration before creating an immutable instance which
   * can be used in your pipeline.
   *
   * <p>
   *
   * @see FirestoreIO#v1()
   * @see #read()
   */
  @Immutable
  public static final class Read {
    private static final Read INSTANCE = new Read();

    private Read() {}

    /**
     * Factory method to create a new type safe builder for {@link ListDocumentsRequest} operations.
     *
     * <p>This method is part of the Firestore Connector DSL and should be accessed via {@link
     * FirestoreIO#v1()}.
     *
     * <p>All request quality-of-service for the built {@link ListDocuments} PTransform is scoped to
     * the worker and configured based on the {@link RpcQosOptions} specified via this builder.
     *
     * <p>All logging for the built instance of {@link ListDocuments} will be sent to appender
     * {@code org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.ListDocuments}.
     *
     * <p>The following metrics will be available for the built instance of {@link ListDocuments}
     *
     * <ol>
     *   <li>{@code org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.ListDocuments.throttlingMs} A
     *       counter tracking the number of milliseconds RPCs are throttled by Qos
     *   <li>{@code org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.ListDocuments.rpcFailures} A
     *       counter tracking the number of failed RPCs
     *   <li>{@code org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.ListDocuments.rpcSuccesses} A
     *       counter tracking the number of successful RPCs
     * </ol>
     *
     * @return A new type safe builder providing configuration for processing of {@link
     *     ListDocumentsRequest}s
     * @see FirestoreIO#v1()
     * @see ListDocuments
     * @see ListDocumentsRequest
     * @see ListDocumentsResponse
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.ListDocuments">google.firestore.v1.Firestore.ListDocuments</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.ListDocumentsRequest">google.firestore.v1.ListDocumentsRequest</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.ListDocumentsResponse">google.firestore.v1.ListDocumentsResponse</a>
     */
    public ListDocuments.Builder listDocuments() {
      return new ListDocuments.Builder();
    }

    /**
     * Factory method to create a new type safe builder for {@link ListCollectionIdsRequest}
     * operations.
     *
     * <p>This method is part of the Firestore Connector DSL and should be accessed via {@link
     * FirestoreIO#v1()}.
     *
     * <p>All request quality-of-service for the built {@link ListCollectionIds} PTransform is
     * scoped to the worker and configured based on the {@link RpcQosOptions} specified via this
     * builder.
     *
     * <p>All logging for the built instance of {@link ListCollectionIds} will be sent to appender
     * {@code org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.ListCollectionIds}.
     *
     * <p>The following metrics will be available for the built instance of {@link
     * ListCollectionIds}
     *
     * <ol>
     *   <li>{@code org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.ListCollectionIds.throttlingMs}
     *       A counter tracking the number of milliseconds RPCs are throttled by Qos
     *   <li>{@code org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.ListCollectionIds.rpcFailures}
     *       A counter tracking the number of failed RPCs
     *   <li>{@code org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.ListCollectionIds.rpcSuccesses}
     *       A counter tracking the number of successful RPCs
     * </ol>
     *
     * @return A new type safe builder providing configuration for processing of {@link
     *     ListCollectionIdsRequest}s
     * @see FirestoreIO#v1()
     * @see ListCollectionIds
     * @see ListCollectionIdsRequest
     * @see ListCollectionIdsResponse
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.ListCollectionIds">google.firestore.v1.Firestore.ListCollectionIds</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.ListCollectionIdsRequest">google.firestore.v1.ListCollectionIdsRequest</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.ListCollectionIdsResponse">google.firestore.v1.ListCollectionIdsResponse</a>
     */
    public ListCollectionIds.Builder listCollectionIds() {
      return new ListCollectionIds.Builder();
    }

    /**
     * Factory method to create a new type safe builder for {@link BatchGetDocumentsRequest}
     * operations.
     *
     * <p>This method is part of the Firestore Connector DSL and should be accessed via {@link
     * FirestoreIO#v1()}.
     *
     * <p>All request quality-of-service for the built {@link BatchGetDocuments} PTransform is
     * scoped to the worker and configured based on the {@link RpcQosOptions} specified via this
     * builder.
     *
     * <p>All logging for the built instance of {@link BatchGetDocuments} will be sent to appender
     * {@code org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.BatchGetDocuments}.
     *
     * <p>The following metrics will be available for the built instance of {@link
     * BatchGetDocuments}
     *
     * <ol>
     *   <li>{@code org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.BatchGetDocuments.throttlingMs}
     *       A counter tracking the number of milliseconds RPCs are throttled by Qos
     *   <li>{@code org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.BatchGetDocuments.rpcFailures}
     *       A counter tracking the number of failed RPCs
     *   <li>{@code org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.BatchGetDocuments.rpcSuccesses}
     *       A counter tracking the number of successful RPCs
     *   <li>{@code
     *       org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.RunQuery.rpcStreamValueReceived} A
     *       counter tracking the number of values received by a streaming RPC
     * </ol>
     *
     * @return A new type safe builder providing configuration for processing of {@link
     *     BatchGetDocumentsRequest}s
     * @see FirestoreIO#v1()
     * @see BatchGetDocuments
     * @see BatchGetDocumentsRequest
     * @see BatchGetDocumentsResponse
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.BatchGetDocuments">google.firestore.v1.Firestore.BatchGetDocuments</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchGetDocumentsRequest">google.firestore.v1.BatchGetDocumentsRequest</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchGetDocumentsResponse">google.firestore.v1.BatchGetDocumentsResponse</a>
     */
    public BatchGetDocuments.Builder batchGetDocuments() {
      return new BatchGetDocuments.Builder();
    }

    /**
     * Factory method to create a new type safe builder for {@link RunQueryRequest} operations.
     *
     * <p>This method is part of the Firestore Connector DSL and should be accessed via {@link
     * FirestoreIO#v1()}.
     *
     * <p>All request quality-of-service for the built {@link RunQuery} PTransform is scoped to the
     * worker and configured based on the {@link RpcQosOptions} specified via this builder.
     *
     * <p>All logging for the built instance of {@link RunQuery} will be sent to appender {@code
     * org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.RunQuery}.
     *
     * <p>The following metrics will be available for the built instance of {@link RunQuery}
     *
     * <ol>
     *   <li>{@code org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.RunQuery.throttlingMs} A
     *       counter tracking the number of milliseconds RPCs are throttled by Qos
     *   <li>{@code org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.RunQuery.rpcFailures} A counter
     *       tracking the number of failed RPCs
     *   <li>{@code org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.RunQuery.rpcSuccesses} A
     *       counter tracking the number of successful RPCs
     *   <li>{@code
     *       org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.RunQuery.rpcStreamValueReceived} A
     *       counter tracking the number of values received by a streaming RPC
     * </ol>
     *
     * @return A new type safe builder providing configuration for processing of {@link
     *     RunQueryRequest}s
     * @see FirestoreIO#v1()
     * @see RunQuery
     * @see RunQueryRequest
     * @see RunQueryResponse
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.RunQuery">google.firestore.v1.Firestore.RunQuery</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.RunQueryRequest">google.firestore.v1.RunQueryRequest</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.RunQueryResponse">google.firestore.v1.RunQueryResponse</a>
     */
    public RunQuery.Builder runQuery() {
      return new RunQuery.Builder();
    }

    /**
     * Factory method to create a new type safe builder for {@link PartitionQueryRequest}
     * operations.
     *
     * <p>This method is part of the Firestore Connector DSL and should be accessed via {@link
     * FirestoreIO#v1()}.
     *
     * <p>All request quality-of-service for the built {@link PartitionQuery} PTransform is scoped
     * to the worker and configured based on the {@link RpcQosOptions} specified via this builder.
     *
     * @return A new type safe builder providing configuration for processing of {@link
     *     PartitionQueryRequest}s
     * @see FirestoreIO#v1()
     * @see PartitionQuery
     * @see PartitionQueryRequest
     * @see RunQueryResponse
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.PartitionQuery">google.firestore.v1.Firestore.PartitionQuery</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.PartitionQueryRequest">google.firestore.v1.PartitionQueryRequest</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.PartitionQueryResponse">google.firestore.v1.PartitionQueryResponse</a>
     */
    public PartitionQuery.Builder partitionQuery() {
      return new PartitionQuery.Builder();
    }
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
   * ListCollectionIdsRequest}{@code >, }{@link PTransform}{@code <}{@link
   * ListCollectionIdsResponse}{@code >>} which will read from Firestore.
   *
   * <p>This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
   * {@link FirestoreIO#v1()}{@code .}{@link FirestoreV1#read() read()}{@code .}{@link
   * FirestoreV1.Read#listCollectionIds() listCollectionIds()}.
   *
   * <p>All request quality-of-service for an instance of this PTransform is scoped to the worker
   * and configured via {@link ListCollectionIds.Builder#withRpcQosOptions(RpcQosOptions)}.
   *
   * @see FirestoreIO#v1()
   * @see FirestoreV1#read()
   * @see FirestoreV1.Read#listCollectionIds()
   * @see FirestoreV1.ListCollectionIds.Builder
   * @see ListCollectionIdsRequest
   * @see ListCollectionIdsResponse
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.ListCollectionIds">google.firestore.v1.Firestore.ListCollectionIds</a>
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.ListCollectionIdsRequest">google.firestore.v1.ListCollectionIdsRequest</a>
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.ListCollectionIdsResponse">google.firestore.v1.ListCollectionIdsResponse</a>
   */
  public static final class ListCollectionIds
      extends ReadTransform<
          PCollection<ListCollectionIdsRequest>,
          PCollection<String>,
          ListCollectionIds,
          ListCollectionIds.Builder> {

    private ListCollectionIds(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        @Nullable Instant readTime) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
    }

    @Override
    public PCollection<String> expand(PCollection<ListCollectionIdsRequest> input) {
      return input
          .apply(
              "listCollectionIds",
              ParDo.of(
                  new ListCollectionIdsFn(
                      clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime)))
          .apply(ParDo.of(new FlattenListCollectionIdsResponse()))
          .apply(Reshuffle.viaRandomKey());
    }

    @Override
    public Builder toBuilder() {
      return new Builder(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
    }

    /**
     * A type safe builder for {@link ListCollectionIds} allowing configuration and instantiation.
     *
     * <p>This class is part of the Firestore Connector DSL, it has a type safe builder accessible
     * via {@link FirestoreIO#v1()}{@code .}{@link FirestoreV1#read() read()}{@code .}{@link
     * FirestoreV1.Read#listCollectionIds() listCollectionIds()}.
     *
     * <p>
     *
     * @see FirestoreIO#v1()
     * @see FirestoreV1#read()
     * @see FirestoreV1.Read#listCollectionIds()
     * @see FirestoreV1.ListCollectionIds
     * @see ListCollectionIdsRequest
     * @see ListCollectionIdsResponse
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.ListCollectionIds">google.firestore.v1.Firestore.ListCollectionIds</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.ListCollectionIdsRequest">google.firestore.v1.ListCollectionIdsRequest</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.ListCollectionIdsResponse">google.firestore.v1.ListCollectionIdsResponse</a>
     */
    public static final class Builder
        extends ReadTransform.Builder<
            PCollection<ListCollectionIdsRequest>,
            PCollection<String>,
            ListCollectionIds,
            ListCollectionIds.Builder> {

      private Builder() {
        super();
      }

      private Builder(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions,
          @Nullable Instant readTime) {
        super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
      }

      @Override
      public ListCollectionIds build() {
        return genericBuild();
      }

      @Override
      ListCollectionIds buildSafe(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions,
          @Nullable Instant readTime) {
        return new ListCollectionIds(
            clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
      }
    }
  }

  /**
   * Concrete class representing a {@link PTransform}{@code <}{@link PCollection}{@code <}{@link
   * ListDocumentsRequest}{@code >, }{@link PTransform}{@code <}{@link ListDocumentsResponse}{@code
   * >>} which will read from Firestore.
   *
   * <p>This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
   * {@link FirestoreIO#v1()}{@code .}{@link FirestoreV1#read() read()}{@code .}{@link
   * FirestoreV1.Read#listDocuments() listDocuments()}.
   *
   * <p>All request quality-of-service for an instance of this PTransform is scoped to the worker
   * and configured via {@link ListDocuments.Builder#withRpcQosOptions(RpcQosOptions)}.
   *
   * @see FirestoreIO#v1()
   * @see FirestoreV1#read()
   * @see FirestoreV1.Read#listDocuments()
   * @see FirestoreV1.ListDocuments.Builder
   * @see ListDocumentsRequest
   * @see ListDocumentsResponse
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.ListDocuments">google.firestore.v1.Firestore.ListDocuments</a>
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.ListDocumentsRequest">google.firestore.v1.ListDocumentsRequest</a>
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.ListDocumentsResponse">google.firestore.v1.ListDocumentsResponse</a>
   */
  public static final class ListDocuments
      extends ReadTransform<
          PCollection<ListDocumentsRequest>,
          PCollection<Document>,
          ListDocuments,
          ListDocuments.Builder> {

    private ListDocuments(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        @Nullable Instant readTime) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
    }

    @Override
    public PCollection<Document> expand(PCollection<ListDocumentsRequest> input) {
      return input
          .apply(
              "listDocuments",
              ParDo.of(
                  new ListDocumentsFn(
                      clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime)))
          .apply(ParDo.of(new ListDocumentsResponseToDocument()))
          .apply(Reshuffle.viaRandomKey());
    }

    @Override
    public Builder toBuilder() {
      return new Builder(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
    }

    /**
     * A type safe builder for {@link ListDocuments} allowing configuration and instantiation.
     *
     * <p>This class is part of the Firestore Connector DSL, it has a type safe builder accessible
     * via {@link FirestoreIO#v1()}{@code .}{@link FirestoreV1#read() read()}{@code .}{@link
     * FirestoreV1.Read#listDocuments() listDocuments()}.
     *
     * <p>
     *
     * @see FirestoreIO#v1()
     * @see FirestoreV1#read()
     * @see FirestoreV1.Read#listDocuments()
     * @see FirestoreV1.ListDocuments
     * @see ListDocumentsRequest
     * @see ListDocumentsResponse
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.ListDocuments">google.firestore.v1.Firestore.ListDocuments</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.ListDocumentsRequest">google.firestore.v1.ListDocumentsRequest</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.ListDocumentsResponse">google.firestore.v1.ListDocumentsResponse</a>
     */
    public static final class Builder
        extends ReadTransform.Builder<
            PCollection<ListDocumentsRequest>,
            PCollection<Document>,
            ListDocuments,
            ListDocuments.Builder> {

      private Builder() {
        super();
      }

      private Builder(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions,
          @Nullable Instant readTime) {
        super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
      }

      @Override
      public ListDocuments build() {
        return genericBuild();
      }

      @Override
      ListDocuments buildSafe(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions,
          @Nullable Instant readTime) {
        return new ListDocuments(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
      }
    }
  }

  /**
   * Concrete class representing a {@link PTransform}{@code <}{@link PCollection}{@code <}{@link
   * RunQueryRequest}{@code >, }{@link PTransform}{@code <}{@link RunQueryResponse}{@code >>} which
   * will read from Firestore.
   *
   * <p>This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
   * {@link FirestoreIO#v1()}{@code .}{@link FirestoreV1#read() read()}{@code .}{@link
   * FirestoreV1.Read#runQuery() runQuery()}.
   *
   * <p>All request quality-of-service for an instance of this PTransform is scoped to the worker
   * and configured via {@link RunQuery.Builder#withRpcQosOptions(RpcQosOptions)}.
   *
   * @see FirestoreIO#v1()
   * @see FirestoreV1#read()
   * @see FirestoreV1.Read#runQuery()
   * @see FirestoreV1.RunQuery.Builder
   * @see RunQueryRequest
   * @see RunQueryResponse
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.RunQuery">google.firestore.v1.Firestore.RunQuery</a>
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.RunQueryRequest">google.firestore.v1.RunQueryRequest</a>
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.RunQueryResponse">google.firestore.v1.RunQueryResponse</a>
   */
  // TODO(https://github.com/apache/beam/issues/21056): Add dynamic work rebalancing to support a
  // Splittable DoFn
  // TODO(https://github.com/apache/beam/issues/21050): Add support for progress reporting
  public static final class RunQuery
      extends ReadTransform<
          PCollection<RunQueryRequest>, PCollection<RunQueryResponse>, RunQuery, RunQuery.Builder> {

    private RunQuery(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        @Nullable Instant readTime) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
    }

    @Override
    public PCollection<RunQueryResponse> expand(PCollection<RunQueryRequest> input) {
      return input
          .apply(
              "runQuery",
              ParDo.of(
                  new RunQueryFn(
                      clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime)))
          .apply(Reshuffle.viaRandomKey());
    }

    @Override
    public Builder toBuilder() {
      return new Builder(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
    }

    /**
     * A type safe builder for {@link RunQuery} allowing configuration and instantiation.
     *
     * <p>This class is part of the Firestore Connector DSL, it has a type safe builder accessible
     * via {@link FirestoreIO#v1()}{@code .}{@link FirestoreV1#read() read()}{@code .}{@link
     * FirestoreV1.Read#runQuery() runQuery()}.
     *
     * <p>
     *
     * @see FirestoreIO#v1()
     * @see FirestoreV1#read()
     * @see FirestoreV1.Read#runQuery()
     * @see FirestoreV1.RunQuery
     * @see RunQueryRequest
     * @see RunQueryResponse
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.RunQuery">google.firestore.v1.Firestore.RunQuery</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.RunQueryRequest">google.firestore.v1.RunQueryRequest</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.RunQueryResponse">google.firestore.v1.RunQueryResponse</a>
     */
    public static final class Builder
        extends ReadTransform.Builder<
            PCollection<RunQueryRequest>,
            PCollection<RunQueryResponse>,
            RunQuery,
            RunQuery.Builder> {

      private Builder() {
        super();
      }

      private Builder(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions,
          @Nullable Instant readTime) {
        super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
      }

      @Override
      public RunQuery build() {
        return genericBuild();
      }

      @Override
      RunQuery buildSafe(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions,
          @Nullable Instant readTime) {
        return new RunQuery(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
      }
    }
  }

  /**
   * Concrete class representing a {@link PTransform}{@code <}{@link PCollection}{@code <}{@link
   * BatchGetDocumentsRequest}{@code >, }{@link PTransform}{@code <}{@link
   * BatchGetDocumentsResponse}{@code >>} which will read from Firestore.
   *
   * <p>This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
   * {@link FirestoreIO#v1()}{@code .}{@link FirestoreV1#read() read()}{@code .}{@link
   * FirestoreV1.Read#batchGetDocuments() batchGetDocuments()}.
   *
   * <p>All request quality-of-service for an instance of this PTransform is scoped to the worker
   * and configured via {@link BatchGetDocuments.Builder#withRpcQosOptions(RpcQosOptions)}.
   *
   * @see FirestoreIO#v1()
   * @see FirestoreV1#read()
   * @see FirestoreV1.Read#batchGetDocuments()
   * @see FirestoreV1.BatchGetDocuments.Builder
   * @see BatchGetDocumentsRequest
   * @see BatchGetDocumentsResponse
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.BatchGetDocuments">google.firestore.v1.Firestore.BatchGetDocuments</a>
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchGetDocumentsRequest">google.firestore.v1.BatchGetDocumentsRequest</a>
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchGetDocumentsResponse">google.firestore.v1.BatchGetDocumentsResponse</a>
   */
  public static final class BatchGetDocuments
      extends ReadTransform<
          PCollection<BatchGetDocumentsRequest>,
          PCollection<BatchGetDocumentsResponse>,
          BatchGetDocuments,
          BatchGetDocuments.Builder> {

    private BatchGetDocuments(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        @Nullable Instant readTime) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
    }

    @Override
    public PCollection<BatchGetDocumentsResponse> expand(
        PCollection<BatchGetDocumentsRequest> input) {
      return input
          .apply(
              "batchGetDocuments",
              ParDo.of(
                  new BatchGetDocumentsFn(
                      clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime)))
          .apply(Reshuffle.viaRandomKey());
    }

    @Override
    public Builder toBuilder() {
      return new Builder(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
    }

    /**
     * A type safe builder for {@link BatchGetDocuments} allowing configuration and instantiation.
     *
     * <p>This class is part of the Firestore Connector DSL, it has a type safe builder accessible
     * via {@link FirestoreIO#v1()}{@code .}{@link FirestoreV1#read() read()}{@code .}{@link
     * FirestoreV1.Read#batchGetDocuments() batchGetDocuments()}.
     *
     * <p>
     *
     * @see FirestoreIO#v1()
     * @see FirestoreV1#read()
     * @see FirestoreV1.Read#batchGetDocuments()
     * @see FirestoreV1.BatchGetDocuments
     * @see BatchGetDocumentsRequest
     * @see BatchGetDocumentsResponse
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.BatchGetDocuments">google.firestore.v1.Firestore.BatchGetDocuments</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchGetDocumentsRequest">google.firestore.v1.BatchGetDocumentsRequest</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.BatchGetDocumentsResponse">google.firestore.v1.BatchGetDocumentsResponse</a>
     */
    public static final class Builder
        extends ReadTransform.Builder<
            PCollection<BatchGetDocumentsRequest>,
            PCollection<BatchGetDocumentsResponse>,
            BatchGetDocuments,
            BatchGetDocuments.Builder> {

      private Builder() {
        super();
      }

      public Builder(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions,
          @Nullable Instant readTime) {
        super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
      }

      @Override
      public BatchGetDocuments build() {
        return genericBuild();
      }

      @Override
      BatchGetDocuments buildSafe(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions,
          @Nullable Instant readTime) {
        return new BatchGetDocuments(
            clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
      }
    }
  }

  /**
   * Concrete class representing a {@link PTransform}{@code <}{@link PCollection}{@code <}{@link
   * PartitionQueryRequest}{@code >, }{@link PTransform}{@code <}{@link RunQueryRequest}{@code >>}
   * which will read from Firestore.
   *
   * <p>Perform the necessary operations and handling of {@link PartitionQueryResponse}s to yield a
   * number of {@link RunQueryRequest} which are friendly to being executed in parallel.
   *
   * <p>This class is part of the Firestore Connector DSL, it has a type safe builder accessible via
   * {@link FirestoreIO#v1()}{@code .}{@link FirestoreV1#read() read()}{@code .}{@link
   * FirestoreV1.Read#partitionQuery() partitionQuery()}.
   *
   * <p>All request quality-of-service for an instance of this PTransform is scoped to the worker
   * and configured via {@link PartitionQuery.Builder#withRpcQosOptions(RpcQosOptions)}.
   *
   * @see FirestoreIO#v1()
   * @see FirestoreV1#read()
   * @see FirestoreV1.Read#partitionQuery()
   * @see FirestoreV1.PartitionQuery.Builder
   * @see FirestoreV1.Read#runQuery()
   * @see FirestoreV1.RunQuery
   * @see FirestoreV1.RunQuery.Builder
   * @see PartitionQueryRequest
   * @see RunQueryResponse
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.PartitionQuery">google.firestore.v1.Firestore.PartitionQuery</a>
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.PartitionQueryRequest">google.firestore.v1.PartitionQueryRequest</a>
   * @see <a target="_blank" rel="noopener noreferrer"
   *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.PartitionQueryResponse">google.firestore.v1.PartitionQueryResponse</a>
   */
  public static final class PartitionQuery
      extends ReadTransform<
          PCollection<PartitionQueryRequest>,
          PCollection<RunQueryRequest>,
          PartitionQuery,
          PartitionQuery.Builder> {

    private final boolean nameOnlyQuery;

    private PartitionQuery(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        boolean nameOnlyQuery,
        @Nullable Instant readTime) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
      this.nameOnlyQuery = nameOnlyQuery;
    }

    @Override
    public PCollection<RunQueryRequest> expand(PCollection<PartitionQueryRequest> input) {
      PCollection<RunQueryRequest> queries =
          input
              .apply(
                  "PartitionQuery",
                  ParDo.of(
                      new PartitionQueryFn(
                          clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime)))
              .apply(
                  "expand queries",
                  ParDo.of(new PartitionQueryResponseToRunQueryRequest(readTime)));
      if (nameOnlyQuery) {
        queries =
            queries.apply(
                "set name only query",
                MapElements.via(
                    new SimpleFunction<RunQueryRequest, RunQueryRequest>() {
                      @Override
                      public RunQueryRequest apply(RunQueryRequest input) {
                        RunQueryRequest.Builder builder = input.toBuilder();
                        builder
                            .getStructuredQueryBuilder()
                            .setSelect(
                                Projection.newBuilder()
                                    .addFields(
                                        FieldReference.newBuilder()
                                            .setFieldPath("__name__")
                                            .build())
                                    .build());
                        return builder.build();
                      }
                    }));
      }
      return queries.apply(Reshuffle.viaRandomKey());
    }

    @Override
    public Builder toBuilder() {
      return new Builder(
          clock, firestoreStatefulComponentFactory, rpcQosOptions, nameOnlyQuery, readTime);
    }

    /**
     * A type safe builder for {@link PartitionQuery} allowing configuration and instantiation.
     *
     * <p>This class is part of the Firestore Connector DSL, it has a type safe builder accessible
     * via {@link FirestoreIO#v1()}{@code .}{@link FirestoreV1#read() read()}{@code .}{@link
     * FirestoreV1.Read#partitionQuery() partitionQuery()}.
     *
     * <p>
     *
     * @see FirestoreIO#v1()
     * @see FirestoreV1#read()
     * @see FirestoreV1.Read#partitionQuery()
     * @see FirestoreV1.PartitionQuery
     * @see PartitionQueryRequest
     * @see RunQueryResponse
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.Firestore.PartitionQuery">google.firestore.v1.Firestore.PartitionQuery</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.PartitionQueryRequest">google.firestore.v1.PartitionQueryRequest</a>
     * @see <a target="_blank" rel="noopener noreferrer"
     *     href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.PartitionQueryResponse">google.firestore.v1.PartitionQueryResponse</a>
     */
    public static final class Builder
        extends ReadTransform.Builder<
            PCollection<PartitionQueryRequest>,
            PCollection<RunQueryRequest>,
            PartitionQuery,
            FirestoreV1.PartitionQuery.Builder> {

      private boolean nameOnlyQuery = false;

      private Builder() {
        super();
      }

      public Builder(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions,
          boolean nameOnlyQuery,
          @Nullable Instant readTime) {
        super(clock, firestoreStatefulComponentFactory, rpcQosOptions, readTime);
        this.nameOnlyQuery = nameOnlyQuery;
      }

      @Override
      public PartitionQuery build() {
        return genericBuild();
      }

      /**
       * Update produced queries to only retrieve their {@code __name__} thereby not retrieving any
       * fields and reducing resource requirements.
       *
       * @return this builder
       */
      public Builder withNameOnlyQuery() {
        this.nameOnlyQuery = true;
        return this;
      }

      @Override
      PartitionQuery buildSafe(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions,
          @Nullable Instant readTime) {
        return new PartitionQuery(
            clock, firestoreStatefulComponentFactory, rpcQosOptions, nameOnlyQuery, readTime);
      }
    }

    /**
     * DoFn which contains the logic necessary to turn a {@link PartitionQueryRequest} and {@link
     * PartitionQueryResponse} pair into {@code N} {@link RunQueryRequest}.
     */
    static final class PartitionQueryResponseToRunQueryRequest
        extends DoFn<PartitionQueryPair, RunQueryRequest> {
      private final @Nullable Instant readTime;

      PartitionQueryResponseToRunQueryRequest() {
        this(null);
      }

      PartitionQueryResponseToRunQueryRequest(@Nullable Instant readTime) {
        this.readTime = readTime;
      }

      /**
       * When fetching cursors that span multiple pages it is expected (per <a
       * href="https://cloud.google.com/firestore/docs/reference/rpc/google.firestore.v1#google.firestore.v1.PartitionQueryRequest">
       * PartitionQueryRequest.page_token</a>) for the client to sort the cursors before processing
       * them to define the sub-queries. So here we're defining a Comparator which will sort Cursors
       * by the first reference value present, then comparing the reference values
       * lexicographically.
       */
      static final Comparator<Cursor> CURSOR_REFERENCE_VALUE_COMPARATOR;

      static {
        Function<Cursor, Optional<Value>> firstReferenceValue =
            (Cursor c) ->
                c.getValuesList().stream()
                    .filter(
                        v -> {
                          String referenceValue = v.getReferenceValue();
                          return !referenceValue.isEmpty();
                        })
                    .findFirst();
        Function<String, String[]> stringToPath = (String s) -> s.split("/");
        // compare references by their path segments rather than as a whole string to ensure
        // per path segment comparison is taken into account.
        Comparator<String[]> pathWiseCompare =
            (String[] path1, String[] path2) -> {
              int minLength = Math.min(path1.length, path2.length);
              for (int i = 0; i < minLength; i++) {
                String pathSegment1 = path1[i];
                String pathSegment2 = path2[i];
                int compare = pathSegment1.compareTo(pathSegment2);
                if (compare != 0) {
                  return compare;
                }
              }
              if (path1.length == path2.length) {
                return 0;
              } else if (minLength == path1.length) {
                return -1;
              } else {
                return 1;
              }
            };

        // Sort those cursors which have no firstReferenceValue at the bottom of the list
        CURSOR_REFERENCE_VALUE_COMPARATOR =
            Comparator.comparing(
                firstReferenceValue,
                (o1, o2) -> {
                  if (o1.isPresent() && o2.isPresent()) {
                    return pathWiseCompare.compare(
                        stringToPath.apply(o1.get().getReferenceValue()),
                        stringToPath.apply(o2.get().getReferenceValue()));
                  } else if (o1.isPresent()) {
                    return -1;
                  } else {
                    return 1;
                  }
                });
      }

      @ProcessElement
      public void processElement(ProcessContext c) {
        PartitionQueryPair pair = c.element();
        PartitionQueryRequest partitionQueryRequest = pair.getRequest();
        String dbRoot = partitionQueryRequest.getParent();
        StructuredQuery structuredQuery = partitionQueryRequest.getStructuredQuery();
        PartitionQueryResponse partitionQueryResponse = pair.getResponse();
        // create a new list before we sort things
        List<Cursor> cursors = new ArrayList<>(partitionQueryResponse.getPartitionsList());
        cursors.sort(CURSOR_REFERENCE_VALUE_COMPARATOR);
        final int size = cursors.size();
        if (size == 0) {
          emit(c, dbRoot, structuredQuery.toBuilder());
          return;
        }
        final int lastIdx = size - 1;
        for (int i = 0; i < size; i++) {
          Cursor curr = cursors.get(i);

          if (i == 0) {
            // first cursor, emit a range of everything up to the current cursor
            emit(c, dbRoot, structuredQuery.toBuilder().setEndAt(curr));
          }

          if (0 < i && i <= lastIdx) {
            Cursor prev = cursors.get(i - 1);
            // emit a range for values between prev and curr
            emit(c, dbRoot, structuredQuery.toBuilder().setStartAt(prev).setEndAt(curr));
          }

          if (i == lastIdx) {
            // last cursor, emit a range of everything from the current cursor onward
            emit(c, dbRoot, structuredQuery.toBuilder().setStartAt(curr));
          }
        }
      }

      private void emit(ProcessContext c, String dbRoot, StructuredQuery.Builder builder) {
        RunQueryRequest.Builder runQueryRequest =
            RunQueryRequest.newBuilder().setParent(dbRoot).setStructuredQuery(builder.build());

        if (readTime != null) {
          runQueryRequest.setReadTime(Timestamps.fromMillis(readTime.getMillis()));
        }
        c.output(runQueryRequest.build());
      }
    }
  }

  /** DoFn to output CollectionIds from a {@link ListCollectionIdsResponse}. */
  private static final class FlattenListCollectionIdsResponse
      extends DoFn<ListCollectionIdsResponse, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.element().getCollectionIdsList().forEach(c::output);
    }
  }

  /** DoFn to output {@link Document}s from a {@link ListDocumentsResponse}. */
  private static final class ListDocumentsResponseToDocument
      extends DoFn<ListDocumentsResponse, Document> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.element().getDocumentsList().forEach(c::output);
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
      BldrT self() {
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
      TrfmT genericBuild() {
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

  private abstract static class ReadTransform<
          InT extends PInput,
          OutT extends POutput,
          TrfmT extends ReadTransform<InT, OutT, TrfmT, BldrT>,
          BldrT extends ReadTransform.Builder<InT, OutT, TrfmT, BldrT>>
      extends Transform<InT, OutT, TrfmT, BldrT> {

    final @Nullable Instant readTime;

    ReadTransform(
        JodaClock clock,
        FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
        RpcQosOptions rpcQosOptions,
        @Nullable Instant readTime) {
      super(clock, firestoreStatefulComponentFactory, rpcQosOptions);
      this.readTime = readTime;
    }

    @Override
    public abstract BldrT toBuilder();

    abstract static class Builder<
            InT extends PInput,
            OutT extends POutput,
            TrfmT extends ReadTransform<InT, OutT, TrfmT, BldrT>,
            BldrT extends ReadTransform.Builder<InT, OutT, TrfmT, BldrT>>
        extends Transform.Builder<InT, OutT, TrfmT, BldrT> {
      @Nullable Instant readTime;

      Builder() {
        super();
        this.readTime = null;
      }

      private Builder(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions,
          @Nullable Instant readTime) {
        super(clock, firestoreStatefulComponentFactory, rpcQosOptions);
        this.readTime = readTime;
      }

      @Override
      final TrfmT genericBuild() {
        return buildSafe(
            requireNonNull(clock, "clock must be non null"),
            requireNonNull(firestoreStatefulComponentFactory, "firestoreFactory must be non null"),
            requireNonNull(rpcQosOptions, "rpcQosOptions must be non null"),
            readTime);
      }

      @Override
      TrfmT buildSafe(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions) {
        throw new UnsupportedOperationException();
      }

      abstract TrfmT buildSafe(
          JodaClock clock,
          FirestoreStatefulComponentFactory firestoreStatefulComponentFactory,
          RpcQosOptions rpcQosOptions,
          @Nullable Instant readTime);

      public final BldrT withReadTime(@Nullable Instant readTime) {
        this.readTime = readTime;
        return self();
      }
    }
  }
}
