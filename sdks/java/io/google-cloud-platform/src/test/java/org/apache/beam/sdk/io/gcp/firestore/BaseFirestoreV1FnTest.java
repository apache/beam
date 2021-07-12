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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.cloud.firestore.v1.stub.FirestoreStub;
import io.grpc.Status.Code;
import java.net.SocketTimeoutException;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1RpcAttemptContexts.HasRpcAttemptContext;
import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1RpcAttemptContexts.V1FnRpcAttemptContext;
import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcAttempt.Context;
import org.joda.time.Instant;
import org.junit.Test;
import org.mockito.Mock;

@SuppressWarnings(
    "initialization.fields.uninitialized") // mockito fields are initialized via the Mockito Runner
abstract class BaseFirestoreV1FnTest<
        InT, OutT, FnT extends FirestoreDoFn<InT, OutT> & HasRpcAttemptContext>
    extends BaseFirestoreFnTest<InT, OutT, FnT> {
  protected static final ApiException RETRYABLE_ERROR =
      ApiExceptionFactory.createException(
          new SocketTimeoutException("retryableError"), GrpcStatusCode.of(Code.CANCELLED), true);

  protected final RpcQosOptions rpcQosOptions = RpcQosOptions.newBuilder().build();

  protected final JodaClock clock = new MonotonicJodaClock();
  @Mock protected FirestoreStatefulComponentFactory ff;
  @Mock protected FirestoreStub stub;
  @Mock protected RpcQos rpcQos;

  @Test
  public abstract void attemptsExhaustedForRetryableError() throws Exception;

  @Test
  public abstract void noRequestIsSentIfNotSafeToProceed() throws Exception;

  @Test
  public final void contextNamespaceMatchesPublicAPIDefinedValue() {
    FnT fn = getFn();
    Context rpcAttemptContext = fn.getRpcAttemptContext();
    if (rpcAttemptContext instanceof V1FnRpcAttemptContext) {
      V1FnRpcAttemptContext v1FnRpcAttemptContext = (V1FnRpcAttemptContext) rpcAttemptContext;
      switch (v1FnRpcAttemptContext) {
        case BatchGetDocuments:
          assertEquals(
              "org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.BatchGetDocuments",
              v1FnRpcAttemptContext.getNamespace());
          break;
        case BatchWrite:
          assertEquals(
              "org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.BatchWrite",
              v1FnRpcAttemptContext.getNamespace());
          break;
        case ListCollectionIds:
          assertEquals(
              "org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.ListCollectionIds",
              v1FnRpcAttemptContext.getNamespace());
          break;
        case ListDocuments:
          assertEquals(
              "org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.ListDocuments",
              v1FnRpcAttemptContext.getNamespace());
          break;
        case PartitionQuery:
          assertEquals(
              "org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.PartitionQuery",
              v1FnRpcAttemptContext.getNamespace());
          break;
        case RunQuery:
          assertEquals(
              "org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.RunQuery",
              v1FnRpcAttemptContext.getNamespace());
          break;
        default:
          fail("Unverified V1FnRpcAttemptContext value");
      }
    }
  }

  private static class MonotonicJodaClock implements JodaClock {
    private long counter = 0;

    @Override
    public Instant instant() {
      return Instant.ofEpochMilli(counter++);
    }
  }
}
