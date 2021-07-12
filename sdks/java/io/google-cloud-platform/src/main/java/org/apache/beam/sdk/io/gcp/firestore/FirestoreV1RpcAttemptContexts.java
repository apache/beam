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

import org.apache.beam.sdk.io.gcp.firestore.RpcQos.RpcAttempt.Context;

final class FirestoreV1RpcAttemptContexts {

  /**
   * The base namespace used for {@link Context RpcAttempt.Context} values in {@link
   * V1FnRpcAttemptContext}. This value directly impacts the names of log appender and metrics.
   *
   * <p>This names is part of the public API and must not be changed outside of a deprecation cycle.
   */
  private static final String CONTEXT_BASE_NAMESPACE =
      "org.apache.beam.sdk.io.gcp.firestore.FirestoreV1";

  interface HasRpcAttemptContext {
    Context getRpcAttemptContext();
  }

  /**
   * A set of defined {@link Context RpcAttempt.Context} values used to determine metrics and
   * logging namespaces. Implemented as an enum to ensure a single instance.
   *
   * <p>These names are part of the public API and must not be changed outside of a deprecation
   * cycle.
   */
  enum V1FnRpcAttemptContext implements Context {
    BatchGetDocuments(),
    BatchWrite(),
    ListCollectionIds(),
    ListDocuments(),
    PartitionQuery(),
    RunQuery();

    private final String namespace;

    V1FnRpcAttemptContext() {
      this.namespace = CONTEXT_BASE_NAMESPACE + "." + this.name();
    }

    @Override
    public final String getNamespace() {
      return namespace;
    }
  }
}
