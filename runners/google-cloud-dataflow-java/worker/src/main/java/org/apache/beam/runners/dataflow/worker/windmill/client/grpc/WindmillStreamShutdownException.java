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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.sdk.annotations.Internal;

/**
 * Indicates that a {@link WindmillStream#shutdown()} was called while waiting for some internal
 * operation to complete. Most common use of this exception should be conversion to a {@link
 * org.apache.beam.runners.dataflow.worker.WorkItemCancelledException} as the {@link
 * org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem} being processed by {@link
 * WindmillStream}.
 */
@Internal
final class WindmillStreamShutdownException extends RuntimeException {
  WindmillStreamShutdownException(String message) {
    super(message);
  }
}
