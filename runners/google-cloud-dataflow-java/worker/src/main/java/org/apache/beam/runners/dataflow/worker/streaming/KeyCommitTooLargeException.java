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
package org.apache.beam.runners.dataflow.worker.streaming;

import org.apache.beam.runners.dataflow.worker.windmill.Windmill;

public final class KeyCommitTooLargeException extends Exception {

  public static KeyCommitTooLargeException causedBy(
      String computationId, long byteLimit, Windmill.WorkItemCommitRequest request) {
    StringBuilder message = new StringBuilder();
    message.append("Commit request for stage ");
    message.append(computationId);
    message.append(" and key ");
    message.append(request.getKey().toStringUtf8());
    if (request.getSerializedSize() > 0) {
      message.append(
          " has size "
              + request.getSerializedSize()
              + " which is more than the limit of "
              + byteLimit);
    } else {
      message.append(" is larger than 2GB and cannot be processed");
    }
    message.append(
        ". This may be caused by grouping a very "
            + "large amount of data in a single window without using Combine,"
            + " or by producing a large amount of data from a single input element."
            + " See https://cloud.google.com/dataflow/docs/guides/common-errors#key-commit-too-large-exception.");
    return new KeyCommitTooLargeException(message.toString());
  }

  private KeyCommitTooLargeException(String message) {
    super(message);
  }
}
