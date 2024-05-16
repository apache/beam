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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing;

import com.google.auto.value.AutoValue;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;

/** Value class that represents the result of executing user DoFns. */
@AutoValue
abstract class ExecuteWorkResult {
  static ExecuteWorkResult create(
      Windmill.WorkItemCommitRequest.Builder commitWorkRequest, long stateBytesRead) {
    return new AutoValue_ExecuteWorkResult(commitWorkRequest, stateBytesRead);
  }

  abstract Windmill.WorkItemCommitRequest.Builder commitWorkRequest();

  abstract long stateBytesRead();
}
