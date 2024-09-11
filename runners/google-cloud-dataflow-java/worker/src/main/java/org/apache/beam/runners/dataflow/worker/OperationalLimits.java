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
package org.apache.beam.runners.dataflow.worker;

import com.google.auto.value.AutoBuilder;

/** Keep track of any operational limits required by the backend. */
public class OperationalLimits {
  // Maximum size of a commit from a single work item.
  public final long maxWorkItemCommitBytes;
  // Maximum size of a single output element's serialized key.
  public final long maxOutputKeyBytes;
  // Maximum size of a single output element's serialized value.
  public final long maxOutputValueBytes;

  OperationalLimits(long maxWorkItemCommitBytes, long maxOutputKeyBytes, long maxOutputValueBytes) {
    this.maxWorkItemCommitBytes = maxWorkItemCommitBytes;
    this.maxOutputKeyBytes = maxOutputKeyBytes;
    this.maxOutputValueBytes = maxOutputValueBytes;
  }

  @AutoBuilder(ofClass = OperationalLimits.class)
  public interface Builder {
    Builder setMaxWorkItemCommitBytes(long bytes);

    Builder setMaxOutputKeyBytes(long bytes);

    Builder setMaxOutputValueBytes(long bytes);

    OperationalLimits build();
  }

  public static Builder builder() {
    return new AutoBuilder_OperationalLimits_Builder()
        .setMaxWorkItemCommitBytes(Long.MAX_VALUE)
        .setMaxOutputKeyBytes(Long.MAX_VALUE)
        .setMaxOutputValueBytes(Long.MAX_VALUE);
  }
}
