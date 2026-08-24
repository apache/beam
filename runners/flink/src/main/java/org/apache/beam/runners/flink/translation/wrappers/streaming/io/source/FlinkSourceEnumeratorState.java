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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Checkpoint state shared by the lazy and static source split assignment strategies. */
public final class FlinkSourceEnumeratorState<T> implements Serializable {
  private static final long serialVersionUID = 1L;

  private final FlinkSourceSplitAssignmentMode assignmentMode;
  private final ArrayList<FlinkSourceSplit<T>> pendingSplits;

  /** Takes ownership of {@code pendingSplits}; the caller must not mutate it afterwards. */
  FlinkSourceEnumeratorState(
      FlinkSourceSplitAssignmentMode assignmentMode, ArrayList<FlinkSourceSplit<T>> pendingSplits) {
    this.assignmentMode = assignmentMode;
    this.pendingSplits = pendingSplits;
  }

  FlinkSourceSplitAssignmentMode getAssignmentMode() {
    return assignmentMode;
  }

  List<FlinkSourceSplit<T>> getPendingSplits() {
    return Collections.unmodifiableList(pendingSplits);
  }
}
