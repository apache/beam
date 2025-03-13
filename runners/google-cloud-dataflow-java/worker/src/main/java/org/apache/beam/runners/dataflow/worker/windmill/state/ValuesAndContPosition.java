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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import java.util.List;
import javax.annotation.Nullable;

/**
 * An in-memory collection of deserialized values and an optional continuation position to pass to
 * Windmill when fetching the next page of values.
 */
public class ValuesAndContPosition<T, ContinuationT> {
  private final List<T> values;

  /** Position to pass to next request for next page of values. Null if done. */
  private final @Nullable ContinuationT continuationPosition;

  public ValuesAndContPosition(List<T> values, @Nullable ContinuationT continuationPosition) {
    this.values = values;
    this.continuationPosition = continuationPosition;
  }

  public List<T> getValues() {
    return values;
  }

  @Nullable
  public ContinuationT getContinuationPosition() {
    return continuationPosition;
  }
}
