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

package org.apache.beam.runners.spark.io;

import org.apache.beam.sdk.io.Source;
import org.apache.spark.Partition;

/**
 * An input partition wrapping the sharded {@link Source}.
 */
public class SourcePartition<T> implements Partition {

  final int rddId;
  final int index;
  final Source<T> source;

  public SourcePartition(int rddId, int index, Source<T> source) {
    this.rddId = rddId;
    this.index = index;
    this.source = source;
  }

  @Override
  public int index() {
    return index;
  }

  @Override
  public int hashCode() {
    return 31 * (31 + rddId) + index;
  }

  @Override
  public boolean equals(Object other) {
    return super.equals(other);
  }
}
