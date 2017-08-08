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
package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Operation that partitions input elements based on their {@link TupleTag} keys.
 */
public class PartitionOperation extends Operation<KV<TupleTag<?>, Object>> {

  private final List<ReadOperation> readOperations;
  private final List<TupleTag<?>> tupleTags;

  public PartitionOperation(List<ReadOperation> readOperations, List<TupleTag<?>> tupleTags) {
    super(readOperations.size());
    this.readOperations = checkNotNull(readOperations, "readOperations");
    this.tupleTags = checkNotNull(tupleTags, "tupleTags");
  }

  public List<ReadOperation> getReadOperations() {
    return readOperations;
  }

  @Override
  public void process(WindowedValue<KV<TupleTag<?>, Object>> elem) throws IOException,
      InterruptedException {
    TupleTag<?> tupleTag = elem.getValue().getKey();
    int outputIndex = getOutputIndex(tupleTag);
    OutputReceiver receiver = getOutputReceivers().get(outputIndex);
    receiver.process((WindowedValue<?>) elem.getValue().getValue());
  }

  @Override
  protected int getOutputIndex(TupleTag<?> tupleTag) {
    int index = tupleTags.indexOf(tupleTag);
    checkState(
        index >= 0,
        String.format("Cannot find index for tuple tag: %s.", tupleTag));
    return index;
  }
}
