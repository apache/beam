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
package org.apache.beam.runners.flink.streaming;

import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.RegularOperatorChain;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

/**
 * {@link StreamSource} utilities, that bridge incompatibilities between Flink releases.
 *
 * <p>This change is becauses RecordWriter is wrapped in RecordWriterDelegate in 1.10, please refer
 * to https://github.com/apache/flink/commit/2c8b4ef572f05bf4740b7e204af1e5e709cd945c for more
 * details.
 */
public class StreamSources {

  /**
   * Backward compatibility helper for {@link OneInputTransformation} `getInput` method, that has
   * been removed in Flink 1.12.
   *
   * @param source Source to get single input from.
   * @return Input transformation.
   */
  public static Transformation<?> getOnlyInput(OneInputTransformation<?, ?> source) {
    return Iterables.getOnlyElement(source.getInputs());
  }

  public static <OutT, SrcT extends SourceFunction<OutT>> void run(
      StreamSource<OutT, SrcT> streamSource,
      Object lockingObject,
      Output<StreamRecord<OutT>> collector)
      throws Exception {
    streamSource.run(lockingObject, collector, createOperatorChain(streamSource));
  }

  private static OperatorChain<?, ?> createOperatorChain(AbstractStreamOperator<?> operator) {
    return new RegularOperatorChain<>(
        operator.getContainingTask(),
        StreamTask.createRecordWriterDelegate(
            operator.getOperatorConfig(), new MockEnvironmentBuilder().build()));
  }

  /** The emitWatermarkStatus method was added in Flink 1.14, so we need to wrap Output. */
  public interface OutputWrapper<T> extends Output<T> {
    @Override
    default void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}
  }
}
