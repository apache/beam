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
package org.apache.beam.runners.twister2.translators.batch;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.tset.sets.batch.BatchTSetImpl;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedTSet;
import java.util.Iterator;
import org.apache.beam.runners.twister2.Twister2BatchTranslationContext;
import org.apache.beam.runners.twister2.translators.BatchTransformTranslator;
import org.apache.beam.runners.twister2.translators.functions.ByteToWindowFunction;
import org.apache.beam.runners.twister2.translators.functions.GroupByWindowFunction;
import org.apache.beam.runners.twister2.translators.functions.MapToTupleFunction;
import org.apache.beam.runners.twister2.translators.functions.internal.SystemReduceFnBuffering;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;

/** GroupByKey translator. */
public class GroupByKeyTranslatorBatch<K, V> implements BatchTransformTranslator<GroupByKey<K, V>> {

  @Override
  public void translateNode(GroupByKey<K, V> transform, Twister2BatchTranslationContext context) {
    PCollection<KV<K, V>> input = context.getInput(transform);
    BatchTSetImpl<WindowedValue<KV<K, V>>> inputTTSet = context.getInputDataSet(input);
    final KvCoder<K, V> coder = (KvCoder<K, V>) input.getCoder();
    Coder<K> inputKeyCoder = coder.getKeyCoder();
    WindowingStrategy windowingStrategy = input.getWindowingStrategy();
    WindowFn<KV<K, V>, BoundedWindow> windowFn =
        (WindowFn<KV<K, V>, BoundedWindow>) windowingStrategy.getWindowFn();
    final WindowedValue.WindowedValueCoder<V> wvCoder =
        WindowedValue.FullWindowedValueCoder.of(coder.getValueCoder(), windowFn.windowCoder());
    KeyedTSet<byte[], byte[]> keyedTSet =
        inputTTSet.mapToTuple(new MapToTupleFunction<K, V>(inputKeyCoder, wvCoder));

    // todo add support for a partition function to be specified, this would use
    // todo keyedPartition function instead of KeyedGather
    ComputeTSet<KV<K, Iterable<WindowedValue<V>>>, Iterator<Tuple<byte[], Iterator<byte[]>>>>
        groupedbyKeyTset =
            keyedTSet.keyedGather().map(new ByteToWindowFunction(inputKeyCoder, wvCoder));

    // --- now group also by window.
    SystemReduceFnBuffering reduceFnBuffering = new SystemReduceFnBuffering(coder.getValueCoder());
    ComputeTSet<WindowedValue<KV<K, Iterable<V>>>, Iterable<KV<K, Iterator<WindowedValue<V>>>>>
        outputTset =
            groupedbyKeyTset
                .direct()
                .<WindowedValue<KV<K, Iterable<V>>>>flatmap(
                    new GroupByWindowFunction(
                        windowingStrategy, reduceFnBuffering, context.getOptions()));
    PCollection output = context.getOutput(transform);
    context.setOutputDataSet(output, outputTset);
  }
}
