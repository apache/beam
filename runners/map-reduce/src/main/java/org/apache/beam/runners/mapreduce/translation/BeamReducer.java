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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Adapter for executing Beam transforms in {@link Reducer}.
 */
public class BeamReducer<ValueInT, ValueOutT>
    extends Reducer<BytesWritable, byte[], Object, WindowedValue<ValueOutT>> {
  private static final Logger LOG = LoggerFactory.getLogger(Reducer.class);

  public static final String BEAM_REDUCER_KV_CODER = "beam-reducer-kv-coder";
  public static final String BEAM_PAR_DO_OPERATION_REDUCER = "beam-par-do-op-reducer";

  private Coder<Object> keyCoder;
  private Coder<Object> valueCoder;
  private Operation operation;

  @Override
  protected void setup(
      Reducer<BytesWritable, byte[], Object, WindowedValue<ValueOutT>>.Context context) {
    String serializedValueCoder = checkNotNull(
        context.getConfiguration().get(BEAM_REDUCER_KV_CODER),
        BEAM_REDUCER_KV_CODER);
    String serializedParDo = checkNotNull(
        context.getConfiguration().get(BEAM_PAR_DO_OPERATION_REDUCER),
        BEAM_PAR_DO_OPERATION_REDUCER);
    KvCoder<Object, Object> kvCoder = (KvCoder<Object, Object>) SerializableUtils
        .deserializeFromByteArray(Base64.decodeBase64(serializedValueCoder), "Coder");
    keyCoder = kvCoder.getKeyCoder();
    valueCoder = kvCoder.getValueCoder();
    operation = (Operation) SerializableUtils.deserializeFromByteArray(
        Base64.decodeBase64(serializedParDo), "Operation");
    operation.start((TaskInputOutputContext) context);
  }

  @Override
  protected void reduce(
      BytesWritable key,
      Iterable<byte[]> values,
      Reducer<BytesWritable, byte[], Object, WindowedValue<ValueOutT>>.Context context)
      throws InterruptedException, IOException {
    List<Object> decodedValues = Lists.newArrayList(FluentIterable.from(values)
        .transform(new Function<byte[], Object>() {
          @Override
          public Object apply(byte[] input) {
            ByteArrayInputStream inStream = new ByteArrayInputStream(input);
            try {
              return valueCoder.decode(inStream);
            } catch (IOException e) {
              Throwables.throwIfUnchecked(e);
              throw new RuntimeException(e);
            }
          }}));
    Object decodedKey = keyCoder.decode(new ByteArrayInputStream(key.getBytes()));
    LOG.info("key: {} value: {}.", decodedKey, decodedValues);
    operation.process(
        WindowedValue.valueInGlobalWindow(KV.of(decodedKey, decodedValues)));
  }

  @Override
  protected void cleanup(
      Reducer<BytesWritable, byte[], Object, WindowedValue<ValueOutT>>.Context context) {
    operation.finish();
  }
}
