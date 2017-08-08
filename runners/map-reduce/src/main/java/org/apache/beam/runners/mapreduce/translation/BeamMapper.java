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

import java.io.IOException;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapter for executing Beam transforms in {@link Mapper}.
 */
public class BeamMapper<ValueInT, ValueOutT>
    extends Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>> {
  private static final Logger LOG = LoggerFactory.getLogger(Mapper.class);

  public static final String BEAM_PAR_DO_OPERATION_MAPPER = "beam-par-do-op-mapper";

  private Operation operation;

  @Override
  protected void setup(
      Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>>.Context context) {
    String serializedParDo = checkNotNull(
        context.getConfiguration().get(BEAM_PAR_DO_OPERATION_MAPPER),
        BEAM_PAR_DO_OPERATION_MAPPER);
    operation = (Operation) SerializableUtils.deserializeFromByteArray(
        Base64.decodeBase64(serializedParDo), "Operation");
    operation.start((TaskInputOutputContext) context);
  }

  @Override
  protected void map(
      Object key,
      WindowedValue<ValueInT> value,
      Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>>.Context context)
      throws IOException, InterruptedException {
    LOG.info("key: {} value: {}.", key, value);
    operation.process(WindowedValue.valueInGlobalWindow(KV.of(key, value)));
  }

  @Override
  protected void cleanup(
      Mapper<Object, WindowedValue<ValueInT>, Object, WindowedValue<ValueOutT>>.Context context) {
    operation.finish();
  }
}
