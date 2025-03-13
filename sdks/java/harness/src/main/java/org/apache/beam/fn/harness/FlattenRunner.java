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
package org.apache.beam.fn.harness;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables.getOnlyElement;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** Executes flatten PTransforms. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class FlattenRunner<InputT> {
  /** A registrar which provides a factory to handle flatten PTransforms. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(PTransformTranslation.FLATTEN_TRANSFORM_URN, new Factory());
    }
  }

  /** A factory for {@link FlattenRunner}. */
  static class Factory<InputT> implements PTransformRunnerFactory<FlattenRunner<InputT>> {
    @Override
    public FlattenRunner<InputT> createRunnerForPTransform(Context context) throws IOException {
      // Give each input a MultiplexingFnDataReceiver to all outputs of the flatten.
      String output = getOnlyElement(context.getPTransform().getOutputsMap().values());
      FnDataReceiver<WindowedValue<?>> receiver = context.getPCollectionConsumer(output);

      FlattenRunner<InputT> runner = new FlattenRunner<>();
      for (String pCollectionId : context.getPTransform().getInputsMap().values()) {
        context.addPCollectionConsumer(pCollectionId, (FnDataReceiver) receiver);
      }

      return runner;
    }
  }
}
