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

package org.apache.beam.runners.samza.translation;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.samza.adapter.BoundedSourceSystem;
import org.apache.beam.runners.samza.adapter.UnboundedSourceSystem;
import org.apache.beam.runners.samza.util.Base64Serializer;
import org.apache.beam.runners.samza.util.SamzaCoders;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * Translates {@link org.apache.beam.sdk.io.Read} to Samza input {@link
 * org.apache.samza.operators.MessageStream}.
 */
public class ReadTranslator<T>
    implements TransformTranslator<PTransform<PBegin, PCollection<T>>>,
        TransformConfigGenerator<PTransform<PBegin, PCollection<T>>> {

  @Override
  public void translate(
      PTransform<PBegin, PCollection<T>> transform,
      TransformHierarchy.Node node,
      TranslationContext ctx) {
    final PCollection<T> output = ctx.getOutput(transform);
    ctx.registerInputMessageStream(output);
  }

  @Override
  public Map<String, String> createConfig(
      PTransform<PBegin, PCollection<T>> transform,
      TransformHierarchy.Node node,
      ConfigContext ctx) {
    final String id = ctx.getOutputId(node);
    final PCollection<T> output = ctx.getOutput(transform);
    final Coder<WindowedValue<T>> coder = SamzaCoders.of(output);
    final Source<?> source =
        transform instanceof Read.Unbounded
            ? ((Read.Unbounded) transform).getSource()
            : ((Read.Bounded) transform).getSource();

    final Map<String, String> config = new HashMap<>();
    final String systemPrefix = "systems." + id;
    final String streamPrefix = "streams." + id;

    config.put(systemPrefix + ".source", Base64Serializer.serializeUnchecked(source));
    config.put(systemPrefix + ".coder", Base64Serializer.serializeUnchecked(coder));
    config.put(systemPrefix + ".stepName", node.getFullName());

    config.put(streamPrefix + ".samza.system", id);

    if (source instanceof BoundedSource) {
      config.put(streamPrefix + ".samza.bounded", "true");
      config.put(systemPrefix + ".samza.factory", BoundedSourceSystem.Factory.class.getName());
    } else {
      config.put(systemPrefix + ".samza.factory", UnboundedSourceSystem.Factory.class.getName());
    }

    return config;
  }
}
