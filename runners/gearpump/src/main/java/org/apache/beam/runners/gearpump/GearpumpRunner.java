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
package org.apache.beam.runners.gearpump;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import java.util.HashMap;
import java.util.Map;

import org.apache.beam.runners.gearpump.translators.TranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.IdentityWindowFn;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

import org.apache.gearpump.cluster.ClusterConfig;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.cluster.embedded.EmbeddedCluster;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStreamApp;

/**
 * A {@link PipelineRunner} that executes the operations in the
 * pipeline by first translating them to Gearpump Stream DSL
 * and then executing them on a Gearpump cluster.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GearpumpRunner extends PipelineRunner<GearpumpPipelineResult> {

  private final GearpumpPipelineOptions options;

  private static final String GEARPUMP_SERIALIZERS = "gearpump.serializers";
  private static final String DEFAULT_APPNAME = "beam_gearpump_app";

  public GearpumpRunner(GearpumpPipelineOptions options) {
    this.options = options;
  }

  public static GearpumpRunner fromOptions(PipelineOptions options) {
    GearpumpPipelineOptions pipelineOptions =
        PipelineOptionsValidator.validate(GearpumpPipelineOptions.class, options);
    return new GearpumpRunner(pipelineOptions);
  }


  public <OutputT extends POutput, InputT extends PInput> OutputT apply(
      PTransform<InputT, OutputT> transform, InputT input) {
    if (Window.Bound.class.equals(transform.getClass())
        && isNullOrIdentityWindowFn(((Window.Bound) transform).getWindowFn())) {
      return (OutputT) super.apply(
              ParDo.of(new IdentityFn()), input);
    } else if (Flatten.FlattenPCollectionList.class.equals(transform.getClass())
            && ((PCollectionList<?>) input).size() == 0) {
      return (OutputT) Pipeline.applyTransform(input.getPipeline().begin(), Create.of());
    } else if (Create.Values.class.equals(transform.getClass())) {
      return (OutputT) PCollection
              .<OutputT>createPrimitiveOutputInternal(
                      input.getPipeline(),
                      WindowingStrategy.globalDefault(),
                      PCollection.IsBounded.BOUNDED);
    } else {
      return super.apply(transform, input);
    }
  }

  @Override
  public GearpumpPipelineResult run(Pipeline pipeline) {
    String appName = options.getApplicationName();
    if (null == appName) {
      appName = DEFAULT_APPNAME;
    }
    Config config = registerSerializers(ClusterConfig.defaultConfig(),
        options.getSerializers());
    ClientContext clientContext = getClientContext(options, config);
    options.setClientContext(clientContext);
    JavaStreamApp streamApp = new JavaStreamApp(
        appName, clientContext, UserConfig.empty());
    TranslationContext translationContext = new TranslationContext(streamApp, options);
    GearpumpPipelineTranslator translator = new GearpumpPipelineTranslator(translationContext);
    translator.translate(pipeline);
    int appId = streamApp.submit();

    return new GearpumpPipelineResult(clientContext, appId);
  }

  private ClientContext getClientContext(GearpumpPipelineOptions options, Config config) {
    EmbeddedCluster cluster = options.getEmbeddedCluster();
    if (cluster != null) {
      return cluster.newClientContext();
    } else {
      return ClientContext.apply(config);
    }
  }

  /**
   * register class with default kryo serializers.
   */
  private Config registerSerializers(Config config, Map<String, String> userSerializers) {
    Map<String, String> serializers = new HashMap<>();
    serializers.put("org.apache.beam.sdk.util.WindowedValue$TimestampedValueInSingleWindow", "");
    serializers.put("org.apache.beam.sdk.transforms.windowing.PaneInfo", "");
    serializers.put("org.apache.beam.sdk.transforms.windowing.PaneInfo$Timing", "");
    serializers.put("org.joda.time.Instant", "");
    serializers.put("org.apache.beam.sdk.values.KV", "");
    serializers.put("org.apache.beam.sdk.transforms.windowing.IntervalWindow", "");
    serializers.put("org.apache.beam.sdk.values.TimestampedValue", "");
    if (userSerializers != null && !userSerializers.isEmpty()) {
      serializers.putAll(userSerializers);
    }
    return config.withValue(GEARPUMP_SERIALIZERS, ConfigValueFactory.fromMap(serializers));
  }

  private static class IdentityFn<T> extends DoFn<T, T> {

    @ProcessElement
    public void process(ProcessContext c) {
      c.output(c.element());
    }
  }

  private boolean isNullOrIdentityWindowFn(WindowFn windowFn) {
    return windowFn == null || windowFn.getClass().equals(IdentityWindowFn.class);
  }
}
