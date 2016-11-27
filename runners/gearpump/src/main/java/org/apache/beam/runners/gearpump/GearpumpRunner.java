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

import org.apache.beam.runners.core.AssignWindows;
import org.apache.beam.runners.gearpump.translators.TranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
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
    if (Window.Bound.class.equals(transform.getClass())) {
      return (OutputT) super.apply(
              new AssignWindowsAndSetStrategy((Window.Bound) transform), input);
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
    streamApp.run();

    return null;
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


  /**
   * copied from DirectPipelineRunner.
   * used to replace Window.Bound till window function is added to Gearpump Stream DSL
   */
  private static class AssignWindowsAndSetStrategy<T, W extends BoundedWindow>
      extends PTransform<PCollection<T>, PCollection<T>> {

    private final Window.Bound<T> wrapped;

    AssignWindowsAndSetStrategy(Window.Bound<T> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public PCollection<T> apply(PCollection<T> input) {
      WindowingStrategy<?, ?> outputStrategy =
          wrapped.getOutputStrategyInternal(input.getWindowingStrategy());

      WindowFn<T, BoundedWindow> windowFn =
          (WindowFn<T, BoundedWindow>) outputStrategy.getWindowFn();

      // If the Window.Bound transform only changed parts other than the WindowFn, then
      // we skip AssignWindows even though it should be harmless in a perfect world.
      // The world is not perfect, and a GBK may have set it to InvalidWindows to forcibly
      // crash if another GBK is performed without explicitly setting the WindowFn. So we skip
      // AssignWindows in this case.
      if (wrapped.getWindowFn() == null) {
        return input.apply("Identity", ParDo.of(new IdentityFn<T>()))
            .setWindowingStrategyInternal(outputStrategy);
      } else {
        return input
            .apply("AssignWindows", new AssignWindows<>(windowFn))
            .setWindowingStrategyInternal(outputStrategy);
      }
    }
  }

  private static class IdentityFn<T> extends OldDoFn<T, T> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }
}
