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
import org.apache.beam.runners.gearpump.translators.GearpumpPipelineTranslator;
import org.apache.beam.runners.gearpump.translators.TranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.gearpump.cluster.ClusterConfig;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.cluster.client.ClientContext;
import org.apache.gearpump.cluster.client.RunningApplication;
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
    UserConfig userConfig = UserConfig.empty();
    JavaStreamApp streamApp = new JavaStreamApp(
        appName, clientContext, userConfig);
    TranslationContext translationContext = new TranslationContext(streamApp, options);
    GearpumpPipelineTranslator translator = new GearpumpPipelineTranslator(translationContext);
    translator.translate(pipeline);
    RunningApplication app = streamApp.submit();

    return new GearpumpPipelineResult(clientContext, app);
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
    serializers.put("org.apache.beam.sdk.util.WindowedValue$ValueInGlobalWindow", "");
    serializers.put("org.apache.beam.sdk.util.WindowedValue$TimestampedValueInSingleWindow", "");
    serializers.put("org.apache.beam.sdk.util.WindowedValue$TimestampedValueInGlobalWindow", "");
    serializers.put("org.apache.beam.sdk.util.WindowedValue$TimestampedValueInMultipleWindows", "");
    serializers.put("org.apache.beam.sdk.transforms.windowing.PaneInfo", "");
    serializers.put("org.apache.beam.sdk.transforms.windowing.PaneInfo$Timing", "");
    serializers.put("org.joda.time.Instant", "");
    serializers.put("org.apache.beam.sdk.values.KV", "");
    serializers.put("org.apache.beam.sdk.transforms.windowing.IntervalWindow", "");
    serializers.put("org.apache.beam.sdk.values.TimestampedValue", "");
    serializers.put(
        "org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils$RawUnionValue", "");

    if (userSerializers != null && !userSerializers.isEmpty()) {
      serializers.putAll(userSerializers);
    }

    return config.withValue(GEARPUMP_SERIALIZERS, ConfigValueFactory.fromMap(serializers));
  }

}
