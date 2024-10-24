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
package org.apache.beam.runners.samza.util;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.errorprone.annotations.DoNotCall;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.samza.SamzaRunner;
import org.apache.beam.runners.samza.translation.ConfigContext;
import org.apache.beam.runners.samza.translation.SamzaPipelineTranslator;
import org.apache.beam.runners.samza.translation.TransformTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.gson.JsonArray;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.gson.JsonObject;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.gson.JsonParser;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterators;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JSON renderer for BEAM {@link Pipeline} DAG. This can help us with visualization of the Beam
 * DAG.
 */
public class PipelineJsonRenderer implements Pipeline.PipelineVisitor {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineJsonRenderer.class);
  private static final String TRANSFORM_IO_MAP_DELIMITER = ",";

  /**
   * Interface to get I/O information for a Beam job. This will help add I/O information to the Beam
   * DAG.
   */
  public interface SamzaIOInfo {

    /** Get I/O topic name and cluster. */
    Optional<String> getIOInfo(TransformHierarchy.Node node);
  }

  /** A registrar for {@link SamzaIOInfo}. */
  public interface SamzaIORegistrar {

    SamzaIOInfo getSamzaIO();
  }

  private static final String OUTERMOST_NODE = "OuterMostNode";
  @Nullable private static final SamzaIOInfo SAMZA_IO_INFO = loadSamzaIOInfo();

  /**
   * This method creates a JSON representation of the Beam pipeline.
   *
   * @param pipeline The beam pipeline
   * @param ctx Config context of the pipeline
   * @return JSON string representation of the pipeline
   */
  public static String toJsonString(Pipeline pipeline, ConfigContext ctx) {
    final PipelineJsonRenderer visitor = new PipelineJsonRenderer(ctx);
    pipeline.traverseTopologically(visitor);
    return visitor.jsonBuilder.toString();
  }

  /**
   * This method creates a JSON representation for Beam Portable Pipeline.
   *
   * @param pipeline The beam portable pipeline
   * @return JSON string representation of the pipeline
   */
  @DoNotCall("JSON DAG for portable pipeline is not supported yet.")
  public static String toJsonString(RunnerApi.Pipeline pipeline) {
    throw new UnsupportedOperationException("JSON DAG for portable pipeline is not supported yet.");
  }

  private final StringBuilder jsonBuilder = new StringBuilder();
  private final StringBuilder graphLinks = new StringBuilder();

  private final StringBuilder transformIoInfo = new StringBuilder();
  private final Map<PValue, String> valueToProducerNodeName = new HashMap<>();
  private final ConfigContext ctx;
  private int indent;

  private PipelineJsonRenderer(ConfigContext ctx) {
    this.ctx = ctx;
  }

  @Nullable
  private static SamzaIOInfo loadSamzaIOInfo() {
    final Iterator<SamzaIORegistrar> beamIORegistrarIterator =
        ServiceLoader.load(SamzaIORegistrar.class).iterator();
    return beamIORegistrarIterator.hasNext()
        ? Iterators.getOnlyElement(beamIORegistrarIterator).getSamzaIO()
        : null;
  }

  @Override
  public void enterPipeline(Pipeline p) {
    writeLine("{ \n \"RootNode\": [");
    graphLinks.append(",\"graphLinks\": [");

    // Do a pre-scan and build transformIoInfo for input and output PValues of each transform
    // TODO: Refactor PipelineJsonRenderer to use SamzaPipelineVisitor instead of PipelineVisitor to
    // build Beam_JSON_GRAPH
    final Map<String, Map.Entry<String, String>> transformIOMap = buildTransformIOMap(p, ctx);
    buildTransformIoJson(transformIOMap);

    enterBlock();
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    String fullName = node.getFullName();
    writeLine("{ \"fullName\":\"%s\",", assignNodeName(fullName));
    if (node.getEnclosingNode() != null) {
      String enclosingNodeName = node.getEnclosingNode().getFullName();
      writeLine("  \"enclosingNode\":\"%s\",", assignNodeName(enclosingNodeName));
    }

    Optional<String> ioInfo = getIOInfo(node);
    if (ioInfo.isPresent() && !ioInfo.get().isEmpty()) {
      writeLine(" \"ioInfo\":\"%s\",", escapeString(ioInfo.get()));
    }

    writeLine("  \"ChildNodes\":[");
    enterBlock();
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    exitBlock();
    writeLine("]},");
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    String fullName = node.getFullName();
    writeLine("{ \"fullName\":\"%s\",", escapeString(fullName));
    String enclosingNodeName = node.getEnclosingNode().getFullName();
    writeLine("  \"enclosingNode\":\"%s\"},", assignNodeName(enclosingNodeName));

    node.getOutputs().values().forEach(x -> valueToProducerNodeName.put(x, fullName));
    node.getInputs()
        .forEach(
            (key, value) -> {
              final String producerName = valueToProducerNodeName.get(value);
              graphLinks.append(
                  String.format("{\"from\":\"%s\"," + "\"to\":\"%s\"},", producerName, fullName));
            });
  }

  @Override
  public void visitValue(PValue value, TransformHierarchy.Node producer) {}

  @Override
  public void leavePipeline(Pipeline pipeline) {
    exitBlock();
    writeLine("]");
    // delete the last comma
    int lastIndex = graphLinks.length() - 1;
    if (graphLinks.charAt(lastIndex) == ',') {
      graphLinks.deleteCharAt(lastIndex);
    }
    graphLinks.append("]");
    jsonBuilder.append(graphLinks);
    // Attach transformIoInfo - transformName to input and output PCollection(PValues)
    jsonBuilder.append(transformIoInfo);
    jsonBuilder.append("}");
  }

  private void buildTransformIoJson(Map<String, Map.Entry<String, String>> transformIOMap) {
    transformIoInfo.append(",\"transformIOInfo\": [");
    transformIOMap.forEach(
        (transform, ioInfo) -> {
          transformIoInfo.append(
              String.format(
                  "{\"transformName\":\"%s\"," + "\"inputs\":\"%s\"," + "\"outputs\":\"%s\"},",
                  transform, ioInfo.getKey(), ioInfo.getValue()));
        });
    // delete the last extra comma
    int lastIndex = transformIoInfo.length() - 1;
    if (transformIoInfo.charAt(lastIndex) == ',') {
      transformIoInfo.deleteCharAt(lastIndex);
    }
    transformIoInfo.append("]");
  }

  private void enterBlock() {
    indent += 4;
  }

  private void exitBlock() {
    indent -= 4;
  }

  @FormatMethod
  private void writeLine(@FormatString String format, Object... args) {
    // Since we append a comma after every entry to the graph, we will need to remove that one extra
    // comma towards the end of the JSON.
    int secondLastCharIndex = jsonBuilder.length() - 2;
    if (jsonBuilder.length() > 1
        && jsonBuilder.charAt(secondLastCharIndex) == ','
        && (format.startsWith("}") || format.startsWith("]"))) {
      jsonBuilder.deleteCharAt(secondLastCharIndex);
    }
    if (indent != 0) {
      jsonBuilder.append(String.format("%-" + indent + "s", ""));
    }
    jsonBuilder.append(String.format(format, args));
    jsonBuilder.append("\n");
  }

  private static String escapeString(String x) {
    return x.replace("\"", "\\\"");
  }

  private String assignNodeName(String nodeName) {
    return escapeString(nodeName.isEmpty() ? OUTERMOST_NODE : nodeName);
  }

  private Optional<String> getIOInfo(TransformHierarchy.Node node) {
    if (SAMZA_IO_INFO == null) {
      return Optional.empty();
    }
    return SAMZA_IO_INFO.getIOInfo(node);
  }

  /**
   * Builds a map from PTransform to its input and output PValues. The map is serialized as part of
   * Beam_JSON_GRAPH
   *
   * <p>Please note this map needs to be built using SamzaPipelineVisitor instead of generic
   * PipelineVisitor used here, reason being SamzaPipelineVisitor traverses the pipeline differently
   * i.e. if a composite transform can be translated directly it won't further expand it.
   * PipelineVisitor used here is not runner dependent visitor, its just used here for rendering
   * purposes
   */
  @VisibleForTesting
  static Map<String, Map.Entry<String, String>> buildTransformIOMap(
      Pipeline pipeline, ConfigContext ctx) {
    final Map<String, Map.Entry<String, String>> pTransformToInputOutputMap = new HashMap<>();
    final SamzaPipelineTranslator.TransformVisitorFn configFn =
        new SamzaPipelineTranslator.TransformVisitorFn() {
          @Override
          public <T extends PTransform<?, ?>> void apply(
              T transform,
              TransformHierarchy.Node node,
              Pipeline pipeline,
              TransformTranslator<T> translator) {
            ctx.setCurrentTransform(node.toAppliedPTransform(pipeline));
            List<String> inputs = getIOPValueList(node.getInputs()).get();
            List<String> outputs = getIOPValueList(node.getOutputs()).get();
            pTransformToInputOutputMap.put(
                node.getFullName(),
                new AbstractMap.SimpleEntry<>(
                    String.join(TRANSFORM_IO_MAP_DELIMITER, inputs),
                    String.join(TRANSFORM_IO_MAP_DELIMITER, outputs)));
            ctx.clearCurrentTransform();
          }
        };

    final SamzaPipelineTranslator.SamzaPipelineVisitor visitor =
        new SamzaPipelineTranslator.SamzaPipelineVisitor(configFn);
    pipeline.traverseTopologically(visitor);
    return pTransformToInputOutputMap;
  }

  private static Supplier<List<String>> getIOPValueList(Map<TupleTag<?>, PCollection<?>> map) {
    return () -> map.values().stream().map(pColl -> pColl.getName()).collect(Collectors.toList());
  }

  // Reads the config to build transformIOMap, i.e. map of inputs & output PValues for each
  // PTransform
  public static Map<String, Map.Entry<List<String>, List<String>>> getTransformIOMap(
      Config config) {
    checkNotNull(config, "Config cannot be null");
    final Map<String, Map.Entry<List<String>, List<String>>> result = new HashMap<>();
    final String pipelineJsonGraph = config.get(SamzaRunner.BEAM_JSON_GRAPH);
    if (pipelineJsonGraph == null) {
      LOG.warn(
          "Cannot build transformIOMap since Config: {} is found null ",
          SamzaRunner.BEAM_JSON_GRAPH);
      return result;
    }
    JsonObject jsonObject = JsonParser.parseString(pipelineJsonGraph).getAsJsonObject();
    JsonArray transformIOInfo = jsonObject.getAsJsonArray("transformIOInfo");
    transformIOInfo.forEach(
        transform -> {
          final String transformName =
              transform.getAsJsonObject().get("transformName").getAsString();
          final String inputs = transform.getAsJsonObject().get("inputs").getAsString();
          final String outputs = transform.getAsJsonObject().get("outputs").getAsString();
          result.put(transformName, new AbstractMap.SimpleEntry<>(ioFunc(inputs), ioFunc(outputs)));
        });
    return result;
  }

  private static List<String> ioFunc(String ioList) {
    return Arrays.stream(ioList.split(PipelineJsonRenderer.TRANSFORM_IO_MAP_DELIMITER))
        .filter(item -> !item.isEmpty())
        .collect(Collectors.toList());
  }
}
