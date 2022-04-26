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

import com.google.errorprone.annotations.DoNotCall;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;

/**
 * A JSON renderer for BEAM {@link Pipeline} DAG. This can help us with visualization of the Beam
 * DAG.
 */
@Experimental
public class PipelineJsonRenderer implements Pipeline.PipelineVisitor {

  /**
   * Interface to get I/O information for a Beam job. This will help add I/O information to the Beam
   * DAG.
   */
  @Experimental
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
   * @return JSON string representation of the pipeline
   */
  public static String toJsonString(Pipeline pipeline) {
    final PipelineJsonRenderer visitor = new PipelineJsonRenderer();
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
  private final Map<PValue, String> valueToProducerNodeName = new HashMap<>();
  private int indent;

  private PipelineJsonRenderer() {}

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
    jsonBuilder.append("}");
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
}
