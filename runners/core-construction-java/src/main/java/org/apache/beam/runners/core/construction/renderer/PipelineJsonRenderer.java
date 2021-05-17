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
package org.apache.beam.runners.core.construction.renderer;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.BeamIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** A JSON renderer for BEAM {@link Pipeline} DAG. */
public class PipelineJsonRenderer implements Pipeline.PipelineVisitor {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineJsonRenderer.class);
    private static final String OUTERMOST_NODE = "OuterMostNode";

    public static String toJsonString(Pipeline pipeline) {
        final PipelineJsonRenderer visitor = new PipelineJsonRenderer();
        visitor.begin();
        pipeline.traverseTopologically(visitor);
        visitor.end();
        return visitor.jsonBuilder.toString();
    }

    public static String toJsonString(RunnerApi.Pipeline pipeline) {
        return null;
    }

    private final StringBuilder jsonBuilder = new StringBuilder();
    private final StringBuilder graphLinks = new StringBuilder();
    private final Map<TransformHierarchy.Node, Integer> nodeToId = new HashMap<>();
    private final Map<PValue, String> valueToProducerNodeName = new HashMap<>();
    private int indent;
    private int nextNodeId;

    private PipelineJsonRenderer() {}

    @Override
    public void enterPipeline(Pipeline p) {}

    @Override
    public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
        String fullName = node.getFullName();
        writeLine(
                "{ \"fullName\":\"%s\",", escapeString(fullName.isEmpty() ? OUTERMOST_NODE : fullName));
        writeLine(
                "  \"shortName\":\"%s\",",
                escapeString(fullName.isEmpty() ? OUTERMOST_NODE : node.getTransform().getName()));
        writeLine("  \"id\":\"%s\",", escapeString(fullName.isEmpty() ? OUTERMOST_NODE : fullName));
        if (!fullName.isEmpty()) {
            String enclosingNodeName = node.getEnclosingNode().getFullName();
            writeLine(
                    "  \"enclosingNode\":\"%s\",",
                    escapeString(enclosingNodeName.isEmpty() ? OUTERMOST_NODE : enclosingNodeName));
        }

        String ioInfo = getIOTopicInfo(node);
        if (!ioInfo.isEmpty()) {
            writeLine(" \"ioInfo\":\"%s\",", escapeString(ioInfo));
        }

        writeLine("  \"ChildNode\":[");
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
        writeLine("  \"shortName\":\"%s\",", escapeString(node.getTransform().getName()));
        String enclosingNodeName = node.getEnclosingNode().getFullName();
        writeLine(
                "  \"enclosingNode\":\"%s\",",
                escapeString(enclosingNodeName.isEmpty() ? OUTERMOST_NODE : enclosingNodeName));
        writeLine("  \"id\":\"%s\"},", escapeString(fullName));

        node.getOutputs().values().forEach(x -> valueToProducerNodeName.put(x, fullName));
        node.getInputs()
                .forEach(
                        (key, value) -> {
                            final String producerName = valueToProducerNodeName.get(value);
                            String style = "solid";
                            if (node.getTransform().getAdditionalInputs().containsKey(key)) {
                                style = "dashed";
                            }
                            graphLinks.append(
                                    String.format(
                                            "{\"from\":\"%s\"," + "\"to\":\"%s\"," + "\"hashId\":\"%s\"},",
                                            producerName, fullName, escapeString(shortenTag(key.getId()))));
                        });
    }

    @Override
    public void visitValue(PValue value, TransformHierarchy.Node producer) {}

    @Override
    public void leavePipeline(Pipeline pipeline) {}

    private void begin() {
        writeLine("{ \n \"RootNode\": [");
        graphLinks.append(",\"graphLinks\": [");
        enterBlock();
    }

    private void end() {
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

    private void writeLine(String format, Object... args) {
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

    private static String shortenTag(String tag) {
        return tag.replaceFirst(".*:([a-zA-Z#0-9]+).*", "$1");
    }

    private String getIOTopicInfo(TransformHierarchy.Node node) {
        final BeamIO beamIO;
        final Iterator<BeamIO.BeamIORegistrar> beamIORegistrarIterator =
                ServiceLoader.load(BeamIO.BeamIORegistrar.class).iterator();
        beamIO = beamIORegistrarIterator.hasNext() ?
                Iterators.getOnlyElement(beamIORegistrarIterator).getBeamIO() : null;

        String nodeInfo = "";
        if (beamIO != null) {
            nodeInfo = beamIO.getIOInfo(node);
        }
        return nodeInfo;
    }

}