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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;

/**
 * This class generates an ID for each {@link PValue} during a topological traversal of the BEAM
 * {@link Pipeline}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PViewToIdMapper extends Pipeline.PipelineVisitor.Defaults {
  private final Map<PValue, String> idMap = new HashMap<>();
  private int nextId;

  public static Map<PValue, String> buildIdMap(Pipeline pipeline) {
    final PViewToIdMapper mapper = new PViewToIdMapper();
    pipeline.traverseTopologically(mapper);
    return mapper.getIdMap();
  }

  private PViewToIdMapper() {}

  @Override
  public void visitValue(PValue value, TransformHierarchy.Node producer) {
    final String valueDesc = pValueToString(value).replaceFirst(".*:([a-zA-Z#0-9]+).*", "$1");

    final String samzaSafeValueDesc = valueDesc.replaceAll("[^A-Za-z0-9_-]", "_");

    idMap.put(value, String.format("%d-%s", nextId++, samzaSafeValueDesc));
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    if (node.getTransform() instanceof SamzaPublishView) {
      final PCollectionView view = ((SamzaPublishView) node.getTransform()).getView();
      visitValue(view, node);
    }
  }

  public Map<PValue, String> getIdMap() {
    return Collections.unmodifiableMap(idMap);
  }

  /**
   * This method is created to replace the {@link org.apache.beam.sdk.values.PValueBase#toString()}
   * with the old implementation that doesn't contain the hashcode.
   */
  private static String pValueToString(PValue value) {
    String name;
    try {
      name = value.getName();
    } catch (IllegalStateException e) {
      name = "<unnamed>";
    }
    return name + " [" + NameUtils.approximateSimpleName(value.getClass()) + "]";
  }
}
