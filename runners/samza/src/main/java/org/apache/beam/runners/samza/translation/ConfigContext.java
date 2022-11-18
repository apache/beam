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
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/** Helper that provides context data such as output for config generation. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ConfigContext {
  private final Map<PValue, String> idMap;
  private AppliedPTransform<?, ?, ?> currentTransform;
  private final SamzaPipelineOptions options;
  private final Map<String, String> usedStateIdMap;
  private final Map<String, String> multiParDoStateIdMap;

  public ConfigContext(
      Map<PValue, String> idMap,
      SamzaPipelineOptions options,
      Map<String, String> multiParDoStateIdMap) {
    this.idMap = idMap;
    this.options = options;
    this.usedStateIdMap = new HashMap<>();
    this.multiParDoStateIdMap = multiParDoStateIdMap;
  }

  public void setCurrentTransform(AppliedPTransform<?, ?, ?> currentTransform) {
    this.currentTransform = currentTransform;
  }

  public void clearCurrentTransform() {
    this.currentTransform = null;
  }

  @SuppressWarnings("unchecked")
  public <OutT extends PValue> OutT getOutput(PTransform<?, OutT> transform) {
    return (OutT) Iterables.getOnlyElement(this.currentTransform.getOutputs().values());
  }

  public String getOutputId(TransformHierarchy.Node node) {
    return getIdForPValue(Iterables.getOnlyElement(node.getOutputs().values()));
  }

  public SamzaPipelineOptions getPipelineOptions() {
    return this.options;
  }

  /** Helper to keep track of used stateIds and return unique store id. */
  public String getUniqueStoreId(String stateId, String parDoName) {
    // Update a map of used state id with parDo name.
    if (!usedStateIdMap.containsKey(stateId)) {
      usedStateIdMap.put(stateId, parDoName);
      return stateId;
    } else {
      // Same state id identified for the first time
      if (!multiParDoStateIdMap.containsKey(stateId)) {
        final String prevParDoName = usedStateIdMap.get(stateId);
        final String prevMultiParDoStateId = String.join("-", stateId, prevParDoName);
        usedStateIdMap.put(prevMultiParDoStateId, prevParDoName);
        // Store the stateId with previous parDo name which will be used for config rewriting
        multiParDoStateIdMap.put(stateId, prevParDoName);
      }
      // Compose a new store id with state id and parDo name (eg) "stateId-parDoName"
      final String multiParDoStateId = String.join("-", stateId, parDoName);
      // Leveraging framework which enforces unique parDo name.
      // If the framework logic changes, this is a safeguard to throw exception to avoid storeId
      // collision
      if (usedStateIdMap.containsKey(multiParDoStateId)) {
        throw new IllegalStateException(
            "Same stateId "
                + stateId
                + " with the same parDoName "
                + parDoName
                + " found in multiple ParDo.");
      }
      usedStateIdMap.put(multiParDoStateId, parDoName);
      return multiParDoStateId;
    }
  }

  private String getIdForPValue(PValue pvalue) {
    final String id = idMap.get(pvalue);
    if (id == null) {
      throw new IllegalArgumentException("No id mapping for value: " + pvalue);
    }
    return id;
  }
}
