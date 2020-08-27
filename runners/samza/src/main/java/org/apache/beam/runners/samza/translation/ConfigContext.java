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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/** Helper that provides context data such as output for config generation. */
public class ConfigContext {
  private final Map<PValue, String> idMap;
  private AppliedPTransform<?, ?, ?> currentTransform;
  private final SamzaPipelineOptions options;
  private final Set<String> stateIds;

  public ConfigContext(Map<PValue, String> idMap, SamzaPipelineOptions options) {
    this.idMap = idMap;
    this.options = options;
    this.stateIds = new HashSet<>();
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

  public boolean addStateId(String stateId) {
    return stateIds.add(stateId);
  }

  private String getIdForPValue(PValue pvalue) {
    final String id = idMap.get(pvalue);
    if (id == null) {
      throw new IllegalArgumentException("No id mapping for value: " + pvalue);
    }
    return id;
  }
}
