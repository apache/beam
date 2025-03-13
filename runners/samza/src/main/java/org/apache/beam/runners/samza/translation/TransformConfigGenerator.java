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
import java.util.Map;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;

/** Generates config for a BEAM PTransform (regular java api or portable api). */
public interface TransformConfigGenerator<T extends PTransform<?, ?>> {
  /** Generate config for regular java api PTransform. */
  default Map<String, String> createConfig(
      T transform, TransformHierarchy.Node node, ConfigContext ctx) {
    return Collections.emptyMap();
  }

  /** Generate config for portable api PTransform. */
  default Map<String, String> createPortableConfig(
      PipelineNode.PTransformNode transform, SamzaPipelineOptions options) {
    return Collections.emptyMap();
  }
}
