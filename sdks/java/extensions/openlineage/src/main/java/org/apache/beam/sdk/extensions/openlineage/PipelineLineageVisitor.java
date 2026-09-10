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
package org.apache.beam.sdk.extensions.openlineage;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * Extracts dataset identities from a specific IO transform in the pipeline graph, mirroring the
 * Flink integration's per-connector {@code Visitor} classes. Implementations must be safe to probe
 * with arbitrary transforms and must never throw from {@link #isDefinedAt}.
 */
abstract class PipelineLineageVisitor {

  /** Returns true when this visitor understands the given transform. */
  abstract boolean isDefinedAt(PTransform<?, ?> transform);

  /** Datasets the transform reads. */
  List<DatasetIdentifier> applyInputs(PTransform<?, ?> transform) {
    return Collections.emptyList();
  }

  /** Datasets the transform writes. */
  List<DatasetIdentifier> applyOutputs(PTransform<?, ?> transform) {
    return Collections.emptyList();
  }

  /** True when the transform's class hierarchy contains the given class name. */
  static boolean extendsClass(Object object, String className) {
    Class<?> cls = object.getClass();
    while (cls != null) {
      if (cls.getName().equals(className)) {
        return true;
      }
      cls = cls.getSuperclass();
    }
    return false;
  }
}
