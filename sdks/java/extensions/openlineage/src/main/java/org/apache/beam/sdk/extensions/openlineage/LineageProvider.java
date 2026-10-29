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

/**
 * Extension point for {@link org.apache.beam.sdk.transforms.PTransform}s to expose the datasets
 * they read or write, mirroring the Flink integration's {@code
 * io.openlineage.flink.api.LineageProvider}. Transforms implementing this interface are picked up
 * automatically during pipeline-graph traversal, without a dedicated visitor.
 */
public interface LineageProvider {

  /** Datasets this transform reads, in OpenLineage naming-convention form. */
  default List<DatasetIdentifier> getInputDatasets() {
    return Collections.emptyList();
  }

  /** Datasets this transform writes, in OpenLineage naming-convention form. */
  default List<DatasetIdentifier> getOutputDatasets() {
    return Collections.emptyList();
  }
}
