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
package org.apache.beam.runners.direct.portable;

import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.local.StructuralKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * A factory that creates {@link UncommittedBundle UncommittedBundles}.
 */
interface BundleFactory {
  /**
   * Create an {@link UncommittedBundle} from an empty input. Elements added to the bundle do not
   * belong to a {@link PCollection}.
   *
   * <p>For use in creating inputs to root transforms.
   */
  <T> UncommittedBundle<T> createRootBundle();

  /**
   * Create an {@link UncommittedBundle} from the specified input. Elements added to the bundle
   * belong to the {@code output} {@link PCollection}.
   */
  <T> UncommittedBundle<T> createBundle(PCollectionNode output);

  /**
   * Create an {@link UncommittedBundle} with the specified keys at the specified step. For use by
   * {@code DirectGroupByKeyOnly} {@link PTransform PTransforms}. Elements added to the bundle
   * belong to the {@code output} {@link PCollection}.
   */
  <K, T> UncommittedBundle<T> createKeyedBundle(StructuralKey<K> key, PCollectionNode output);
}
