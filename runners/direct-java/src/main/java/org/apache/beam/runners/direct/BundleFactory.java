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
package org.apache.beam.runners.direct;

import org.apache.beam.runners.direct.InProcessGroupByKey.InProcessGroupByKeyOnly;
import org.apache.beam.runners.direct.InProcessPipelineRunner.CommittedBundle;
import org.apache.beam.runners.direct.InProcessPipelineRunner.UncommittedBundle;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * A factory that creates {@link UncommittedBundle UncommittedBundles}.
 */
public interface BundleFactory {
  /**
   * Create an {@link UncommittedBundle} from an empty input. Elements added to the bundle belong to
   * the {@code output} {@link PCollection}.
   */
  public <T> UncommittedBundle<T> createRootBundle(PCollection<T> output);

  /**
   * Create an {@link UncommittedBundle} from the specified input. Elements added to the bundle
   * belong to the {@code output} {@link PCollection}.
   */
  public <T> UncommittedBundle<T> createBundle(CommittedBundle<?> input, PCollection<T> output);

  /**
   * Create an {@link UncommittedBundle} with the specified keys at the specified step. For use by
   * {@link InProcessGroupByKeyOnly} {@link PTransform PTransforms}. Elements added to the bundle
   * belong to the {@code output} {@link PCollection}.
   */
  public <K, T> UncommittedBundle<T> createKeyedBundle(
      CommittedBundle<?> input, StructuralKey<K> key, PCollection<T> output);
}
