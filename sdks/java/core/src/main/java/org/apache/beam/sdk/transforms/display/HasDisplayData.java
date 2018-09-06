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
package org.apache.beam.sdk.transforms.display;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * Marker interface for {@link PTransform PTransforms} and components to specify display data used
 * within UIs and diagnostic tools.
 *
 * <p>Display data is registered by overriding {@link #populateDisplayData(DisplayData.Builder)} in
 * a component which implements {@code HasDisplayData}. Display data is available for {@link
 * PipelineOptions} and {@link PTransform} implementations.
 *
 * <pre><code>{@literal @Override}
 * public void populateDisplayData(DisplayData.Builder builder) {
 *  super.populateDisplayData(builder);
 *
 *  builder
 *     .include(subComponent)
 *     .add(DisplayData.item("minFilter", 42))
 *     .addIfNotDefault(DisplayData.item("useTransactions", this.txn), false)
 *     .add(DisplayData.item("topic", "projects/myproject/topics/mytopic")
 *       .withLabel("Pub/Sub Topic"))
 *     .add(DisplayData.item("serviceInstance", "myservice.com/fizzbang")
 *       .withLinkUrl("http://www.myservice.com/fizzbang"));
 * }
 * </code></pre>
 *
 * <p>Display data is optional and may be collected during pipeline construction. It should only be
 * used for informational purposes. Tools and components should not assume that display data will
 * always be collected, or that collected display data will always be displayed.
 *
 * @see #populateDisplayData(DisplayData.Builder)
 */
public interface HasDisplayData {
  /**
   * Register display data for the given transform or component.
   *
   * <p>{@code populateDisplayData(DisplayData.Builder)} is invoked by Pipeline runners to collect
   * display data via {@link DisplayData#from(HasDisplayData)}. Implementations may call {@code
   * super.populateDisplayData(builder)} in order to register display data in the current namespace,
   * but should otherwise use {@code subcomponent.populateDisplayData(builder)} to use the namespace
   * of the subcomponent.
   *
   * @param builder The builder to populate with display data.
   * @see HasDisplayData
   */
  void populateDisplayData(DisplayData.Builder builder);
}
