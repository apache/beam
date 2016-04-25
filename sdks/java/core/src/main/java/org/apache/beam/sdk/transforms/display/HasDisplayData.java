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

import org.apache.beam.sdk.transforms.PTransform;

/**
 * Marker interface for {@link PTransform PTransforms} and components used within
 * {@link PTransform PTransforms} to specify display data to be used within UIs and diagnostic
 * tools.
 *
 * <p>Display data is optional and may be collected during pipeline construction. It should
 * only be used to informational purposes. Tools and components should not assume that display data
 * will always be collected, or that collected display data will always be displayed.
 */
public interface HasDisplayData {
  /**
   * Register display data for the given transform or component. Metadata can be registered
   * directly on the provided builder, as well as via included sub-components.
   *
   * <pre>
   * {@code
   * @Override
   * public void populateDisplayData(DisplayData.Builder builder) {
   *  builder
   *     .include(subComponent)
   *     .add(DisplayData.item("minFilter", 42))
   *     .addIfNotDefault(DisplayData.item("useTransactions", this.txn), false)
   *     .add(DisplayData.item("topic", "projects/myproject/topics/mytopic")
   *       .withLabel("Pub/Sub Topic"))
   *     .add(DisplayData.item("serviceInstance", "myservice.com/fizzbang")
   *       .withLinkUrl("http://www.myservice.com/fizzbang"));
   * }
   * }
   * </pre>
   *
   * @param builder The builder to populate with display data.
   */
  void populateDisplayData(DisplayData.Builder builder);
}
