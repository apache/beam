/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms.display;

import com.google.cloud.dataflow.sdk.transforms.PTransform;

/**
 * Marker interface for {@link PTransform PTransforms} and components used within
 * {@link PTransform PTransforms} to specify display metadata to be used within UIs and diagnostic
 * tools.
 *
 * <p>Display metadata is optional and may be collected during pipeline construction. It should
 * only be used to informational purposes. Tools and components should not assume that display data
 * will always be collected, or that collected display data will always be displayed.
 */
public interface HasDisplayData {
  /**
   * Register display metadata for the given transform or component. Metadata can be registered
   * directly on the provided builder, as well as via included sub-components.
   *
   * <pre>
   * {@code
   * @Override
   * public void populateDisplayData(DisplayData.Builder builder) {
   *  builder
   *     .include(subComponent)
   *     .add("minFilter", 42)
   *     .add("topic", "projects/myproject/topics/mytopic")
   *       .withLabel("Pub/Sub Topic")
   *     .add("serviceInstance", "myservice.com/fizzbang")
   *       .withLinkUrl("http://www.myservice.com/fizzbang");
   * }
   * }
   * </pre>
   *
   * @param builder The builder to populate with display metadata.
   */
  void populateDisplayData(DisplayData.Builder builder);
}
