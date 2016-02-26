/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.testing;

/**
 * Category tag for tests that can be run on the
 * {@link com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner} if the
 * {@code runIntegrationTestOnService} System property is set to true.
 * Example usage:
 * <pre><code>
 *     {@literal @}Test
 *     {@literal @}Category(RunnableOnService.class)
 *     public void testParDo() {...
 * </code></pre>
 */
public interface RunnableOnService {}
