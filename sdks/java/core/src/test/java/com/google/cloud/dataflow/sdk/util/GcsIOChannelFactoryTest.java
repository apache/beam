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

package com.google.cloud.dataflow.sdk.util;

import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GcsIOChannelFactoryTest}. */
@RunWith(JUnit4.class)
public class GcsIOChannelFactoryTest {
  private GcsIOChannelFactory factory;

  @Before
  public void setUp() {
    factory = new GcsIOChannelFactory(PipelineOptionsFactory.as(GcsOptions.class));
  }

  @Test
  public void testResolve() throws Exception {
    assertEquals("gs://bucket/object", factory.resolve("gs://bucket", "object"));
  }
}
