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
package org.apache.beam.examples.webapis;

import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link UsingHttpClientExample}. */
@RunWith(JUnit4.class)
public class UsingHttpClientExampleTest {
  @Test
  public void testReadFromGetEndpointExample() {
    Pipeline pipeline = Pipeline.create();

    List<String> urls =
        ImmutableList.of(
            "https://storage.googleapis.com/generativeai-downloads/images/cake.jpg",
            "https://storage.googleapis.com/generativeai-downloads/images/chocolate.png",
            "https://storage.googleapis.com/generativeai-downloads/images/croissant.jpg",
            "https://storage.googleapis.com/generativeai-downloads/images/dog_form.jpg",
            "https://storage.googleapis.com/generativeai-downloads/images/factory.png",
            "https://storage.googleapis.com/generativeai-downloads/images/scones.jpg");
    UsingHttpClientExample.readFromGetEndpointExample(urls, pipeline);

    pipeline.run();
  }
}
