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
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link GeminiAIExample}. */
@RunWith(JUnit4.class)
public class GeminiAIExampleTest {
  private static final GeminiAIOptions OPTIONS =
      PipelineOptionsFactory.create().as(GeminiAIOptions.class);

  static {
    OPTIONS.setLocation("us-central1");
    if (OPTIONS.getProject() == null || OPTIONS.getProject().isEmpty()) {
      OPTIONS.setProject("apache-beam-testing");
    }
  }

  @Test
  public void testWhatIsThisImage() {
    List<String> urls =
        ImmutableList.of(
            "https://storage.googleapis.com/generativeai-downloads/images/cake.jpg",
            "https://storage.googleapis.com/generativeai-downloads/images/chocolate.png",
            "https://storage.googleapis.com/generativeai-downloads/images/croissant.jpg",
            "https://storage.googleapis.com/generativeai-downloads/images/dog_form.jpg",
            "https://storage.googleapis.com/generativeai-downloads/images/factory.png",
            "https://storage.googleapis.com/generativeai-downloads/images/scones.jpg");
    GeminiAIExample.whatIsThisImage(urls, OPTIONS);
  }
}
