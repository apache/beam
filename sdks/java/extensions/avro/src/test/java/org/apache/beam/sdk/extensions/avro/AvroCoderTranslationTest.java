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
package org.apache.beam.sdk.extensions.avro;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.avro.SchemaBuilder;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link AvroCoder} translation. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
@RunWith(JUnit4.class)
public class AvroCoderTranslationTest {
  /** Tests round-trip coder encodings for both known and unknown {@link Coder coders}. */
  @Test
  public void toAndFromProto() throws Exception {
    Coder<?> coder = AvroCoder.of(SchemaBuilder.record("record").fields().endRecord());
    SdkComponents sdkComponents = SdkComponents.create();
    sdkComponents.registerEnvironment(Environments.createDockerEnvironment("java"));
    RunnerApi.Coder coderProto = CoderTranslation.toProto(coder, sdkComponents);

    Components encodedComponents = sdkComponents.toComponents();
    Coder<?> decodedCoder =
        CoderTranslation.fromProto(
            coderProto,
            RehydratedComponents.forComponents(encodedComponents),
            CoderTranslation.TranslationContext.DEFAULT);
    assertThat(decodedCoder, equalTo(coder));
  }
}
