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

package org.apache.beam.runners.core.construction;

import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.common.runner.v1.RunnerApi.ParDoPayload;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite;

/** Tests for {@link CreatePCollectionViewTranslation}. */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  CreatePCollectionViewTranslationTest.TestCreatePCollectionViewPayloadTranslation.class,
})
public class CreatePCollectionViewTranslationTest {

  /** Tests for translating various {@link ParDo} transforms to/from {@link ParDoPayload} protos. */
  @RunWith(Parameterized.class)
  public static class TestCreatePCollectionViewPayloadTranslation {

    // Two parameters suffices because the nature of the serialization/deserialization of
    // the view is not what is being tested; it is just important that the round trip
    // is not vacuous.
    @Parameters(name = "{index}: {0}")
    public static Iterable<CreatePCollectionView<?, ?>> data() {
      return ImmutableList.<CreatePCollectionView<?, ?>>of(
          CreatePCollectionView.of(
              PCollectionViews.singletonView(
                  testPCollection,
                  testPCollection.getWindowingStrategy(),
                  false,
                  null,
                  testPCollection.getCoder())),
          CreatePCollectionView.of(
              PCollectionViews.listView(
                  testPCollection,
                  testPCollection.getWindowingStrategy(),
                  testPCollection.getCoder())));
    }

    @Parameter(0)
    public CreatePCollectionView<?, ?> createViewTransform;

    public static TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    private static final PCollection<String> testPCollection = p.apply(Create.of("one"));

    @Test
    public void testEncodedProto() throws Exception {
      SdkComponents components = SdkComponents.create();
      components.registerPCollection(testPCollection);

      AppliedPTransform<?, ?, ?> appliedPTransform =
          AppliedPTransform.of(
              "foo",
              testPCollection.expand(),
              createViewTransform.getView().expand(),
              createViewTransform,
              p);

      FunctionSpec payload = PTransformTranslation.toProto(appliedPTransform, components).getSpec();

      // Checks that the payload is what it should be
      PCollectionView<?> deserializedView =
          (PCollectionView<?>)
              SerializableUtils.deserializeFromByteArray(
                  payload.getPayload().toByteArray(), PCollectionView.class.getSimpleName());

      assertThat(
          deserializedView, Matchers.<PCollectionView<?>>equalTo(createViewTransform.getView()));
    }

    @Test
    public void testExtractionDirectFromTransform() throws Exception {
      SdkComponents components = SdkComponents.create();
      components.registerPCollection(testPCollection);

      AppliedPTransform<?, ?, ?> appliedPTransform =
          AppliedPTransform.of(
              "foo",
              testPCollection.expand(),
              createViewTransform.getView().expand(),
              createViewTransform,
              p);

      CreatePCollectionViewTranslation.getView((AppliedPTransform) appliedPTransform);

      FunctionSpec payload = PTransformTranslation.toProto(appliedPTransform, components).getSpec();

      // Checks that the payload is what it should be
      PCollectionView<?> deserializedView =
          (PCollectionView<?>)
              SerializableUtils.deserializeFromByteArray(
                  payload.getPayload().toByteArray(),
                  PCollectionView.class.getSimpleName());

      assertThat(
          deserializedView, Matchers.<PCollectionView<?>>equalTo(createViewTransform.getView()));
    }
  }
}
