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
package org.apache.beam.sdk.util.construction;

import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PCollectionViews.TypeDescriptorSupplier;
import org.apache.beam.sdk.values.PCollectionViews.ValueOrMetadata;
import org.apache.beam.sdk.values.PCollectionViews.ValueOrMetadataCoder;
import org.apache.beam.sdk.values.PValues;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link CreatePCollectionViewTranslation}. */
@RunWith(Parameterized.class)
public class CreatePCollectionViewTranslationTest {
  // Two parameters suffices because the nature of the serialization/deserialization of
  // the view is not what is being tested; it is just important that the round trip
  // is not vacuous.
  @Parameters(name = "{index}: {0} {1}")
  public static Iterable<Object[]> data() {
    PCollection<String> singletonTestPCollection = p.apply(Create.of("one"));
    PCollection<KV<Long, ValueOrMetadata<String, OffsetRange>>> listTestPCollection =
        p.apply(
            Create.of(KV.of(0L, ValueOrMetadata.<String, OffsetRange>create("one")))
                .withCoder(
                    KvCoder.of(
                        BigEndianLongCoder.of(),
                        ValueOrMetadataCoder.create(
                            StringUtf8Coder.of(), OffsetRange.Coder.of()))));

    return ImmutableList.of(
        new Object[] {
          CreatePCollectionView.of(
              PCollectionViews.singletonView(
                  singletonTestPCollection,
                  (TypeDescriptorSupplier<String>) () -> TypeDescriptors.strings(),
                  singletonTestPCollection.getWindowingStrategy(),
                  false,
                  null,
                  StringUtf8Coder.of())),
          singletonTestPCollection
        },
        new Object[] {
          CreatePCollectionView.of(
              PCollectionViews.listViewWithRandomAccess(
                  listTestPCollection,
                  (TypeDescriptorSupplier<String>) () -> TypeDescriptors.strings(),
                  listTestPCollection.getWindowingStrategy())),
          listTestPCollection
        });
  }

  @Parameter(0)
  public CreatePCollectionView<?, ?> createViewTransform;

  @Parameter(1)
  public PCollection<?> testPCollection;

  public static TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void testEncodedProto() throws Exception {
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    components.registerPCollection(testPCollection);

    AppliedPTransform<?, ?, ?> appliedPTransform =
        AppliedPTransform.of(
            "foo",
            PValues.expandInput(testPCollection),
            PValues.expandOutput(createViewTransform.getView()),
            createViewTransform,
            ResourceHints.create(),
            p);

    FunctionSpec payload = PTransformTranslation.toProto(appliedPTransform, components).getSpec();

    // Checks that the payload is what it should be
    PCollectionView<?> deserializedView =
        (PCollectionView<?>)
            SerializableUtils.deserializeFromByteArray(
                payload.getPayload().toByteArray(), PCollectionView.class.getSimpleName());

    assertThat(deserializedView, Matchers.equalTo(createViewTransform.getView()));
  }

  @Test
  public void testExtractionDirectFromTransform() throws Exception {
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    components.registerPCollection(testPCollection);

    AppliedPTransform<?, ?, ?> appliedPTransform =
        AppliedPTransform.of(
            "foo",
            PValues.expandInput(testPCollection),
            PValues.expandOutput(createViewTransform.getView()),
            createViewTransform,
            ResourceHints.create(),
            p);

    CreatePCollectionViewTranslation.getView((AppliedPTransform) appliedPTransform);

    FunctionSpec payload = PTransformTranslation.toProto(appliedPTransform, components).getSpec();

    // Checks that the payload is what it should be
    PCollectionView<?> deserializedView =
        (PCollectionView<?>)
            SerializableUtils.deserializeFromByteArray(
                payload.getPayload().toByteArray(), PCollectionView.class.getSimpleName());

    assertThat(deserializedView, Matchers.equalTo(createViewTransform.getView()));
  }
}
