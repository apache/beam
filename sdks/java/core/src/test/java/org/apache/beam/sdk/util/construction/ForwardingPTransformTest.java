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
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link ForwardingPTransform}. */
@RunWith(JUnit4.class)
public class ForwardingPTransformTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private PTransform<PCollection<Integer>, PCollection<String>> delegate;

  private ForwardingPTransform<PCollection<Integer>, PCollection<String>> forwarding;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    forwarding =
        new ForwardingPTransform<PCollection<Integer>, PCollection<String>>() {
          @Override
          protected PTransform<PCollection<Integer>, PCollection<String>> delegate() {
            return delegate;
          }
        };
  }

  @Test
  public void applyDelegates() {
    @SuppressWarnings("unchecked")
    PCollection<Integer> collection = mock(PCollection.class);
    @SuppressWarnings("unchecked")
    PCollection<String> output = mock(PCollection.class);
    when(delegate.expand(collection)).thenReturn(output);
    PCollection<String> result = forwarding.expand(collection);
    assertThat(result, equalTo(output));
  }

  @Test
  public void getNameDelegates() {
    String name = "My_forwardingptransform-name;for!thisTest";
    when(delegate.getName()).thenReturn(name);
    assertThat(forwarding.getName(), equalTo(name));
  }

  @Test
  public void getAdditionalInputsDelegates() {
    Map<TupleTag<?>, PValue> additionalInputs =
        ImmutableMap.of(new TupleTag<>("test_tag"), Pipeline.create().apply(Create.of("1")));
    when(delegate.getAdditionalInputs()).thenReturn(additionalInputs);
    assertThat(forwarding.getAdditionalInputs(), equalTo(additionalInputs));
  }

  @Test
  public void validateDelegates() {
    @SuppressWarnings("unchecked")
    PipelineOptions options = mock(PipelineOptions.class);
    doThrow(RuntimeException.class).when(delegate).validate(options);

    thrown.expect(RuntimeException.class);
    forwarding.validate(options);
  }

  @Test
  public void getDefaultOutputCoderDelegates() throws Exception {
    @SuppressWarnings("unchecked")
    PCollection<Integer> input =
        PCollection.createPrimitiveOutputInternal(
            null /* pipeline */,
            WindowingStrategy.globalDefault(),
            PCollection.IsBounded.BOUNDED,
            null /* coder */);
    @SuppressWarnings("unchecked")
    PCollection<String> output =
        PCollection.createPrimitiveOutputInternal(
            null /* pipeline */,
            WindowingStrategy.globalDefault(),
            PCollection.IsBounded.BOUNDED,
            null /* coder */);
    @SuppressWarnings("unchecked")
    Coder<String> outputCoder = mock(Coder.class);

    when(delegate.expand(input)).thenReturn(output);
    when(delegate.getDefaultOutputCoder(input, output)).thenReturn(outputCoder);
    assertThat(forwarding.expand(input).getCoder(), equalTo(outputCoder));
  }

  @Test
  public void populateDisplayDataDelegates() {
    doThrow(RuntimeException.class)
        .when(delegate)
        .populateDisplayData(any(DisplayData.Builder.class));

    thrown.expect(RuntimeException.class);
    DisplayData.from(forwarding);
  }
}
