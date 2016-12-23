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
package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link ForwardingPTransform}.
 */
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
  public void validateDelegates() {
    @SuppressWarnings("unchecked")
    PCollection<Integer> input = mock(PCollection.class);
    doThrow(RuntimeException.class).when(delegate).validate(input);

    thrown.expect(RuntimeException.class);
    forwarding.validate(input);
  }

  @Test
  public void getDefaultOutputCoderDelegates() throws Exception {
    @SuppressWarnings("unchecked")
    PCollection<Integer> input = mock(PCollection.class);
    @SuppressWarnings("unchecked")
    PCollection<String> output = mock(PCollection.class);
    @SuppressWarnings("unchecked")
    Coder<String> outputCoder = mock(Coder.class);

    when(delegate.getDefaultOutputCoder(input, output)).thenReturn(outputCoder);
    assertThat(forwarding.getDefaultOutputCoder(input, output), equalTo(outputCoder));
  }

  @Test
  public void populateDisplayDataDelegates() {
    doThrow(RuntimeException.class)
        .when(delegate).populateDisplayData(any(DisplayData.Builder.class));

    thrown.expect(RuntimeException.class);
    DisplayData.from(forwarding);
  }
}
