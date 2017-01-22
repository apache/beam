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
package org.apache.beam.runners.core;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Sum;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link OldDoFn.Context}.
 */
@RunWith(JUnit4.class)
public class OldDoFnContextTest {

  @Mock
  private Aggregator<Long, Long> agg;

  private OldDoFn<Object, Object> fn;
  private OldDoFn<Object, Object>.Context context;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    // Need to be real objects to call the constructor, and to reference the
    // outer instance of OldDoFn
    NoOpOldDoFn<Object, Object> noOpFn = new NoOpOldDoFn<>();
    OldDoFn<Object, Object>.Context noOpContext = noOpFn.context();

    fn = spy(noOpFn);
    context = spy(noOpContext);
  }

  @Test
  public void testSetupDelegateAggregatorsCreatesAndLinksDelegateAggregators() {
    Combine.BinaryCombineLongFn combiner = Sum.ofLongs();
    Aggregator<Long, Long> delegateAggregator =
        fn.createAggregator("test", combiner);

    when(context.createAggregatorInternal("test", combiner)).thenReturn(agg);

    context.setupDelegateAggregators();
    delegateAggregator.addValue(1L);

    verify(agg).addValue(1L);
  }
}
