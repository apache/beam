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

package com.google.cloud.dataflow.sdk.transforms;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link DoFn.Context}.
 */
@RunWith(JUnit4.class)
public class DoFnContextTest {

  @Mock
  private Aggregator<Long, Long> agg;

  private DoFn<Object, Object> fn;
  private DoFn<Object, Object>.Context context;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    // Need to be real objects to call the constructor, and to reference the
    // outer instance of DoFn
    NoOpDoFn<Object, Object> noOpFn = new NoOpDoFn<>();
    DoFn<Object, Object>.Context noOpContext = noOpFn.context();

    fn = spy(noOpFn);
    context = spy(noOpContext);
  }

  @Test
  public void testSetupDelegateAggregatorsCreatesAndLinksDelegateAggregators() {
    Sum.SumLongFn combiner = new Sum.SumLongFn();
    Aggregator<Long, Long> delegateAggregator =
        fn.createAggregator("test", combiner);

    when(context.createAggregatorInternal("test", combiner)).thenReturn(agg);

    context.setupDelegateAggregators();
    delegateAggregator.addValue(1L);

    verify(agg).addValue(1L);
  }
}
