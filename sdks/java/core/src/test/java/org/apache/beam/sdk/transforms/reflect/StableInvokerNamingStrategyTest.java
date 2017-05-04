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
package org.apache.beam.sdk.transforms.reflect;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import net.bytebuddy.NamingStrategy;
import net.bytebuddy.description.type.TypeDescription;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link StableInvokerNamingStrategy}. */
@RunWith(JUnit4.class)
public class StableInvokerNamingStrategyTest {

  private class FooFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void process() {}
  }

  @Test
  public void testStableName() {
    NamingStrategy strategy = StableInvokerNamingStrategy.forDoFnClass(FooFn.class);

    String name1 =
        strategy.subclass(
            new TypeDescription.Generic.OfNonGenericType.ForLoadedType(DoFnInvoker.class));

    String name2 =
        strategy.subclass(
            new TypeDescription.Generic.OfNonGenericType.ForLoadedType(DoFnInvoker.class));

    assertThat(name1, equalTo(name2));
  }

  @Test
  public void testDifferentSuffixes() {
    NamingStrategy strategy1 = StableInvokerNamingStrategy.forDoFnClass(FooFn.class);
    NamingStrategy strategy2 = StableInvokerNamingStrategy.forDoFnClass(FooFn.class)
        .withSuffix("OnTimerInvoker$timerId1$hash");
    NamingStrategy strategy3 = StableInvokerNamingStrategy.forDoFnClass(FooFn.class)
        .withSuffix("OnTimerInvoker$timerId2$hash");

    TypeDescription.Generic doFnInvokerType =
        new TypeDescription.Generic.OfNonGenericType.ForLoadedType(DoFnInvoker.class);

    TypeDescription.Generic onTimerInvokerType =
        new TypeDescription.Generic.OfNonGenericType.ForLoadedType(OnTimerInvoker.class);

    String name1 = strategy1.subclass(doFnInvokerType);
    String name2 = strategy2.subclass(onTimerInvokerType);
    String name3 = strategy3.subclass(onTimerInvokerType);

    assertThat(name1, not(equalTo(name2)));
    assertThat(name1, not(equalTo(name3)));
    assertThat(name2, not(equalTo(name3)));
  }
}
