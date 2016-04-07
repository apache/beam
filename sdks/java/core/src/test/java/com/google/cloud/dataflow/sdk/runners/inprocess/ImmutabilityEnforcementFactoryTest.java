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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.IllegalMutationException;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Collections;

/**
 * Tests for {@link ImmutabilityEnforcementFactory}.
 */
@RunWith(JUnit4.class)
public class ImmutabilityEnforcementFactoryTest implements Serializable {
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  private transient ImmutabilityEnforcementFactory factory;
  private transient PCollection<byte[]> pcollection;
  private transient AppliedPTransform<?, ?, ?> consumer;

  @Before
  public void setup() {
    factory = new ImmutabilityEnforcementFactory();
    TestPipeline p = TestPipeline.create();
    pcollection =
        p.apply(Create.of("foo".getBytes(), "spamhameggs".getBytes()))
            .apply(
                ParDo.of(
                    new DoFn<byte[], byte[]>() {
                      @Override
                      public void processElement(DoFn<byte[], byte[]>.ProcessContext c)
                          throws Exception {
                        c.element()[0] = 'b';
                      }
                    }));
    consumer = pcollection.apply(Count.<byte[]>globally()).getProducingTransformInternal();
  }

  @Test
  public void unchangedSucceeds() {
    WindowedValue<byte[]> element = WindowedValue.valueInGlobalWindow("bar".getBytes());
    CommittedBundle<byte[]> elements =
        InProcessBundle.unkeyed(pcollection).add(element).commit(Instant.now());

    ModelEnforcement<byte[]> enforcement = factory.forBundle(elements, consumer);
    enforcement.beforeElement(element);
    enforcement.afterElement(element);
    enforcement.afterFinish(
        elements,
        StepTransformResult.withoutHold(consumer).build(),
        Collections.<CommittedBundle<?>>emptyList());
  }

  @Test
  public void mutatedDuringProcessElementThrows() {
    WindowedValue<byte[]> element = WindowedValue.valueInGlobalWindow("bar".getBytes());
    CommittedBundle<byte[]> elements =
        InProcessBundle.unkeyed(pcollection).add(element).commit(Instant.now());

    ModelEnforcement<byte[]> enforcement = factory.forBundle(elements, consumer);
    enforcement.beforeElement(element);
    element.getValue()[0] = 'f';
    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage(consumer.getFullName());
    thrown.expectMessage("illegaly mutated");
    thrown.expectMessage("Input values must not be mutated");
    enforcement.afterElement(element);
    enforcement.afterFinish(
        elements,
        StepTransformResult.withoutHold(consumer).build(),
        Collections.<CommittedBundle<?>>emptyList());
  }

  @Test
  public void mutatedAfterProcessElementFails() {

    WindowedValue<byte[]> element = WindowedValue.valueInGlobalWindow("bar".getBytes());
    CommittedBundle<byte[]> elements =
        InProcessBundle.unkeyed(pcollection).add(element).commit(Instant.now());

    ModelEnforcement<byte[]> enforcement = factory.forBundle(elements, consumer);
    enforcement.beforeElement(element);
    enforcement.afterElement(element);

    element.getValue()[0] = 'f';
    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage(consumer.getFullName());
    thrown.expectMessage("illegaly mutated");
    thrown.expectMessage("Input values must not be mutated");
    enforcement.afterFinish(
        elements,
        StepTransformResult.withoutHold(consumer).build(),
        Collections.<CommittedBundle<?>>emptyList());
  }
}

