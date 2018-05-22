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

package org.apache.beam.runners.direct.portable;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.SdkComponents;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.wire.WireCoders;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BundleFactoryOutputRecieverFactory}. */
@RunWith(JUnit4.class)
public class BundleFactoryOutputRecieverFactoryTest {
  private final BundleFactory bundleFactory = ImmutableListBundleFactory.create();
  private PCollectionNode fooPC;
  private PCollectionNode barPC;
  private RunnerApi.Components components;

  private OutputReceiverFactory factory;
  private Collection<UncommittedBundle<?>> outputBundles;

  @Before
  public void setup() throws IOException {
    Pipeline p = Pipeline.create();
    PCollection<String> foo =
        p.apply("createFoo", Create.of("1", "2", "3"))
            .apply("windowFoo", Window.into(FixedWindows.of(Duration.standardMinutes(5L))));
    PCollection<Integer> bar = p.apply("bar", Create.of(1, 2, 3));

    SdkComponents sdkComponents = SdkComponents.create();
    String fooId = sdkComponents.registerPCollection(foo);
    String barId = sdkComponents.registerPCollection(bar);
    components = sdkComponents.toComponents();

    fooPC = PipelineNode.pCollection(fooId, components.getPcollectionsOrThrow(fooId));
    barPC = PipelineNode.pCollection(barId, components.getPcollectionsOrThrow(barId));

    outputBundles = new ArrayList<>();
    factory =
        BundleFactoryOutputRecieverFactory.create(bundleFactory, components, outputBundles::add);
  }

  @Test
  public void addsBundlesToResult() {
    factory.create(fooPC.getId());
    factory.create(barPC.getId());

    assertThat(Iterables.size(outputBundles), equalTo(2));

    Collection<PCollectionNode> pcollections = new ArrayList<>();
    for (UncommittedBundle<?> bundle : outputBundles) {
      pcollections.add(bundle.getPCollection());
    }
    assertThat(pcollections, containsInAnyOrder(fooPC, barPC));
  }

  @Test
  public void receiverAddsElementsToBundle() throws Exception {
    FnDataReceiver<WindowedValue<byte[]>> receiver = factory.create(fooPC.getId());
    MessageWithComponents sdkWireCoder =
        WireCoders.createSdkWireCoder(fooPC, components, components::containsCoders);
    Coder<WindowedValue<String>> sdkCoder =
        (Coder<WindowedValue<String>>)
            CoderTranslation.fromProto(
                sdkWireCoder.getCoder(),
                RehydratedComponents.forComponents(sdkWireCoder.getComponents()));
    Coder<WindowedValue<byte[]>> runnerCoder =
        WireCoders.instantiateRunnerWireCoder(fooPC, components);

    WindowedValue<byte[]> firstElem =
        CoderUtils.decodeFromByteArray(
            runnerCoder,
            CoderUtils.encodeToByteArray(
                sdkCoder,
                WindowedValue.of(
                    "1",
                    new Instant(120),
                    new IntervalWindow(new Instant(0), Duration.standardMinutes(5)),
                    PaneInfo.NO_FIRING)));
    WindowedValue<byte[]> secondElem =
        CoderUtils.decodeFromByteArray(
            runnerCoder,
            CoderUtils.encodeToByteArray(
                sdkCoder,
                WindowedValue.of(
                    "2",
                    new Instant(240),
                    new IntervalWindow(new Instant(0), Duration.standardMinutes(5)),
                    PaneInfo.NO_FIRING)));
    receiver.accept(firstElem);
    receiver.accept(secondElem);

    CommittedBundle<?> output = getOnlyElement(outputBundles).commit(Instant.now());
    assertThat(output, containsInAnyOrder(firstElem, secondElem));
  }

  /**
   * Tests that if a {@link
   * org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode} is provided
   * multiple times, the returned {@link
   * org.apache.beam.runners.fnexecution.control.RemoteOutputReceiver} instances are independent.
   */
  @Test
  public void multipleInstancesOfPCollectionIndependent() throws Exception {
    FnDataReceiver<WindowedValue<byte[]>> firstReceiver = factory.create(fooPC.getId());
    FnDataReceiver<WindowedValue<byte[]>> secondReceiver = factory.create(fooPC.getId());
    MessageWithComponents sdkWireCoder =
        WireCoders.createSdkWireCoder(fooPC, components, components::containsCoders);
    Coder<WindowedValue<String>> sdkCoder =
        (Coder<WindowedValue<String>>)
            CoderTranslation.fromProto(
                sdkWireCoder.getCoder(),
                RehydratedComponents.forComponents(sdkWireCoder.getComponents()));
    Coder<WindowedValue<byte[]>> runnerCoder =
        WireCoders.instantiateRunnerWireCoder(fooPC, components);

    WindowedValue<byte[]> firstElem =
        CoderUtils.decodeFromByteArray(
            runnerCoder,
            CoderUtils.encodeToByteArray(
                sdkCoder,
                WindowedValue.of(
                    "1",
                    new Instant(120),
                    new IntervalWindow(new Instant(0), Duration.standardMinutes(5)),
                    PaneInfo.NO_FIRING)));
    firstReceiver.accept(firstElem);

    WindowedValue<byte[]> secondElem =
        CoderUtils.decodeFromByteArray(
            runnerCoder,
            CoderUtils.encodeToByteArray(
                sdkCoder,
                WindowedValue.of(
                    "2",
                    new Instant(240),
                    new IntervalWindow(new Instant(0), Duration.standardMinutes(5)),
                    PaneInfo.NO_FIRING)));
    secondReceiver.accept(secondElem);

    Collection<WindowedValue<?>> outputs = new ArrayList<>();
    for (UncommittedBundle<?> uncommitted : outputBundles) {
      assertThat(uncommitted.getPCollection(), equalTo(fooPC));
      Iterable<? extends WindowedValue<?>> elements =
          uncommitted.commit(Instant.now()).getElements();
      Iterables.addAll(outputs, elements);
      assertThat(Iterables.size(elements), equalTo(1));
    }
    assertThat(outputs, containsInAnyOrder(firstElem, secondElem));
  }

  @Test
  public void differentPCollectionsIndependent() throws Exception {
    FnDataReceiver<WindowedValue<byte[]>> fooReceiver = factory.create(fooPC.getId());
    MessageWithComponents fooSdkWireCoder =
        WireCoders.createSdkWireCoder(fooPC, components, components::containsCoders);
    Coder<WindowedValue<String>> fooSdkCoder =
        (Coder<WindowedValue<String>>)
            CoderTranslation.fromProto(
                fooSdkWireCoder.getCoder(),
                RehydratedComponents.forComponents(fooSdkWireCoder.getComponents()));
    Coder<WindowedValue<byte[]>> fooRunnerCoder =
        WireCoders.instantiateRunnerWireCoder(fooPC, components);

    FnDataReceiver<WindowedValue<byte[]>> barReceiver = factory.create(barPC.getId());
    MessageWithComponents barSdkWireCoder =
        WireCoders.createSdkWireCoder(barPC, components, components::containsCoders);
    Coder<WindowedValue<Integer>> barSdkCoder =
        (Coder<WindowedValue<Integer>>)
            CoderTranslation.fromProto(
                barSdkWireCoder.getCoder(),
                RehydratedComponents.forComponents(barSdkWireCoder.getComponents()));
    Coder<WindowedValue<byte[]>> barRunnerCoder =
        WireCoders.instantiateRunnerWireCoder(barPC, components);

    WindowedValue<byte[]> fooElem =
        CoderUtils.decodeFromByteArray(
            fooRunnerCoder,
            CoderUtils.encodeToByteArray(
                fooSdkCoder,
                WindowedValue.of(
                    "1",
                    new Instant(120),
                    new IntervalWindow(new Instant(0), Duration.standardMinutes(5)),
                    PaneInfo.NO_FIRING)));
    fooReceiver.accept(fooElem);

    WindowedValue<byte[]> barElem =
        CoderUtils.decodeFromByteArray(
            barRunnerCoder,
            CoderUtils.encodeToByteArray(
                barSdkCoder, WindowedValue.timestampedValueInGlobalWindow(2, new Instant(240))));
    barReceiver.accept(barElem);

    Collection<? super WindowedValue<?>> outputs = new ArrayList<>();
    for (UncommittedBundle<?> uncommitted : outputBundles) {
      WindowedValue<?> output = getOnlyElement(uncommitted.commit(Instant.now()).getElements());
      if (fooPC.equals(uncommitted.getPCollection())) {
        assertThat(output, equalTo(fooElem));
      } else if (barPC.equals(uncommitted.getPCollection())) {
        assertThat(output, equalTo(barElem));
      } else {
        fail(
            String.format(
                "Output %s should be either 'foo' or 'bar', got '%s",
                PCollection.class.getSimpleName(), uncommitted.getPCollection().getId()));
      }
      outputs.add(output);
    }
    assertThat(outputs, containsInAnyOrder(fooElem, barElem));
  }
}
