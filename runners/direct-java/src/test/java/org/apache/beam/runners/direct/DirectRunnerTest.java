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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.direct.DirectRunner.DirectPipelineResult;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.IllegalMutationException;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for basic {@link DirectRunner} functionality. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class DirectRunnerTest implements Serializable {
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private Pipeline getPipeline() {
    return getPipeline(true);
  }

  private Pipeline getPipeline(boolean blockOnRun) {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(DirectRunner.class);
    opts.as(DirectOptions.class).setBlockOnRun(blockOnRun);
    return Pipeline.create(opts);
  }

  @Test
  public void defaultRunnerLoaded() {
    assertThat(
        DirectRunner.class,
        Matchers.<Class<? extends PipelineRunner>>equalTo(
            PipelineOptionsFactory.create().getRunner()));
  }

  @Test
  public void wordCountShouldSucceed() throws Throwable {
    Pipeline p = getPipeline();

    PCollection<KV<String, Long>> counts =
        p.apply(Create.of("foo", "bar", "foo", "baz", "bar", "foo"))
            .apply(
                MapElements.via(
                    new SimpleFunction<String, String>() {
                      @Override
                      public String apply(String input) {
                        return input;
                      }
                    }))
            .apply(Count.perElement());
    PCollection<String> countStrs =
        counts.apply(
            MapElements.via(
                new SimpleFunction<KV<String, Long>, String>() {
                  @Override
                  public String apply(KV<String, Long> input) {
                    return String.format("%s: %s", input.getKey(), input.getValue());
                  }
                }));

    PAssert.that(countStrs).containsInAnyOrder("baz: 1", "bar: 2", "foo: 3");

    DirectPipelineResult result = (DirectPipelineResult) p.run();
    result.waitUntilFinish();
  }

  private static AtomicInteger changed;

  @Test
  public void reusePipelineSucceeds() throws Throwable {
    Pipeline p = getPipeline();

    changed = new AtomicInteger(0);
    PCollection<KV<String, Long>> counts =
        p.apply(Create.of("foo", "bar", "foo", "baz", "bar", "foo"))
            .apply(
                MapElements.via(
                    new SimpleFunction<String, String>() {
                      @Override
                      public String apply(String input) {
                        return input;
                      }
                    }))
            .apply(Count.perElement());
    PCollection<String> countStrs =
        counts.apply(
            MapElements.via(
                new SimpleFunction<KV<String, Long>, String>() {
                  @Override
                  public String apply(KV<String, Long> input) {
                    return String.format("%s: %s", input.getKey(), input.getValue());
                  }
                }));

    counts.apply(
        ParDo.of(
            new DoFn<KV<String, Long>, Void>() {
              @ProcessElement
              public void updateChanged(ProcessContext c) {
                changed.getAndIncrement();
              }
            }));

    PAssert.that(countStrs).containsInAnyOrder("baz: 1", "bar: 2", "foo: 3");

    DirectPipelineResult result = (DirectPipelineResult) p.run();
    result.waitUntilFinish();

    DirectPipelineResult otherResult = (DirectPipelineResult) p.run();
    otherResult.waitUntilFinish();

    assertThat("Each element should have been processed twice", changed.get(), equalTo(6));
  }

  @Test
  public void byteArrayCountShouldSucceed() {
    Pipeline p = getPipeline();

    SerializableFunction<Integer, byte[]> getBytes =
        input -> {
          try {
            return CoderUtils.encodeToByteArray(VarIntCoder.of(), input);
          } catch (CoderException e) {
            fail("Unexpected Coder Exception " + e);
            throw new AssertionError("Unreachable");
          }
        };
    TypeDescriptor<byte[]> td = new TypeDescriptor<byte[]>() {};
    PCollection<byte[]> foos =
        p.apply(Create.of(1, 1, 1, 2, 2, 3)).apply(MapElements.into(td).via(getBytes));
    PCollection<byte[]> msync =
        p.apply(Create.of(1, -2, -8, -16)).apply(MapElements.into(td).via(getBytes));
    PCollection<byte[]> bytes = PCollectionList.of(foos).and(msync).apply(Flatten.pCollections());
    PCollection<KV<byte[], Long>> counts = bytes.apply(Count.perElement());
    PCollection<KV<Integer, Long>> countsBackToString =
        counts.apply(
            MapElements.via(
                new SimpleFunction<KV<byte[], Long>, KV<Integer, Long>>() {
                  @Override
                  public KV<Integer, Long> apply(KV<byte[], Long> input) {
                    try {
                      return KV.of(
                          CoderUtils.decodeFromByteArray(VarIntCoder.of(), input.getKey()),
                          input.getValue());
                    } catch (CoderException e) {
                      fail("Unexpected Coder Exception " + e);
                      throw new AssertionError("Unreachable");
                    }
                  }
                }));

    Map<Integer, Long> expected =
        ImmutableMap.<Integer, Long>builder()
            .put(1, 4L)
            .put(2, 2L)
            .put(3, 1L)
            .put(-2, 1L)
            .put(-8, 1L)
            .put(-16, 1L)
            .build();
    PAssert.thatMap(countsBackToString).isEqualTo(expected);
  }

  @Test
  public void splitsInputs() {
    Pipeline p = getPipeline();
    PCollection<Long> longs = p.apply(Read.from(MustSplitSource.of(CountingSource.upTo(3))));

    PAssert.that(longs).containsInAnyOrder(0L, 1L, 2L);
    p.run();
  }

  @Test
  public void cancelShouldStopPipeline() throws Exception {
    PipelineOptions opts = TestPipeline.testingPipelineOptions();
    opts.as(DirectOptions.class).setBlockOnRun(false);
    opts.setRunner(DirectRunner.class);

    final Pipeline p = Pipeline.create(opts);
    p.apply(GenerateSequence.from(0).withRate(1L, Duration.standardSeconds(1)));

    final BlockingQueue<PipelineResult> resultExchange = new ArrayBlockingQueue<>(1);
    Runnable cancelRunnable =
        () -> {
          try {
            resultExchange.take().cancel();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          } catch (IOException e) {
            throw new IllegalStateException(e);
          }
        };

    Callable<PipelineResult> runPipelineRunnable =
        () -> {
          PipelineResult res = p.run();
          try {
            resultExchange.put(res);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
          return res;
        };

    ExecutorService executor = Executors.newCachedThreadPool();
    Future<?> cancelResult = executor.submit(cancelRunnable);
    Future<PipelineResult> result = executor.submit(runPipelineRunnable);

    cancelResult.get();
    // If cancel doesn't work, this will hang forever
    result.get().waitUntilFinish();
  }

  @Test
  public void testWaitUntilFinishTimeout() throws Exception {
    DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
    options.setBlockOnRun(false);
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);
    p.apply(Create.of(1L))
        .apply(
            ParDo.of(
                new DoFn<Long, Long>() {
                  @ProcessElement
                  public void hang(ProcessContext context) throws InterruptedException {
                    // Hangs "forever"
                    Thread.sleep(Long.MAX_VALUE);
                  }
                }));
    PipelineResult result = p.run();
    // The pipeline should never complete;
    assertThat(result.getState(), is(State.RUNNING));
    // Must time out, otherwise this test will never complete
    assertEquals(null, result.waitUntilFinish(Duration.millis(1L)));
    // Ensure multiple calls complete
    assertEquals(null, result.waitUntilFinish(Duration.millis(1L)));
  }

  @Test
  public void testNoBlockOnRunState() {

    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
    final Future handler =
        executor.submit(
            () -> {
              PipelineOptions options =
                  PipelineOptionsFactory.fromArgs("--blockOnRun=false").create();
              Pipeline pipeline = Pipeline.create(options);
              pipeline.apply(GenerateSequence.from(0).to(100));

              PipelineResult result = pipeline.run();
              while (true) {
                if (result.getState() == State.DONE) {
                  return result;
                }
                Thread.sleep(100);
              }
            });
    try {
      handler.get(10, TimeUnit.SECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      // timeout means it never reaches DONE state
      throw new RuntimeException(e);
    } finally {
      executor.shutdownNow();
    }
  }

  private static final AtomicLong TEARDOWN_CALL = new AtomicLong(-1);

  @Test
  public void tearsDownFnsBeforeFinishing() {
    TEARDOWN_CALL.set(-1);
    final Pipeline pipeline = getPipeline();
    pipeline
        .apply(Create.of("a"))
        .apply(
            ParDo.of(
                new DoFn<String, String>() {
                  @ProcessElement
                  public void onElement(final ProcessContext ctx) {
                    // no-op
                  }

                  @Teardown
                  public void teardown() {
                    // just to not have a fast execution hiding an issue until we have a shutdown
                    // callback
                    try {
                      Thread.sleep(1000);
                    } catch (final InterruptedException e) {
                      throw new AssertionError(e);
                    }
                    TEARDOWN_CALL.set(System.nanoTime());
                  }
                }));
    final PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();

    final long doneTs = System.nanoTime();
    final long tearDownTs = TEARDOWN_CALL.get();
    assertThat(tearDownTs, greaterThan(0L));
    assertThat(doneTs, greaterThan(tearDownTs));
  }

  @Test
  public void transformDisplayDataExceptionShouldFail() {
    DoFn<Integer, Integer> brokenDoFn =
        new DoFn<Integer, Integer>() {
          @ProcessElement
          public void processElement(ProcessContext c) throws Exception {}

          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            throw new RuntimeException("oh noes!");
          }
        };

    Pipeline p = getPipeline();
    p.apply(Create.of(1, 2, 3)).apply(ParDo.of(brokenDoFn));

    thrown.expectMessage(brokenDoFn.getClass().getName());
    thrown.expectCause(ThrowableMessageMatcher.hasMessage(is("oh noes!")));
    p.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an output with a good equals() fails in the {@link
   * DirectRunner}.
   */
  @Test
  public void testMutatingOutputThenOutputDoFnError() throws Exception {
    Pipeline pipeline = getPipeline();

    pipeline
        .apply(Create.of(42))
        .apply(
            ParDo.of(
                new DoFn<Integer, List<Integer>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    List<Integer> outputList = Arrays.asList(1, 2, 3, 4);
                    c.output(outputList);
                    outputList.set(0, 37);
                    c.output(outputList);
                  }
                }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("output");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an output with a good equals() fails in the {@link
   * DirectRunner}.
   */
  @Test
  public void testMutatingOutputWithEnforcementDisabledSucceeds() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    options.as(DirectOptions.class).setEnforceImmutability(false);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(Create.of(42))
        .apply(
            ParDo.of(
                new DoFn<Integer, List<Integer>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    List<Integer> outputList = Arrays.asList(1, 2, 3, 4);
                    c.output(outputList);
                    outputList.set(0, 37);
                    c.output(outputList);
                  }
                }));

    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an output with a good equals() fails in the {@link
   * DirectRunner}.
   */
  @Test
  public void testMutatingOutputThenTerminateDoFnError() throws Exception {
    Pipeline pipeline = getPipeline();

    pipeline
        .apply(Create.of(42))
        .apply(
            ParDo.of(
                new DoFn<Integer, List<Integer>>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    List<Integer> outputList = Arrays.asList(1, 2, 3, 4);
                    c.output(outputList);
                    outputList.set(0, 37);
                  }
                }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("output");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an output with a bad equals() still fails in the {@link
   * DirectRunner}.
   */
  @Test
  public void testMutatingOutputCoderDoFnError() throws Exception {
    Pipeline pipeline = getPipeline();

    pipeline
        .apply(Create.of(42))
        .apply(
            ParDo.of(
                new DoFn<Integer, byte[]>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    byte[] outputArray = new byte[] {0x1, 0x2, 0x3};
                    c.output(outputArray);
                    outputArray[0] = 0xa;
                    c.output(outputArray);
                  }
                }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("output");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates its input with a good equals() fails in the {@link
   * DirectRunner}.
   */
  @Test
  public void testMutatingInputDoFnError() throws Exception {
    Pipeline pipeline = getPipeline();

    pipeline
        .apply(
            Create.of(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6))
                .withCoder(ListCoder.of(VarIntCoder.of())))
        .apply(
            ParDo.of(
                new DoFn<List<Integer>, Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    List<Integer> inputList = c.element();
                    inputList.set(0, 37);
                    c.output(12);
                  }
                }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("Input");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an input with a bad equals() still fails in the {@link
   * DirectRunner}.
   */
  @Test
  public void testMutatingInputCoderDoFnError() throws Exception {
    Pipeline pipeline = getPipeline();

    pipeline
        .apply(Create.of(new byte[] {0x1, 0x2, 0x3}, new byte[] {0x4, 0x5, 0x6}))
        .apply(
            ParDo.of(
                new DoFn<byte[], Integer>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    byte[] inputArray = c.element();
                    inputArray[0] = 0xa;
                    c.output(13);
                  }
                }));

    thrown.expect(IllegalMutationException.class);
    thrown.expectMessage("Input");
    thrown.expectMessage("must not be mutated");
    pipeline.run();
  }

  @Test
  public void testUnencodableOutputElement() throws Exception {
    Pipeline p = getPipeline();
    PCollection<Long> pcollection =
        p.apply(Create.of((Void) null))
            .apply(
                ParDo.of(
                    new DoFn<Void, Long>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        c.output(null);
                      }
                    }))
            .setCoder(VarLongCoder.of());
    pcollection.apply(
        ParDo.of(
            new DoFn<Long, Long>() {
              @ProcessElement
              public void unreachable(ProcessContext c) {
                fail("Pipeline should fail to encode a null Long in VarLongCoder");
              }
            }));

    thrown.expectCause(isA(CoderException.class));
    thrown.expectMessage("cannot encode a null Long");
    p.run();
  }

  @Test
  public void testUnencodableOutputFromBoundedRead() throws Exception {
    Pipeline p = getPipeline();
    p.apply(GenerateSequence.from(0).to(10)).setCoder(new LongNoDecodeCoder());

    thrown.expectCause(isA(CoderException.class));
    thrown.expectMessage("Cannot decode a long");
    p.run();
  }

  @Test
  public void testUnencodableOutputFromUnboundedRead() {
    Pipeline p = getPipeline();
    p.apply(GenerateSequence.from(0)).setCoder(new LongNoDecodeCoder());

    thrown.expectMessage("Cannot decode a long");
    p.run();
  }

  /**
   * Tests that {@link DirectRunner#fromOptions(PipelineOptions)} drops {@link PipelineOptions}
   * marked with {@link JsonIgnore} fields.
   */
  @Test
  public void testFromOptionsIfIgnoredFieldsGettingDropped() {
    TestSerializationOfOptions options =
        PipelineOptionsFactory.fromArgs(
                "--foo=testValue", "--ignoredField=overridden", "--runner=DirectRunner")
            .as(TestSerializationOfOptions.class);

    assertEquals("testValue", options.getFoo());
    assertEquals("overridden", options.getIgnoredField());
    Pipeline p = Pipeline.create(options);
    PCollection<Integer> pc =
        p.apply(Create.of("1"))
            .apply(
                ParDo.of(
                    new DoFn<String, Integer>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        TestSerializationOfOptions options =
                            c.getPipelineOptions().as(TestSerializationOfOptions.class);
                        assertEquals("testValue", options.getFoo());
                        assertEquals("not overridden", options.getIgnoredField());
                        c.output(Integer.parseInt(c.element()));
                      }
                    }));
    PAssert.that(pc).containsInAnyOrder(1);
    p.run();
  }

  /**
   * Test running of {@link Pipeline} which has two {@link POutput POutputs} and finishing the first
   * one triggers data being fed into the second one.
   */
  @Test(timeout = 10000)
  public void testTwoPOutputsInPipelineWithCascade() throws InterruptedException {

    StaticQueue<Integer> start = StaticQueue.of("start", VarIntCoder.of());
    StaticQueue<Integer> messages = StaticQueue.of("messages", VarIntCoder.of());

    Pipeline pipeline = getPipeline(false);
    pipeline.begin().apply("outputStartSignal", outputStartTo(start));
    PCollection<Integer> result =
        pipeline
            .apply("processMessages", messages.read())
            .apply(
                Window.<Integer>into(new GlobalWindows())
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO))
            .apply(Sum.integersGlobally());

    // the result should be 6, after the data will have been written
    PAssert.that(result).containsInAnyOrder(6);

    PipelineResult run = pipeline.run();

    // wait until a message has been written to the start queue
    while (start.take() == null) {}

    // and publish messages
    messages.add(1).add(2).add(3).terminate();

    run.waitUntilFinish();
  }

  private PTransform<PBegin, PDone> outputStartTo(StaticQueue<Integer> queue) {
    return new PTransform<PBegin, PDone>() {
      @Override
      public PDone expand(PBegin input) {
        input
            .apply(Create.of(1))
            .apply(
                MapElements.into(TypeDescriptors.voids())
                    .via(
                        in -> {
                          queue.add(in);
                          return null;
                        }));
        return PDone.in(input.getPipeline());
      }
    };
  }

  /**
   * Options for testing if {@link DirectRunner} drops {@link PipelineOptions} marked with {@link
   * JsonIgnore} fields.
   */
  public interface TestSerializationOfOptions extends PipelineOptions {
    String getFoo();

    void setFoo(String foo);

    @JsonIgnore
    @Default.String("not overridden")
    String getIgnoredField();

    void setIgnoredField(String value);
  }

  private static class LongNoDecodeCoder extends AtomicCoder<Long> {
    @Override
    public void encode(Long value, OutputStream outStream) throws IOException {}

    @Override
    public Long decode(InputStream inStream) throws IOException {
      throw new CoderException("Cannot decode a long");
    }
  }

  private static class MustSplitSource<T> extends BoundedSource<T> {
    public static <T> BoundedSource<T> of(BoundedSource<T> underlying) {
      return new MustSplitSource<>(underlying);
    }

    private final BoundedSource<T> underlying;

    public MustSplitSource(BoundedSource<T> underlying) {
      this.underlying = underlying;
    }

    @Override
    public List<? extends BoundedSource<T>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      // Must have more than
      checkState(
          desiredBundleSizeBytes < getEstimatedSizeBytes(options),
          "Must split into more than one source");
      return underlying.split(desiredBundleSizeBytes, options);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return underlying.getEstimatedSizeBytes(options);
    }

    @Override
    public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
      throw new IllegalStateException(
          "The MustSplitSource cannot create a reader without being split first");
    }

    @Override
    public void validate() {
      underlying.validate();
    }

    @Override
    public Coder<T> getOutputCoder() {
      return underlying.getOutputCoder();
    }
  }

  private static class StaticQueue<T> implements Serializable {

    static class StaticQueueSource<T> extends UnboundedSource<T, StaticQueueSource.Checkpoint<T>> {

      static class Checkpoint<T> implements UnboundedSource.CheckpointMark, Serializable {

        final T read;

        Checkpoint(T read) {
          this.read = read;
        }

        @Override
        public void finalizeCheckpoint() throws IOException {
          // nop
        }
      }

      final StaticQueue<T> queue;

      StaticQueueSource(StaticQueue<T> queue) {
        this.queue = queue;
      }

      @Override
      public List<? extends UnboundedSource<T, Checkpoint<T>>> split(
          int desiredNumSplits, PipelineOptions options) throws Exception {
        return Arrays.asList(this);
      }

      @Override
      public UnboundedReader<T> createReader(PipelineOptions po, Checkpoint<T> cmt) {
        return new UnboundedReader<T>() {

          T read = cmt == null ? null : cmt.read;
          boolean finished = false;

          @Override
          public boolean start() throws IOException {
            return advance();
          }

          @Override
          public boolean advance() throws IOException {
            try {
              Optional<T> taken = queue.take();
              if (taken.isPresent()) {
                read = taken.get();
                return true;
              }
              finished = true;
              return false;
            } catch (InterruptedException ex) {
              throw new IOException(ex);
            }
          }

          @Override
          public Instant getWatermark() {
            if (finished) {
              return BoundedWindow.TIMESTAMP_MAX_VALUE;
            }
            return BoundedWindow.TIMESTAMP_MIN_VALUE;
          }

          @Override
          public CheckpointMark getCheckpointMark() {
            return new Checkpoint(read);
          }

          @Override
          public UnboundedSource<T, ?> getCurrentSource() {
            return StaticQueueSource.this;
          }

          @Override
          public T getCurrent() throws NoSuchElementException {
            return read;
          }

          @Override
          public Instant getCurrentTimestamp() {
            return getWatermark();
          }

          @Override
          public void close() throws IOException {
            // nop
          }
        };
      }

      @SuppressWarnings("unchecked")
      @Override
      public Coder<Checkpoint<T>> getCheckpointMarkCoder() {
        return (Coder) SerializableCoder.of(Checkpoint.class);
      }

      @Override
      public Coder<T> getOutputCoder() {
        return queue.coder;
      }
    }

    static final Map<String, StaticQueue<?>> QUEUES = new ConcurrentHashMap<>();

    static <T> StaticQueue<T> of(String name, Coder<T> coder) {
      return new StaticQueue<>(name, coder);
    }

    private final String name;
    private final Coder<T> coder;
    private final transient BlockingQueue<Optional<T>> queue = new ArrayBlockingQueue<>(10);

    StaticQueue(String name, Coder<T> coder) {
      this.name = name;
      this.coder = coder;
      Preconditions.checkState(
          QUEUES.put(name, this) == null, "Queue " + name + " already exists.");
    }

    StaticQueue<T> add(T elem) {
      queue.add(Optional.of(elem));
      return this;
    }

    @Nullable
    Optional<T> take() throws InterruptedException {
      return queue.take();
    }

    PTransform<PBegin, PCollection<T>> read() {
      return new PTransform<PBegin, PCollection<T>>() {
        @Override
        public PCollection<T> expand(PBegin input) {
          return input.apply("readFrom:" + name, Read.from(asSource()));
        }
      };
    }

    UnboundedSource<T, ?> asSource() {
      return new StaticQueueSource<>(this);
    }

    void terminate() {
      queue.add(Optional.empty());
    }

    private Object readResolve() {
      return QUEUES.get(name);
    }
  }
}
