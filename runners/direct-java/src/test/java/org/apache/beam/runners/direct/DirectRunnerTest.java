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

import static com.google.common.base.Preconditions.checkState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.direct.DirectRunner.DirectPipelineResult;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.IllegalMutationException;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for basic {@link DirectRunner} functionality.
 */
@RunWith(JUnit4.class)
public class DirectRunnerTest implements Serializable {
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private Pipeline getPipeline() {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(DirectRunner.class);

    return Pipeline.create(opts);
  }

  @Test
  public void defaultRunnerLoaded() {
    assertThat(DirectRunner.class,
        Matchers.<Class<? extends PipelineRunner>>equalTo(PipelineOptionsFactory.create()
            .getRunner()));
  }

  @Test
  public void wordCountShouldSucceed() throws Throwable {
    Pipeline p = getPipeline();

    PCollection<KV<String, Long>> counts =
        p.apply(Create.of("foo", "bar", "foo", "baz", "bar", "foo"))
            .apply(MapElements.via(new SimpleFunction<String, String>() {
              @Override
              public String apply(String input) {
                return input;
              }
            }))
            .apply(Count.<String>perElement());
    PCollection<String> countStrs =
        counts.apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
          @Override
          public String apply(KV<String, Long> input) {
            String str = String.format("%s: %s", input.getKey(), input.getValue());
            return str;
          }
        }));

    PAssert.that(countStrs).containsInAnyOrder("baz: 1", "bar: 2", "foo: 3");

    DirectPipelineResult result = ((DirectPipelineResult) p.run());
    result.waitUntilFinish();
  }

  private static AtomicInteger changed;
  @Test
  public void reusePipelineSucceeds() throws Throwable {
    Pipeline p = getPipeline();

    changed = new AtomicInteger(0);
    PCollection<KV<String, Long>> counts =
        p.apply(Create.of("foo", "bar", "foo", "baz", "bar", "foo"))
            .apply(MapElements.via(new SimpleFunction<String, String>() {
              @Override
              public String apply(String input) {
                return input;
              }
            }))
            .apply(Count.<String>perElement());
    PCollection<String> countStrs =
        counts.apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
          @Override
          public String apply(KV<String, Long> input) {
            String str = String.format("%s: %s", input.getKey(), input.getValue());
            return str;
          }
        }));

    counts.apply(ParDo.of(new DoFn<KV<String, Long>, Void>() {
      @ProcessElement
      public void updateChanged(ProcessContext c) {
        changed.getAndIncrement();
      }
    }));


    PAssert.that(countStrs).containsInAnyOrder("baz: 1", "bar: 2", "foo: 3");

    DirectPipelineResult result = ((DirectPipelineResult) p.run());
    result.waitUntilFinish();

    DirectPipelineResult otherResult = ((DirectPipelineResult) p.run());
    otherResult.waitUntilFinish();

    assertThat("Each element should have been processed twice", changed.get(), equalTo(6));
  }

  @Test
  public void byteArrayCountShouldSucceed() {
    Pipeline p = getPipeline();

    SerializableFunction<Integer, byte[]> getBytes = new SerializableFunction<Integer, byte[]>() {
      @Override
      public byte[] apply(Integer input) {
        try {
          return CoderUtils.encodeToByteArray(VarIntCoder.of(), input);
        } catch (CoderException e) {
          fail("Unexpected Coder Exception " + e);
          throw new AssertionError("Unreachable");
        }
      }
    };
    TypeDescriptor<byte[]> td = new TypeDescriptor<byte[]>() {
    };
    PCollection<byte[]> foos =
        p.apply(Create.of(1, 1, 1, 2, 2, 3))
            .apply(MapElements.into(td).via(getBytes));
    PCollection<byte[]> msync =
        p.apply(Create.of(1, -2, -8, -16)).apply(MapElements.into(td).via(getBytes));
    PCollection<byte[]> bytes =
        PCollectionList.of(foos).and(msync).apply(Flatten.<byte[]>pCollections());
    PCollection<KV<byte[], Long>> counts = bytes.apply(Count.<byte[]>perElement());
    PCollection<KV<Integer, Long>> countsBackToString =
        counts.apply(MapElements.via(new SimpleFunction<KV<byte[], Long>, KV<Integer, Long>>() {
          @Override
          public KV<Integer, Long> apply(KV<byte[], Long> input) {
            try {
              return KV.of(CoderUtils.decodeFromByteArray(VarIntCoder.of(), input.getKey()),
                  input.getValue());
            } catch (CoderException e) {
              fail("Unexpected Coder Exception " + e);
              throw new AssertionError("Unreachable");
        }
      }
    }));

    Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder().put(1, 4L)
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
    Runnable cancelRunnable = new Runnable() {
      @Override
      public void run() {
        try {
          resultExchange.take().cancel();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }
    };

    Callable<PipelineResult> runPipelineRunnable = new Callable<PipelineResult>() {
      @Override
      public PipelineResult call() {
        PipelineResult res = p.run();
        try {
          resultExchange.put(res);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
        return res;
      }
    };

    ExecutorService executor = Executors.newCachedThreadPool();
    executor.submit(cancelRunnable);
    Future<PipelineResult> result = executor.submit(runPipelineRunnable);

    // If cancel doesn't work, this will hang forever
    result.get().waitUntilFinish();
  }

  @Test
  public void testWaitUntilFinishTimeout() throws Exception {
    DirectOptions options = PipelineOptionsFactory.as(DirectOptions.class);
    options.setBlockOnRun(false);
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);
    p
      .apply(Create.of(1L))
      .apply(ParDo.of(
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
    result.waitUntilFinish(Duration.millis(1L));
    assertThat(result.getState(), is(State.RUNNING));
  }

  @Test
  public void transformDisplayDataExceptionShouldFail() {
    DoFn<Integer, Integer> brokenDoFn = new DoFn<Integer, Integer>() {
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {}

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        throw new RuntimeException("oh noes!");
      }
    };

    Pipeline p = getPipeline();
    p
        .apply(Create.of(1, 2, 3))
        .apply(ParDo.of(brokenDoFn));

    thrown.expectMessage(brokenDoFn.getClass().getName());
    thrown.expectCause(ThrowableMessageMatcher.hasMessage(is("oh noes!")));
    p.run();
  }

  /**
   * Tests that a {@link DoFn} that mutates an output with a good equals() fails in the
   * {@link DirectRunner}.
   */
  @Test
  public void testMutatingOutputThenOutputDoFnError() throws Exception {
    Pipeline pipeline = getPipeline();

    pipeline
        .apply(Create.of(42))
        .apply(ParDo.of(new DoFn<Integer, List<Integer>>() {
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
   * Tests that a {@link DoFn} that mutates an output with a good equals() fails in the
   * {@link DirectRunner}.
   */
  @Test
  public void testMutatingOutputWithEnforcementDisabledSucceeds() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    options.as(DirectOptions.class).setEnforceImmutability(false);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(Create.of(42))
        .apply(ParDo.of(new DoFn<Integer, List<Integer>>() {
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
   * Tests that a {@link DoFn} that mutates an output with a good equals() fails in the
   * {@link DirectRunner}.
   */
  @Test
  public void testMutatingOutputThenTerminateDoFnError() throws Exception {
    Pipeline pipeline = getPipeline();

    pipeline
        .apply(Create.of(42))
        .apply(ParDo.of(new DoFn<Integer, List<Integer>>() {
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
   * Tests that a {@link DoFn} that mutates an output with a bad equals() still fails
   * in the {@link DirectRunner}.
   */
  @Test
  public void testMutatingOutputCoderDoFnError() throws Exception {
    Pipeline pipeline = getPipeline();

    pipeline
        .apply(Create.of(42))
        .apply(ParDo.of(new DoFn<Integer, byte[]>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            byte[] outputArray = new byte[]{0x1, 0x2, 0x3};
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
   * Tests that a {@link DoFn} that mutates its input with a good equals() fails in the
   * {@link DirectRunner}.
   */
  @Test
  public void testMutatingInputDoFnError() throws Exception {
    Pipeline pipeline = getPipeline();

    pipeline
        .apply(Create.of(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6))
            .withCoder(ListCoder.of(VarIntCoder.of())))
        .apply(ParDo.of(new DoFn<List<Integer>, Integer>() {
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
   * Tests that a {@link DoFn} that mutates an input with a bad equals() still fails
   * in the {@link DirectRunner}.
   */
  @Test
  public void testMutatingInputCoderDoFnError() throws Exception {
    Pipeline pipeline = getPipeline();

    pipeline
        .apply(Create.of(new byte[]{0x1, 0x2, 0x3}, new byte[]{0x4, 0x5, 0x6}))
        .apply(ParDo.of(new DoFn<byte[], Integer>() {
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
        p.apply(Create.of((Void) null)).apply(ParDo.of(new DoFn<Void, Long>() {
          @ProcessElement
          public void processElement(ProcessContext c) {
            c.output(null);
          }
        })).setCoder(VarLongCoder.of());
    pcollection
        .apply(
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

    thrown.expectCause(isA(CoderException.class));
    thrown.expectMessage("Cannot decode a long");
    p.run();
  }

  private static class LongNoDecodeCoder extends CustomCoder<Long> {

    @Override
    public void encode(
        Long value, OutputStream outStream, Context context) throws IOException {
    }

    @Override
    public Long decode(InputStream inStream, Context context) throws IOException {
      throw new CoderException("Cannot decode a long");
    }
  }

  private static class MustSplitSource<T> extends BoundedSource<T>{
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
    public Coder<T> getDefaultOutputCoder() {
      return underlying.getDefaultOutputCoder();
    }
  }

  @Test
  public void fallbackCoderProviderAllowsInference() {
    // See https://issues.apache.org/jira/browse/BEAM-1642
    Pipeline p = getPipeline();
    p.getCoderRegistry().setFallbackCoderProvider(
        org.apache.beam.sdk.coders.AvroCoder.PROVIDER);
    p.apply(Create.of(Arrays.asList(100, 200))).apply(Count.<Integer>globally());
    p.run().waitUntilFinish();
  }
}
