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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.runners.direct.CommittedResult.OutputType;
import org.apache.beam.runners.direct.DirectRunner.CommittedBundle;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.IllegalMutationException;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link TransformExecutor}. */
@RunWith(JUnit4.class)
public class TransformExecutorTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  private PCollection<String> created;
  private PCollection<KV<Integer, String>> downstream;

  private CountDownLatch evaluatorCompleted;

  private RegisteringCompletionCallback completionCallback;
  private TransformExecutorService transformEvaluationState;
  private BundleFactory bundleFactory;
  @Mock private EvaluationContext evaluationContext;
  @Mock private TransformEvaluatorRegistry registry;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    bundleFactory = ImmutableListBundleFactory.create();

    transformEvaluationState =
        TransformExecutorServices.parallel(MoreExecutors.newDirectExecutorService());

    evaluatorCompleted = new CountDownLatch(1);
    completionCallback = new RegisteringCompletionCallback(evaluatorCompleted);

    TestPipeline p = TestPipeline.create();
    created = p.apply(Create.of("foo", "spam", "third"));
    downstream = created.apply(WithKeys.<Integer, String>of(3));
  }

  @Test
  public void callWithNullInputBundleFinishesBundleAndCompletes() throws Exception {
    final TransformResult result =
        StepTransformResult.withoutHold(created.getProducingTransformInternal()).build();
    final AtomicBoolean finishCalled = new AtomicBoolean(false);
    TransformEvaluator<Object> evaluator =
        new TransformEvaluator<Object>() {
          @Override
          public void processElement(WindowedValue<Object> element) throws Exception {
            throw new IllegalArgumentException("Shouldn't be called");
          }

          @Override
          public TransformResult finishBundle() throws Exception {
            finishCalled.set(true);
            return result;
          }
        };

    when(registry.forApplication(created.getProducingTransformInternal(), null))
        .thenReturn(evaluator);

    TransformExecutor<Object> executor =
        TransformExecutor.create(
            registry,
            Collections.<ModelEnforcementFactory>emptyList(),
            null,
            created.getProducingTransformInternal(),
            completionCallback,
            transformEvaluationState);
    executor.run();

    assertThat(finishCalled.get(), is(true));
    assertThat(completionCallback.handledResult, equalTo(result));
    assertThat(completionCallback.handledThrowable, is(nullValue()));
  }

  @Test
  public void nullTransformEvaluatorTerminates() throws Exception {
    when(registry.forApplication(created.getProducingTransformInternal(), null)).thenReturn(null);

    TransformExecutor<Object> executor =
        TransformExecutor.create(
            registry,
            Collections.<ModelEnforcementFactory>emptyList(),
            null,
            created.getProducingTransformInternal(),
            completionCallback,
            transformEvaluationState);
    executor.run();

    assertThat(completionCallback.handledResult, is(nullValue()));
    assertThat(completionCallback.handledEmpty, equalTo(true));
    assertThat(completionCallback.handledThrowable, is(nullValue()));
  }

  @Test
  public void inputBundleProcessesEachElementFinishesAndCompletes() throws Exception {
    final TransformResult result =
        StepTransformResult.withoutHold(downstream.getProducingTransformInternal()).build();
    final Collection<WindowedValue<String>> elementsProcessed = new ArrayList<>();
    TransformEvaluator<String> evaluator =
        new TransformEvaluator<String>() {
          @Override
          public void processElement(WindowedValue<String> element) throws Exception {
            elementsProcessed.add(element);
            return;
          }

          @Override
          public TransformResult finishBundle() throws Exception {
            return result;
          }
        };

    WindowedValue<String> foo = WindowedValue.valueInGlobalWindow("foo");
    WindowedValue<String> spam = WindowedValue.valueInGlobalWindow("spam");
    WindowedValue<String> third = WindowedValue.valueInGlobalWindow("third");
    CommittedBundle<String> inputBundle =
        bundleFactory.createBundle(created).add(foo).add(spam).add(third).commit(Instant.now());
    when(registry.<String>forApplication(downstream.getProducingTransformInternal(), inputBundle))
        .thenReturn(evaluator);

    TransformExecutor<String> executor =
        TransformExecutor.create(
            registry,
            Collections.<ModelEnforcementFactory>emptyList(),
            inputBundle,
            downstream.getProducingTransformInternal(),
            completionCallback,
            transformEvaluationState);

    Executors.newSingleThreadExecutor().submit(executor);

    evaluatorCompleted.await();

    assertThat(elementsProcessed, containsInAnyOrder(spam, third, foo));
    assertThat(completionCallback.handledResult, equalTo(result));
    assertThat(completionCallback.handledThrowable, is(nullValue()));
  }

  @Test
  public void processElementThrowsExceptionCallsback() throws Exception {
    final TransformResult result =
        StepTransformResult.withoutHold(downstream.getProducingTransformInternal()).build();
    final Exception exception = new Exception();
    TransformEvaluator<String> evaluator =
        new TransformEvaluator<String>() {
          @Override
          public void processElement(WindowedValue<String> element) throws Exception {
            throw exception;
          }

          @Override
          public TransformResult finishBundle() throws Exception {
            return result;
          }
        };

    WindowedValue<String> foo = WindowedValue.valueInGlobalWindow("foo");
    CommittedBundle<String> inputBundle =
        bundleFactory.createBundle(created).add(foo).commit(Instant.now());
    when(registry.<String>forApplication(downstream.getProducingTransformInternal(), inputBundle))
        .thenReturn(evaluator);

    TransformExecutor<String> executor =
        TransformExecutor.create(
            registry,
            Collections.<ModelEnforcementFactory>emptyList(),
            inputBundle,
            downstream.getProducingTransformInternal(),
            completionCallback,
            transformEvaluationState);
    Executors.newSingleThreadExecutor().submit(executor);

    evaluatorCompleted.await();

    assertThat(completionCallback.handledResult, is(nullValue()));
    assertThat(completionCallback.handledThrowable, Matchers.<Throwable>equalTo(exception));
  }

  @Test
  public void finishBundleThrowsExceptionCallsback() throws Exception {
    final Exception exception = new Exception();
    TransformEvaluator<String> evaluator =
        new TransformEvaluator<String>() {
          @Override
          public void processElement(WindowedValue<String> element) throws Exception {}

          @Override
          public TransformResult finishBundle() throws Exception {
            throw exception;
          }
        };

    CommittedBundle<String> inputBundle =
        bundleFactory.createBundle(created).commit(Instant.now());
    when(registry.<String>forApplication(downstream.getProducingTransformInternal(), inputBundle))
        .thenReturn(evaluator);

    TransformExecutor<String> executor =
        TransformExecutor.create(
            registry,
            Collections.<ModelEnforcementFactory>emptyList(),
            inputBundle,
            downstream.getProducingTransformInternal(),
            completionCallback,
            transformEvaluationState);
    Executors.newSingleThreadExecutor().submit(executor);

    evaluatorCompleted.await();

    assertThat(completionCallback.handledResult, is(nullValue()));
    assertThat(completionCallback.handledThrowable, Matchers.<Throwable>equalTo(exception));
  }

  @Test
  public void duringCallGetThreadIsNonNull() throws Exception {
    final TransformResult result =
        StepTransformResult.withoutHold(downstream.getProducingTransformInternal()).build();
    final CountDownLatch testLatch = new CountDownLatch(1);
    final CountDownLatch evaluatorLatch = new CountDownLatch(1);
    TransformEvaluator<Object> evaluator =
        new TransformEvaluator<Object>() {
          @Override
          public void processElement(WindowedValue<Object> element) throws Exception {
            throw new IllegalArgumentException("Shouldn't be called");
          }

          @Override
          public TransformResult finishBundle() throws Exception {
            testLatch.countDown();
            evaluatorLatch.await();
            return result;
          }
        };

    when(registry.forApplication(created.getProducingTransformInternal(), null))
        .thenReturn(evaluator);

    TransformExecutor<String> executor =
        TransformExecutor.create(
            registry,
            Collections.<ModelEnforcementFactory>emptyList(),
            null,
            created.getProducingTransformInternal(),
            completionCallback,
            transformEvaluationState);

    Executors.newSingleThreadExecutor().submit(executor);
    testLatch.await();
    assertThat(executor.getThread(), not(nullValue()));

    // Finish the execution so everything can get closed down cleanly.
    evaluatorLatch.countDown();
  }

  @Test
  public void callWithEnforcementAppliesEnforcement() throws Exception {
    final TransformResult result =
        StepTransformResult.withoutHold(downstream.getProducingTransformInternal()).build();

    TransformEvaluator<Object> evaluator =
        new TransformEvaluator<Object>() {
          @Override
          public void processElement(WindowedValue<Object> element) throws Exception {}

          @Override
          public TransformResult finishBundle() throws Exception {
            return result;
          }
        };

    WindowedValue<String> fooElem = WindowedValue.valueInGlobalWindow("foo");
    WindowedValue<String> barElem = WindowedValue.valueInGlobalWindow("bar");
    CommittedBundle<String> inputBundle =
        bundleFactory.createBundle(created).add(fooElem).add(barElem).commit(Instant.now());
    when(registry.forApplication(downstream.getProducingTransformInternal(), inputBundle))
        .thenReturn(evaluator);

    TestEnforcementFactory enforcement = new TestEnforcementFactory();
    TransformExecutor<String> executor =
        TransformExecutor.create(
            registry,
            Collections.<ModelEnforcementFactory>singleton(enforcement),
            inputBundle,
            downstream.getProducingTransformInternal(),
            completionCallback,
            transformEvaluationState);

    executor.run();
    TestEnforcement<?> testEnforcement = enforcement.instance;
    assertThat(
        testEnforcement.beforeElements,
        Matchers.<WindowedValue<?>>containsInAnyOrder(barElem, fooElem));
    assertThat(
        testEnforcement.afterElements,
        Matchers.<WindowedValue<?>>containsInAnyOrder(barElem, fooElem));
    assertThat(testEnforcement.finishedBundles, contains(result));
  }

  @Test
  public void callWithEnforcementThrowsOnFinishPropagates() throws Exception {
    PCollection<byte[]> pcBytes =
        created.apply(
            new PTransform<PCollection<String>, PCollection<byte[]>>() {
              @Override
              public PCollection<byte[]> apply(PCollection<String> input) {
                return PCollection.<byte[]>createPrimitiveOutputInternal(
                        input.getPipeline(), input.getWindowingStrategy(), input.isBounded())
                    .setCoder(ByteArrayCoder.of());
              }
            });

    final TransformResult result =
        StepTransformResult.withoutHold(pcBytes.getProducingTransformInternal()).build();
    final CountDownLatch testLatch = new CountDownLatch(1);
    final CountDownLatch evaluatorLatch = new CountDownLatch(1);

    TransformEvaluator<Object> evaluator =
        new TransformEvaluator<Object>() {
          @Override
          public void processElement(WindowedValue<Object> element) throws Exception {}

          @Override
          public TransformResult finishBundle() throws Exception {
            testLatch.countDown();
            evaluatorLatch.await();
            return result;
          }
        };

    WindowedValue<byte[]> fooBytes = WindowedValue.valueInGlobalWindow("foo".getBytes());
    CommittedBundle<byte[]> inputBundle =
        bundleFactory.createBundle(pcBytes).add(fooBytes).commit(Instant.now());
    when(registry.forApplication(pcBytes.getProducingTransformInternal(), inputBundle))
        .thenReturn(evaluator);

    TransformExecutor<byte[]> executor =
        TransformExecutor.create(
            registry,
            Collections.<ModelEnforcementFactory>singleton(ImmutabilityEnforcementFactory.create()),
            inputBundle,
            pcBytes.getProducingTransformInternal(),
            completionCallback,
            transformEvaluationState);

    Future<?> task = Executors.newSingleThreadExecutor().submit(executor);
    testLatch.await();
    fooBytes.getValue()[0] = 'b';
    evaluatorLatch.countDown();

    thrown.expectCause(isA(IllegalMutationException.class));
    task.get();
  }

  @Test
  public void callWithEnforcementThrowsOnElementPropagates() throws Exception {
    PCollection<byte[]> pcBytes =
        created.apply(
            new PTransform<PCollection<String>, PCollection<byte[]>>() {
              @Override
              public PCollection<byte[]> apply(PCollection<String> input) {
                return PCollection.<byte[]>createPrimitiveOutputInternal(
                        input.getPipeline(), input.getWindowingStrategy(), input.isBounded())
                    .setCoder(ByteArrayCoder.of());
              }
            });

    final TransformResult result =
        StepTransformResult.withoutHold(pcBytes.getProducingTransformInternal()).build();
    final CountDownLatch testLatch = new CountDownLatch(1);
    final CountDownLatch evaluatorLatch = new CountDownLatch(1);

    TransformEvaluator<Object> evaluator =
        new TransformEvaluator<Object>() {
          @Override
          public void processElement(WindowedValue<Object> element) throws Exception {
            testLatch.countDown();
            evaluatorLatch.await();
          }

          @Override
          public TransformResult finishBundle() throws Exception {
            return result;
          }
        };

    WindowedValue<byte[]> fooBytes = WindowedValue.valueInGlobalWindow("foo".getBytes());
    CommittedBundle<byte[]> inputBundle =
        bundleFactory.createBundle(pcBytes).add(fooBytes).commit(Instant.now());
    when(registry.forApplication(pcBytes.getProducingTransformInternal(), inputBundle))
        .thenReturn(evaluator);

    TransformExecutor<byte[]> executor =
        TransformExecutor.create(
            registry,
            Collections.<ModelEnforcementFactory>singleton(ImmutabilityEnforcementFactory.create()),
            inputBundle,
            pcBytes.getProducingTransformInternal(),
            completionCallback,
            transformEvaluationState);

    Future<?> task = Executors.newSingleThreadExecutor().submit(executor);
    testLatch.await();
    fooBytes.getValue()[0] = 'b';
    evaluatorLatch.countDown();

    thrown.expectCause(isA(IllegalMutationException.class));
    task.get();
  }

  private static class RegisteringCompletionCallback implements CompletionCallback {
    private TransformResult handledResult = null;
    private boolean handledEmpty = false;
    private Throwable handledThrowable = null;
    private final CountDownLatch onMethod;

    private RegisteringCompletionCallback(CountDownLatch onMethod) {
      this.onMethod = onMethod;
    }

    @Override
    public CommittedResult handleResult(CommittedBundle<?> inputBundle, TransformResult result) {
      handledResult = result;
      onMethod.countDown();
      @SuppressWarnings("rawtypes")
      Iterable unprocessedElements =
          result.getUnprocessedElements() == null
              ? Collections.emptyList()
              : result.getUnprocessedElements();

      CommittedBundle<?> unprocessedBundle =
          inputBundle == null ? null : inputBundle.withElements(unprocessedElements);
      return CommittedResult.create(
          result,
          unprocessedBundle,
          Collections.<CommittedBundle<?>>emptyList(),
          EnumSet.noneOf(OutputType.class));
    }

    @Override
    public void handleEmpty(AppliedPTransform<?, ?, ?> transform) {
      handledEmpty = true;
      onMethod.countDown();
    }

    @Override
    public void handleThrowable(CommittedBundle<?> inputBundle, Throwable t) {
      handledThrowable = t;
      onMethod.countDown();
    }
  }

  private static class TestEnforcementFactory implements ModelEnforcementFactory {
    private TestEnforcement<?> instance;

    @Override
    public <T> TestEnforcement<T> forBundle(
        CommittedBundle<T> input, AppliedPTransform<?, ?, ?> consumer) {
      TestEnforcement<T> newEnforcement = new TestEnforcement<>();
      instance = newEnforcement;
      return newEnforcement;
    }
  }

  private static class TestEnforcement<T> implements ModelEnforcement<T> {
    private final List<WindowedValue<T>> beforeElements = new ArrayList<>();
    private final List<WindowedValue<T>> afterElements = new ArrayList<>();
    private final List<TransformResult> finishedBundles = new ArrayList<>();

    @Override
    public void beforeElement(WindowedValue<T> element) {
      beforeElements.add(element);
    }

    @Override
    public void afterElement(WindowedValue<T> element) {
      afterElements.add(element);
    }

    @Override
    public void afterFinish(
        CommittedBundle<T> input,
        TransformResult result,
        Iterable<? extends CommittedBundle<?>> outputs) {
      finishedBundles.add(result);
    }
  }
}
