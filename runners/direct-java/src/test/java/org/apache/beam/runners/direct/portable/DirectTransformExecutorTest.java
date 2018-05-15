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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.portable.CommittedResult.OutputType;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.WindowedValue;
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

/** Tests for {@link DirectTransformExecutor}. */
@RunWith(JUnit4.class)
public class DirectTransformExecutorTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  private final PCollectionNode created =
      PipelineNode.pCollection(
          "created", PCollection.newBuilder().setUniqueName("created").build());

  private final PTransformNode createdProducer =
      PipelineNode.pTransform(
          "create",
          PTransform.newBuilder().putOutputs("created", "created").setUniqueName("create").build());
  private final PTransformNode downstreamProducer =
      PipelineNode.pTransform(
          "downstream",
          PTransform.newBuilder().putInputs("input", "created").setUniqueName("create").build());

  private CountDownLatch evaluatorCompleted;

  private RegisteringCompletionCallback completionCallback;
  private TransformExecutorService transformEvaluationState;
  private BundleFactory bundleFactory;
  @Mock private DirectMetrics metrics;
  @Mock private EvaluationContext evaluationContext;
  @Mock private TransformEvaluatorRegistry registry;

  @Rule public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    bundleFactory = ImmutableListBundleFactory.create();

    transformEvaluationState =
        TransformExecutorServices.parallel(MoreExecutors.newDirectExecutorService());

    evaluatorCompleted = new CountDownLatch(1);
    completionCallback = new RegisteringCompletionCallback(evaluatorCompleted);

    PipelineNode.pCollection(
        "created", RunnerApi.PCollection.newBuilder().setUniqueName("created").build());

    when(evaluationContext.getMetrics()).thenReturn(metrics);
  }

  @Test
  public void callWithNullInputBundleFinishesBundleAndCompletes() throws Exception {
    final TransformResult<Object> result = StepTransformResult.withoutHold(createdProducer).build();
    final AtomicBoolean finishCalled = new AtomicBoolean(false);
    TransformEvaluator<Object> evaluator =
        new TransformEvaluator<Object>() {
          @Override
          public void processElement(WindowedValue<Object> element) throws Exception {
            throw new IllegalArgumentException("Shouldn't be called");
          }

          @Override
          public TransformResult<Object> finishBundle() throws Exception {
            finishCalled.set(true);
            return result;
          }
        };

    when(registry.forApplication(createdProducer, null)).thenReturn(evaluator);

    DirectTransformExecutor<Object> executor =
        new DirectTransformExecutor<>(
            evaluationContext,
            registry,
            null,
            createdProducer,
            completionCallback,
            transformEvaluationState);
    executor.run();

    assertThat(finishCalled.get(), is(true));
    assertThat(completionCallback.handledResult, Matchers.equalTo(result));
    assertThat(completionCallback.handledException, is(nullValue()));
  }

  @Test
  public void nullTransformEvaluatorTerminates() throws Exception {
    when(registry.forApplication(createdProducer, null)).thenReturn(null);

    DirectTransformExecutor<Object> executor =
        new DirectTransformExecutor<>(
            evaluationContext,
            registry,
            null,
            createdProducer,
            completionCallback,
            transformEvaluationState);
    executor.run();

    assertThat(completionCallback.handledResult, is(nullValue()));
    assertThat(completionCallback.handledEmpty, equalTo(true));
    assertThat(completionCallback.handledException, is(nullValue()));
  }

  @Test
  public void inputBundleProcessesEachElementFinishesAndCompletes() throws Exception {
    final TransformResult<String> result =
        StepTransformResult.<String>withoutHold(downstreamProducer).build();
    final Collection<WindowedValue<String>> elementsProcessed = new ArrayList<>();
    TransformEvaluator<String> evaluator =
        new TransformEvaluator<String>() {
          @Override
          public void processElement(WindowedValue<String> element) throws Exception {
            elementsProcessed.add(element);
          }

          @Override
          public TransformResult<String> finishBundle() throws Exception {
            return result;
          }
        };

    WindowedValue<String> foo = WindowedValue.valueInGlobalWindow("foo");
    WindowedValue<String> spam = WindowedValue.valueInGlobalWindow("spam");
    WindowedValue<String> third = WindowedValue.valueInGlobalWindow("third");
    CommittedBundle<String> inputBundle =
        bundleFactory
            .<String>createBundle(created)
            .add(foo)
            .add(spam)
            .add(third)
            .commit(Instant.now());
    when(registry.<String>forApplication(downstreamProducer, inputBundle)).thenReturn(evaluator);

    DirectTransformExecutor<String> executor =
        new DirectTransformExecutor<>(
            evaluationContext,
            registry,
            inputBundle,
            downstreamProducer,
            completionCallback,
            transformEvaluationState);

    Executors.newSingleThreadExecutor().submit(executor);

    evaluatorCompleted.await();

    assertThat(elementsProcessed, containsInAnyOrder(spam, third, foo));
    assertThat(completionCallback.handledResult, Matchers.equalTo(result));
    assertThat(completionCallback.handledException, is(nullValue()));
  }

  @Test
  public void processElementThrowsExceptionCallsback() throws Exception {
    final TransformResult<String> result =
        StepTransformResult.<String>withoutHold(downstreamProducer).build();
    final Exception exception = new Exception();
    TransformEvaluator<String> evaluator =
        new TransformEvaluator<String>() {
          @Override
          public void processElement(WindowedValue<String> element) throws Exception {
            throw exception;
          }

          @Override
          public TransformResult<String> finishBundle() throws Exception {
            return result;
          }
        };

    WindowedValue<String> foo = WindowedValue.valueInGlobalWindow("foo");
    CommittedBundle<String> inputBundle =
        bundleFactory.<String>createBundle(created).add(foo).commit(Instant.now());
    when(registry.<String>forApplication(downstreamProducer, inputBundle)).thenReturn(evaluator);

    DirectTransformExecutor<String> executor =
        new DirectTransformExecutor<>(
            evaluationContext,
            registry,
            inputBundle,
            downstreamProducer,
            completionCallback,
            transformEvaluationState);
    Executors.newSingleThreadExecutor().submit(executor);

    evaluatorCompleted.await();

    assertThat(completionCallback.handledResult, is(nullValue()));
    assertThat(completionCallback.handledException, Matchers.<Throwable>equalTo(exception));
  }

  @Test
  public void finishBundleThrowsExceptionCallsback() throws Exception {
    final Exception exception = new Exception();
    TransformEvaluator<String> evaluator =
        new TransformEvaluator<String>() {
          @Override
          public void processElement(WindowedValue<String> element) throws Exception {}

          @Override
          public TransformResult<String> finishBundle() throws Exception {
            throw exception;
          }
        };

    CommittedBundle<String> inputBundle =
        bundleFactory.<String>createBundle(created).commit(Instant.now());
    when(registry.<String>forApplication(downstreamProducer, inputBundle)).thenReturn(evaluator);

    DirectTransformExecutor<String> executor =
        new DirectTransformExecutor<>(
            evaluationContext,
            registry,
            inputBundle,
            downstreamProducer,
            completionCallback,
            transformEvaluationState);
    Executors.newSingleThreadExecutor().submit(executor);

    evaluatorCompleted.await();

    assertThat(completionCallback.handledResult, is(nullValue()));
    assertThat(completionCallback.handledException, Matchers.<Throwable>equalTo(exception));
  }

  private static class RegisteringCompletionCallback implements CompletionCallback {
    private TransformResult<?> handledResult = null;
    private boolean handledEmpty = false;
    private Exception handledException = null;
    private final CountDownLatch onMethod;

    private RegisteringCompletionCallback(CountDownLatch onMethod) {
      this.onMethod = onMethod;
    }

    @Override
    public CommittedResult handleResult(CommittedBundle<?> inputBundle, TransformResult<?> result) {
      handledResult = result;
      onMethod.countDown();
      @SuppressWarnings("rawtypes")
      Iterable unprocessedElements =
          result.getUnprocessedElements() == null
              ? Collections.emptyList()
              : result.getUnprocessedElements();

      Optional<? extends CommittedBundle<?>> unprocessedBundle;
      if (inputBundle == null || Iterables.isEmpty(unprocessedElements)) {
        unprocessedBundle = Optional.absent();
      } else {
        unprocessedBundle =
            Optional.<CommittedBundle<?>>of(inputBundle.withElements(unprocessedElements));
      }
      return CommittedResult.create(
          result, unprocessedBundle, Collections.emptyList(), EnumSet.noneOf(OutputType.class));
    }

    @Override
    public void handleEmpty(PTransformNode transform) {
      handledEmpty = true;
      onMethod.countDown();
    }

    @Override
    public void handleException(CommittedBundle<?> inputBundle, Exception e) {
      handledException = e;
      onMethod.countDown();
    }

    @Override
    public void handleError(Error err) {
      throw err;
    }
  }
}
