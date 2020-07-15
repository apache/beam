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

import static org.apache.beam.sdk.transforms.reflect.DoFnSignaturesTestUtils.analyzeProcessElementMethod;
import static org.apache.beam.sdk.transforms.reflect.DoFnSignaturesTestUtils.errors;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.Restriction;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.RestrictionParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.WatermarkEstimatorStateParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures.FnAnalysisContext;
import org.apache.beam.sdk.transforms.reflect.DoFnSignaturesTestUtils.AnonymousMethod;
import org.apache.beam.sdk.transforms.reflect.DoFnSignaturesTestUtils.FakeDoFn;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.TruncateResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link DoFnSignatures} focused on methods related to <a
 * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}.
 */
@SuppressWarnings("unused")
@RunWith(JUnit4.class)
public class DoFnSignaturesSplittableDoFnTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private abstract static class SomeRestriction
      implements HasDefaultTracker<SomeRestriction, SomeRestrictionTracker> {}

  private abstract static class SomeRestrictionTracker
      extends RestrictionTracker<SomeRestriction, Void> {}

  private abstract static class SomeRestrictionCoder extends StructuredCoder<SomeRestriction> {}

  @Test
  public void testReturnsProcessContinuation() throws Exception {
    DoFnSignature.ProcessElementMethod signature =
        analyzeProcessElementMethod(
            new AnonymousMethod() {
              private DoFn.ProcessContinuation method(
                  DoFn<Integer, String>.ProcessContext context) {
                return null;
              }
            });

    assertTrue(signature.hasReturnValue());
  }

  @Test
  public void testHasRestrictionTracker() throws Exception {
    DoFnSignature.ProcessElementMethod signature =
        analyzeProcessElementMethod(
            new AnonymousMethod() {
              private void method(
                  DoFn<Integer, String>.ProcessContext context, SomeRestrictionTracker tracker) {}
            });

    assertTrue(signature.isSplittable());
    assertTrue(
        signature.extraParameters().stream()
            .anyMatch(
                Predicates.instanceOf(DoFnSignature.Parameter.RestrictionTrackerParameter.class)
                    ::apply));
    assertEquals(SomeRestrictionTracker.class, signature.trackerT().getRawType());
  }

  @Test
  public void testSplittableProcessElementMustNotHaveUnsupportedParams() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Illegal parameter");
    thrown.expectMessage("ValueState");

    DoFn<Integer, String> doFn =
        new DoFn<Integer, String>() {
          @StateId("my-state-id")
          public final StateSpec<ValueState<String>> myStateSpec =
              StateSpecs.value(StringUtf8Coder.of());

          @ProcessElement
          public void method(
              DoFn<Integer, String>.ProcessContext context,
              SomeRestrictionTracker tracker,
              @StateId("my-state-id") ValueState<String> myState) {}
        };
    Method processElementMethod = null;
    for (Method method : doFn.getClass().getDeclaredMethods()) {
      if ("method".equals(method.getName())) {
        processElementMethod = method;
      }
    }
    checkState(processElementMethod != null);

    FnAnalysisContext context = FnAnalysisContext.create();
    context.addStateDeclaration(
        DoFnSignature.StateDeclaration.create(
            "my-state-id",
            doFn.getClass().getField("myStateSpec"),
            new TypeDescriptor<ValueState<String>>() {}));

    DoFnSignatures.analyzeProcessElementMethod(
        errors(),
        new TypeDescriptor<DoFn<Integer, String>>() {},
        processElementMethod,
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(String.class),
        context);
  }

  @Test
  public void testInfersBoundednessFromAnnotation() throws Exception {
    class BaseSplittableFn extends DoFn<Integer, String> {
      @ProcessElement
      public void processElement(
          ProcessContext context, RestrictionTracker<SomeRestriction, Void> tracker) {}

      @GetInitialRestriction
      public SomeRestriction getInitialRestriction(@Element Integer element) {
        return null;
      }
    }

    @BoundedPerElement
    class BoundedSplittableFn extends BaseSplittableFn {}

    @UnboundedPerElement
    class UnboundedSplittableFn extends BaseSplittableFn {}

    assertEquals(
        PCollection.IsBounded.BOUNDED,
        DoFnSignatures.getSignature(BaseSplittableFn.class).isBoundedPerElement());
    assertEquals(
        PCollection.IsBounded.BOUNDED,
        DoFnSignatures.getSignature(BoundedSplittableFn.class).isBoundedPerElement());
    assertEquals(
        PCollection.IsBounded.UNBOUNDED,
        DoFnSignatures.getSignature(UnboundedSplittableFn.class).isBoundedPerElement());
  }

  private static class BaseFnWithoutContinuation extends DoFn<Integer, String> {
    @ProcessElement
    public void processElement(
        ProcessContext context, RestrictionTracker<SomeRestriction, Void> tracker) {}

    @GetInitialRestriction
    public SomeRestriction getInitialRestriction(@Element Integer element) {
      return null;
    }
  }

  private static class BaseFnWithContinuation extends DoFn<Integer, String> {
    @ProcessElement
    public ProcessContinuation processElement(
        ProcessContext context, RestrictionTracker<SomeRestriction, Void> tracker) {
      return null;
    }

    @GetInitialRestriction
    public SomeRestriction getInitialRestriction(@Element Integer element) {
      return null;
    }
  }

  @Test
  public void testSplittableBoundednessInferredFromReturnValue() throws Exception {
    assertEquals(
        PCollection.IsBounded.BOUNDED,
        DoFnSignatures.getSignature(BaseFnWithoutContinuation.class).isBoundedPerElement());
    assertEquals(
        PCollection.IsBounded.UNBOUNDED,
        DoFnSignatures.getSignature(BaseFnWithContinuation.class).isBoundedPerElement());
  }

  @Test
  public void testSplittableRespectsBoundednessAnnotation() throws Exception {
    @BoundedPerElement
    class BoundedFnWithContinuation extends BaseFnWithContinuation {}

    assertEquals(
        PCollection.IsBounded.BOUNDED,
        DoFnSignatures.getSignature(BoundedFnWithContinuation.class).isBoundedPerElement());

    @UnboundedPerElement
    class UnboundedFnWithContinuation extends BaseFnWithContinuation {}

    assertEquals(
        PCollection.IsBounded.UNBOUNDED,
        DoFnSignatures.getSignature(UnboundedFnWithContinuation.class).isBoundedPerElement());
  }

  @Test
  public void testUnsplittableIsBounded() throws Exception {
    class UnsplittableFn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(ProcessContext context) {}
    }

    assertEquals(
        PCollection.IsBounded.BOUNDED,
        DoFnSignatures.getSignature(UnsplittableFn.class).isBoundedPerElement());
  }

  @Test
  public void testUnsplittableButDeclaresBounded() throws Exception {
    @BoundedPerElement
    class SomeFn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(ProcessContext context) {}
    }

    thrown.expectMessage("Non-splittable, but annotated as @Bounded");
    DoFnSignatures.getSignature(SomeFn.class);
  }

  @Test
  public void testUnsplittableButDeclaresUnbounded() throws Exception {
    @UnboundedPerElement
    class SomeFn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(ProcessContext context) {}
    }

    thrown.expectMessage("Non-splittable, but annotated as @Unbounded");
    DoFnSignatures.getSignature(SomeFn.class);
  }

  /** Tests a splittable {@link DoFn} that defines all methods in their full form, correctly. */
  @Test
  public void testSplittableWithAllFunctionsAndAllParameters() throws Exception {
    class GoodSplittableDoFn extends DoFn<Integer, String> {
      @ProcessElement
      public ProcessContinuation processElement(
          ProcessContext context,
          RestrictionTracker<SomeRestriction, Void> tracker,
          ManualWatermarkEstimator<Instant> watermarkEstimator) {
        return null;
      }

      @GetInitialRestriction
      public SomeRestriction getInitialRestriction(
          @Element Integer element,
          PipelineOptions pipelineOptions,
          BoundedWindow boundedWindow,
          PaneInfo paneInfo,
          @Timestamp Instant timestamp) {
        return null;
      }

      @SplitRestriction
      public void splitRestriction(
          @Element Integer element,
          @Restriction SomeRestriction restriction,
          RestrictionTracker<SomeRestriction, Void> restrictionTracker,
          OutputReceiver<SomeRestriction> receiver,
          PipelineOptions pipelineOptions,
          BoundedWindow boundedWindow,
          PaneInfo paneInfo,
          @Timestamp Instant timestamp) {}

      @TruncateRestriction
      public TruncateResult<SomeRestriction> truncateRestriction(
          @Element Integer element,
          @Restriction SomeRestriction restriction,
          RestrictionTracker<SomeRestriction, Void> restrictionTracker,
          PipelineOptions pipelineOptions,
          BoundedWindow boundedWindow,
          PaneInfo paneInfo,
          @Timestamp Instant timestamp) {
        return TruncateResult.of(null);
      }

      @NewTracker
      public SomeRestrictionTracker newTracker(
          @Element Integer element,
          @Restriction SomeRestriction restriction,
          PipelineOptions pipelineOptions,
          BoundedWindow boundedWindow,
          PaneInfo paneInfo,
          @Timestamp Instant timestamp) {
        return null;
      }

      @GetSize
      public double getSize(
          @Element Integer element,
          @Restriction SomeRestriction restriction,
          PipelineOptions pipelineOptions,
          BoundedWindow boundedWindow,
          PaneInfo paneInfo,
          @Timestamp Instant timestamp) {
        return 1.0;
      }

      @GetRestrictionCoder
      public SomeRestrictionCoder getRestrictionCoder() {
        return null;
      }

      @GetInitialWatermarkEstimatorState
      public Instant getInitialWatermarkEstimatorState(
          @Element Integer element,
          @Restriction SomeRestriction restriction,
          PipelineOptions pipelineOptions,
          BoundedWindow boundedWindow,
          PaneInfo paneInfo,
          @Timestamp Instant timestamp) {
        return null;
      }

      @GetWatermarkEstimatorStateCoder
      public InstantCoder getWatermarkEstimatorStateCoder() {
        return null;
      }

      @NewWatermarkEstimator
      public WatermarkEstimators.Manual newWatermarkEstimator(
          @WatermarkEstimatorState Instant watermarkEstimatorState,
          @Element Integer element,
          @Restriction SomeRestriction restriction,
          PipelineOptions pipelineOptions,
          BoundedWindow boundedWindow,
          PaneInfo paneInfo,
          @Timestamp Instant timestamp) {
        return null;
      }
    }

    DoFnSignature signature = DoFnSignatures.getSignature(GoodSplittableDoFn.class);
    assertEquals(RestrictionTracker.class, signature.processElement().trackerT().getRawType());
    assertEquals(
        ManualWatermarkEstimator.class,
        signature.processElement().watermarkEstimatorT().getRawType());
    assertTrue(signature.processElement().isSplittable());
    assertTrue(signature.processElement().hasReturnValue());
    assertEquals(
        SomeRestriction.class, signature.getInitialRestriction().restrictionT().getRawType());
    assertEquals(
        SomeRestriction.class,
        getParameterOfType(
                signature.splitRestriction().extraParameters(), RestrictionParameter.class)
            .restrictionT()
            .getRawType());
    assertEquals(
        SomeRestriction.class,
        getParameterOfType(
                signature.truncateRestriction().extraParameters(), RestrictionParameter.class)
            .restrictionT()
            .getRawType());
    assertEquals(SomeRestrictionTracker.class, signature.newTracker().trackerT().getRawType());
    assertEquals(
        SomeRestriction.class,
        getParameterOfType(signature.newTracker().extraParameters(), RestrictionParameter.class)
            .restrictionT()
            .getRawType());
    assertEquals(SomeRestrictionCoder.class, signature.getRestrictionCoder().coderT().getRawType());
    assertEquals(
        SomeRestriction.class,
        getParameterOfType(signature.getSize().extraParameters(), RestrictionParameter.class)
            .restrictionT()
            .getRawType());
    assertEquals(
        Instant.class,
        signature.getInitialWatermarkEstimatorState().watermarkEstimatorStateT().getRawType());
    assertEquals(
        Instant.class,
        getParameterOfType(
                signature.newWatermarkEstimator().extraParameters(),
                WatermarkEstimatorStateParameter.class)
            .estimatorStateT()
            .getRawType());
    assertEquals(
        InstantCoder.class, signature.getWatermarkEstimatorStateCoder().coderT().getRawType());
  }

  /**
   * Tests a splittable {@link DoFn} that defines all methods in their full form, correctly, using
   * generic types.
   */
  @Test
  public void testSplittableWithAllFunctionsGeneric() throws Exception {
    class GoodGenericSplittableDoFn<
            RestrictionT,
            TrackerT,
            RestrictionCoderT,
            WatermarkEstimatorStateT,
            WatermarkEstimatorStateCoderT,
            WatermarkEstimatorT>
        extends DoFn<Integer, String> {
      @ProcessElement
      public ProcessContinuation processElement(
          ProcessContext context, TrackerT tracker, WatermarkEstimatorT watermarkEstimatorT) {
        return null;
      }

      @GetInitialRestriction
      public RestrictionT getInitialRestriction(@Element Integer element) {
        return null;
      }

      @SplitRestriction
      public void splitRestriction(
          @Restriction RestrictionT restriction, OutputReceiver<RestrictionT> receiver) {}

      @TruncateRestriction
      public TruncateResult<RestrictionT> truncateRestriction(
          @Restriction RestrictionT restriction) {
        return TruncateResult.of(null);
      }

      @NewTracker
      public TrackerT newTracker(@Restriction RestrictionT restriction) {
        return null;
      }

      @GetRestrictionCoder
      public RestrictionCoderT getRestrictionCoder() {
        return null;
      }

      @GetSize
      public double getSize(@Restriction RestrictionT restriction) {
        return 1.0;
      }

      @GetInitialWatermarkEstimatorState
      public WatermarkEstimatorStateT getInitialWatermarkEstimatorState() {
        return null;
      }

      @GetWatermarkEstimatorStateCoder
      public WatermarkEstimatorStateCoderT getWatermarkEstimatorStateCoder() {
        return null;
      }

      @NewWatermarkEstimator
      public WatermarkEstimatorT newWatermarkEstimator(
          @WatermarkEstimatorState WatermarkEstimatorStateT watermarkEstimatorState) {
        return null;
      }
    }

    DoFnSignature signature =
        DoFnSignatures.getSignature(
            new GoodGenericSplittableDoFn<
                SomeRestriction,
                RestrictionTracker<SomeRestriction, ?>,
                SomeRestrictionCoder,
                Instant,
                InstantCoder,
                ManualWatermarkEstimator<Instant>>() {}.getClass());
    assertEquals(RestrictionTracker.class, signature.processElement().trackerT().getRawType());
    assertEquals(
        ManualWatermarkEstimator.class,
        signature.processElement().watermarkEstimatorT().getRawType());
    assertTrue(signature.processElement().isSplittable());
    assertTrue(signature.processElement().hasReturnValue());
    assertEquals(
        SomeRestriction.class, signature.getInitialRestriction().restrictionT().getRawType());
    assertEquals(
        SomeRestriction.class,
        getParameterOfType(
                signature.splitRestriction().extraParameters(), RestrictionParameter.class)
            .restrictionT()
            .getRawType());
    assertEquals(
        SomeRestriction.class,
        getParameterOfType(
                signature.truncateRestriction().extraParameters(), RestrictionParameter.class)
            .restrictionT()
            .getRawType());
    assertEquals(RestrictionTracker.class, signature.newTracker().trackerT().getRawType());
    assertEquals(
        SomeRestriction.class,
        getParameterOfType(signature.newTracker().extraParameters(), RestrictionParameter.class)
            .restrictionT()
            .getRawType());
    assertEquals(SomeRestrictionCoder.class, signature.getRestrictionCoder().coderT().getRawType());
    assertEquals(
        Instant.class,
        signature.getInitialWatermarkEstimatorState().watermarkEstimatorStateT().getRawType());
    assertEquals(
        Instant.class,
        getParameterOfType(
                signature.newWatermarkEstimator().extraParameters(),
                WatermarkEstimatorStateParameter.class)
            .estimatorStateT()
            .getRawType());
    assertEquals(
        InstantCoder.class, signature.getWatermarkEstimatorStateCoder().coderT().getRawType());
  }

  @Test
  public void testSplittableMissingGetInitialRestrictionMethod() throws Exception {
    class BadFn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(
          ProcessContext context, RestrictionTracker<SomeRestriction, Void> tracker) {}
    }

    thrown.expectMessage(
        "Splittable, but does not define the required @GetInitialRestriction method.");
    DoFnSignatures.getSignature(BadFn.class);
  }

  @Test
  public void testGetInitialRestrictionUnsupportedSchemaElementArgument() throws Exception {
    thrown.expectMessage(
        "Schema @Element are not supported for @GetInitialRestriction method. Found String, did you mean to use Integer?");
    DoFnSignatures.analyzeGetInitialRestrictionMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new AnonymousMethod() {
          SomeRestriction method(@Element String element) {
            return null;
          }
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(String.class),
        FnAnalysisContext.create());
  }

  @Test
  public void testGetInitialWatermarkEstimatorStateUnsupportedSchemaElementArgument()
      throws Exception {
    thrown.expectMessage(
        "Schema @Element are not supported for @GetInitialWatermarkEstimatorState method. Found String, did you mean to use Integer?");
    DoFnSignatures.analyzeGetInitialWatermarkEstimatorStateMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new AnonymousMethod() {
          SomeRestriction method(@Element String element) {
            return null;
          }
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(String.class),
        FnAnalysisContext.create());
  }

  @Test
  public void testNewWatermarkEstimatorUnsupportedSchemaElementArgument() throws Exception {
    thrown.expectMessage(
        "Schema @Element are not supported for @NewWatermarkEstimator method. Found String, did you mean to use Integer?");
    DoFnSignatures.analyzeNewWatermarkEstimatorMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new AnonymousMethod() {
          WatermarkEstimator<Instant> method(@Element String element) {
            return null;
          }
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(String.class),
        TypeDescriptor.of(SomeRestriction.class),
        TypeDescriptor.of(Instant.class),
        FnAnalysisContext.create());
  }

  @Test
  public void testMissingNewWatermarkEstimatorMethod() throws Exception {
    class BadFn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(
          ProcessContext context,
          RestrictionTracker<SomeRestriction, Void> tracker,
          ManualWatermarkEstimator<Instant> watermarkEstimator) {}

      @GetInitialRestriction
      public SomeRestriction getInitialRestriction() {
        return null;
      }

      @GetInitialWatermarkEstimatorState
      public Instant getInitialWatermarkEstimatorState() {
        return null;
      }
    }

    thrown.expectMessage(
        "Splittable, either @NewWatermarkEstimator method must be defined or Instant must implement HasDefaultWatermarkEstimator.");
    DoFnSignatures.getSignature(BadFn.class);
  }

  @Test
  public void testSplittableMissingNewTrackerMethod() throws Exception {
    class OtherRestriction {}

    class BadFn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(
          ProcessContext context, RestrictionTracker<OtherRestriction, Void> tracker) {}

      @GetInitialRestriction
      public OtherRestriction getInitialRestriction() {
        return null;
      }
    }

    thrown.expectMessage(
        "Splittable, either @NewTracker method must be defined or OtherRestriction must implement HasDefaultTracker.");
    DoFnSignatures.getSignature(BadFn.class);
  }

  abstract static class SomeDefaultTracker
      extends RestrictionTracker<RestrictionWithDefaultTracker, Void> {}

  abstract static class RestrictionWithDefaultTracker
      implements HasDefaultTracker<RestrictionWithDefaultTracker, SomeDefaultTracker> {}

  @Test
  public void testHasDefaultTracker() throws Exception {
    class Fn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(
          ProcessContext c, RestrictionTracker<RestrictionWithDefaultTracker, Void> tracker) {}

      @GetInitialRestriction
      public RestrictionWithDefaultTracker getInitialRestriction(@Element Integer element) {
        return null;
      }
    }

    DoFnSignature signature = DoFnSignatures.getSignature(Fn.class);
    assertEquals(RestrictionTracker.class, signature.processElement().trackerT().getRawType());
  }

  abstract static class SomeDefaultWatermarkEstimator
      implements WatermarkEstimator<WatermarkEstimatorStateWithDefaultWatermarkEstimator> {}

  abstract static class WatermarkEstimatorStateWithDefaultWatermarkEstimator
      implements HasDefaultWatermarkEstimator<
          WatermarkEstimatorStateWithDefaultWatermarkEstimator, SomeDefaultWatermarkEstimator> {}

  @Test
  public void testHasDefaultWatermarkEstimator() throws Exception {
    class Fn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(
          ProcessContext c,
          RestrictionTracker<SomeRestriction, Void> tracker,
          WatermarkEstimator<WatermarkEstimatorStateWithDefaultWatermarkEstimator>
              watermarkEstimator) {}

      @GetInitialRestriction
      public SomeRestriction getInitialRestriction(@Element Integer element) {
        return null;
      }

      @GetInitialWatermarkEstimatorState
      public WatermarkEstimatorStateWithDefaultWatermarkEstimator
          getInitialWatermarkEstimatorState() {
        return null;
      }
    }

    DoFnSignature signature = DoFnSignatures.getSignature(Fn.class);
    assertEquals(
        WatermarkEstimator.class, signature.processElement().watermarkEstimatorT().getRawType());
  }

  @Test
  public void testRestrictionHasDefaultTrackerProcessUsesWrongTracker() throws Exception {
    class Fn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(ProcessContext c, SomeRestrictionTracker tracker) {}

      @GetInitialRestriction
      public RestrictionWithDefaultTracker getInitialRestriction(@Element Integer element) {
        return null;
      }
    }

    thrown.expectMessage(
        "Has tracker type SomeRestrictionTracker, "
            + "but the DoFn's tracker type must be of type RestrictionTracker.");
    DoFnSignatures.getSignature(Fn.class);
  }

  @Test
  public void
      testWatermarkEstimatorStateHasDefaultWatermarkEstimatorProcessUsesWrongWatermarkEstimator()
          throws Exception {
    class Fn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(
          ProcessContext c,
          RestrictionTracker<SomeRestriction, Void> tracker,
          SomeDefaultWatermarkEstimator watermarkEstimator) {}

      @GetInitialRestriction
      public SomeRestriction getInitialRestriction(@Element Integer element) {
        return null;
      }

      @GetInitialWatermarkEstimatorState
      public WatermarkEstimatorStateWithDefaultWatermarkEstimator
          getInitialWatermarkEstimatorState() {
        return null;
      }
    }

    thrown.expectMessage(
        "Has watermark estimator type SomeDefaultWatermarkEstimator, but the DoFn's watermark estimator type must be one of [WatermarkEstimator, ManualWatermarkEstimator] types.");
    DoFnSignature signature = DoFnSignatures.getSignature(Fn.class);
  }

  @Test
  public void testNewTrackerReturnsWrongType() throws Exception {
    class BadFn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(
          ProcessContext context, RestrictionTracker<SomeRestriction, Void> tracker) {}

      @NewTracker
      public void newTracker(@Restriction SomeRestriction restriction) {}

      @GetInitialRestriction
      public SomeRestriction getInitialRestriction(@Element Integer element) {
        return null;
      }
    }

    thrown.expectMessage(
        "Returns void, but must return a subtype of RestrictionTracker<SomeRestriction, ?>");
    DoFnSignatures.getSignature(BadFn.class);
  }

  @Test
  public void testNewWatermarkEstimatorReturnsWrongType() throws Exception {
    class BadFn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(
          ProcessContext context, RestrictionTracker<SomeRestriction, Void> tracker) {}

      @GetInitialRestriction
      public SomeRestriction getInitialRestriction(@Element Integer element) {
        return null;
      }

      @NewWatermarkEstimator
      public void newWatermarkEstimator() {}
    }

    thrown.expectMessage("Returns void, but must return a subtype of WatermarkEstimator<Void>");
    DoFnSignatures.getSignature(BadFn.class);
  }

  @Test
  public void testGetInitialRestrictionMismatchesNewTracker() throws Exception {
    class BadFn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(
          ProcessContext context, RestrictionTracker<SomeRestriction, Void> tracker) {}

      @NewTracker
      public SomeRestrictionTracker newTracker(@Restriction SomeRestriction restriction) {
        return null;
      }

      @GetInitialRestriction
      public String getInitialRestriction(@Element Integer element) {
        return null;
      }
    }

    thrown.expectMessage("but must return a subtype of RestrictionTracker<String, ?>");
    thrown.expectMessage("newTracker(SomeRestriction): Returns SomeRestrictionTracker");
    DoFnSignatures.getSignature(BadFn.class);
  }

  @Test
  public void testGetInitialWatermarkEstimatorStateMismatchesNewWatermarkEstimator()
      throws Exception {
    class BadFn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(
          ProcessContext context, RestrictionTracker<SomeRestriction, Void> tracker) {}

      @GetInitialRestriction
      public SomeRestriction getInitialRestriction(@Element Integer element) {
        return null;
      }

      @GetInitialWatermarkEstimatorState
      public Instant getInitalWatermarkEstimatorState() {
        return null;
      }

      @NewWatermarkEstimator
      public WatermarkEstimator<Void> newWatermarkEstimator(
          @WatermarkEstimatorState Instant watermarkEstimatorState) {
        return null;
      }
    }

    thrown.expectMessage("but must return a subtype of WatermarkEstimator<Instant>");
    thrown.expectMessage("newWatermarkEstimator(Instant): Returns WatermarkEstimator<Void>");
    DoFnSignatures.getSignature(BadFn.class);
  }

  @Test
  public void testGetRestrictionCoderReturnsWrongType() throws Exception {
    class BadFn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(
          ProcessContext context, RestrictionTracker<SomeRestriction, Void> tracker) {}

      @NewTracker
      public SomeRestrictionTracker newTracker(@Restriction SomeRestriction restriction) {
        return null;
      }

      @GetInitialRestriction
      public SomeRestriction getInitialRestriction(@Element Integer element) {
        return null;
      }

      @GetRestrictionCoder
      public KvCoder getRestrictionCoder() {
        return null;
      }
    }

    thrown.expectMessage(
        "getRestrictionCoder() returns KvCoder which is not a subtype of Coder<SomeRestriction>");
    DoFnSignatures.getSignature(BadFn.class);
  }

  @Test
  public void testGetWatermarkEstimatorStateCoderReturnsWrongType() throws Exception {
    class BadFn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(
          ProcessContext context, RestrictionTracker<SomeRestriction, Void> tracker) {}

      @GetInitialRestriction
      public SomeRestriction getInitialRestriction(@Element Integer element) {
        return null;
      }

      @GetWatermarkEstimatorStateCoder
      public KvCoder getWatermarkEstimatorStateCoder() {
        return null;
      }
    }

    thrown.expectMessage(
        "getWatermarkEstimatorStateCoder() returns KvCoder which is not a subtype of Coder<Void>");
    DoFnSignatures.getSignature(BadFn.class);
  }

  @Test
  public void testSplitRestrictionReturnsWrongType() throws Exception {
    thrown.expectMessage(
        "OutputReceiver should be parameterized by "
            + "org.apache.beam.sdk.transforms.reflect.DoFnSignaturesSplittableDoFnTest$SomeRestriction");
    DoFnSignatures.analyzeSplitRestrictionMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new AnonymousMethod() {
          void method(
              @Element Integer element,
              @Restriction SomeRestriction restriction,
              DoFn.OutputReceiver<String> receiver) {}
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(String.class),
        TypeDescriptor.of(SomeRestriction.class),
        FnAnalysisContext.create());
  }

  @Test
  public void testSplitRestrictionUnsupportedSchemaElementArgument() throws Exception {
    thrown.expectMessage(
        "Schema @Element are not supported for @SplitRestriction method. Found String, did you mean to use Integer?");
    DoFnSignatures.analyzeSplitRestrictionMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new AnonymousMethod() {
          void method(
              @Element String element,
              @Restriction SomeRestriction restriction,
              DoFn.OutputReceiver<SomeRestriction> receiver) {}
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(String.class),
        TypeDescriptor.of(SomeRestriction.class),
        FnAnalysisContext.create());
  }

  @Test
  public void testSplitRestrictionWrongArgumentType() throws Exception {
    thrown.expectMessage("Object is not a valid context parameter.");
    DoFnSignatures.analyzeSplitRestrictionMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new AnonymousMethod() {
          private void method(
              @Element Integer element,
              @Restriction SomeRestriction restriction,
              DoFn.OutputReceiver<SomeRestriction> receiver,
              Object extra) {}
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(String.class),
        TypeDescriptor.of(SomeRestriction.class),
        FnAnalysisContext.create());
  }

  @Test
  public void testSplitRestrictionConsistentButWrongType() throws Exception {
    class OtherRestriction {}

    class BadFn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(
          ProcessContext context, RestrictionTracker<SomeRestriction, Void> tracker) {}

      @NewTracker
      public SomeRestrictionTracker newTracker(@Restriction SomeRestriction restriction) {
        return null;
      }

      @GetInitialRestriction
      public SomeRestriction getInitialRestriction(@Element Integer element) {
        return null;
      }

      @DoFn.SplitRestriction
      public void splitRestriction(
          @Element Integer element,
          @Restriction OtherRestriction restriction,
          OutputReceiver<OtherRestriction> receiver) {}
    }

    thrown.expectMessage("@GetInitialRestriction method uses restriction type SomeRestriction");
    thrown.expectMessage(
        "splitRestriction(Integer, OtherRestriction, OutputReceiver): Uses restriction type OtherRestriction");
    DoFnSignatures.getSignature(BadFn.class);
  }

  @Test
  public void testTruncateRestrictionReturnsWrongType() throws Exception {
    thrown.expectMessage("Must return TruncateResult<Restriction>");
    DoFnSignatures.analyzeTruncateRestrictionMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new AnonymousMethod() {
          void method(@Element Integer element, @Restriction SomeRestriction restriction) {}
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(SomeRestriction.class),
        FnAnalysisContext.create());
  }

  @Test
  public void testTruncateRestrictionUnsupportedOutputReceiverArgument() throws Exception {
    thrown.expectMessage("Illegal parameter type: OutputReceiverParameter");
    DoFnSignatures.analyzeTruncateRestrictionMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new AnonymousMethod() {
          Optional<SomeRestriction> method(
              @Element Integer element,
              @Restriction SomeRestriction restriction,
              DoFn.OutputReceiver<SomeRestriction> receiver) {
            return Optional.empty();
          }
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(SomeRestriction.class),
        FnAnalysisContext.create());
  }

  @Test
  public void testTruncateRestrictionUnsupportedSchemaElementArgument() throws Exception {
    thrown.expectMessage(
        "Schema @Element are not supported for @TruncateRestriction method. Found String, did you mean to use Integer?");
    DoFnSignatures.analyzeTruncateRestrictionMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new AnonymousMethod() {
          Optional<SomeRestriction> method(
              @Element String element, @Restriction SomeRestriction restriction) {
            return Optional.empty();
          }
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(SomeRestriction.class),
        FnAnalysisContext.create());
  }

  @Test
  public void testTruncateRestrictionWrongArgumentType() throws Exception {
    thrown.expectMessage("Object is not a valid context parameter.");
    DoFnSignatures.analyzeTruncateRestrictionMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new AnonymousMethod() {
          private Optional<SomeRestriction> method(
              @Element Integer element, @Restriction SomeRestriction restriction, Object extra) {
            return Optional.empty();
          }
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(SomeRestriction.class),
        FnAnalysisContext.create());
  }

  @Test
  public void testTruncateRestrictionConsistentButWrongType() throws Exception {
    class OtherRestriction {}

    class BadFn extends DoFn<Integer, String> {
      @ProcessElement
      public void process(
          ProcessContext context, RestrictionTracker<SomeRestriction, Void> tracker) {}

      @NewTracker
      public SomeRestrictionTracker newTracker(@Restriction SomeRestriction restriction) {
        return null;
      }

      @GetInitialRestriction
      public SomeRestriction getInitialRestriction(@Element Integer element) {
        return null;
      }

      @DoFn.TruncateRestriction
      public TruncateResult<OtherRestriction> truncateRestriction(
          @Element Integer element, @Restriction OtherRestriction restriction) {
        return TruncateResult.of(null);
      }
    }

    thrown.expectMessage("@GetInitialRestriction method uses restriction type SomeRestriction");
    thrown.expectMessage(
        "truncateRestriction(Integer, OtherRestriction): Uses restriction type OtherRestriction");
    DoFnSignatures.getSignature(BadFn.class);
  }

  @Test
  public void testUnsplittableMustNotDefineExtraMethods() throws Exception {
    class BadFn extends DoFn<Integer, String> {
      @ProcessElement
      public void processElement(ProcessContext context) {}

      @GetInitialRestriction
      public SomeRestriction getInitialRestriction() {
        return null;
      }

      @SplitRestriction
      public void splitRestriction(OutputReceiver<SomeRestriction> receiver) {}

      @NewTracker
      public SomeRestrictionTracker newTracker() {
        return null;
      }

      @GetRestrictionCoder
      public SomeRestrictionCoder getRestrictionCoder() {
        return null;
      }

      @GetSize
      public double getSize() {
        return 1.0;
      }

      @GetInitialWatermarkEstimatorState
      public Instant getInitialWatermarkEstimatorState() {
        return null;
      }

      @GetWatermarkEstimatorStateCoder
      public InstantCoder getWatermarkEstimatorStateCoder() {
        return null;
      }

      @NewWatermarkEstimator
      public WatermarkEstimator<Instant> newWatermarkEstimator() {
        return null;
      }
    }

    thrown.expectMessage(
        "Non-splittable, but defines methods: "
            + "[@GetInitialRestriction, @SplitRestriction, @NewTracker, @GetRestrictionCoder, @GetSize, @GetInitialWatermarkEstimatorState, @GetWatermarkEstimatorStateCoder, @NewWatermarkEstimator]");
    DoFnSignatures.getSignature(BadFn.class);
  }

  @Test
  public void testNewTrackerUnsupportedSchemaElementArgument() throws Exception {
    thrown.expectMessage(
        "Schema @Element are not supported for @NewTracker method. Found String, did you mean to use Integer?");
    DoFnSignatures.analyzeNewTrackerMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new AnonymousMethod() {
          SomeRestrictionTracker method(
              @Element String element, @Restriction SomeRestriction restriction) {
            return null;
          }
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(String.class),
        TypeDescriptor.of(SomeRestriction.class),
        FnAnalysisContext.create());
  }

  @Test
  public void testNewTrackerWrongArgumentType() throws Exception {
    thrown.expectMessage("Object is not a valid context parameter.");
    DoFnSignatures.analyzeNewTrackerMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new AnonymousMethod() {
          private SomeRestrictionTracker method(
              @Restriction SomeRestriction restriction, Object extra) {
            return null;
          }
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(String.class),
        TypeDescriptor.of(SomeRestriction.class),
        FnAnalysisContext.create());
  }

  @Test
  public void testNewTrackerInconsistent() throws Exception {
    thrown.expectMessage(
        "Returns SomeRestrictionTracker, "
            + "but must return a subtype of RestrictionTracker<String, ?>");
    DoFnSignatures.analyzeNewTrackerMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new AnonymousMethod() {
          private SomeRestrictionTracker method(@Restriction String restriction) {
            return null;
          }
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(String.class),
        TypeDescriptor.of(String.class),
        FnAnalysisContext.create());
  }

  @Test
  public void testGetSizeInvalidReturnType() throws Exception {
    thrown.expectMessage("Returns void, but must return a double");
    DoFnSignatures.analyzeGetSizeMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new AnonymousMethod() {
          void method(@Element Integer element, @Restriction SomeRestriction restriction) {}
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(String.class),
        TypeDescriptor.of(SomeRestriction.class),
        FnAnalysisContext.create());
  }

  @Test
  public void testGetSizeUnsupportedSchemaElementArgument() throws Exception {
    thrown.expectMessage(
        "Schema @Element are not supported for @GetSize method. Found String, did you mean to use Integer?");
    DoFnSignatures.analyzeGetSizeMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new AnonymousMethod() {
          double method(@Element String element, @Restriction SomeRestriction restriction) {
            return 1.0;
          }
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(String.class),
        TypeDescriptor.of(SomeRestriction.class),
        FnAnalysisContext.create());
  }

  private static <T extends Parameter> T getParameterOfType(
      List<Parameter> parameters, Class<T> type) {
    return (T)
        Iterables.getOnlyElement(Iterables.filter(parameters, input -> type.isInstance(input)));
  }
}
