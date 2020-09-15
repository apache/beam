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

import static org.apache.beam.sdk.transforms.reflect.DoFnSignaturesTestUtils.errors;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.GroupingState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.testing.SerializableMatchers;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.BundleFinalizerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.ElementParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.FinishBundleContextParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.OutputReceiverParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.PaneInfoParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.PipelineOptionsParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.ProcessContextParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.SchemaElementParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.SideInputParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.StartBundleContextParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.StateParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.TaggedOutputReceiverParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.TimeDomainParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.TimerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.TimestampParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.WindowParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures.FnAnalysisContext;
import org.apache.beam.sdk.transforms.reflect.DoFnSignaturesTestUtils.FakeDoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DoFnSignatures}. */
@RunWith(JUnit4.class)
public class DoFnSignaturesTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBasicDoFnProcessContext() throws Exception {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @ProcessElement
              public void process(ProcessContext c) {}
            }.getClass());

    assertThat(sig.processElement().extraParameters().size(), equalTo(1));
    assertThat(
        sig.processElement().extraParameters().get(0), instanceOf(ProcessContextParameter.class));
  }

  @Test
  public void testBasicDoFnAllParameters() throws Exception {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @ProcessElement
              public void process(
                  @Element String element,
                  @Timestamp Instant timestamp,
                  BoundedWindow window,
                  PaneInfo paneInfo,
                  OutputReceiver<String> receiver,
                  PipelineOptions options,
                  @SideInput("tag1") String input1,
                  @SideInput("tag2") Integer input2,
                  BundleFinalizer bundleFinalizer) {}
            }.getClass());

    assertThat(sig.processElement().extraParameters().size(), equalTo(9));
    assertThat(sig.processElement().extraParameters().get(0), instanceOf(ElementParameter.class));
    assertThat(sig.processElement().extraParameters().get(1), instanceOf(TimestampParameter.class));
    assertThat(sig.processElement().extraParameters().get(2), instanceOf(WindowParameter.class));
    assertThat(sig.processElement().extraParameters().get(3), instanceOf(PaneInfoParameter.class));
    assertThat(
        sig.processElement().extraParameters().get(4), instanceOf(OutputReceiverParameter.class));
    assertThat(
        sig.processElement().extraParameters().get(5), instanceOf(PipelineOptionsParameter.class));
    assertThat(sig.processElement().extraParameters().get(6), instanceOf(SideInputParameter.class));
    assertThat(sig.processElement().extraParameters().get(7), instanceOf(SideInputParameter.class));
    assertThat(
        sig.processElement().extraParameters().get(8), instanceOf(BundleFinalizerParameter.class));
  }

  @Test
  public void testBasicDoFnMultiOutputReceiver() throws Exception {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @ProcessElement
              public void process(MultiOutputReceiver receiver) {}
            }.getClass());

    assertThat(sig.processElement().extraParameters().size(), equalTo(1));
    assertThat(
        sig.processElement().extraParameters().get(0),
        instanceOf(TaggedOutputReceiverParameter.class));
  }

  @Test
  public void testMismatchingElementType() throws Exception {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @ProcessElement
              public void process(@Element Integer element) {}
            }.getClass());
    assertThat(sig.processElement().extraParameters().size(), equalTo(1));
    assertThat(
        sig.processElement().extraParameters().get(0), instanceOf(SchemaElementParameter.class));
  }

  @Test
  public void testWrongTimestampType() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("@Timestamp argument must have type org.joda.time.Instant");
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @ProcessElement
              public void process(@Timestamp String timestamp) {}
            }.getClass());
  }

  @Test
  public void testWrongOutputReceiverType() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("OutputReceiver should be parameterized by java.lang.String");
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @ProcessElement
              public void process(OutputReceiver<Integer> receiver) {}
            }.getClass());
  }

  @Test
  public void testRowParameterWithoutFieldAccess() {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @ProcessElement
              public void process(@Element Row row) {}
            }.getClass());
    assertFalse(sig.processElement().getSchemaElementParameters().isEmpty());
  }

  @Test
  public void testMultipleSchemaParameters() {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @ProcessElement
              public void process(
                  @Element Row row1,
                  @Timestamp Instant ts,
                  @Element Row row2,
                  OutputReceiver<String> o,
                  @Element Integer intParameter) {}
            }.getClass());
    assertEquals(3, sig.processElement().getSchemaElementParameters().size());
    assertEquals(0, sig.processElement().getSchemaElementParameters().get(0).index());
    assertEquals(
        TypeDescriptors.rows(),
        sig.processElement().getSchemaElementParameters().get(0).elementT());
    assertEquals(1, sig.processElement().getSchemaElementParameters().get(1).index());
    assertEquals(
        TypeDescriptors.rows(),
        sig.processElement().getSchemaElementParameters().get(1).elementT());
    assertEquals(2, sig.processElement().getSchemaElementParameters().get(2).index());
    assertEquals(
        TypeDescriptors.integers(),
        sig.processElement().getSchemaElementParameters().get(2).elementT());
  }

  @Test
  public void testFieldAccess() throws IllegalAccessException {
    FieldAccessDescriptor descriptor = FieldAccessDescriptor.withFieldNames("foo", "bar");
    DoFn<String, String> doFn =
        new DoFn<String, String>() {
          @FieldAccess("foo")
          final FieldAccessDescriptor fieldAccess = descriptor;

          @ProcessElement
          public void process(@FieldAccess("foo") @Element Row row) {}
        };

    DoFnSignature sig = DoFnSignatures.getSignature(doFn.getClass());
    assertThat(sig.fieldAccessDeclarations().get("foo"), notNullValue());
    Field field = sig.fieldAccessDeclarations().get("foo").field();
    assertThat(field.getName(), equalTo("fieldAccess"));
    assertThat(field.get(doFn), equalTo(descriptor));

    assertFalse(sig.processElement().getSchemaElementParameters().isEmpty());
  }

  @Test
  public void testRowReceiver() {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @ProcessElement
              public void process(OutputReceiver<Row> rowReceiver) {}
            }.getClass());
    assertThat(sig.processElement().getMainOutputReceiver().isRowReceiver(), is(true));
  }

  @Test
  public void testIsAsync() {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @ProcessElement
              public void process(OutputReceiver<CompletionStage<String>> asyncReceiver) {}
            }.getClass());
    assertThat(sig.processElement().getMainOutputReceiver().isAsync(), is(true));
  }

  @Test
  public void testBadOutputTForAsync() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("OutputReceiver should be parameterized by java.lang.Integer");
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, Integer>() {
              @ProcessElement
              public void process(OutputReceiver<CompletionStage<String>> asyncReceiver) {}
            }.getClass());
  }

  @Test
  public void testRequiresStableInputProcessElement() throws Exception {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @ProcessElement
              @RequiresStableInput
              public void process(ProcessContext c) {}
            }.getClass());

    assertThat(sig.processElement().requiresStableInput(), is(true));
  }

  @Test
  public void testBadExtraContext() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("int is not a valid context parameter");

    DoFnSignatures.analyzeStartBundleMethod(
        errors(),
        TypeDescriptor.of(FakeDoFn.class),
        new DoFnSignaturesTestUtils.AnonymousMethod() {
          void method(DoFn<Integer, String>.StartBundleContext c, int n) {}
        }.getMethod(),
        TypeDescriptor.of(Integer.class),
        TypeDescriptor.of(String.class),
        FnAnalysisContext.create());
  }

  @Test
  public void testMultipleStartBundleElement() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Found multiple methods annotated with @StartBundle");
    thrown.expectMessage("bar()");
    thrown.expectMessage("baz()");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.getSignature(
        new DoFn<String, String>() {
          @ProcessElement
          public void foo() {}

          @StartBundle
          public void bar() {}

          @StartBundle
          public void baz() {}
        }.getClass());
  }

  @Test
  public void testMultipleFinishBundleMethods() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Found multiple methods annotated with @FinishBundle");
    thrown.expectMessage("bar(FinishBundleContext)");
    thrown.expectMessage("baz(FinishBundleContext)");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.getSignature(
        new DoFn<String, String>() {
          @ProcessElement
          public void foo(ProcessContext context) {}

          @FinishBundle
          public void bar(FinishBundleContext context) {}

          @FinishBundle
          public void baz(FinishBundleContext context) {}
        }.getClass());
  }

  @Test
  public void testPrivateStartBundle() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("startBundle()");
    thrown.expectMessage("Must be public");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.getSignature(
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement() {}

          @StartBundle
          void startBundle() {}
        }.getClass());
  }

  @Test
  public void testStartBundleWithAllParameters() throws Exception {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @ProcessElement
              public void processElement() {}

              @StartBundle
              public void startBundle(
                  StartBundleContext context,
                  BundleFinalizer bundleFinalizer,
                  PipelineOptions options) {}
            }.getClass());
    assertThat(sig.startBundle().extraParameters().size(), equalTo(3));
    assertThat(
        sig.startBundle().extraParameters().get(0), instanceOf(StartBundleContextParameter.class));
    assertThat(
        sig.startBundle().extraParameters().get(1), instanceOf(BundleFinalizerParameter.class));
    assertThat(
        sig.startBundle().extraParameters().get(2), instanceOf(PipelineOptionsParameter.class));
  }

  @Test
  public void testPrivateFinishBundle() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("finishBundle()");
    thrown.expectMessage("Must be public");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.getSignature(
        new DoFn<String, String>() {
          @ProcessElement
          public void processElement() {}

          @FinishBundle
          void finishBundle() {}
        }.getClass());
  }

  @Test
  public void testFinishBundleWithAllParameters() throws Exception {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @ProcessElement
              public void processElement() {}

              @FinishBundle
              public void finishBundle(
                  FinishBundleContext context,
                  BundleFinalizer bundleFinalizer,
                  PipelineOptions pipelineOptions) {}
            }.getClass());
    assertThat(sig.finishBundle().extraParameters().size(), equalTo(3));
    assertThat(
        sig.finishBundle().extraParameters().get(0),
        instanceOf(FinishBundleContextParameter.class));
    assertThat(
        sig.finishBundle().extraParameters().get(1), instanceOf(BundleFinalizerParameter.class));
    assertThat(
        sig.finishBundle().extraParameters().get(2), instanceOf(PipelineOptionsParameter.class));
  }

  @Test
  public void testTimerIdWithWrongType() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("TimerId");
    thrown.expectMessage("TimerSpec");
    thrown.expectMessage("bizzle");
    thrown.expectMessage(not(mentionsState()));
    DoFnSignatures.getSignature(
        new DoFn<String, String>() {
          @TimerId("foo")
          private final String bizzle = "bazzle";

          @ProcessElement
          public void foo(ProcessContext context) {}
        }.getClass());
  }

  @Test
  public void testTimerIdNoCallback() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No callback registered");
    thrown.expectMessage("my-id");
    thrown.expectMessage(not(mentionsState()));
    thrown.expectMessage(mentionsTimers());
    DoFnSignatures.getSignature(
        new DoFn<KV<String, Integer>, Long>() {
          @TimerId("my-id")
          private final TimerSpec myfield1 = TimerSpecs.timer(TimeDomain.EVENT_TIME);

          @ProcessElement
          public void foo(ProcessContext context) {}
        }.getClass());
  }

  @Test
  public void testOnTimerNoDeclaration() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Callback");
    thrown.expectMessage("undeclared timer");
    thrown.expectMessage("onFoo");
    thrown.expectMessage("my-id");
    thrown.expectMessage(not(mentionsState()));
    thrown.expectMessage(mentionsTimers());
    DoFnSignatures.getSignature(
        new DoFn<KV<String, Integer>, Long>() {
          @OnTimer("my-id")
          public void onFoo() {}

          @ProcessElement
          public void foo(ProcessContext context) {}
        }.getClass());
  }

  @Test
  public void testOnTimerDeclaredInSuperclass() throws Exception {
    class DoFnDeclaringTimerAndProcessElement extends DoFn<KV<String, Integer>, Long> {
      public static final String TIMER_ID = "my-timer-id";

      @TimerId(TIMER_ID)
      private final TimerSpec bizzle = TimerSpecs.timer(TimeDomain.EVENT_TIME);

      @ProcessElement
      public void foo(ProcessContext context) {}
    }

    DoFnDeclaringTimerAndProcessElement fn =
        new DoFnDeclaringTimerAndProcessElement() {
          @OnTimer(DoFnDeclaringTimerAndProcessElement.TIMER_ID)
          public void onTimerFoo() {}
        };

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Callback");
    thrown.expectMessage("declared in a different class");
    thrown.expectMessage(DoFnDeclaringTimerAndProcessElement.TIMER_ID);
    thrown.expectMessage(fn.getClass().getSimpleName());
    thrown.expectMessage(not(mentionsState()));
    thrown.expectMessage(mentionsTimers());
    DoFnSignatures.getSignature(fn.getClass());
  }

  @Test
  public void testUsageOfTimerDeclaredInSuperclass() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("process");
    thrown.expectMessage("declared in a different class");
    thrown.expectMessage(DoFnDeclaringTimerAndCallback.TIMER_ID);
    thrown.expectMessage(not(mentionsState()));
    thrown.expectMessage(mentionsTimers());
    DoFnSignatures.getSignature(
        new DoFnDeclaringTimerAndCallback() {
          @ProcessElement
          public void process(
              ProcessContext context,
              @TimerId(DoFnDeclaringTimerAndCallback.TIMER_ID) Timer timer) {}
        }.getClass());
  }

  @Test
  public void testTimerParameterDuplicate() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("duplicate");
    thrown.expectMessage("my-id");
    thrown.expectMessage("myProcessElement");
    thrown.expectMessage("index 2");
    thrown.expectMessage(not(mentionsState()));
    DoFnSignatures.getSignature(
        new DoFn<KV<String, Integer>, Long>() {
          @TimerId("my-id")
          private final TimerSpec myfield = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

          @ProcessElement
          public void myProcessElement(
              ProcessContext context, @TimerId("my-id") Timer one, @TimerId("my-id") Timer two) {}

          @OnTimer("my-id")
          public void onWhatever() {}
        }.getClass());
  }

  @Test
  public void testOnTimerDeclaredInSubclass() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Callback");
    thrown.expectMessage("declared in a different class");
    thrown.expectMessage(DoFnWithOnlyCallback.TIMER_ID);
    thrown.expectMessage(not(mentionsState()));
    thrown.expectMessage(mentionsTimers());
    DoFnSignatures.getSignature(
        new DoFnWithOnlyCallback() {
          @TimerId(DoFnWithOnlyCallback.TIMER_ID)
          private final TimerSpec myfield1 = TimerSpecs.timer(TimeDomain.EVENT_TIME);

          @ProcessElement
          @Override
          public void foo(ProcessContext context) {}
        }.getClass());
  }

  @Test
  public void testWindowParamOnTimer() throws Exception {
    final String timerId = "some-timer-id";
    final String timerDeclarationId = TimerDeclaration.PREFIX + timerId;

    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @TimerId(timerId)
              private final TimerSpec myfield1 = TimerSpecs.timer(TimeDomain.EVENT_TIME);

              @ProcessElement
              public void process(ProcessContext c) {}

              @OnTimer(timerId)
              public void onTimer(BoundedWindow w) {}
            }.getClass());

    assertThat(sig.onTimerMethods().get(timerDeclarationId).extraParameters().size(), equalTo(1));
    assertThat(
        sig.onTimerMethods().get(timerDeclarationId).extraParameters().get(0),
        instanceOf(WindowParameter.class));
  }

  @Test
  public void testAllParamsOnTimer() throws Exception {
    final String timerId = "some-timer-id";
    final String timerDeclarationId = TimerDeclaration.PREFIX + timerId;

    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @TimerId(timerId)
              private final TimerSpec myfield1 = TimerSpecs.timer(TimeDomain.EVENT_TIME);

              @ProcessElement
              public void process(ProcessContext c) {}

              @OnTimer(timerId)
              public void onTimer(
                  @Timestamp Instant timestamp, TimeDomain timeDomain, BoundedWindow w) {}
            }.getClass());

    assertThat(sig.onTimerMethods().get(timerDeclarationId).extraParameters().size(), equalTo(3));
    assertThat(
        sig.onTimerMethods().get(timerDeclarationId).extraParameters().get(0),
        instanceOf(TimestampParameter.class));
    assertThat(
        sig.onTimerMethods().get(timerDeclarationId).extraParameters().get(1),
        instanceOf(TimeDomainParameter.class));
    assertThat(
        sig.onTimerMethods().get(timerDeclarationId).extraParameters().get(2),
        instanceOf(WindowParameter.class));
  }

  @Test
  public void testPipelineOptionsParameter() throws Exception {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @ProcessElement
              public void process(ProcessContext c, PipelineOptions options) {}
            }.getClass());

    assertThat(
        sig.processElement().extraParameters(),
        Matchers.hasItem(instanceOf(Parameter.PipelineOptionsParameter.class)));
  }

  @Test
  public void testDeclAndUsageOfTimerInSuperclass() throws Exception {
    DoFnSignature sig =
        DoFnSignatures.getSignature(new DoFnOverridingAbstractTimerUse().getClass());

    assertThat(sig.timerDeclarations().size(), equalTo(1));
    assertThat(sig.processElement().extraParameters().size(), equalTo(2));

    DoFnSignature.TimerDeclaration decl =
        sig.timerDeclarations()
            .get(TimerDeclaration.PREFIX + DoFnOverridingAbstractTimerUse.TIMER_ID);
    TimerParameter timerParam = (TimerParameter) sig.processElement().extraParameters().get(1);

    assertThat(
        decl.field(),
        equalTo(DoFnDeclaringTimerAndAbstractUse.class.getDeclaredField("myTimerSpec")));

    // The method we pull out is the superclass method; this is what allows validation to remain
    // simple. The later invokeDynamic instruction causes it to invoke the actual implementation.
    assertThat(timerParam.referent(), equalTo(decl));
  }

  /**
   * In this particular test, the super class annotated both the timer and the callback, and the
   * subclass overrides an abstract method. This is allowed.
   */
  @Test
  public void testOnTimerDeclaredAndUsedInSuperclass() throws Exception {
    DoFnSignature sig =
        DoFnSignatures.getSignature(new DoFnOverridingAbstractCallback().getClass());

    assertThat(sig.timerDeclarations().size(), equalTo(1));
    assertThat(sig.onTimerMethods().size(), equalTo(1));

    DoFnSignature.TimerDeclaration decl =
        sig.timerDeclarations()
            .get(TimerDeclaration.PREFIX + DoFnDeclaringTimerAndAbstractCallback.TIMER_ID);
    DoFnSignature.OnTimerMethod callback =
        sig.onTimerMethods()
            .get(TimerDeclaration.PREFIX + DoFnDeclaringTimerAndAbstractCallback.TIMER_ID);

    assertThat(
        decl.field(),
        equalTo(DoFnDeclaringTimerAndAbstractCallback.class.getDeclaredField("myTimerSpec")));

    // The method we pull out is the superclass method; this is what allows validation to remain
    // simple. The later invokeDynamic instruction causes it to invoke the actual implementation.
    assertThat(
        callback.targetMethod(),
        equalTo(DoFnDeclaringTimerAndAbstractCallback.class.getDeclaredMethod("onMyTimer")));
  }

  @Test
  public void testTimerIdDuplicate() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Duplicate");
    thrown.expectMessage("TimerId");
    thrown.expectMessage("my-id");
    thrown.expectMessage("myfield1");
    thrown.expectMessage("myfield2");
    thrown.expectMessage(not(containsString("State"))); // lowercase "state" is in the package name
    thrown.expectMessage(mentionsTimers());
    DoFnSignatures.getSignature(
        new DoFn<KV<String, Integer>, Long>() {
          @TimerId("my-id")
          private final TimerSpec myfield1 = TimerSpecs.timer(TimeDomain.EVENT_TIME);

          @TimerId("my-id")
          private final TimerSpec myfield2 = TimerSpecs.timer(TimeDomain.EVENT_TIME);

          @ProcessElement
          public void foo(ProcessContext context) {}
        }.getClass());
  }

  @Test
  public void testTimerIdNonFinal() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Timer declarations must be final");
    thrown.expectMessage("Non-final field");
    thrown.expectMessage("myfield");
    thrown.expectMessage(not(containsString("State"))); // lowercase "state" is in the package name
    thrown.expectMessage(mentionsTimers());
    DoFnSignatures.getSignature(
        new DoFn<KV<String, Integer>, Long>() {
          @TimerId("my-timer-id")
          private TimerSpec myfield = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

          @ProcessElement
          public void foo(ProcessContext context) {}
        }.getClass());
  }

  @Test
  public void testSimpleTimerIdAnonymousDoFn() throws Exception {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<KV<String, Integer>, Long>() {
              @TimerId("foo")
              private final TimerSpec bizzle = TimerSpecs.timer(TimeDomain.EVENT_TIME);

              @ProcessElement
              public void foo(ProcessContext context) {}

              @OnTimer("foo")
              public void onFoo() {}
            }.getClass());

    final String timerDeclarationId = TimerDeclaration.PREFIX + "foo";
    assertThat(sig.timerDeclarations().size(), equalTo(1));
    DoFnSignature.TimerDeclaration decl = sig.timerDeclarations().get(timerDeclarationId);

    assertThat(decl.id(), equalTo(timerDeclarationId));
    assertThat(decl.field().getName(), equalTo("bizzle"));
  }

  @Test
  public void testSimpleTimerWithContext() throws Exception {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<KV<String, Integer>, Long>() {
              @TimerId("foo")
              private final TimerSpec bizzle = TimerSpecs.timer(TimeDomain.EVENT_TIME);

              @ProcessElement
              public void foo(ProcessContext context) {}

              @OnTimer("foo")
              public void onFoo(OnTimerContext c) {}
            }.getClass());

    final String timerDeclarationId = TimerDeclaration.PREFIX + "foo";
    assertThat(sig.timerDeclarations().size(), equalTo(1));
    DoFnSignature.TimerDeclaration decl = sig.timerDeclarations().get(timerDeclarationId);

    assertThat(decl.id(), equalTo(timerDeclarationId));
    assertThat(decl.field().getName(), equalTo("bizzle"));

    assertThat(
        sig.onTimerMethods().get(timerDeclarationId).extraParameters().get(0),
        equalTo((Parameter) Parameter.onTimerContext()));
  }

  @Test
  public void testProcessElementWithOnTimerContextRejected() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    // The message should at least mention @ProcessElement and OnTimerContext
    thrown.expectMessage("@" + DoFn.ProcessElement.class.getSimpleName());
    thrown.expectMessage(DoFn.OnTimerContext.class.getSimpleName());

    DoFnSignatures.getSignature(
        new DoFn<KV<String, Integer>, Long>() {
          @TimerId("foo")
          private final TimerSpec bizzle = TimerSpecs.timer(TimeDomain.EVENT_TIME);

          @ProcessElement
          public void foo(ProcessContext context, OnTimerContext bogus) {}

          @OnTimer("foo")
          public void onFoo() {}
        }.getClass());
  }

  @Test
  public void testSimpleTimerIdNamedDoFn() throws Exception {
    class DoFnForTestSimpleTimerIdNamedDoFn extends DoFn<KV<String, Integer>, Long> {
      @TimerId("foo")
      private final TimerSpec bizzle = TimerSpecs.timer(TimeDomain.EVENT_TIME);

      @ProcessElement
      public void foo(ProcessContext context) {}

      @OnTimer("foo")
      public void onFoo() {}
    }

    // Test classes at the bottom of the file
    DoFnSignature sig = DoFnSignatures.signatureForDoFn(new DoFnForTestSimpleTimerIdNamedDoFn());

    final String timerDeclarationId = TimerDeclaration.PREFIX + "foo";

    assertThat(sig.timerDeclarations().size(), equalTo(1));
    DoFnSignature.TimerDeclaration decl = sig.timerDeclarations().get(timerDeclarationId);

    assertThat(decl.id(), equalTo(timerDeclarationId));
    assertThat(
        decl.field(), equalTo(DoFnForTestSimpleTimerIdNamedDoFn.class.getDeclaredField("bizzle")));
  }

  @Test
  public void testStateIdWithWrongType() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("StateId");
    thrown.expectMessage("StateSpec");
    thrown.expectMessage(not(mentionsTimers()));
    DoFnSignatures.getSignature(
        new DoFn<String, String>() {
          @StateId("foo")
          private final String bizzle = "bazzle";

          @ProcessElement
          public void foo(ProcessContext context) {}
        }.getClass());
  }

  @Test
  public void testStateIdDuplicate() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Duplicate");
    thrown.expectMessage("StateId");
    thrown.expectMessage("my-id");
    thrown.expectMessage("myfield1");
    thrown.expectMessage("myfield2");
    thrown.expectMessage(not(mentionsTimers()));
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<KV<String, Integer>, Long>() {
              @StateId("my-id")
              private final StateSpec<ValueState<Integer>> myfield1 =
                  StateSpecs.value(VarIntCoder.of());

              @StateId("my-id")
              private final StateSpec<ValueState<Long>> myfield2 =
                  StateSpecs.value(VarLongCoder.of());

              @ProcessElement
              public void foo(ProcessContext context) {}
            }.getClass());
  }

  @Test
  public void testStateIdNonFinal() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("State declarations must be final");
    thrown.expectMessage("Non-final field");
    thrown.expectMessage("myfield");
    thrown.expectMessage(not(mentionsTimers()));
    DoFnSignatures.getSignature(
        new DoFn<KV<String, Integer>, Long>() {
          @StateId("my-id")
          private StateSpec<ValueState<Integer>> myfield = StateSpecs.value(VarIntCoder.of());

          @ProcessElement
          public void foo(ProcessContext context) {}
        }.getClass());
  }

  @Test
  public void testStateParameterNoAnnotation() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("missing StateId annotation");
    thrown.expectMessage("myProcessElement");
    thrown.expectMessage("index 1");
    thrown.expectMessage(not(mentionsTimers()));
    DoFnSignatures.getSignature(
        new DoFn<KV<String, Integer>, Long>() {
          @ProcessElement
          public void myProcessElement(ProcessContext context, ValueState<Integer> noAnnotation) {}
        }.getClass());
  }

  @Test
  public void testStateParameterUndeclared() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("undeclared");
    thrown.expectMessage("my-id");
    thrown.expectMessage("myProcessElement");
    thrown.expectMessage("index 1");
    thrown.expectMessage(not(mentionsTimers()));
    DoFnSignatures.getSignature(
        new DoFn<KV<String, Integer>, Long>() {
          @ProcessElement
          public void myProcessElement(
              ProcessContext context, @StateId("my-id") ValueState<Integer> undeclared) {}
        }.getClass());
  }

  @Test
  public void testStateParameterAlwaysFetched() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("ReadableStates");
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<KV<String, Integer>, Long>() {
              @StateId("my-id")
              private final StateSpec<MapState<Integer, Integer>> myfield =
                  StateSpecs.map(VarIntCoder.of(), VarIntCoder.of());

              @ProcessElement
              public void myProcessElement(
                  ProcessContext context,
                  @AlwaysFetched @StateId("my-id") MapState<Integer, Integer> one) {}
            }.getClass());
    StateParameter stateParameter = (StateParameter) sig.processElement().extraParameters().get(1);
    assertTrue(stateParameter.alwaysFetched());
  }

  @Test
  public void testStateParameterAlwaysFetchNonReadableState() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("ReadableStates");
    DoFnSignatures.getSignature(
        new DoFn<KV<String, Integer>, Long>() {
          @StateId("my-id")
          private final StateSpec<MapState<Integer, Integer>> myfield =
              StateSpecs.map(VarIntCoder.of(), VarIntCoder.of());

          @ProcessElement
          public void myProcessElement(
              ProcessContext context,
              @AlwaysFetched @StateId("my-id") MapState<Integer, Integer> one) {}
        }.getClass());
  }

  @Test
  public void testStateParameterDuplicate() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("duplicate");
    thrown.expectMessage("my-id");
    thrown.expectMessage("myProcessElement");
    thrown.expectMessage("index 2");
    thrown.expectMessage(not(mentionsTimers()));
    DoFnSignatures.getSignature(
        new DoFn<KV<String, Integer>, Long>() {
          @StateId("my-id")
          private final StateSpec<ValueState<Integer>> myfield = StateSpecs.value(VarIntCoder.of());

          @ProcessElement
          public void myProcessElement(
              ProcessContext context,
              @StateId("my-id") ValueState<Integer> one,
              @StateId("my-id") ValueState<Integer> two) {}
        }.getClass());
  }

  @Test
  public void testStateParameterWrongStateType() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("WatermarkHoldState");
    thrown.expectMessage("reference to");
    thrown.expectMessage("supertype");
    thrown.expectMessage("ValueState");
    thrown.expectMessage("my-id");
    thrown.expectMessage("myProcessElement");
    thrown.expectMessage("index 1");
    thrown.expectMessage(not(mentionsTimers()));
    DoFnSignatures.getSignature(
        new DoFn<KV<String, Integer>, Long>() {
          @StateId("my-id")
          private final StateSpec<ValueState<Integer>> myfield = StateSpecs.value(VarIntCoder.of());

          @ProcessElement
          public void myProcessElement(
              ProcessContext context, @StateId("my-id") WatermarkHoldState watermark) {}
        }.getClass());
  }

  @Test
  public void testStateParameterWrongGenericType() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("ValueState<String>");
    thrown.expectMessage("reference to");
    thrown.expectMessage("supertype");
    thrown.expectMessage("ValueState<Integer>");
    thrown.expectMessage("my-id");
    thrown.expectMessage("myProcessElement");
    thrown.expectMessage("index 1");
    thrown.expectMessage(not(mentionsTimers()));
    DoFnSignatures.getSignature(
        new DoFn<KV<String, Integer>, Long>() {
          @StateId("my-id")
          private final StateSpec<ValueState<Integer>> myfield = StateSpecs.value(VarIntCoder.of());

          @ProcessElement
          public void myProcessElement(
              ProcessContext context, @StateId("my-id") ValueState<String> stringState) {}
        }.getClass());
  }

  @Test
  public void testGoodStateParameterSuperclassStateType() throws Exception {
    DoFnSignatures.getSignature(
        new DoFn<KV<String, Integer>, Long>() {
          @StateId("my-id")
          private final StateSpec<CombiningState<Integer, int[], Integer>> state =
              StateSpecs.combining(Sum.ofIntegers());

          @ProcessElement
          public void myProcessElement(
              ProcessContext context,
              @StateId("my-id") GroupingState<Integer, Integer> groupingState) {}
        }.getClass());
  }

  @Test
  public void testSimpleStateIdAnonymousDoFn() throws Exception {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<KV<String, Integer>, Long>() {
              @StateId("foo")
              private final StateSpec<ValueState<Integer>> bizzle =
                  StateSpecs.value(VarIntCoder.of());

              @ProcessElement
              public void foo(ProcessContext context) {}
            }.getClass());

    assertThat(sig.stateDeclarations().size(), equalTo(1));
    DoFnSignature.StateDeclaration decl = sig.stateDeclarations().get("foo");

    assertThat(decl.id(), equalTo("foo"));
    assertThat(decl.field().getName(), equalTo("bizzle"));
    assertThat(
        decl.stateType(),
        Matchers.<TypeDescriptor<?>>equalTo(new TypeDescriptor<ValueState<Integer>>() {}));
  }

  @Test
  public void testUsageOfStateDeclaredInSuperclass() throws Exception {
    DoFnDeclaringState fn =
        new DoFnDeclaringState() {
          @ProcessElement
          public void process(
              ProcessContext context,
              @StateId(DoFnDeclaringState.STATE_ID) ValueState<Integer> state) {}
        };

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("process");
    thrown.expectMessage("declared in a different class");
    thrown.expectMessage(DoFnDeclaringState.STATE_ID);
    thrown.expectMessage(fn.getClass().getSimpleName());
    DoFnSignatures.getSignature(fn.getClass());
  }

  @Test
  public void testDeclOfStateUsedInSuperclass() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("process");
    thrown.expectMessage("declared in a different class");
    thrown.expectMessage(DoFnUsingState.STATE_ID);
    DoFnSignatures.getSignature(
        new DoFnUsingState() {
          @StateId(DoFnUsingState.STATE_ID)
          private final StateSpec<ValueState<Integer>> spec = StateSpecs.value(VarIntCoder.of());
        }.getClass());
  }

  @Test
  public void testDeclAndUsageOfStateInSuperclass() throws Exception {
    class DoFnOverridingAbstractStateUse extends DoFnDeclaringStateAndAbstractUse {

      @Override
      public void processWithState(ProcessContext c, ValueState<String> state) {}
    }

    DoFnSignature sig =
        DoFnSignatures.getSignature(new DoFnOverridingAbstractStateUse().getClass());

    assertThat(sig.stateDeclarations().size(), equalTo(1));
    assertThat(sig.processElement().extraParameters().size(), equalTo(2));

    DoFnSignature.StateDeclaration decl =
        sig.stateDeclarations().get(DoFnOverridingAbstractStateUse.STATE_ID);
    StateParameter stateParam = (StateParameter) sig.processElement().extraParameters().get(1);
    assertFalse(stateParam.alwaysFetched());
    assertThat(
        decl.field(),
        equalTo(DoFnDeclaringStateAndAbstractUse.class.getDeclaredField("myStateSpec")));

    // The method we pull out is the superclass method; this is what allows validation to remain
    // simple. The later invokeDynamic instruction causes it to invoke the actual implementation.
    assertThat(stateParam.referent(), equalTo(decl));
  }

  /**
   * Assuming the proper parsing of declarations, testing elsewhere, this test ensures that a simple
   * reference to such a declaration is correctly resolved.
   */
  @Test
  public void testSimpleStateIdRefAnonymousDoFn() throws Exception {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<KV<String, Integer>, Long>() {
              @StateId("foo")
              private final StateSpec<ValueState<Integer>> bizzleDecl =
                  StateSpecs.value(VarIntCoder.of());

              @ProcessElement
              public void foo(ProcessContext context, @StateId("foo") ValueState<Integer> bizzle) {}
            }.getClass());

    assertThat(sig.processElement().extraParameters().size(), equalTo(2));

    final DoFnSignature.StateDeclaration decl = sig.stateDeclarations().get("foo");
    sig.processElement()
        .extraParameters()
        .get(1)
        .match(
            new Parameter.Cases.WithDefault<Void>() {
              @Override
              protected Void dispatchDefault(Parameter p) {
                fail(String.format("Expected a state parameter but got %s", p));
                return null;
              }

              @Override
              public Void dispatch(StateParameter stateParam) {
                assertThat(stateParam.referent(), equalTo(decl));
                return null;
              }

              @Override
              public Void dispatch(Parameter.TimerIdParameter p) {
                return null;
              }
            });
  }

  @Test
  public void testSimpleStateIdNamedDoFn() throws Exception {
    class DoFnForTestSimpleStateIdNamedDoFn extends DoFn<KV<String, Integer>, Long> {
      @StateId("foo")
      private final StateSpec<ValueState<Integer>> bizzle = StateSpecs.value(VarIntCoder.of());

      @ProcessElement
      public void foo(ProcessContext context) {}
    }

    // Test classes at the bottom of the file
    DoFnSignature sig = DoFnSignatures.signatureForDoFn(new DoFnForTestSimpleStateIdNamedDoFn());

    assertThat(sig.stateDeclarations().size(), equalTo(1));
    DoFnSignature.StateDeclaration decl = sig.stateDeclarations().get("foo");

    assertThat(decl.id(), equalTo("foo"));
    assertThat(
        decl.field(), equalTo(DoFnForTestSimpleStateIdNamedDoFn.class.getDeclaredField("bizzle")));
    assertThat(
        decl.stateType(),
        Matchers.<TypeDescriptor<?>>equalTo(new TypeDescriptor<ValueState<Integer>>() {}));
  }

  @Test
  public void testGenericStatefulDoFn() throws Exception {
    class DoFnForTestGenericStatefulDoFn<T> extends DoFn<KV<String, T>, Long> {
      // Note that in order to have a coder for T it will require initialization in the constructor,
      // but that isn't important for this test
      @StateId("foo")
      private final StateSpec<ValueState<T>> bizzle = null;

      @ProcessElement
      public void foo(ProcessContext context) {}
    }

    // Test classes at the bottom of the file
    DoFn<KV<String, Integer>, Long> myDoFn = new DoFnForTestGenericStatefulDoFn<Integer>() {};

    DoFnSignature sig = DoFnSignatures.signatureForDoFn(myDoFn);

    assertThat(sig.stateDeclarations().size(), equalTo(1));
    DoFnSignature.StateDeclaration decl = sig.stateDeclarations().get("foo");

    assertThat(decl.id(), equalTo("foo"));
    assertThat(
        decl.field(), equalTo(DoFnForTestGenericStatefulDoFn.class.getDeclaredField("bizzle")));
    assertThat(
        decl.stateType(),
        Matchers.<TypeDescriptor<?>>equalTo(new TypeDescriptor<ValueState<Integer>>() {}));
  }

  @Test
  public void testOnWindowExpirationMultipleAnnotation() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Found multiple methods annotated with @OnWindowExpiration");
    thrown.expectMessage("bar()");
    thrown.expectMessage("baz()");
    thrown.expectMessage(getClass().getName() + "$");
    DoFnSignatures.getSignature(
        new DoFn<String, String>() {
          @ProcessElement
          public void foo() {}

          @OnWindowExpiration
          public void bar() {}

          @OnWindowExpiration
          public void baz() {}
        }.getClass());
  }

  @Test
  public void testOnWindowExpirationMustBePublic() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("OnWindowExpiration");
    thrown.expectMessage("Must be public");
    thrown.expectMessage("bar()");

    DoFnSignatures.getSignature(
        new DoFn<String, String>() {
          @ProcessElement
          public void foo() {}

          @OnWindowExpiration
          void bar() {}
        }.getClass());
  }

  @Test
  public void testOnWindowExpirationDisallowedParameter() throws Exception {
    // Timers are not allowed in OnWindowExpiration
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Illegal parameter type");
    thrown.expectMessage("TimerParameter");
    thrown.expectMessage("myTimer");
    DoFnSignatures.getSignature(
        new DoFn<String, String>() {
          @TimerId("foo")
          private final TimerSpec myTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

          @ProcessElement
          public void foo() {}

          @OnTimer("foo")
          public void onFoo() {}

          @OnWindowExpiration
          public void bar(@TimerId("foo") Timer t) {}
        }.getClass());
  }

  @Test
  public void testOnWindowExpirationNoParam() {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {

              @ProcessElement
              public void process(ProcessContext c) {}

              @OnWindowExpiration
              public void bar() {}
            }.getClass());

    assertThat(sig.onWindowExpiration().extraParameters().size(), equalTo(0));
  }

  @Test
  public void testOnWindowExpirationWithAllowedParams() {
    DoFnSignature sig =
        DoFnSignatures.getSignature(
            new DoFn<String, String>() {
              @StateId("foo")
              private final StateSpec<ValueState<Integer>> bizzle =
                  StateSpecs.value(VarIntCoder.of());

              @ProcessElement
              public void process(ProcessContext c) {}

              @OnWindowExpiration
              public void bar(
                  BoundedWindow b,
                  @StateId("foo") ValueState<Integer> s,
                  PipelineOptions p,
                  OutputReceiver<String> o,
                  MultiOutputReceiver m) {}
            }.getClass());

    List<Parameter> params = sig.onWindowExpiration().extraParameters();
    assertThat(params.size(), equalTo(5));
    assertThat(params.get(0), instanceOf(WindowParameter.class));
    assertThat(params.get(1), instanceOf(StateParameter.class));
    assertThat(params.get(2), instanceOf(PipelineOptionsParameter.class));
    assertThat(params.get(3), instanceOf(OutputReceiverParameter.class));
    assertThat(params.get(4), instanceOf(TaggedOutputReceiverParameter.class));
  }

  private interface FeatureTest {
    void test();
  }

  private static class StatelessDoFn extends DoFn<String, String> implements FeatureTest {
    @ProcessElement
    public void process(@Element String input) {}

    @Override
    public void test() {
      assertThat(DoFnSignatures.isSplittable(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.isStateful(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesTimers(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesBagState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesMapState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesSetState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesValueState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesWatermarkHold(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.requiresTimeSortedInput(this), SerializableMatchers.equalTo(false));
    }
  }

  private static class StatefulWithValueState extends DoFn<KV<String, String>, String>
      implements FeatureTest {
    @StateId("state")
    private final StateSpec<ValueState<String>> state = StateSpecs.value();

    @ProcessElement
    public void process(@Element KV<String, String> input) {}

    @Override
    public void test() {
      assertThat(DoFnSignatures.isSplittable(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.isStateful(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesTimers(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesState(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesBagState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesMapState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesSetState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesValueState(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesWatermarkHold(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.requiresTimeSortedInput(this), SerializableMatchers.equalTo(false));
    }
  }

  private static class StatefulWithTimers extends DoFn<KV<String, String>, String>
      implements FeatureTest {
    @TimerId("timer")
    private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(@Element KV<String, String> input) {}

    @Override
    public void test() {
      assertThat(DoFnSignatures.isSplittable(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.isStateful(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesTimers(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesBagState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesMapState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesSetState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesValueState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesWatermarkHold(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.requiresTimeSortedInput(this), SerializableMatchers.equalTo(false));
    }

    @OnTimer("timer")
    public void onTimer() {}
  }

  private static class StatefulWithTimersAndValueState extends DoFn<KV<String, String>, String>
      implements FeatureTest {
    @TimerId("timer")
    private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @StateId("state")
    private final StateSpec<SetState<String>> state = StateSpecs.set();

    @ProcessElement
    public void process(@Element KV<String, String> input) {}

    @Override
    public void test() {
      assertThat(DoFnSignatures.isSplittable(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.isStateful(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesTimers(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesState(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesBagState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesMapState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesSetState(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesValueState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesWatermarkHold(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.requiresTimeSortedInput(this), SerializableMatchers.equalTo(false));
    }

    @OnTimer("timer")
    public void onTimer() {}
  }

  private static class StatefulWithSetState extends DoFn<KV<String, String>, String>
      implements FeatureTest {
    @StateId("state")
    private final StateSpec<SetState<String>> spec = StateSpecs.set();

    @ProcessElement
    public void process(@Element KV<String, String> input) {}

    @Override
    public void test() {
      assertThat(DoFnSignatures.isSplittable(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.isStateful(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesTimers(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesState(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesBagState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesMapState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesSetState(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesValueState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesWatermarkHold(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.requiresTimeSortedInput(this), SerializableMatchers.equalTo(false));
    }
  }

  private static class StatefulWithMapState extends DoFn<KV<String, String>, String>
      implements FeatureTest {
    @StateId("state")
    private final StateSpec<MapState<String, String>> spec = StateSpecs.map();

    @ProcessElement
    public void process(@Element KV<String, String> input) {}

    @Override
    public void test() {
      assertThat(DoFnSignatures.isSplittable(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.isStateful(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesTimers(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesState(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesBagState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesMapState(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesSetState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesValueState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesWatermarkHold(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.requiresTimeSortedInput(this), SerializableMatchers.equalTo(false));
    }
  }

  private static class StatefulWithWatermarkHoldState extends DoFn<KV<String, String>, String>
      implements FeatureTest {
    @StateId("state")
    private final StateSpec<WatermarkHoldState> spec =
        StateSpecs.watermarkStateInternal(TimestampCombiner.LATEST);

    @ProcessElement
    public void process(@Element KV<String, String> input) {}

    @Override
    public void test() {
      assertThat(DoFnSignatures.isSplittable(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.isStateful(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesTimers(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesState(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesBagState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesMapState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesSetState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesValueState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesWatermarkHold(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.requiresTimeSortedInput(this), SerializableMatchers.equalTo(false));
    }
  }

  private static class RequiresTimeSortedInput extends DoFn<KV<String, String>, String>
      implements FeatureTest {
    @ProcessElement
    @RequiresTimeSortedInput
    public void process(@Element KV<String, String> input) {}

    @Override
    public void test() {
      assertThat(DoFnSignatures.isSplittable(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.isStateful(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesTimers(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesState(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesBagState(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesMapState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesSetState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesValueState(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.usesWatermarkHold(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.requiresTimeSortedInput(this), SerializableMatchers.equalTo(true));
    }
  }

  private static class Splittable extends DoFn<KV<String, Long>, String> implements FeatureTest {
    @ProcessElement
    public void process(ProcessContext c, RestrictionTracker<OffsetRange, ?> tracker) {}

    @GetInitialRestriction
    public OffsetRange getInitialRange(@Element KV<String, Long> element) {
      return new OffsetRange(0L, element.getValue());
    }

    @Override
    public void test() {
      assertThat(DoFnSignatures.isSplittable(this), SerializableMatchers.equalTo(true));
      assertThat(DoFnSignatures.isStateful(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesTimers(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesBagState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesMapState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesSetState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesValueState(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.usesWatermarkHold(this), SerializableMatchers.equalTo(false));
      assertThat(DoFnSignatures.requiresTimeSortedInput(this), SerializableMatchers.equalTo(false));
    }
  }

  private final List<FeatureTest> tests =
      Lists.newArrayList(
          new StatelessDoFn(),
          new StatefulWithValueState(),
          new StatefulWithTimers(),
          new StatefulWithTimersAndValueState(),
          new StatefulWithSetState(),
          new StatefulWithMapState(),
          new StatefulWithWatermarkHoldState(),
          new RequiresTimeSortedInput(),
          new Splittable());

  @Test
  public void testAllDoFnFeatures() {
    tests.forEach(FeatureTest::test);
  }

  private Matcher<String> mentionsTimers() {
    return anyOf(containsString("timer"), containsString("Timer"));
  }

  private Matcher<String> mentionsState() {
    return anyOf(containsString("state"), containsString("State"));
  }

  private abstract static class DoFnDeclaringState extends DoFn<KV<String, Integer>, Long> {

    public static final String STATE_ID = "my-state-id";

    @StateId(STATE_ID)
    private final StateSpec<ValueState<Integer>> bizzle = StateSpecs.value(VarIntCoder.of());
  }

  private abstract static class DoFnUsingState extends DoFn<KV<String, Integer>, Long> {
    public static final String STATE_ID = "my-state-id";

    @ProcessElement
    public void process(ProcessContext context, @StateId(STATE_ID) ValueState<Integer> state) {}
  }

  private abstract static class DoFnDeclaringStateAndAbstractUse
      extends DoFn<KV<String, Integer>, Long> {
    public static final String STATE_ID = "my-state-id";

    @StateId(STATE_ID)
    private final StateSpec<ValueState<String>> myStateSpec =
        StateSpecs.value(StringUtf8Coder.of());

    @ProcessElement
    public abstract void processWithState(
        ProcessContext context, @StateId(STATE_ID) ValueState<String> state);
  }

  private abstract static class DoFnDeclaringMyTimerId extends DoFn<KV<String, Integer>, Long> {
    @TimerId("my-timer-id")
    private final TimerSpec bizzle = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void foo(ProcessContext context) {}
  }

  private abstract static class DoFnDeclaringTimerAndCallback
      extends DoFn<KV<String, Integer>, Long> {
    public static final String TIMER_ID = "my-timer-id";

    @TimerId(TIMER_ID)
    private final TimerSpec bizzle = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @OnTimer(TIMER_ID)
    public void onTimer() {}
  }

  private abstract static class DoFnWithOnlyCallback extends DoFn<KV<String, Integer>, Long> {
    public static final String TIMER_ID = "my-timer-id";

    @OnTimer(TIMER_ID)
    public void onMyTimer() {}

    @ProcessElement
    public void foo(ProcessContext context) {}
  }

  private abstract static class DoFnDeclaringTimerAndAbstractCallback
      extends DoFn<KV<String, Integer>, Long> {
    public static final String TIMER_ID = "my-timer-id";

    @TimerId(TIMER_ID)
    private final TimerSpec myTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void foo(ProcessContext context) {}

    @OnTimer(TIMER_ID)
    public abstract void onMyTimer();
  }

  private static class DoFnOverridingAbstractCallback
      extends DoFnDeclaringTimerAndAbstractCallback {

    @Override
    public void onMyTimer() {}

    @ProcessElement
    @Override
    public void foo(ProcessContext context) {}
  }

  private abstract static class DoFnDeclaringTimerAndAbstractUse
      extends DoFn<KV<String, Integer>, Long> {
    public static final String TIMER_ID = "my-timer-id";

    @TimerId(TIMER_ID)
    private final TimerSpec myTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public abstract void processWithTimer(ProcessContext context, @TimerId(TIMER_ID) Timer timer);

    @OnTimer(TIMER_ID)
    public abstract void onMyTimer();
  }

  private static class DoFnOverridingAbstractTimerUse extends DoFnDeclaringTimerAndAbstractUse {

    @Override
    public void onMyTimer() {}

    @Override
    public void processWithTimer(ProcessContext context, Timer timer) {}
  }
}
