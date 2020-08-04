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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.SideInput;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.DoFn.TruncateRestriction;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.FieldAccessDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.GetInitialRestrictionMethod;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.GetInitialWatermarkEstimatorStateMethod;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.PipelineOptionsParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.RestrictionParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.RestrictionTrackerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.SchemaElementParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.StateParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.TimerFamilyParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.TimerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.WatermarkEstimatorParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.WatermarkEstimatorStateParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.WindowParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.StateDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerFamilyDeclaration;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.TruncateResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.TypeParameter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** Utilities for working with {@link DoFnSignature}. See {@link #getSignature}. */
@Internal
public class DoFnSignatures {

  private DoFnSignatures() {}

  private static final Map<Class<?>, DoFnSignature> signatureCache = new LinkedHashMap<>();

  private static final ImmutableList<Class<? extends Parameter>>
      ALLOWED_NON_SPLITTABLE_PROCESS_ELEMENT_PARAMETERS =
          ImmutableList.of(
              Parameter.ProcessContextParameter.class,
              Parameter.ElementParameter.class,
              Parameter.SchemaElementParameter.class,
              Parameter.TimestampParameter.class,
              Parameter.OutputReceiverParameter.class,
              Parameter.TaggedOutputReceiverParameter.class,
              Parameter.WindowParameter.class,
              Parameter.PaneInfoParameter.class,
              Parameter.PipelineOptionsParameter.class,
              Parameter.TimerParameter.class,
              Parameter.StateParameter.class,
              Parameter.SideInputParameter.class,
              Parameter.TimerFamilyParameter.class,
              Parameter.BundleFinalizerParameter.class);

  private static final ImmutableList<Class<? extends Parameter>>
      ALLOWED_SPLITTABLE_PROCESS_ELEMENT_PARAMETERS =
          ImmutableList.of(
              Parameter.WindowParameter.class,
              Parameter.PaneInfoParameter.class,
              Parameter.PipelineOptionsParameter.class,
              Parameter.ElementParameter.class,
              Parameter.TimestampParameter.class,
              Parameter.OutputReceiverParameter.class,
              Parameter.TaggedOutputReceiverParameter.class,
              Parameter.ProcessContextParameter.class,
              Parameter.RestrictionTrackerParameter.class,
              Parameter.WatermarkEstimatorParameter.class,
              Parameter.SideInputParameter.class,
              Parameter.BundleFinalizerParameter.class);

  private static final ImmutableList<Class<? extends Parameter>> ALLOWED_START_BUNDLE_PARAMETERS =
      ImmutableList.of(
          Parameter.PipelineOptionsParameter.class,
          Parameter.StartBundleContextParameter.class,
          Parameter.BundleFinalizerParameter.class);

  private static final ImmutableList<Class<? extends Parameter>> ALLOWED_FINISH_BUNDLE_PARAMETERS =
      ImmutableList.of(
          Parameter.PipelineOptionsParameter.class,
          Parameter.FinishBundleContextParameter.class,
          Parameter.BundleFinalizerParameter.class);

  private static final ImmutableList<Class<? extends Parameter>> ALLOWED_ON_TIMER_PARAMETERS =
      ImmutableList.of(
          Parameter.OnTimerContextParameter.class,
          Parameter.TimestampParameter.class,
          Parameter.TimeDomainParameter.class,
          Parameter.WindowParameter.class,
          Parameter.PipelineOptionsParameter.class,
          Parameter.OutputReceiverParameter.class,
          Parameter.TaggedOutputReceiverParameter.class,
          Parameter.TimerParameter.class,
          Parameter.StateParameter.class,
          Parameter.TimerFamilyParameter.class,
          Parameter.TimerIdParameter.class,
          Parameter.KeyParameter.class);

  private static final ImmutableList<Class<? extends Parameter>>
      ALLOWED_ON_TIMER_FAMILY_PARAMETERS =
          ImmutableList.of(
              Parameter.OnTimerContextParameter.class,
              Parameter.TimestampParameter.class,
              Parameter.TimeDomainParameter.class,
              Parameter.WindowParameter.class,
              Parameter.PipelineOptionsParameter.class,
              Parameter.OutputReceiverParameter.class,
              Parameter.TaggedOutputReceiverParameter.class,
              Parameter.TimerParameter.class,
              Parameter.StateParameter.class,
              Parameter.TimerFamilyParameter.class,
              Parameter.TimerIdParameter.class);

  private static final Collection<Class<? extends Parameter>>
      ALLOWED_ON_WINDOW_EXPIRATION_PARAMETERS =
          ImmutableList.of(
              Parameter.WindowParameter.class,
              Parameter.PipelineOptionsParameter.class,
              Parameter.OutputReceiverParameter.class,
              Parameter.TaggedOutputReceiverParameter.class,
              Parameter.StateParameter.class,
              Parameter.TimestampParameter.class,
              Parameter.KeyParameter.class);

  private static final Collection<Class<? extends Parameter>>
      ALLOWED_GET_INITIAL_RESTRICTION_PARAMETERS =
          ImmutableList.of(
              Parameter.ElementParameter.class,
              Parameter.WindowParameter.class,
              Parameter.TimestampParameter.class,
              Parameter.PaneInfoParameter.class,
              Parameter.PipelineOptionsParameter.class);

  private static final Collection<Class<? extends Parameter>> ALLOWED_SPLIT_RESTRICTION_PARAMETERS =
      ImmutableList.of(
          Parameter.ElementParameter.class,
          Parameter.RestrictionParameter.class,
          Parameter.RestrictionTrackerParameter.class,
          Parameter.OutputReceiverParameter.class,
          Parameter.WindowParameter.class,
          Parameter.TimestampParameter.class,
          Parameter.PaneInfoParameter.class,
          Parameter.PipelineOptionsParameter.class);

  private static final Collection<Class<? extends Parameter>>
      ALLOWED_TRUNCATE_RESTRICTION_PARAMETERS =
          ImmutableList.of(
              Parameter.ElementParameter.class,
              Parameter.RestrictionParameter.class,
              Parameter.RestrictionTrackerParameter.class,
              Parameter.WindowParameter.class,
              Parameter.TimestampParameter.class,
              Parameter.PaneInfoParameter.class,
              Parameter.PipelineOptionsParameter.class);

  private static final Collection<Class<? extends Parameter>> ALLOWED_NEW_TRACKER_PARAMETERS =
      ImmutableList.of(
          Parameter.ElementParameter.class,
          Parameter.RestrictionParameter.class,
          Parameter.WindowParameter.class,
          Parameter.TimestampParameter.class,
          Parameter.PaneInfoParameter.class,
          Parameter.PipelineOptionsParameter.class);

  private static final Collection<Class<? extends Parameter>> ALLOWED_GET_SIZE_PARAMETERS =
      ImmutableList.of(
          Parameter.ElementParameter.class,
          Parameter.RestrictionParameter.class,
          Parameter.WindowParameter.class,
          Parameter.TimestampParameter.class,
          Parameter.PaneInfoParameter.class,
          Parameter.PipelineOptionsParameter.class);

  private static final Collection<Class<? extends Parameter>>
      ALLOWED_GET_INITIAL_WATERMARK_ESTIMATOR_STATE_PARAMETERS =
          ImmutableList.of(
              Parameter.ElementParameter.class,
              Parameter.RestrictionParameter.class,
              Parameter.WindowParameter.class,
              Parameter.TimestampParameter.class,
              Parameter.PaneInfoParameter.class,
              Parameter.PipelineOptionsParameter.class);

  private static final Collection<Class<? extends Parameter>>
      ALLOWED_NEW_WATERMARK_ESTIMATOR_PARAMETERS =
          ImmutableList.of(
              Parameter.WatermarkEstimatorStateParameter.class,
              Parameter.ElementParameter.class,
              Parameter.RestrictionParameter.class,
              Parameter.WindowParameter.class,
              Parameter.TimestampParameter.class,
              Parameter.PaneInfoParameter.class,
              Parameter.PipelineOptionsParameter.class);

  /** @return the {@link DoFnSignature} for the given {@link DoFn} instance. */
  public static <FnT extends DoFn<?, ?>> DoFnSignature signatureForDoFn(FnT fn) {
    return getSignature(fn.getClass());
  }

  /** @return the {@link DoFnSignature} for the given {@link DoFn} subclass. */
  public static synchronized <FnT extends DoFn<?, ?>> DoFnSignature getSignature(Class<FnT> fn) {
    return signatureCache.computeIfAbsent(fn, k -> parseSignature(fn));
  }

  /**
   * The context for a {@link DoFn} class, for use in analysis.
   *
   * <p>It contains much of the information that eventually becomes part of the {@link
   * DoFnSignature}, but in an intermediate state.
   */
  @VisibleForTesting
  static class FnAnalysisContext {

    private final Map<String, StateDeclaration> stateDeclarations = new HashMap<>();
    private final Map<String, TimerDeclaration> timerDeclarations = new HashMap<>();
    private final Map<String, TimerFamilyDeclaration> timerFamilyDeclarations = new HashMap<>();
    private final Map<String, FieldAccessDeclaration> fieldAccessDeclarations = new HashMap<>();

    private FnAnalysisContext() {}

    /** Create an empty context, with no declarations. */
    public static FnAnalysisContext create() {
      return new FnAnalysisContext();
    }

    /** State parameters declared in this context, keyed by {@link StateId}. Unmodifiable. */
    public Map<String, StateDeclaration> getStateDeclarations() {
      return Collections.unmodifiableMap(stateDeclarations);
    }

    /** Timer parameters declared in this context, keyed by {@link TimerId}. Unmodifiable. */
    public Map<String, TimerDeclaration> getTimerDeclarations() {
      return Collections.unmodifiableMap(timerDeclarations);
    }

    /**
     * TimerMap parameters declared in this context, keyed by {@link
     * org.apache.beam.sdk.transforms.DoFn.TimerFamily}. Unmodifiable.
     */
    public Map<String, TimerFamilyDeclaration> getTimerFamilyDeclarations() {
      return Collections.unmodifiableMap(timerFamilyDeclarations);
    }

    /** Field access declaration declared in this context. */
    public @Nullable Map<String, FieldAccessDeclaration> getFieldAccessDeclarations() {
      return fieldAccessDeclarations;
    }

    public void addStateDeclaration(StateDeclaration decl) {
      stateDeclarations.put(decl.id(), decl);
    }

    public void addStateDeclarations(Iterable<StateDeclaration> decls) {
      for (StateDeclaration decl : decls) {
        addStateDeclaration(decl);
      }
    }

    public void addTimerDeclaration(TimerDeclaration decl) {
      timerDeclarations.put(decl.id(), decl);
    }

    public void addTimerFamilyDeclaration(TimerFamilyDeclaration decl) {
      timerFamilyDeclarations.put(decl.id(), decl);
    }

    public void addTimerDeclarations(Iterable<TimerDeclaration> decls) {
      for (TimerDeclaration decl : decls) {
        addTimerDeclaration(decl);
      }
    }

    public void addTimerFamilyDeclarations(Iterable<TimerFamilyDeclaration> decls) {
      for (TimerFamilyDeclaration decl : decls) {
        addTimerFamilyDeclaration(decl);
      }
    }

    public void addFieldAccessDeclaration(FieldAccessDeclaration decl) {
      fieldAccessDeclarations.put(decl.id(), decl);
    }

    public void addFieldAccessDeclarations(Iterable<FieldAccessDeclaration> decls) {
      for (FieldAccessDeclaration decl : decls) {
        addFieldAccessDeclaration(decl);
      }
    }
  }

  /**
   * The context of analysis within a particular method.
   *
   * <p>It contains much of the information that eventually becomes part of the {@link
   * DoFnSignature.MethodWithExtraParameters}, but in an intermediate state.
   */
  private static class MethodAnalysisContext {

    private final Map<String, StateParameter> stateParameters = new HashMap<>();
    private final Map<String, TimerParameter> timerParameters = new HashMap<>();
    private final Map<String, TimerFamilyParameter> timerFamilyParameters = new HashMap<>();
    private final List<Parameter> extraParameters = new ArrayList<>();

    private MethodAnalysisContext() {}

    /** Indicates whether the specified {@link Parameter} is known in this context. */
    public boolean hasParameter(Class<? extends Parameter> type) {
      return extraParameters.stream().anyMatch(Predicates.instanceOf(type)::apply);
    }

    /**
     * Returns the specified {@link Parameter} if it is known in this context. Throws {@link
     * IllegalStateException} if there is more than one instance of the parameter.
     */
    public @Nullable <T extends Parameter> Optional<T> findParameter(Class<T> type) {
      List<T> parameters = findParameters(type);
      switch (parameters.size()) {
        case 0:
          return Optional.empty();
        case 1:
          return Optional.of(parameters.get(0));
        default:
          throw new IllegalStateException(
              String.format(
                  "Expected to have found at most one parameter of type %s but found %s.",
                  type, parameters));
      }
    }

    public <T extends Parameter> List<T> findParameters(Class<T> type) {
      return (List<T>)
          extraParameters.stream().filter(Predicates.instanceOf(type)).collect(Collectors.toList());
    }

    /** State parameters declared in this context, keyed by {@link StateId}. */
    public Map<String, StateParameter> getStateParameters() {
      return Collections.unmodifiableMap(stateParameters);
    }

    /** Timer parameters declared in this context, keyed by {@link TimerId}. */
    public Map<String, TimerParameter> getTimerParameters() {
      return Collections.unmodifiableMap(timerParameters);
    }
    /**
     * TimerMap parameters declared in this context, keyed by {@link
     * org.apache.beam.sdk.transforms.DoFn.TimerFamily}.
     */
    public Map<String, TimerFamilyParameter> getTimerFamilyParameters() {
      return Collections.unmodifiableMap(timerFamilyParameters);
    }
    /** Extra parameters in their entirety. Unmodifiable. */
    public List<Parameter> getExtraParameters() {
      return Collections.unmodifiableList(extraParameters);
    }

    public void setParameter(int index, Parameter parameter) {
      extraParameters.set(index, parameter);
    }

    /**
     * Returns an {@link MethodAnalysisContext} like this one but including the provided {@link
     * StateParameter}.
     */
    public void addParameter(Parameter param) {
      extraParameters.add(param);

      if (param instanceof StateParameter) {
        StateParameter stateParameter = (StateParameter) param;
        stateParameters.put(stateParameter.referent().id(), stateParameter);
      }
      if (param instanceof TimerParameter) {
        TimerParameter timerParameter = (TimerParameter) param;
        timerParameters.put(timerParameter.referent().id(), timerParameter);
      }
      if (param instanceof TimerFamilyParameter) {
        TimerFamilyParameter timerFamilyParameter = (TimerFamilyParameter) param;
        timerFamilyParameters.put(timerFamilyParameter.referent().id(), timerFamilyParameter);
      }
    }

    /** Create an empty context, with no declarations. */
    public static MethodAnalysisContext create() {
      return new MethodAnalysisContext();
    }
  }

  @AutoValue
  abstract static class ParameterDescription {
    public abstract Method getMethod();

    public abstract int getIndex();

    public abstract TypeDescriptor<?> getType();

    public abstract List<Annotation> getAnnotations();

    public static ParameterDescription of(
        Method method, int index, TypeDescriptor<?> type, List<Annotation> annotations) {
      return new AutoValue_DoFnSignatures_ParameterDescription(method, index, type, annotations);
    }

    public static ParameterDescription of(
        Method method, int index, TypeDescriptor<?> type, Annotation[] annotations) {
      return new AutoValue_DoFnSignatures_ParameterDescription(
          method, index, type, Arrays.asList(annotations));
    }
  }

  /** Analyzes a given {@link DoFn} class and extracts its {@link DoFnSignature}. */
  private static DoFnSignature parseSignature(Class<? extends DoFn<?, ?>> fnClass) {
    DoFnSignature.Builder signatureBuilder = DoFnSignature.builder();

    ErrorReporter errors = new ErrorReporter(null, fnClass.getName());
    errors.checkArgument(DoFn.class.isAssignableFrom(fnClass), "Must be subtype of DoFn");
    signatureBuilder.setFnClass(fnClass);

    TypeDescriptor<? extends DoFn<?, ?>> fnT = TypeDescriptor.of(fnClass);

    // Extract the input and output type, and whether the fn is bounded.
    TypeDescriptor<?> inputT = null;
    TypeDescriptor<?> outputT = null;
    for (TypeDescriptor<?> supertype : fnT.getTypes()) {
      if (!supertype.getRawType().equals(DoFn.class)) {
        continue;
      }
      Type[] args = ((ParameterizedType) supertype.getType()).getActualTypeArguments();
      inputT = TypeDescriptor.of(args[0]);
      outputT = TypeDescriptor.of(args[1]);
    }
    errors.checkNotNull(inputT, "Unable to determine input type");

    // Find the state and timer declarations in advance of validating
    // method parameter lists
    FnAnalysisContext fnContext = FnAnalysisContext.create();
    fnContext.addStateDeclarations(analyzeStateDeclarations(errors, fnClass).values());
    fnContext.addTimerDeclarations(analyzeTimerDeclarations(errors, fnClass).values());
    fnContext.addTimerFamilyDeclarations(analyzeTimerFamilyDeclarations(errors, fnClass).values());
    fnContext.addFieldAccessDeclarations(analyzeFieldAccessDeclaration(errors, fnClass).values());

    Method processElementMethod =
        findAnnotatedMethod(errors, DoFn.ProcessElement.class, fnClass, true);
    Method startBundleMethod = findAnnotatedMethod(errors, DoFn.StartBundle.class, fnClass, false);
    Method finishBundleMethod =
        findAnnotatedMethod(errors, DoFn.FinishBundle.class, fnClass, false);
    Method setupMethod = findAnnotatedMethod(errors, DoFn.Setup.class, fnClass, false);
    Method teardownMethod = findAnnotatedMethod(errors, DoFn.Teardown.class, fnClass, false);
    Method onWindowExpirationMethod =
        findAnnotatedMethod(errors, DoFn.OnWindowExpiration.class, fnClass, false);
    Method getInitialRestrictionMethod =
        findAnnotatedMethod(errors, DoFn.GetInitialRestriction.class, fnClass, false);
    Method splitRestrictionMethod =
        findAnnotatedMethod(errors, DoFn.SplitRestriction.class, fnClass, false);
    Method truncateRestrictionMethod =
        findAnnotatedMethod(errors, TruncateRestriction.class, fnClass, false);
    Method getRestrictionCoderMethod =
        findAnnotatedMethod(errors, DoFn.GetRestrictionCoder.class, fnClass, false);
    Method newTrackerMethod = findAnnotatedMethod(errors, DoFn.NewTracker.class, fnClass, false);
    Method getSizeMethod = findAnnotatedMethod(errors, DoFn.GetSize.class, fnClass, false);
    Method getWatermarkEstimatorStateCoderMethod =
        findAnnotatedMethod(errors, DoFn.GetWatermarkEstimatorStateCoder.class, fnClass, false);
    Method getInitialWatermarkEstimatorStateMethod =
        findAnnotatedMethod(errors, DoFn.GetInitialWatermarkEstimatorState.class, fnClass, false);
    Method newWatermarkEstimatorMethod =
        findAnnotatedMethod(errors, DoFn.NewWatermarkEstimator.class, fnClass, false);

    Collection<Method> onTimerMethods =
        declaredMethodsWithAnnotation(DoFn.OnTimer.class, fnClass, DoFn.class);
    HashMap<String, DoFnSignature.OnTimerMethod> onTimerMethodMap =
        Maps.newHashMapWithExpectedSize(onTimerMethods.size());
    for (Method onTimerMethod : onTimerMethods) {
      String id = TimerDeclaration.PREFIX + onTimerMethod.getAnnotation(DoFn.OnTimer.class).value();
      errors.checkArgument(
          fnContext.getTimerDeclarations().containsKey(id),
          "Callback %s is for undeclared timer %s",
          onTimerMethod,
          id);

      TimerDeclaration timerDecl = fnContext.getTimerDeclarations().get(id);
      errors.checkArgument(
          timerDecl.field().getDeclaringClass().equals(getDeclaringClass(onTimerMethod)),
          "Callback %s is for timer %s declared in a different class %s."
              + " Timer callbacks must be declared in the same lexical scope as their timer",
          onTimerMethod,
          id,
          timerDecl.field().getDeclaringClass().getCanonicalName());

      onTimerMethodMap.put(
          id, analyzeOnTimerMethod(errors, fnT, onTimerMethod, id, inputT, outputT, fnContext));
    }
    signatureBuilder.setOnTimerMethods(onTimerMethodMap);

    // Check for TimerFamily
    Collection<Method> onTimerFamilyMethods =
        declaredMethodsWithAnnotation(DoFn.OnTimerFamily.class, fnClass, DoFn.class);
    HashMap<String, DoFnSignature.OnTimerFamilyMethod> onTimerFamilyMethodMap =
        Maps.newHashMapWithExpectedSize(onTimerFamilyMethods.size());

    for (Method onTimerFamilyMethod : onTimerFamilyMethods) {
      String id =
          TimerFamilyDeclaration.PREFIX
              + onTimerFamilyMethod.getAnnotation(DoFn.OnTimerFamily.class).value();
      errors.checkArgument(
          fnContext.getTimerFamilyDeclarations().containsKey(id),
          "Callback %s is for undeclared timerFamily %s",
          onTimerFamilyMethod,
          id);

      TimerFamilyDeclaration timerDecl = fnContext.getTimerFamilyDeclarations().get(id);
      errors.checkArgument(
          timerDecl.field().getDeclaringClass().equals(getDeclaringClass(onTimerFamilyMethod)),
          "Callback %s is for timerFamily %s declared in a different class %s."
              + " TimerFamily callbacks must be declared in the same lexical scope as their timer",
          onTimerFamilyMethod,
          id,
          timerDecl.field().getDeclaringClass().getCanonicalName());

      onTimerFamilyMethodMap.put(
          id,
          analyzeOnTimerFamilyMethod(
              errors, fnT, onTimerFamilyMethod, id, inputT, outputT, fnContext));
    }
    signatureBuilder.setOnTimerFamilyMethods(onTimerFamilyMethodMap);

    // Check the converse - that all timers have a callback. This could be relaxed to only
    // those timers used in methods, once method parameter lists support timers.
    for (TimerDeclaration decl : fnContext.getTimerDeclarations().values()) {
      errors.checkArgument(
          onTimerMethodMap.containsKey(decl.id()),
          "No callback registered via %s for timer %s",
          format(DoFn.OnTimer.class),
          decl.id());
    }

    // Check the converse - that all timer family have a callback.

    for (TimerFamilyDeclaration decl : fnContext.getTimerFamilyDeclarations().values()) {
      errors.checkArgument(
          onTimerFamilyMethodMap.containsKey(decl.id()),
          "No callback registered via %s for timerFamily %s",
          format(DoFn.OnTimerFamily.class),
          decl.id());
    }

    ErrorReporter processElementErrors =
        errors.forMethod(DoFn.ProcessElement.class, processElementMethod);
    DoFnSignature.ProcessElementMethod processElement =
        analyzeProcessElementMethod(
            processElementErrors, fnT, processElementMethod, inputT, outputT, fnContext);
    signatureBuilder.setProcessElement(processElement);

    if (startBundleMethod != null) {
      ErrorReporter startBundleErrors = errors.forMethod(DoFn.StartBundle.class, startBundleMethod);
      signatureBuilder.setStartBundle(
          analyzeStartBundleMethod(
              startBundleErrors, fnT, startBundleMethod, inputT, outputT, fnContext));
    }

    if (finishBundleMethod != null) {
      ErrorReporter finishBundleErrors =
          errors.forMethod(DoFn.FinishBundle.class, finishBundleMethod);
      signatureBuilder.setFinishBundle(
          analyzeFinishBundleMethod(
              finishBundleErrors, fnT, finishBundleMethod, inputT, outputT, fnContext));
    }

    if (setupMethod != null) {
      signatureBuilder.setSetup(
          analyzeLifecycleMethod(errors.forMethod(DoFn.Setup.class, setupMethod), setupMethod));
    }

    if (teardownMethod != null) {
      signatureBuilder.setTeardown(
          analyzeLifecycleMethod(
              errors.forMethod(DoFn.Teardown.class, teardownMethod), teardownMethod));
    }

    if (onWindowExpirationMethod != null) {
      signatureBuilder.setOnWindowExpiration(
          analyzeOnWindowExpirationMethod(
              errors, fnT, onWindowExpirationMethod, inputT, outputT, fnContext));
    }

    if (processElement.isSplittable()) {
      ErrorReporter getInitialRestrictionErrors =
          errors.forMethod(DoFn.GetInitialRestriction.class, getInitialRestrictionMethod);
      getInitialRestrictionErrors.checkNotNull(
          getInitialRestrictionMethod,
          "Splittable, but does not define the required @%s method.",
          DoFnSignatures.format(DoFn.GetInitialRestriction.class));

      GetInitialRestrictionMethod initialRestrictionMethod =
          analyzeGetInitialRestrictionMethod(
              errors.forMethod(DoFn.GetInitialRestriction.class, getInitialRestrictionMethod),
              fnT,
              getInitialRestrictionMethod,
              inputT,
              outputT,
              fnContext);

      signatureBuilder.setGetInitialRestriction(initialRestrictionMethod);
      TypeDescriptor<?> restrictionT = initialRestrictionMethod.restrictionT();

      TypeDescriptor<?> watermarkEstimatorStateT = TypeDescriptors.voids();
      if (getInitialWatermarkEstimatorStateMethod != null) {
        GetInitialWatermarkEstimatorStateMethod initialWatermarkEstimatorStateMethod =
            analyzeGetInitialWatermarkEstimatorStateMethod(
                errors.forMethod(
                    DoFn.GetInitialWatermarkEstimatorState.class,
                    getInitialWatermarkEstimatorStateMethod),
                fnT,
                getInitialWatermarkEstimatorStateMethod,
                inputT,
                outputT,
                fnContext);
        watermarkEstimatorStateT = initialWatermarkEstimatorStateMethod.watermarkEstimatorStateT();
        signatureBuilder.setGetInitialWatermarkEstimatorState(initialWatermarkEstimatorStateMethod);
      }

      if (newTrackerMethod != null) {
        signatureBuilder.setNewTracker(
            analyzeNewTrackerMethod(
                errors.forMethod(DoFn.NewTracker.class, newTrackerMethod),
                fnT,
                newTrackerMethod,
                inputT,
                outputT,
                restrictionT,
                fnContext));
      } else {
        errors
            .forMethod(DoFn.NewTracker.class, null)
            .checkArgument(
                restrictionT.isSubtypeOf(TypeDescriptor.of(HasDefaultTracker.class)),
                "Splittable, either @%s method must be defined or %s must implement %s.",
                format(DoFn.NewTracker.class),
                format(restrictionT),
                format(HasDefaultTracker.class));
      }

      if (splitRestrictionMethod != null) {
        signatureBuilder.setSplitRestriction(
            analyzeSplitRestrictionMethod(
                errors.forMethod(DoFn.SplitRestriction.class, splitRestrictionMethod),
                fnT,
                splitRestrictionMethod,
                inputT,
                outputT,
                restrictionT,
                fnContext));
      }

      if (truncateRestrictionMethod != null) {
        signatureBuilder.setTruncateRestriction(
            analyzeTruncateRestrictionMethod(
                errors.forMethod(TruncateRestriction.class, truncateRestrictionMethod),
                fnT,
                truncateRestrictionMethod,
                inputT,
                restrictionT,
                fnContext));
      }

      if (getSizeMethod != null) {
        signatureBuilder.setGetSize(
            analyzeGetSizeMethod(
                errors.forMethod(DoFn.GetSize.class, getSizeMethod),
                fnT,
                getSizeMethod,
                inputT,
                outputT,
                restrictionT,
                fnContext));
      }

      if (getRestrictionCoderMethod != null) {
        signatureBuilder.setGetRestrictionCoder(
            analyzeGetRestrictionCoderMethod(
                errors.forMethod(DoFn.GetRestrictionCoder.class, getRestrictionCoderMethod),
                fnT,
                getRestrictionCoderMethod));
      }

      if (getWatermarkEstimatorStateCoderMethod != null) {
        signatureBuilder.setGetWatermarkEstimatorStateCoder(
            analyzeGetWatermarkEstimatorStateCoderMethod(
                errors.forMethod(
                    DoFn.GetWatermarkEstimatorStateCoder.class,
                    getWatermarkEstimatorStateCoderMethod),
                fnT,
                getWatermarkEstimatorStateCoderMethod));
      }

      if (newWatermarkEstimatorMethod != null) {
        signatureBuilder.setNewWatermarkEstimator(
            analyzeNewWatermarkEstimatorMethod(
                errors.forMethod(DoFn.NewWatermarkEstimator.class, newWatermarkEstimatorMethod),
                fnT,
                newWatermarkEstimatorMethod,
                inputT,
                outputT,
                restrictionT,
                watermarkEstimatorStateT,
                fnContext));
      } else if (getInitialWatermarkEstimatorStateMethod != null) {
        errors
            .forMethod(DoFn.NewWatermarkEstimator.class, null)
            .checkArgument(
                watermarkEstimatorStateT.isSubtypeOf(
                    TypeDescriptor.of(HasDefaultWatermarkEstimator.class)),
                "Splittable, either @%s method must be defined or %s must implement %s.",
                format(DoFn.NewWatermarkEstimator.class),
                format(watermarkEstimatorStateT),
                format(HasDefaultWatermarkEstimator.class));
      }
    } else {
      // Validate that none of the splittable DoFn only methods have been declared.
      List<String> forbiddenMethods = new ArrayList<>();
      if (getInitialRestrictionMethod != null) {
        forbiddenMethods.add("@" + format(DoFn.GetInitialRestriction.class));
      }
      if (splitRestrictionMethod != null) {
        forbiddenMethods.add("@" + format(DoFn.SplitRestriction.class));
      }
      if (truncateRestrictionMethod != null) {
        forbiddenMethods.add("@" + format(TruncateRestriction.class));
      }
      if (newTrackerMethod != null) {
        forbiddenMethods.add("@" + format(DoFn.NewTracker.class));
      }
      if (getRestrictionCoderMethod != null) {
        forbiddenMethods.add("@" + format(DoFn.GetRestrictionCoder.class));
      }
      if (getSizeMethod != null) {
        forbiddenMethods.add("@" + format(DoFn.GetSize.class));
      }
      if (getInitialWatermarkEstimatorStateMethod != null) {
        forbiddenMethods.add("@" + format(DoFn.GetInitialWatermarkEstimatorState.class));
      }
      if (getWatermarkEstimatorStateCoderMethod != null) {
        forbiddenMethods.add("@" + format(DoFn.GetWatermarkEstimatorStateCoder.class));
      }
      if (newWatermarkEstimatorMethod != null) {
        forbiddenMethods.add("@" + format(DoFn.NewWatermarkEstimator.class));
      }
      errors.checkArgument(
          forbiddenMethods.isEmpty(), "Non-splittable, but defines methods: %s", forbiddenMethods);
    }

    signatureBuilder.setIsBoundedPerElement(inferBoundedness(fnT, processElement, errors));

    signatureBuilder.setStateDeclarations(fnContext.getStateDeclarations());
    signatureBuilder.setTimerDeclarations(fnContext.getTimerDeclarations());
    signatureBuilder.setTimerFamilyDeclarations(fnContext.getTimerFamilyDeclarations());
    signatureBuilder.setFieldAccessDeclarations(fnContext.getFieldAccessDeclarations());

    DoFnSignature signature = signatureBuilder.build();

    // Additional validation for splittable DoFn's.
    if (processElement.isSplittable()) {
      verifySplittableMethods(signature, errors);
    }

    return signature;
  }

  private static Class<?> getDeclaringClass(Method onTimerMethod) {
    Class<?> declaringClass = onTimerMethod.getDeclaringClass();
    if (declaringClass.getName().contains("$MockitoMock$")) {
      declaringClass = declaringClass.getSuperclass();
    }
    return declaringClass;
  }

  /**
   * Infers the boundedness of the {@link DoFn.ProcessElement} method (whether or not it performs a
   * bounded amount of work per element) using the following criteria:
   *
   * <ol>
   *   <li>If the {@link DoFn} is not splittable, then it is bounded, it must not be annotated as
   *       {@link DoFn.BoundedPerElement} or {@link DoFn.UnboundedPerElement}, and {@link
   *       DoFn.ProcessElement} must return {@code void}.
   *   <li>If the {@link DoFn} (or any of its supertypes) is annotated as {@link
   *       DoFn.BoundedPerElement} or {@link DoFn.UnboundedPerElement}, use that. Only one of these
   *       must be specified.
   *   <li>If {@link DoFn.ProcessElement} returns {@link DoFn.ProcessContinuation}, assume it is
   *       unbounded. Otherwise (if it returns {@code void}), assume it is bounded.
   *   <li>If {@link DoFn.ProcessElement} returns {@code void}, but the {@link DoFn} is annotated
   *       {@link DoFn.UnboundedPerElement}, this is an error.
   * </ol>
   */
  private static PCollection.IsBounded inferBoundedness(
      TypeDescriptor<? extends DoFn> fnT,
      DoFnSignature.ProcessElementMethod processElement,
      ErrorReporter errors) {
    PCollection.IsBounded isBounded = null;
    for (TypeDescriptor<?> supertype : fnT.getTypes()) {
      if (supertype.getRawType().isAnnotationPresent(DoFn.BoundedPerElement.class)
          || supertype.getRawType().isAnnotationPresent(DoFn.UnboundedPerElement.class)) {
        errors.checkArgument(
            isBounded == null,
            "Both @%s and @%s specified",
            format(DoFn.BoundedPerElement.class),
            format(DoFn.UnboundedPerElement.class));
        isBounded =
            supertype.getRawType().isAnnotationPresent(DoFn.BoundedPerElement.class)
                ? PCollection.IsBounded.BOUNDED
                : PCollection.IsBounded.UNBOUNDED;
      }
    }
    if (processElement.isSplittable()) {
      if (isBounded == null) {
        isBounded =
            processElement.hasReturnValue()
                ? PCollection.IsBounded.UNBOUNDED
                : PCollection.IsBounded.BOUNDED;
      }
    } else {
      errors.checkArgument(
          isBounded == null,
          "Non-splittable, but annotated as @"
              + ((isBounded == PCollection.IsBounded.BOUNDED)
                  ? format(DoFn.BoundedPerElement.class)
                  : format(DoFn.UnboundedPerElement.class)));
      checkState(!processElement.hasReturnValue(), "Should have been inferred splittable");
      isBounded = PCollection.IsBounded.BOUNDED;
    }
    return isBounded;
  }

  /**
   * Verifies properties related to methods of splittable {@link DoFn}:
   *
   * <ul>
   *   <li>Must declare the required {@link DoFn.GetInitialRestriction} and {@link DoFn.NewTracker}
   *       methods.
   *   <li>Types of restrictions and trackers must match exactly between {@link
   *       DoFn.ProcessElement}, {@link DoFn.GetInitialRestriction}, {@link DoFn.NewTracker}, {@link
   *       DoFn.GetRestrictionCoder}, {@link DoFn.SplitRestriction}.
   * </ul>
   */
  private static void verifySplittableMethods(DoFnSignature signature, ErrorReporter errors) {
    DoFnSignature.ProcessElementMethod processElement = signature.processElement();
    DoFnSignature.GetInitialRestrictionMethod getInitialRestriction =
        signature.getInitialRestriction();
    DoFnSignature.NewTrackerMethod newTracker = signature.newTracker();
    DoFnSignature.GetRestrictionCoderMethod getRestrictionCoder = signature.getRestrictionCoder();
    DoFnSignature.GetInitialWatermarkEstimatorStateMethod getInitialWatermarkEstimatorState =
        signature.getInitialWatermarkEstimatorState();
    DoFnSignature.GetWatermarkEstimatorStateCoderMethod getWatermarkEstimatorStateCoder =
        signature.getWatermarkEstimatorStateCoder();

    ErrorReporter processElementErrors =
        errors.forMethod(DoFn.ProcessElement.class, processElement.targetMethod());

    TypeDescriptor<?> restrictionT = getInitialRestriction.restrictionT();
    TypeDescriptor<?> watermarkEstimatorStateT =
        getInitialWatermarkEstimatorState == null
            ? TypeDescriptors.voids()
            : getInitialWatermarkEstimatorState.watermarkEstimatorStateT();

    if (newTracker == null) {
      ErrorReporter newTrackerErrors = errors.forMethod(DoFn.NewTracker.class, null);
      newTrackerErrors.checkArgument(
          restrictionT.isSubtypeOf(TypeDescriptor.of(HasDefaultTracker.class)),
          "Splittable, but does not define @%s method or %s does not implement %s.",
          format(DoFn.NewTracker.class),
          format(restrictionT),
          format(HasDefaultTracker.class));
    }

    processElementErrors.checkArgument(
        processElement.trackerT().getRawType().equals(RestrictionTracker.class),
        "Has tracker type %s, but the DoFn's tracker type must be of type RestrictionTracker.",
        format(processElement.trackerT()));

    if (processElement.watermarkEstimatorT() != null) {
      processElementErrors.checkArgument(
          processElement.watermarkEstimatorT().getRawType().equals(WatermarkEstimator.class)
              || processElement
                  .watermarkEstimatorT()
                  .getRawType()
                  .equals(ManualWatermarkEstimator.class),
          "Has watermark estimator type %s, but the DoFn's watermark estimator type must be one of [WatermarkEstimator, ManualWatermarkEstimator] types.",
          format(processElement.watermarkEstimatorT()));
    }

    if (getRestrictionCoder != null) {
      ErrorReporter getInitialRestrictionErrors =
          errors.forMethod(DoFn.GetInitialRestriction.class, getInitialRestriction.targetMethod());
      getInitialRestrictionErrors.checkArgument(
          getRestrictionCoder.coderT().isSubtypeOf(coderTypeOf(restrictionT)),
          "Uses restriction type %s, but @%s method %s returns %s "
              + "which is not a subtype of %s",
          format(restrictionT),
          format(DoFn.GetRestrictionCoder.class),
          format(getRestrictionCoder.targetMethod()),
          format(getRestrictionCoder.coderT()),
          format(coderTypeOf(restrictionT)));
    }

    if (getWatermarkEstimatorStateCoder != null) {
      ErrorReporter getInitialWatermarkEstimatorStateReporter =
          errors.forMethod(
              DoFn.GetInitialWatermarkEstimatorState.class,
              getInitialWatermarkEstimatorState == null
                  ? null
                  : getInitialWatermarkEstimatorState.targetMethod());
      getInitialWatermarkEstimatorStateReporter.checkArgument(
          getWatermarkEstimatorStateCoder
              .coderT()
              .isSubtypeOf(coderTypeOf(watermarkEstimatorStateT)),
          "Uses watermark estimator state type %s, but @%s method %s returns %s "
              + "which is not a subtype of %s",
          format(watermarkEstimatorStateT),
          format(DoFn.GetInitialWatermarkEstimatorState.class),
          format(getWatermarkEstimatorStateCoder.targetMethod()),
          format(getWatermarkEstimatorStateCoder.coderT()),
          format(coderTypeOf(watermarkEstimatorStateT)));
    }
  }

  /**
   * Generates a {@link TypeDescriptor} for {@code DoFn<InputT, OutputT>.ProcessContext} given
   * {@code InputT} and {@code OutputT}.
   */
  private static <InputT, OutputT>
      TypeDescriptor<DoFn<InputT, OutputT>.ProcessContext> doFnProcessContextTypeOf(
          TypeDescriptor<InputT> inputT, TypeDescriptor<OutputT> outputT) {
    return new TypeDescriptor<DoFn<InputT, OutputT>.ProcessContext>() {}.where(
            new TypeParameter<InputT>() {}, inputT)
        .where(new TypeParameter<OutputT>() {}, outputT);
  }

  /**
   * Generates a {@link TypeDescriptor} for {@code DoFn<InputT, OutputT>.StartBundleContext} given
   * {@code InputT} and {@code OutputT}.
   */
  private static <InputT, OutputT>
      TypeDescriptor<DoFn<InputT, OutputT>.StartBundleContext> doFnStartBundleContextTypeOf(
          TypeDescriptor<InputT> inputT, TypeDescriptor<OutputT> outputT) {
    return new TypeDescriptor<DoFn<InputT, OutputT>.StartBundleContext>() {}.where(
            new TypeParameter<InputT>() {}, inputT)
        .where(new TypeParameter<OutputT>() {}, outputT);
  }

  /**
   * Generates a {@link TypeDescriptor} for {@code DoFn<InputT, OutputT>.FinishBundleContext} given
   * {@code InputT} and {@code OutputT}.
   */
  private static <InputT, OutputT>
      TypeDescriptor<DoFn<InputT, OutputT>.FinishBundleContext> doFnFinishBundleContextTypeOf(
          TypeDescriptor<InputT> inputT, TypeDescriptor<OutputT> outputT) {
    return new TypeDescriptor<DoFn<InputT, OutputT>.FinishBundleContext>() {}.where(
            new TypeParameter<InputT>() {}, inputT)
        .where(new TypeParameter<OutputT>() {}, outputT);
  }

  /**
   * Generates a {@link TypeDescriptor} for {@code DoFn<InputT, OutputT>.Context} given {@code
   * InputT} and {@code OutputT}.
   */
  private static <InputT, OutputT>
      TypeDescriptor<DoFn<InputT, OutputT>.OnTimerContext> doFnOnTimerContextTypeOf(
          TypeDescriptor<InputT> inputT, TypeDescriptor<OutputT> outputT) {
    return new TypeDescriptor<DoFn<InputT, OutputT>.OnTimerContext>() {}.where(
            new TypeParameter<InputT>() {}, inputT)
        .where(new TypeParameter<OutputT>() {}, outputT);
  }

  /**
   * Generates a {@link TypeDescriptor} for {@code DoFn<InputT, OutputT>.Context} given {@code
   * InputT} and {@code OutputT}.
   */
  private static <InputT, OutputT>
      TypeDescriptor<DoFn<InputT, OutputT>.OnWindowExpirationContext>
          doFnOnWindowExpirationContextTypeOf(
              TypeDescriptor<InputT> inputT, TypeDescriptor<OutputT> outputT) {
    return new TypeDescriptor<DoFn<InputT, OutputT>.OnWindowExpirationContext>() {}.where(
            new TypeParameter<InputT>() {}, inputT)
        .where(new TypeParameter<OutputT>() {}, outputT);
  }

  @VisibleForTesting
  static DoFnSignature.OnTimerMethod analyzeOnTimerMethod(
      ErrorReporter errors,
      TypeDescriptor<? extends DoFn<?, ?>> fnClass,
      Method m,
      String timerId,
      TypeDescriptor<?> inputT,
      TypeDescriptor<?> outputT,
      FnAnalysisContext fnContext) {
    errors.checkArgument(void.class.equals(m.getReturnType()), "Must return void");

    Type[] params = m.getGenericParameterTypes();

    MethodAnalysisContext methodContext = MethodAnalysisContext.create();

    boolean requiresStableInput = m.isAnnotationPresent(DoFn.RequiresStableInput.class);

    @Nullable TypeDescriptor<? extends BoundedWindow> windowT = getWindowType(fnClass, m);

    List<Parameter> extraParameters = new ArrayList<>();
    ErrorReporter onTimerErrors = errors.forMethod(DoFn.OnTimer.class, m);
    for (int i = 0; i < params.length; ++i) {
      Parameter parameter =
          analyzeExtraParameter(
              onTimerErrors,
              fnContext,
              methodContext,
              fnClass,
              ParameterDescription.of(
                  m,
                  i,
                  fnClass.resolveType(params[i]),
                  Arrays.asList(m.getParameterAnnotations()[i])),
              inputT,
              outputT);

      checkParameterOneOf(errors, parameter, ALLOWED_ON_TIMER_PARAMETERS);

      extraParameters.add(parameter);
    }

    return DoFnSignature.OnTimerMethod.create(
        m, timerId, requiresStableInput, windowT, extraParameters);
  }

  @VisibleForTesting
  static DoFnSignature.OnTimerFamilyMethod analyzeOnTimerFamilyMethod(
      ErrorReporter errors,
      TypeDescriptor<? extends DoFn<?, ?>> fnClass,
      Method m,
      String timerFamilyId,
      TypeDescriptor<?> inputT,
      TypeDescriptor<?> outputT,
      FnAnalysisContext fnContext) {
    errors.checkArgument(void.class.equals(m.getReturnType()), "Must return void");

    Type[] params = m.getGenericParameterTypes();

    MethodAnalysisContext methodContext = MethodAnalysisContext.create();

    boolean requiresStableInput = m.isAnnotationPresent(DoFn.RequiresStableInput.class);

    @Nullable TypeDescriptor<? extends BoundedWindow> windowT = getWindowType(fnClass, m);

    List<DoFnSignature.Parameter> extraParameters = new ArrayList<>();
    ErrorReporter onTimerErrors = errors.forMethod(DoFn.OnTimerFamily.class, m);
    for (int i = 0; i < params.length; ++i) {
      Parameter parameter =
          analyzeExtraParameter(
              onTimerErrors,
              fnContext,
              methodContext,
              fnClass,
              ParameterDescription.of(
                  m,
                  i,
                  fnClass.resolveType(params[i]),
                  Arrays.asList(m.getParameterAnnotations()[i])),
              inputT,
              outputT);

      checkParameterOneOf(errors, parameter, ALLOWED_ON_TIMER_FAMILY_PARAMETERS);

      extraParameters.add(parameter);
    }

    return DoFnSignature.OnTimerFamilyMethod.create(
        m, timerFamilyId, requiresStableInput, windowT, extraParameters);
  }

  @VisibleForTesting
  static DoFnSignature.OnWindowExpirationMethod analyzeOnWindowExpirationMethod(
      ErrorReporter errors,
      TypeDescriptor<? extends DoFn<?, ?>> fnClass,
      Method m,
      TypeDescriptor<?> inputT,
      TypeDescriptor<?> outputT,
      FnAnalysisContext fnContext) {
    errors.checkArgument(void.class.equals(m.getReturnType()), "Must return void");

    Type[] params = m.getGenericParameterTypes();

    MethodAnalysisContext methodContext = MethodAnalysisContext.create();

    boolean requiresStableInput = m.isAnnotationPresent(DoFn.RequiresStableInput.class);

    @Nullable TypeDescriptor<? extends BoundedWindow> windowT = getWindowType(fnClass, m);

    List<Parameter> extraParameters = new ArrayList<>();
    ErrorReporter onWindowExpirationErrors = errors.forMethod(DoFn.OnWindowExpiration.class, m);
    for (int i = 0; i < params.length; ++i) {
      Parameter parameter =
          analyzeExtraParameter(
              onWindowExpirationErrors,
              fnContext,
              methodContext,
              fnClass,
              ParameterDescription.of(
                  m,
                  i,
                  fnClass.resolveType(params[i]),
                  Arrays.asList(m.getParameterAnnotations()[i])),
              inputT,
              outputT);

      checkParameterOneOf(errors, parameter, ALLOWED_ON_WINDOW_EXPIRATION_PARAMETERS);

      extraParameters.add(parameter);
    }

    return DoFnSignature.OnWindowExpirationMethod.create(
        m, requiresStableInput, windowT, extraParameters);
  }

  @VisibleForTesting
  static DoFnSignature.ProcessElementMethod analyzeProcessElementMethod(
      ErrorReporter errors,
      TypeDescriptor<? extends DoFn<?, ?>> fnClass,
      Method m,
      TypeDescriptor<?> inputT,
      TypeDescriptor<?> outputT,
      FnAnalysisContext fnContext) {
    errors.checkArgument(
        void.class.equals(m.getReturnType())
            || DoFn.ProcessContinuation.class.equals(m.getReturnType()),
        "Must return void or %s",
        format(DoFn.ProcessContinuation.class));

    MethodAnalysisContext methodContext = MethodAnalysisContext.create();

    boolean requiresStableInput = m.isAnnotationPresent(DoFn.RequiresStableInput.class);
    boolean requiresTimeSortedInput = m.isAnnotationPresent(DoFn.RequiresTimeSortedInput.class);

    TypeDescriptor<? extends BoundedWindow> windowT = getWindowType(fnClass, m);

    Type[] params = m.getGenericParameterTypes();
    for (int i = 0; i < params.length; ++i) {
      Parameter extraParam =
          analyzeExtraParameter(
              errors.forMethod(DoFn.ProcessElement.class, m),
              fnContext,
              methodContext,
              fnClass,
              ParameterDescription.of(
                  m,
                  i,
                  fnClass.resolveType(params[i]),
                  Arrays.asList(m.getParameterAnnotations()[i])),
              inputT,
              outputT);

      methodContext.addParameter(extraParam);
    }
    int schemaElementIndex = 0;
    for (int i = 0; i < methodContext.getExtraParameters().size(); ++i) {
      Parameter parameter = methodContext.getExtraParameters().get(i);
      if (parameter instanceof SchemaElementParameter) {
        SchemaElementParameter schemaParameter = (SchemaElementParameter) parameter;
        schemaParameter = schemaParameter.toBuilder().setIndex(schemaElementIndex).build();
        methodContext.setParameter(i, schemaParameter);
        ++schemaElementIndex;
      }
    }

    TypeDescriptor<?> trackerT =
        methodContext
            .findParameter(RestrictionTrackerParameter.class)
            .map(p -> p.trackerT())
            .orElse(null);
    TypeDescriptor<?> watermarkEstimatorT =
        methodContext
            .findParameter(WatermarkEstimatorParameter.class)
            .map(p -> p.estimatorT())
            .orElse(null);

    // The allowed parameters depend on whether this DoFn is splittable
    if (trackerT != null) {
      for (Parameter parameter : methodContext.getExtraParameters()) {
        checkParameterOneOf(errors, parameter, ALLOWED_SPLITTABLE_PROCESS_ELEMENT_PARAMETERS);
      }
    } else {
      for (Parameter parameter : methodContext.getExtraParameters()) {
        checkParameterOneOf(errors, parameter, ALLOWED_NON_SPLITTABLE_PROCESS_ELEMENT_PARAMETERS);
      }
    }

    return DoFnSignature.ProcessElementMethod.create(
        m,
        methodContext.getExtraParameters(),
        requiresStableInput,
        requiresTimeSortedInput,
        trackerT,
        watermarkEstimatorT,
        windowT,
        DoFn.ProcessContinuation.class.equals(m.getReturnType()));
  }

  private static void checkParameterOneOf(
      ErrorReporter errors,
      Parameter parameter,
      Collection<Class<? extends Parameter>> allowedParameterClasses) {

    for (Class<? extends Parameter> paramClass : allowedParameterClasses) {
      if (paramClass.isAssignableFrom(parameter.getClass())) {
        return;
      }
    }

    // If we get here, none matched
    errors.throwIllegalArgument("Illegal parameter type: %s", parameter);
  }

  private static Parameter analyzeExtraParameter(
      ErrorReporter methodErrors,
      FnAnalysisContext fnContext,
      MethodAnalysisContext methodContext,
      TypeDescriptor<? extends DoFn<?, ?>> fnClass,
      ParameterDescription param,
      TypeDescriptor<?> inputT,
      TypeDescriptor<?> outputT) {

    TypeDescriptor<?> expectedProcessContextT = doFnProcessContextTypeOf(inputT, outputT);
    TypeDescriptor<?> expectedStartBundleContextT = doFnStartBundleContextTypeOf(inputT, outputT);
    TypeDescriptor<?> expectedFinishBundleContextT = doFnFinishBundleContextTypeOf(inputT, outputT);
    TypeDescriptor<?> expectedOnTimerContextT = doFnOnTimerContextTypeOf(inputT, outputT);
    TypeDescriptor<?> expectedOnWindowExpirationContextT =
        doFnOnWindowExpirationContextTypeOf(inputT, outputT);

    TypeDescriptor<?> paramT = param.getType();
    Class<?> rawType = paramT.getRawType();

    ErrorReporter paramErrors = methodErrors.forParameter(param);

    String fieldAccessString = getFieldAccessId(param.getAnnotations());
    if (fieldAccessString != null) {
      return Parameter.schemaElementParameter(paramT, fieldAccessString, param.getIndex());
    } else if (hasAnnotation(DoFn.Element.class, param.getAnnotations())) {
      return (paramT.equals(inputT))
          ? Parameter.elementParameter(paramT)
          : Parameter.schemaElementParameter(paramT, null, param.getIndex());
    } else if (hasAnnotation(DoFn.Restriction.class, param.getAnnotations())) {
      return Parameter.restrictionParameter(paramT);
    } else if (hasAnnotation(DoFn.WatermarkEstimatorState.class, param.getAnnotations())) {
      return Parameter.watermarkEstimatorState(paramT);
    } else if (hasAnnotation(DoFn.Timestamp.class, param.getAnnotations())) {
      methodErrors.checkArgument(
          rawType.equals(Instant.class),
          "@Timestamp argument must have type org.joda.time.Instant.");
      return Parameter.timestampParameter();
    } else if (hasAnnotation(DoFn.Key.class, param.getAnnotations())) {
      methodErrors.checkArgument(
          KV.class.equals(inputT.getRawType()),
          "@Key argument is expected to be use with input element of type KV.");

      Type keyType = ((ParameterizedType) inputT.getType()).getActualTypeArguments()[0];
      methodErrors.checkArgument(
          TypeDescriptor.of(keyType).equals(paramT),
          "@Key argument is expected to be type of %s, but found %s.",
          keyType,
          rawType);
      return Parameter.keyT(paramT);
    } else if (rawType.equals(TimeDomain.class)) {
      return Parameter.timeDomainParameter();
    } else if (hasAnnotation(DoFn.SideInput.class, param.getAnnotations())) {
      String sideInputId = getSideInputId(param.getAnnotations());
      paramErrors.checkArgument(
          sideInputId != null, "%s missing %s annotation", format(SideInput.class));
      return Parameter.sideInputParameter(paramT, sideInputId);
    } else if (rawType.equals(PaneInfo.class)) {
      return Parameter.paneInfoParameter();
    } else if (rawType.equals(DoFn.BundleFinalizer.class)) {
      return Parameter.bundleFinalizer();
    } else if (rawType.equals(DoFn.ProcessContext.class)) {
      paramErrors.checkArgument(
          paramT.equals(expectedProcessContextT),
          "ProcessContext argument must have type %s",
          format(expectedProcessContextT));
      return Parameter.processContext();
    } else if (rawType.equals(DoFn.StartBundleContext.class)) {
      paramErrors.checkArgument(
          paramT.equals(expectedStartBundleContextT),
          "StartBundleContext argument must have type %s",
          format(expectedProcessContextT));
      return Parameter.startBundleContext();
    } else if (rawType.equals(DoFn.FinishBundleContext.class)) {
      paramErrors.checkArgument(
          paramT.equals(expectedFinishBundleContextT),
          "FinishBundleContext argument must have type %s",
          format(expectedProcessContextT));
      return Parameter.finishBundleContext();
    } else if (rawType.equals(DoFn.OnTimerContext.class)) {
      paramErrors.checkArgument(
          paramT.equals(expectedOnTimerContextT),
          "OnTimerContext argument must have type %s",
          format(expectedOnTimerContextT));
      return Parameter.onTimerContext();
    } else if (rawType.equals(DoFn.OnWindowExpirationContext.class)) {
      paramErrors.checkArgument(
          paramT.equals(expectedOnWindowExpirationContextT),
          "OnWindowExpirationContext argument must have type %s",
          format(expectedOnWindowExpirationContextT));
      return Parameter.onWindowExpirationContext();
    } else if (BoundedWindow.class.isAssignableFrom(rawType)) {
      methodErrors.checkArgument(
          !methodContext.hasParameter(WindowParameter.class),
          "Multiple %s parameters",
          format(BoundedWindow.class));
      return Parameter.boundedWindow((TypeDescriptor<? extends BoundedWindow>) paramT);
    } else if (rawType.equals(OutputReceiver.class)) {
      // It's a schema row receiver if it's an OutputReceiver<Row> _and_ the output type is not
      // already Row.
      boolean schemaRowReceiver =
          paramT.equals(outputReceiverTypeOf(TypeDescriptor.of(Row.class)))
              && !outputT.equals(TypeDescriptor.of(Row.class));
      if (!schemaRowReceiver) {
        TypeDescriptor<?> expectedReceiverT = outputReceiverTypeOf(outputT);
        paramErrors.checkArgument(
            paramT.equals(expectedReceiverT),
            "OutputReceiver should be parameterized by %s",
            outputT);
      }
      return Parameter.outputReceiverParameter(schemaRowReceiver);
    } else if (rawType.equals(MultiOutputReceiver.class)) {
      return Parameter.taggedOutputReceiverParameter();
    } else if (PipelineOptions.class.equals(rawType)) {
      methodErrors.checkArgument(
          !methodContext.hasParameter(PipelineOptionsParameter.class),
          "Multiple %s parameters",
          format(PipelineOptions.class));
      return Parameter.pipelineOptions();
    } else if (RestrictionTracker.class.isAssignableFrom(rawType)) {
      methodErrors.checkArgument(
          !methodContext.hasParameter(RestrictionTrackerParameter.class),
          "Multiple %s parameters",
          format(RestrictionTracker.class));
      return Parameter.restrictionTracker(paramT);
    } else if (WatermarkEstimator.class.isAssignableFrom(rawType)) {
      methodErrors.checkArgument(
          !methodContext.hasParameter(WatermarkEstimatorParameter.class),
          "Multiple %s parameters",
          format(WatermarkEstimator.class));
      return Parameter.watermarkEstimator(paramT);
    } else if (rawType.equals(Timer.class)) {
      // m.getParameters() is not available until Java 8
      String id = getTimerId(param.getAnnotations());

      paramErrors.checkArgument(
          id != null, "%s missing %s annotation", format(Timer.class), format(TimerId.class));

      paramErrors.checkArgument(
          !methodContext.getTimerParameters().containsKey(id),
          "duplicate %s: \"%s\"",
          format(TimerId.class),
          id);

      TimerDeclaration timerDecl = fnContext.getTimerDeclarations().get(id);
      paramErrors.checkArgument(
          timerDecl != null, "reference to undeclared %s: \"%s\"", format(TimerId.class), id);

      paramErrors.checkArgument(
          timerDecl.field().getDeclaringClass().equals(getDeclaringClass(param.getMethod())),
          "%s %s declared in a different class %s."
              + " Timers may be referenced only in the lexical scope where they are declared.",
          format(TimerId.class),
          id,
          timerDecl.field().getDeclaringClass().getName());

      return Parameter.timerParameter(timerDecl);

    } else if (hasAnnotation(DoFn.TimerId.class, param.getAnnotations())) {
      boolean isValidTimerIdForTimerFamily =
          fnContext.getTimerFamilyDeclarations().size() > 0 && rawType.equals(String.class);
      paramErrors.checkArgument(
          isValidTimerIdForTimerFamily, "%s not allowed here", format(DoFn.TimerId.class));
      return Parameter.timerIdParameter();
    } else if (rawType.equals(TimerMap.class)) {
      String id = getTimerFamilyId(param.getAnnotations());

      paramErrors.checkArgument(
          id != null,
          "%s missing %s annotation",
          format(TimerMap.class),
          format(DoFn.TimerFamily.class));

      paramErrors.checkArgument(
          !methodContext.getTimerFamilyParameters().containsKey(id),
          "duplicate %s: \"%s\"",
          format(DoFn.TimerFamily.class),
          id);

      TimerFamilyDeclaration timerDecl = fnContext.getTimerFamilyDeclarations().get(id);
      paramErrors.checkArgument(
          timerDecl != null,
          "reference to undeclared %s: \"%s\"",
          format(DoFn.TimerFamily.class),
          id);

      paramErrors.checkArgument(
          timerDecl.field().getDeclaringClass().equals(getDeclaringClass(param.getMethod())),
          "%s %s declared in a different class %s."
              + " Timers may be referenced only in the lexical scope where they are declared.",
          format(DoFn.TimerFamily.class),
          id,
          timerDecl.field().getDeclaringClass().getName());

      return Parameter.timerFamilyParameter(timerDecl);
    } else if (State.class.isAssignableFrom(rawType)) {
      // m.getParameters() is not available until Java 8
      String id = getStateId(param.getAnnotations());
      paramErrors.checkArgument(id != null, "missing %s annotation", format(DoFn.StateId.class));

      paramErrors.checkArgument(
          !methodContext.getStateParameters().containsKey(id),
          "duplicate %s: \"%s\"",
          format(DoFn.StateId.class),
          id);

      // By static typing this is already a well-formed State subclass
      TypeDescriptor<? extends State> stateType = (TypeDescriptor<? extends State>) param.getType();

      StateDeclaration stateDecl = fnContext.getStateDeclarations().get(id);
      paramErrors.checkArgument(
          stateDecl != null, "reference to undeclared %s: \"%s\"", format(DoFn.StateId.class), id);

      paramErrors.checkArgument(
          stateDecl.stateType().isSubtypeOf(stateType),
          "data type of reference to %s %s must be a supertype of %s",
          format(StateId.class),
          id,
          format(stateDecl.stateType()));

      paramErrors.checkArgument(
          stateDecl.field().getDeclaringClass().equals(getDeclaringClass(param.getMethod())),
          "%s %s declared in a different class %s."
              + " State may be referenced only in the class where it is declared.",
          format(StateId.class),
          id,
          stateDecl.field().getDeclaringClass().getName());

      boolean alwaysFetched = getStateAlwaysFetched(param.getAnnotations());
      if (alwaysFetched) {
        paramErrors.checkArgument(
            ReadableState.class.isAssignableFrom(rawType),
            "@AlwaysFetched can only be used on ReadableStates. It cannot be used on %s",
            format(stateDecl.stateType()));
      }
      return Parameter.stateParameter(stateDecl, alwaysFetched);
    } else {
      paramErrors.throwIllegalArgument("%s is not a valid context parameter.", format(paramT));
      // Unreachable
      return null;
    }
  }

  private static @Nullable String getTimerId(List<Annotation> annotations) {
    DoFn.TimerId timerId = findFirstOfType(annotations, DoFn.TimerId.class);
    return timerId != null ? TimerDeclaration.PREFIX + timerId.value() : null;
  }

  private static @Nullable String getTimerFamilyId(List<Annotation> annotations) {
    DoFn.TimerFamily timerFamilyId = findFirstOfType(annotations, DoFn.TimerFamily.class);
    return timerFamilyId != null ? TimerFamilyDeclaration.PREFIX + timerFamilyId.value() : null;
  }

  private static @Nullable String getStateId(List<Annotation> annotations) {
    DoFn.StateId stateId = findFirstOfType(annotations, DoFn.StateId.class);
    return stateId != null ? stateId.value() : null;
  }

  private static boolean getStateAlwaysFetched(List<Annotation> annotations) {
    DoFn.AlwaysFetched alwaysFetched = findFirstOfType(annotations, DoFn.AlwaysFetched.class);
    return alwaysFetched != null;
  }

  private static @Nullable String getFieldAccessId(List<Annotation> annotations) {
    DoFn.FieldAccess access = findFirstOfType(annotations, DoFn.FieldAccess.class);
    return access != null ? access.value() : null;
  }

  private static @Nullable String getSideInputId(List<Annotation> annotations) {
    DoFn.SideInput sideInputId = findFirstOfType(annotations, DoFn.SideInput.class);
    return sideInputId != null ? sideInputId.value() : null;
  }

  static @Nullable <T> T findFirstOfType(List<Annotation> annotations, Class<T> clazz) {
    Optional<Annotation> annotation =
        annotations.stream().filter(a -> a.annotationType().equals(clazz)).findFirst();
    return annotation.isPresent() ? (T) annotation.get() : null;
  }

  private static boolean hasAnnotation(Class<?> annotation, List<Annotation> annotations) {
    return annotations.stream().anyMatch(a -> a.annotationType().equals(annotation));
  }

  private static @Nullable TypeDescriptor<? extends BoundedWindow> getWindowType(
      TypeDescriptor<?> fnClass, Method method) {
    Type[] params = method.getGenericParameterTypes();
    for (Type param : params) {
      TypeDescriptor<?> paramT = fnClass.resolveType(param);
      if (BoundedWindow.class.isAssignableFrom(paramT.getRawType())) {
        return (TypeDescriptor<? extends BoundedWindow>) paramT;
      }
    }
    return null;
  }

  @VisibleForTesting
  static DoFnSignature.BundleMethod analyzeStartBundleMethod(
      ErrorReporter errors,
      TypeDescriptor<? extends DoFn<?, ?>> fnT,
      Method m,
      TypeDescriptor<?> inputT,
      TypeDescriptor<?> outputT,
      FnAnalysisContext fnContext) {
    errors.checkArgument(void.class.equals(m.getReturnType()), "Must return void");
    Type[] params = m.getGenericParameterTypes();
    MethodAnalysisContext methodContext = MethodAnalysisContext.create();
    for (int i = 0; i < params.length; ++i) {
      Parameter extraParam =
          analyzeExtraParameter(
              errors,
              fnContext,
              methodContext,
              fnT,
              ParameterDescription.of(
                  m, i, fnT.resolveType(params[i]), Arrays.asList(m.getParameterAnnotations()[i])),
              inputT,
              outputT);
      methodContext.addParameter(extraParam);
    }

    for (Parameter parameter : methodContext.getExtraParameters()) {
      checkParameterOneOf(errors, parameter, ALLOWED_START_BUNDLE_PARAMETERS);
    }

    return DoFnSignature.BundleMethod.create(m, methodContext.extraParameters);
  }

  @VisibleForTesting
  static DoFnSignature.BundleMethod analyzeFinishBundleMethod(
      ErrorReporter errors,
      TypeDescriptor<? extends DoFn<?, ?>> fnT,
      Method m,
      TypeDescriptor<?> inputT,
      TypeDescriptor<?> outputT,
      FnAnalysisContext fnContext) {
    errors.checkArgument(void.class.equals(m.getReturnType()), "Must return void");
    Type[] params = m.getGenericParameterTypes();
    MethodAnalysisContext methodContext = MethodAnalysisContext.create();
    for (int i = 0; i < params.length; ++i) {
      Parameter extraParam =
          analyzeExtraParameter(
              errors,
              fnContext,
              methodContext,
              fnT,
              ParameterDescription.of(
                  m, i, fnT.resolveType(params[i]), Arrays.asList(m.getParameterAnnotations()[i])),
              inputT,
              outputT);
      methodContext.addParameter(extraParam);
    }

    for (Parameter parameter : methodContext.getExtraParameters()) {
      checkParameterOneOf(errors, parameter, ALLOWED_FINISH_BUNDLE_PARAMETERS);
    }

    return DoFnSignature.BundleMethod.create(m, methodContext.extraParameters);
  }

  private static DoFnSignature.LifecycleMethod analyzeLifecycleMethod(
      ErrorReporter errors, Method m) {
    errors.checkArgument(void.class.equals(m.getReturnType()), "Must return void");
    errors.checkArgument(m.getGenericParameterTypes().length == 0, "Must take zero arguments");
    return DoFnSignature.LifecycleMethod.create(m);
  }

  @VisibleForTesting
  static DoFnSignature.GetInitialRestrictionMethod analyzeGetInitialRestrictionMethod(
      ErrorReporter errors,
      TypeDescriptor<? extends DoFn<?, ?>> fnT,
      Method m,
      TypeDescriptor<?> inputT,
      TypeDescriptor<?> outputT,
      FnAnalysisContext fnContext) {
    // Method is of the form:
    // @GetInitialRestriction
    // RestrictionT getInitialRestriction(... parameters ...);

    Type[] params = m.getGenericParameterTypes();
    MethodAnalysisContext methodContext = MethodAnalysisContext.create();
    TypeDescriptor<? extends BoundedWindow> windowT = getWindowType(fnT, m);
    for (int i = 0; i < params.length; ++i) {
      Parameter extraParam =
          analyzeExtraParameter(
              errors,
              fnContext,
              methodContext,
              fnT,
              ParameterDescription.of(
                  m, i, fnT.resolveType(params[i]), Arrays.asList(m.getParameterAnnotations()[i])),
              inputT,
              outputT);
      if (extraParam instanceof SchemaElementParameter) {
        errors.throwIllegalArgument(
            "Schema @%s are not supported for @%s method. Found %s, did you mean to use %s?",
            format(DoFn.Element.class),
            format(DoFn.GetInitialRestriction.class),
            format(((SchemaElementParameter) extraParam).elementT()),
            format(inputT));
      }
      methodContext.addParameter(extraParam);
    }

    for (Parameter parameter : methodContext.getExtraParameters()) {
      checkParameterOneOf(errors, parameter, ALLOWED_GET_INITIAL_RESTRICTION_PARAMETERS);
    }

    return DoFnSignature.GetInitialRestrictionMethod.create(
        m, fnT.resolveType(m.getGenericReturnType()), windowT, methodContext.extraParameters);
  }

  @VisibleForTesting
  static DoFnSignature.GetInitialWatermarkEstimatorStateMethod
      analyzeGetInitialWatermarkEstimatorStateMethod(
          ErrorReporter errors,
          TypeDescriptor<? extends DoFn<?, ?>> fnT,
          Method m,
          TypeDescriptor<?> inputT,
          TypeDescriptor<?> outputT,
          FnAnalysisContext fnContext) {
    // Method is of the form:
    // @GetInitialWatermarkEstimatorState
    // WatermarkEstimatorStateT getInitialWatermarkEstimatorState(... parameters ...);

    Type[] params = m.getGenericParameterTypes();
    MethodAnalysisContext methodContext = MethodAnalysisContext.create();
    TypeDescriptor<? extends BoundedWindow> windowT = getWindowType(fnT, m);
    for (int i = 0; i < params.length; ++i) {
      Parameter extraParam =
          analyzeExtraParameter(
              errors,
              fnContext,
              methodContext,
              fnT,
              ParameterDescription.of(
                  m, i, fnT.resolveType(params[i]), Arrays.asList(m.getParameterAnnotations()[i])),
              inputT,
              outputT);
      if (extraParam instanceof SchemaElementParameter) {
        errors.throwIllegalArgument(
            "Schema @%s are not supported for @%s method. Found %s, did you mean to use %s?",
            format(DoFn.Element.class),
            format(DoFn.GetInitialWatermarkEstimatorState.class),
            format(((SchemaElementParameter) extraParam).elementT()),
            format(inputT));
      }
      methodContext.addParameter(extraParam);
    }

    for (Parameter parameter : methodContext.getExtraParameters()) {
      checkParameterOneOf(
          errors, parameter, ALLOWED_GET_INITIAL_WATERMARK_ESTIMATOR_STATE_PARAMETERS);
    }

    return DoFnSignature.GetInitialWatermarkEstimatorStateMethod.create(
        m, fnT.resolveType(m.getGenericReturnType()), windowT, methodContext.extraParameters);
  }

  /**
   * Generates a {@link TypeDescriptor} for {@code DoFn.OutputReceiver<OutputT>} given {@code
   * OutputT}.
   */
  private static <OutputT> TypeDescriptor<DoFn.OutputReceiver<OutputT>> outputReceiverTypeOf(
      TypeDescriptor<OutputT> outputT) {
    return new TypeDescriptor<DoFn.OutputReceiver<OutputT>>() {}.where(
        new TypeParameter<OutputT>() {}, outputT);
  }

  @VisibleForTesting
  static DoFnSignature.SplitRestrictionMethod analyzeSplitRestrictionMethod(
      ErrorReporter errors,
      TypeDescriptor<? extends DoFn<?, ?>> fnT,
      Method m,
      TypeDescriptor<?> inputT,
      TypeDescriptor<?> outputT,
      TypeDescriptor<?> restrictionT,
      FnAnalysisContext fnContext) {
    // Method is of the form:
    // @SplitRestriction
    // void splitRestriction(... parameters ...);
    errors.checkArgument(void.class.equals(m.getReturnType()), "Must return void");

    Type[] params = m.getGenericParameterTypes();
    MethodAnalysisContext methodContext = MethodAnalysisContext.create();
    TypeDescriptor<? extends BoundedWindow> windowT = getWindowType(fnT, m);
    for (int i = 0; i < params.length; ++i) {
      Parameter extraParam =
          analyzeExtraParameter(
              errors,
              fnContext,
              methodContext,
              fnT,
              ParameterDescription.of(
                  m, i, fnT.resolveType(params[i]), Arrays.asList(m.getParameterAnnotations()[i])),
              inputT,
              restrictionT);
      if (extraParam instanceof SchemaElementParameter) {
        errors.throwIllegalArgument(
            "Schema @%s are not supported for @%s method. Found %s, did you mean to use %s?",
            format(DoFn.Element.class),
            format(DoFn.SplitRestriction.class),
            format(((SchemaElementParameter) extraParam).elementT()),
            format(inputT));
      } else if (extraParam instanceof RestrictionParameter) {
        errors.checkArgument(
            restrictionT.equals(((RestrictionParameter) extraParam).restrictionT()),
            "Uses restriction type %s, but @%s method uses restriction type %s",
            format(((RestrictionParameter) extraParam).restrictionT()),
            format(DoFn.GetInitialRestriction.class),
            format(restrictionT));
      }
      methodContext.addParameter(extraParam);
    }

    for (Parameter parameter : methodContext.getExtraParameters()) {
      checkParameterOneOf(errors, parameter, ALLOWED_SPLIT_RESTRICTION_PARAMETERS);
    }

    return DoFnSignature.SplitRestrictionMethod.create(
        m, windowT, methodContext.getExtraParameters());
  }

  @VisibleForTesting
  static DoFnSignature.TruncateRestrictionMethod analyzeTruncateRestrictionMethod(
      ErrorReporter errors,
      TypeDescriptor<? extends DoFn<?, ?>> fnT,
      Method m,
      TypeDescriptor<?> inputT,
      TypeDescriptor<?> restrictionT,
      FnAnalysisContext fnContext) {
    // Method is of the form:
    // @TruncateRestriction
    // TruncateResult<RestrictionT> truncateRestriction(... parameters ...);
    errors.checkArgument(
        TruncateResult.class.equals(m.getReturnType()), "Must return TruncateResult<Restriction>");
    Type[] params = m.getGenericParameterTypes();
    MethodAnalysisContext methodContext = MethodAnalysisContext.create();
    TypeDescriptor<? extends BoundedWindow> windowT = getWindowType(fnT, m);
    for (int i = 0; i < params.length; ++i) {
      Parameter extraParam =
          analyzeExtraParameter(
              errors,
              fnContext,
              methodContext,
              fnT,
              ParameterDescription.of(
                  m, i, fnT.resolveType(params[i]), Arrays.asList(m.getParameterAnnotations()[i])),
              inputT,
              restrictionT);
      if (extraParam instanceof SchemaElementParameter) {
        errors.throwIllegalArgument(
            "Schema @%s are not supported for @%s method. Found %s, did you mean to use %s?",
            format(DoFn.Element.class),
            format(TruncateRestriction.class),
            format(((SchemaElementParameter) extraParam).elementT()),
            format(inputT));
      } else if (extraParam instanceof RestrictionParameter) {
        errors.checkArgument(
            restrictionT.equals(((RestrictionParameter) extraParam).restrictionT()),
            "Uses restriction type %s, but @%s method uses restriction type %s",
            format(((RestrictionParameter) extraParam).restrictionT()),
            format(DoFn.GetInitialRestriction.class),
            format(restrictionT));
      }
      methodContext.addParameter(extraParam);
    }

    for (Parameter parameter : methodContext.getExtraParameters()) {
      checkParameterOneOf(errors, parameter, ALLOWED_TRUNCATE_RESTRICTION_PARAMETERS);
    }

    return DoFnSignature.TruncateRestrictionMethod.create(
        m, windowT, methodContext.getExtraParameters());
  }

  private static ImmutableMap<String, TimerFamilyDeclaration> analyzeTimerFamilyDeclarations(
      ErrorReporter errors, Class<?> fnClazz) {
    Map<String, TimerFamilyDeclaration> declarations = new HashMap<>();
    for (Field field : declaredFieldsWithAnnotation(DoFn.TimerFamily.class, fnClazz, DoFn.class)) {
      // TimerSpec fields may generally be private, but will be accessed via the signature
      field.setAccessible(true);
      String id =
          TimerFamilyDeclaration.PREFIX + field.getAnnotation(DoFn.TimerFamily.class).value();
      validateTimerFamilyField(errors, declarations, id, field);
      declarations.put(id, TimerFamilyDeclaration.create(id, field));
    }

    return ImmutableMap.copyOf(declarations);
  }

  private static ImmutableMap<String, TimerDeclaration> analyzeTimerDeclarations(
      ErrorReporter errors, Class<?> fnClazz) {
    Map<String, DoFnSignature.TimerDeclaration> declarations = new HashMap<>();
    for (Field field : declaredFieldsWithAnnotation(DoFn.TimerId.class, fnClazz, DoFn.class)) {
      // TimerSpec fields may generally be private, but will be accessed via the signature
      field.setAccessible(true);
      // Add fixed prefix to avoid key collision with TimerFamily.
      String id =
          DoFnSignature.TimerDeclaration.PREFIX + field.getAnnotation(DoFn.TimerId.class).value();
      validateTimerField(errors, declarations, id, field);
      declarations.put(id, DoFnSignature.TimerDeclaration.create(id, field));
    }

    return ImmutableMap.copyOf(declarations);
  }

  /**
   * Returns successfully if the field is valid, otherwise throws an exception via its {@link
   * ErrorReporter} parameter describing validation failures for the timer declaration.
   */
  private static void validateTimerField(
      ErrorReporter errors, Map<String, TimerDeclaration> declarations, String id, Field field) {
    if (declarations.containsKey(id)) {
      errors.throwIllegalArgument(
          "Duplicate %s \"%s\", used on both of [%s] and [%s]",
          format(DoFn.TimerId.class),
          id,
          field.toString(),
          declarations.get(id).field().toString());
    }

    Class<?> timerSpecRawType = field.getType();
    if (!(timerSpecRawType.equals(TimerSpec.class))) {
      errors.throwIllegalArgument(
          "%s annotation on non-%s field [%s]",
          format(DoFn.TimerId.class), format(TimerSpec.class), field.toString());
    }

    if (!Modifier.isFinal(field.getModifiers())) {
      errors.throwIllegalArgument(
          "Non-final field %s annotated with %s. Timer declarations must be final.",
          field.toString(), format(DoFn.TimerId.class));
    }
  }

  /**
   * Returns successfully if the field is valid, otherwise throws an exception via its {@link
   * ErrorReporter} parameter describing validation failures for the timer family declaration.
   */
  private static void validateTimerFamilyField(
      ErrorReporter errors,
      Map<String, TimerFamilyDeclaration> declarations,
      String id,
      Field field) {
    if (id.isEmpty()) {
      errors.throwIllegalArgument("TimerFamily id must not be empty");
    }

    if (declarations.containsKey(id)) {
      errors.throwIllegalArgument(
          "Duplicate %s \"%s\", used on both of [%s] and [%s]",
          format(DoFn.TimerFamily.class),
          id,
          field.toString(),
          declarations.get(id).field().toString());
    }

    Class<?> timerSpecRawType = field.getType();
    if (!(timerSpecRawType.equals(TimerSpec.class))) {
      errors.throwIllegalArgument(
          "%s annotation on non-%s field [%s]",
          format(DoFn.TimerFamily.class), format(TimerSpec.class), field.toString());
    }

    if (!Modifier.isFinal(field.getModifiers())) {
      errors.throwIllegalArgument(
          "Non-final field %s annotated with %s. TimerMap declarations must be final.",
          field.toString(), format(DoFn.TimerFamily.class));
    }
  }

  /** Generates a {@link TypeDescriptor} for {@code Coder<T>} given {@code T}. */
  private static <T> TypeDescriptor<Coder<T>> coderTypeOf(TypeDescriptor<T> elementT) {
    return new TypeDescriptor<Coder<T>>() {}.where(new TypeParameter<T>() {}, elementT);
  }

  @VisibleForTesting
  static DoFnSignature.GetRestrictionCoderMethod analyzeGetRestrictionCoderMethod(
      ErrorReporter errors, TypeDescriptor<? extends DoFn> fnT, Method m) {
    errors.checkArgument(m.getParameterTypes().length == 0, "Must have zero arguments");
    TypeDescriptor<?> resT = fnT.resolveType(m.getGenericReturnType());
    errors.checkArgument(
        resT.isSubtypeOf(TypeDescriptor.of(Coder.class)),
        "Must return a Coder, but returns %s",
        format(resT));
    return DoFnSignature.GetRestrictionCoderMethod.create(m, resT);
  }

  @VisibleForTesting
  static DoFnSignature.GetWatermarkEstimatorStateCoderMethod
      analyzeGetWatermarkEstimatorStateCoderMethod(
          ErrorReporter errors, TypeDescriptor<? extends DoFn> fnT, Method m) {
    errors.checkArgument(m.getParameterTypes().length == 0, "Must have zero arguments");
    TypeDescriptor<?> resT = fnT.resolveType(m.getGenericReturnType());
    errors.checkArgument(
        resT.isSubtypeOf(TypeDescriptor.of(Coder.class)),
        "Must return a Coder, but returns %s",
        format(resT));
    return DoFnSignature.GetWatermarkEstimatorStateCoderMethod.create(m, resT);
  }

  /**
   * Generates a {@link TypeDescriptor} for {@code RestrictionTracker<RestrictionT, ?>} given {@code
   * RestrictionT}.
   */
  private static <RestrictionT>
      TypeDescriptor<RestrictionTracker<RestrictionT, ?>> restrictionTrackerTypeOf(
          TypeDescriptor<RestrictionT> restrictionT) {
    return new TypeDescriptor<RestrictionTracker<RestrictionT, ?>>() {}.where(
        new TypeParameter<RestrictionT>() {}, restrictionT);
  }

  /**
   * Generates a {@link TypeDescriptor} for {@code WatermarkEstimator<WatermarkEstimatorStateT>}
   * given {@code WatermarkEstimatorStateT}.
   */
  private static <WatermarkEstimatorStateT>
      TypeDescriptor<WatermarkEstimator<WatermarkEstimatorStateT>> watermarkEstimatorTypeOf(
          TypeDescriptor<WatermarkEstimatorStateT> watermarkEstimatorStateT) {
    return new TypeDescriptor<WatermarkEstimator<WatermarkEstimatorStateT>>() {}.where(
        new TypeParameter<WatermarkEstimatorStateT>() {}, watermarkEstimatorStateT);
  }

  @VisibleForTesting
  static DoFnSignature.NewTrackerMethod analyzeNewTrackerMethod(
      ErrorReporter errors,
      TypeDescriptor<? extends DoFn<?, ?>> fnT,
      Method m,
      TypeDescriptor<?> inputT,
      TypeDescriptor<?> outputT,
      TypeDescriptor<?> restrictionT,
      FnAnalysisContext fnContext) {
    // Method is of the form:
    // @NewTracker
    // TrackerT newTracker(... parameters ...);
    Type[] params = m.getGenericParameterTypes();
    TypeDescriptor<?> trackerT = fnT.resolveType(m.getGenericReturnType());
    TypeDescriptor<?> expectedTrackerT = restrictionTrackerTypeOf(restrictionT);
    errors.checkArgument(
        trackerT.isSubtypeOf(expectedTrackerT),
        "Returns %s, but must return a subtype of %s",
        format(trackerT),
        format(expectedTrackerT));

    MethodAnalysisContext methodContext = MethodAnalysisContext.create();
    TypeDescriptor<? extends BoundedWindow> windowT = getWindowType(fnT, m);
    for (int i = 0; i < params.length; ++i) {
      Parameter extraParam =
          analyzeExtraParameter(
              errors,
              fnContext,
              methodContext,
              fnT,
              ParameterDescription.of(
                  m, i, fnT.resolveType(params[i]), Arrays.asList(m.getParameterAnnotations()[i])),
              inputT,
              outputT);
      if (extraParam instanceof SchemaElementParameter) {
        errors.throwIllegalArgument(
            "Schema @%s are not supported for @%s method. Found %s, did you mean to use %s?",
            format(DoFn.Element.class),
            format(DoFn.NewTracker.class),
            format(((SchemaElementParameter) extraParam).elementT()),
            format(inputT));
      } else if (extraParam instanceof RestrictionParameter) {
        errors.checkArgument(
            restrictionT.equals(((RestrictionParameter) extraParam).restrictionT()),
            "Uses restriction type %s, but @%s method uses restriction type %s",
            format(((RestrictionParameter) extraParam).restrictionT()),
            format(DoFn.GetInitialRestriction.class),
            format(restrictionT));
      }
      methodContext.addParameter(extraParam);
    }

    for (Parameter parameter : methodContext.getExtraParameters()) {
      checkParameterOneOf(errors, parameter, ALLOWED_NEW_TRACKER_PARAMETERS);
    }

    return DoFnSignature.NewTrackerMethod.create(
        m, fnT.resolveType(m.getGenericReturnType()), windowT, methodContext.getExtraParameters());
  }

  @VisibleForTesting
  static DoFnSignature.NewWatermarkEstimatorMethod analyzeNewWatermarkEstimatorMethod(
      ErrorReporter errors,
      TypeDescriptor<? extends DoFn<?, ?>> fnT,
      Method m,
      TypeDescriptor<?> inputT,
      TypeDescriptor<?> outputT,
      TypeDescriptor<?> restrictionT,
      TypeDescriptor<?> watermarkEstimatorStateT,
      FnAnalysisContext fnContext) {
    // Method is of the form:
    // @NewWatermarkEstimator
    // WatermarkEstimatorT newWatermarkEstimator(... parameters ...);
    Type[] params = m.getGenericParameterTypes();
    TypeDescriptor<?> watermarkEstimatorT = fnT.resolveType(m.getGenericReturnType());
    TypeDescriptor<?> expectedWatermarkEstimatorT =
        watermarkEstimatorTypeOf(watermarkEstimatorStateT);
    errors.checkArgument(
        watermarkEstimatorT.isSubtypeOf(expectedWatermarkEstimatorT),
        "Returns %s, but must return a subtype of %s",
        format(watermarkEstimatorT),
        format(expectedWatermarkEstimatorT));

    MethodAnalysisContext methodContext = MethodAnalysisContext.create();
    TypeDescriptor<? extends BoundedWindow> windowT = getWindowType(fnT, m);
    for (int i = 0; i < params.length; ++i) {
      Parameter extraParam =
          analyzeExtraParameter(
              errors,
              fnContext,
              methodContext,
              fnT,
              ParameterDescription.of(
                  m, i, fnT.resolveType(params[i]), Arrays.asList(m.getParameterAnnotations()[i])),
              inputT,
              outputT);
      if (extraParam instanceof SchemaElementParameter) {
        errors.throwIllegalArgument(
            "Schema @%s are not supported for @%s method. Found %s, did you mean to use %s?",
            format(DoFn.Element.class),
            format(DoFn.NewWatermarkEstimator.class),
            format(((SchemaElementParameter) extraParam).elementT()),
            format(inputT));
      } else if (extraParam instanceof RestrictionParameter) {
        errors.checkArgument(
            restrictionT.equals(((RestrictionParameter) extraParam).restrictionT()),
            "Uses restriction type %s, but @%s method uses restriction type %s",
            format(((RestrictionParameter) extraParam).restrictionT()),
            format(DoFn.GetInitialWatermarkEstimatorState.class),
            format(restrictionT));
      } else if (extraParam instanceof WatermarkEstimatorStateParameter) {
        errors.checkArgument(
            watermarkEstimatorStateT.equals(
                ((WatermarkEstimatorStateParameter) extraParam).estimatorStateT()),
            "Uses watermark estimator state type %s, but @%s method uses watermark estimator state type %s",
            format(((WatermarkEstimatorStateParameter) extraParam).estimatorStateT()),
            format(DoFn.GetInitialWatermarkEstimatorState.class),
            format(watermarkEstimatorStateT));
      }
      methodContext.addParameter(extraParam);
    }

    for (Parameter parameter : methodContext.getExtraParameters()) {
      checkParameterOneOf(errors, parameter, ALLOWED_NEW_WATERMARK_ESTIMATOR_PARAMETERS);
    }

    return DoFnSignature.NewWatermarkEstimatorMethod.create(
        m, fnT.resolveType(m.getGenericReturnType()), windowT, methodContext.getExtraParameters());
  }

  @VisibleForTesting
  static DoFnSignature.GetSizeMethod analyzeGetSizeMethod(
      ErrorReporter errors,
      TypeDescriptor<? extends DoFn<?, ?>> fnT,
      Method m,
      TypeDescriptor<?> inputT,
      TypeDescriptor<?> outputT,
      TypeDescriptor<?> restrictionT,
      FnAnalysisContext fnContext) {
    // Method is of the form:
    // @GetSize
    // double getSize(... parameters ...);
    Type[] params = m.getGenericParameterTypes();

    errors.checkArgument(
        m.getGenericReturnType().equals(Double.TYPE),
        "Returns %s, but must return a double",
        format(TypeDescriptor.of(m.getGenericReturnType())));

    MethodAnalysisContext methodContext = MethodAnalysisContext.create();
    TypeDescriptor<? extends BoundedWindow> windowT = getWindowType(fnT, m);
    for (int i = 0; i < params.length; ++i) {
      Parameter extraParam =
          analyzeExtraParameter(
              errors,
              fnContext,
              methodContext,
              fnT,
              ParameterDescription.of(
                  m, i, fnT.resolveType(params[i]), Arrays.asList(m.getParameterAnnotations()[i])),
              inputT,
              outputT);
      if (extraParam instanceof SchemaElementParameter) {
        errors.throwIllegalArgument(
            "Schema @%s are not supported for @%s method. Found %s, did you mean to use %s?",
            format(DoFn.Element.class),
            format(DoFn.GetSize.class),
            format(((SchemaElementParameter) extraParam).elementT()),
            format(inputT));
      } else if (extraParam instanceof RestrictionParameter) {
        errors.checkArgument(
            restrictionT.equals(((RestrictionParameter) extraParam).restrictionT()),
            "Uses restriction type %s, but @%s method uses restriction type %s",
            format(((RestrictionParameter) extraParam).restrictionT()),
            format(DoFn.GetInitialRestriction.class),
            format(restrictionT));
      }

      methodContext.addParameter(extraParam);
    }

    for (Parameter parameter : methodContext.getExtraParameters()) {
      checkParameterOneOf(errors, parameter, ALLOWED_GET_SIZE_PARAMETERS);
    }

    return DoFnSignature.GetSizeMethod.create(m, windowT, methodContext.getExtraParameters());
  }

  private static Collection<Method> declaredMethodsWithAnnotation(
      Class<? extends Annotation> anno, Class<?> startClass, Class<?> stopClass) {
    return declaredMembersWithAnnotation(anno, startClass, stopClass, GET_METHODS);
  }

  private static Collection<Field> declaredFieldsWithAnnotation(
      Class<? extends Annotation> anno, Class<?> startClass, Class<?> stopClass) {
    return declaredMembersWithAnnotation(anno, startClass, stopClass, GET_FIELDS);
  }

  private interface MemberGetter<MemberT> {
    MemberT[] getMembers(Class<?> clazz);
  }

  private static final MemberGetter<Method> GET_METHODS = Class::getDeclaredMethods;

  private static final MemberGetter<Field> GET_FIELDS = Class::getDeclaredFields;

  private static <MemberT extends AnnotatedElement>
      Collection<MemberT> declaredMembersWithAnnotation(
          Class<? extends Annotation> anno,
          Class<?> startClass,
          Class<?> stopClass,
          MemberGetter<MemberT> getter) {
    Collection<MemberT> matches = new ArrayList<>();

    Class<?> clazz = startClass;
    LinkedHashSet<Class<?>> interfaces = new LinkedHashSet<>();

    // First, find all declared methods on the startClass and parents (up to stopClass)
    while (clazz != null && !clazz.equals(stopClass)) {
      for (MemberT member : getter.getMembers(clazz)) {
        if (member.isAnnotationPresent(anno)) {
          matches.add(member);
        }
      }

      // Add all interfaces, including transitive
      for (TypeDescriptor<?> iface : TypeDescriptor.of(clazz).getInterfaces()) {
        interfaces.add(iface.getRawType());
      }

      clazz = clazz.getSuperclass();
    }

    // Now, iterate over all the discovered interfaces
    for (Class<?> iface : interfaces) {
      for (MemberT member : getter.getMembers(iface)) {
        if (member.isAnnotationPresent(anno)) {
          matches.add(member);
        }
      }
    }
    return matches;
  }

  private static Map<String, DoFnSignature.FieldAccessDeclaration> analyzeFieldAccessDeclaration(
      ErrorReporter errors, Class<?> fnClazz) {
    Map<String, FieldAccessDeclaration> fieldAccessDeclarations = new HashMap<>();
    for (Field field : declaredFieldsWithAnnotation(DoFn.FieldAccess.class, fnClazz, DoFn.class)) {
      field.setAccessible(true);
      DoFn.FieldAccess fieldAccessAnnotation = field.getAnnotation(DoFn.FieldAccess.class);
      if (!Modifier.isFinal(field.getModifiers())) {
        errors.throwIllegalArgument(
            "Non-final field %s annotated with %s. Field access declarations must be final.",
            field.toString(), format(DoFn.FieldAccess.class));
        continue;
      }
      Class<?> fieldAccessRawType = field.getType();
      if (!fieldAccessRawType.equals(FieldAccessDescriptor.class)) {
        errors.throwIllegalArgument(
            "Field %s annotated with %s, but the value was not of type %s",
            field.toString(), format(DoFn.FieldAccess.class), format(FieldAccessDescriptor.class));
      }
      fieldAccessDeclarations.put(
          fieldAccessAnnotation.value(),
          FieldAccessDeclaration.create(fieldAccessAnnotation.value(), field));
    }
    return fieldAccessDeclarations;
  }

  private static Map<String, DoFnSignature.StateDeclaration> analyzeStateDeclarations(
      ErrorReporter errors, Class<?> fnClazz) {

    Map<String, DoFnSignature.StateDeclaration> declarations = new HashMap<>();

    for (Field field : declaredFieldsWithAnnotation(DoFn.StateId.class, fnClazz, DoFn.class)) {
      // StateSpec fields may generally be private, but will be accessed via the signature
      field.setAccessible(true);
      String id = field.getAnnotation(DoFn.StateId.class).value();

      if (declarations.containsKey(id)) {
        errors.throwIllegalArgument(
            "Duplicate %s \"%s\", used on both of [%s] and [%s]",
            format(DoFn.StateId.class),
            id,
            field.toString(),
            declarations.get(id).field().toString());
        continue;
      }

      Class<?> stateSpecRawType = field.getType();
      if (!(TypeDescriptor.of(stateSpecRawType).isSubtypeOf(TypeDescriptor.of(StateSpec.class)))) {
        errors.throwIllegalArgument(
            "%s annotation on non-%s field [%s] that has class %s",
            format(DoFn.StateId.class),
            format(StateSpec.class),
            field.toString(),
            stateSpecRawType.getName());
        continue;
      }

      if (!Modifier.isFinal(field.getModifiers())) {
        errors.throwIllegalArgument(
            "Non-final field %s annotated with %s. State declarations must be final.",
            field.toString(), format(DoFn.StateId.class));
        continue;
      }

      Type stateSpecType = field.getGenericType();

      // A type descriptor for whatever type the @StateId-annotated class has, which
      // must be some subtype of StateSpec
      TypeDescriptor<? extends StateSpec<?>> stateSpecSubclassTypeDescriptor =
          (TypeDescriptor) TypeDescriptor.of(stateSpecType);

      // A type descriptor for StateSpec, with the generic type parameters filled
      // in according to the specialization of the subclass (or just straight params)
      TypeDescriptor<StateSpec<?>> stateSpecTypeDescriptor =
          (TypeDescriptor) stateSpecSubclassTypeDescriptor.getSupertype(StateSpec.class);

      // The type of the state, which may still have free type variables from the
      // context
      Type unresolvedStateType =
          ((ParameterizedType) stateSpecTypeDescriptor.getType()).getActualTypeArguments()[0];

      // By static typing this is already a well-formed State subclass
      TypeDescriptor<? extends State> stateType =
          (TypeDescriptor<? extends State>)
              TypeDescriptor.of(fnClazz).resolveType(unresolvedStateType);

      declarations.put(id, DoFnSignature.StateDeclaration.create(id, field, stateType));
    }

    return ImmutableMap.copyOf(declarations);
  }

  private static @Nullable Method findAnnotatedMethod(
      ErrorReporter errors, Class<? extends Annotation> anno, Class<?> fnClazz, boolean required) {
    Collection<Method> matches = declaredMethodsWithAnnotation(anno, fnClazz, DoFn.class);

    if (matches.isEmpty()) {
      errors.checkArgument(!required, "No method annotated with @%s found", format(anno));
      return null;
    }

    // If we have at least one match, then either it should be the only match
    // or it should be an extension of the other matches (which came from parent
    // classes).
    Method first = matches.iterator().next();
    for (Method other : matches) {
      errors.checkArgument(
          first.getName().equals(other.getName())
              && Arrays.equals(first.getParameterTypes(), other.getParameterTypes()),
          "Found multiple methods annotated with @%s. [%s] and [%s]",
          format(anno),
          format(first),
          format(other));
    }

    ErrorReporter methodErrors = errors.forMethod(anno, first);
    // We need to be able to call it. We require it is public.
    methodErrors.checkArgument((first.getModifiers() & Modifier.PUBLIC) != 0, "Must be public");
    // And make sure its not static.
    methodErrors.checkArgument((first.getModifiers() & Modifier.STATIC) == 0, "Must not be static");

    return first;
  }

  private static String format(Method method) {
    return ReflectHelpers.formatMethod(method);
  }

  private static String format(TypeDescriptor<?> t) {
    return ReflectHelpers.simpleTypeDescription(t.getType());
  }

  private static String format(Class<?> kls) {
    return kls.getSimpleName();
  }

  static class ErrorReporter {
    private final String label;

    ErrorReporter(@Nullable ErrorReporter root, String label) {
      this.label = (root == null) ? label : String.format("%s, %s", root.label, label);
    }

    ErrorReporter forMethod(Class<? extends Annotation> annotation, Method method) {
      return new ErrorReporter(
          this,
          String.format(
              "@%s %s", format(annotation), (method == null) ? "(absent)" : format(method)));
    }

    ErrorReporter forParameter(ParameterDescription param) {
      return new ErrorReporter(
          this,
          String.format(
              "parameter of type %s at index %s", format(param.getType()), param.getIndex()));
    }

    void throwIllegalArgument(String message, Object... args) {
      throw new IllegalArgumentException(label + ": " + String.format(message, args));
    }

    public void checkArgument(boolean condition, String message, Object... args) {
      if (!condition) {
        throwIllegalArgument(message, args);
      }
    }

    public void checkNotNull(Object value, String message, Object... args) {
      if (value == null) {
        throwIllegalArgument(message, args);
      }
    }
  }

  public static StateSpec<?> getStateSpecOrThrow(
      StateDeclaration stateDeclaration, DoFn<?, ?> target) {
    try {
      Object fieldValue = stateDeclaration.field().get(target);
      checkState(
          fieldValue instanceof StateSpec,
          "Malformed %s class %s: state declaration field %s does not have type %s.",
          format(DoFn.class),
          target.getClass().getName(),
          stateDeclaration.field().getName(),
          StateSpec.class);

      return (StateSpec<?>) stateDeclaration.field().get(target);
    } catch (IllegalAccessException exc) {
      throw new RuntimeException(
          String.format(
              "Malformed %s class %s: state declaration field %s is not accessible.",
              format(DoFn.class), target.getClass().getName(), stateDeclaration.field().getName()));
    }
  }

  public static TimerSpec getTimerSpecOrThrow(
      TimerDeclaration timerDeclaration, DoFn<?, ?> target) {
    try {
      Object fieldValue = timerDeclaration.field().get(target);
      checkState(
          fieldValue instanceof TimerSpec,
          "Malformed %s class %s: timer declaration field %s does not have type %s.",
          format(DoFn.class),
          target.getClass().getName(),
          timerDeclaration.field().getName(),
          TimerSpec.class);

      return (TimerSpec) timerDeclaration.field().get(target);
    } catch (IllegalAccessException exc) {
      throw new RuntimeException(
          String.format(
              "Malformed %s class %s: timer declaration field %s is not accessible.",
              format(DoFn.class), target.getClass().getName(), timerDeclaration.field().getName()));
    }
  }

  public static TimerSpec getTimerFamilySpecOrThrow(
      TimerFamilyDeclaration timerFamilyDeclaration, DoFn<?, ?> target) {
    try {
      Object fieldValue = timerFamilyDeclaration.field().get(target);
      checkState(
          fieldValue instanceof TimerSpec,
          "Malformed %s class %s: timer declaration field %s does not have type %s.",
          format(DoFn.class),
          target.getClass().getName(),
          timerFamilyDeclaration.field().getName(),
          TimerSpec.class);

      return (TimerSpec) timerFamilyDeclaration.field().get(target);
    } catch (IllegalAccessException exc) {
      throw new RuntimeException(
          String.format(
              "Malformed %s class %s: timer declaration field %s is not accessible.",
              format(DoFn.class),
              target.getClass().getName(),
              timerFamilyDeclaration.field().getName()));
    }
  }

  public static boolean isSplittable(DoFn<?, ?> doFn) {
    return signatureForDoFn(doFn).processElement().isSplittable();
  }

  public static boolean isStateful(DoFn<?, ?> doFn) {
    return usesState(doFn) || usesTimers(doFn);
  }

  public static boolean usesMapState(DoFn<?, ?> doFn) {
    return usesGivenStateClass(doFn, MapState.class);
  }

  public static boolean usesSetState(DoFn<?, ?> doFn) {
    return usesGivenStateClass(doFn, SetState.class);
  }

  public static boolean usesValueState(DoFn<?, ?> doFn) {
    return usesGivenStateClass(doFn, ValueState.class) || requiresTimeSortedInput(doFn);
  }

  public static boolean usesBagState(DoFn<?, ?> doFn) {
    return usesGivenStateClass(doFn, BagState.class) || requiresTimeSortedInput(doFn);
  }

  public static boolean usesWatermarkHold(DoFn<?, ?> doFn) {
    return usesGivenStateClass(doFn, WatermarkHoldState.class) || requiresTimeSortedInput(doFn);
  }

  public static boolean usesTimers(DoFn<?, ?> doFn) {
    return signatureForDoFn(doFn).usesTimers() || requiresTimeSortedInput(doFn);
  }

  public static boolean usesState(DoFn<?, ?> doFn) {
    return signatureForDoFn(doFn).usesState() || requiresTimeSortedInput(doFn);
  }

  public static boolean requiresTimeSortedInput(DoFn<?, ?> doFn) {
    return signatureForDoFn(doFn).processElement().requiresTimeSortedInput();
  }

  private static boolean usesGivenStateClass(DoFn<?, ?> doFn, Class<? extends State> stateClass) {
    return signatureForDoFn(doFn).stateDeclarations().values().stream()
        .anyMatch(d -> d.stateType().isSubtypeOf(TypeDescriptor.of(stateClass)));
  }
}
