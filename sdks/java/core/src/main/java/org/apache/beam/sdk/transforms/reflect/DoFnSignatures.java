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
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.SideInput;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.FieldAccessDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.GetInitialRestrictionMethod;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.RestrictionParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.RestrictionTrackerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.SchemaElementParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.StateParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.TimerFamilyParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.TimerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.WindowParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.StateDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerFamilyDeclaration;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
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
              Parameter.TimerFamilyParameter.class);

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
              Parameter.SideInputParameter.class);

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
          Parameter.TimerIdParameter.class);

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
              Parameter.StateParameter.class);

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
          Parameter.RestrictionTrackerParameter.class,
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
    @Nullable
    public Map<String, FieldAccessDeclaration> getFieldAccessDeclarations() {
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

    @Nullable private TypeDescriptor<? extends BoundedWindow> windowT;

    private MethodAnalysisContext() {}

    /** Indicates whether a {@link RestrictionTrackerParameter} is known in this context. */
    public boolean hasRestrictionTrackerParameter() {
      return extraParameters.stream()
          .anyMatch(Predicates.instanceOf(RestrictionTrackerParameter.class)::apply);
    }

    /** Indicates whether a {@link WindowParameter} is known in this context. */
    public boolean hasWindowParameter() {
      return extraParameters.stream().anyMatch(Predicates.instanceOf(WindowParameter.class)::apply);
    }

    /** Indicates whether a {@link Parameter.PipelineOptionsParameter} is known in this context. */
    public boolean hasPipelineOptionsParamter() {
      return extraParameters.stream()
          .anyMatch(Predicates.instanceOf(Parameter.PipelineOptionsParameter.class)::apply);
    }

    /** The window type, if any, used by this method. */
    @Nullable
    public TypeDescriptor<? extends BoundedWindow> getWindowType() {
      return windowT;
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
    Method getRestrictionCoderMethod =
        findAnnotatedMethod(errors, DoFn.GetRestrictionCoder.class, fnClass, false);
    Method newTrackerMethod = findAnnotatedMethod(errors, DoFn.NewTracker.class, fnClass, false);
    Method getSizeMethod = findAnnotatedMethod(errors, DoFn.GetSize.class, fnClass, false);

    Collection<Method> onTimerMethods =
        declaredMethodsWithAnnotation(DoFn.OnTimer.class, fnClass, DoFn.class);
    HashMap<String, DoFnSignature.OnTimerMethod> onTimerMethodMap =
        Maps.newHashMapWithExpectedSize(onTimerMethods.size());
    for (Method onTimerMethod : onTimerMethods) {
      String id = onTimerMethod.getAnnotation(DoFn.OnTimer.class).value();
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
      String id = onTimerFamilyMethod.getAnnotation(DoFn.OnTimerFamily.class).value();
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
          analyzeStartBundleMethod(startBundleErrors, fnT, startBundleMethod, inputT, outputT));
    }

    if (finishBundleMethod != null) {
      ErrorReporter finishBundleErrors =
          errors.forMethod(DoFn.FinishBundle.class, finishBundleMethod);
      signatureBuilder.setFinishBundle(
          analyzeFinishBundleMethod(finishBundleErrors, fnT, finishBundleMethod, inputT, outputT));
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

      GetInitialRestrictionMethod method =
          analyzeGetInitialRestrictionMethod(
              errors.forMethod(DoFn.GetInitialRestriction.class, getInitialRestrictionMethod),
              fnT,
              getInitialRestrictionMethod,
              inputT,
              outputT,
              fnContext);

      signatureBuilder.setGetInitialRestriction(method);
      TypeDescriptor<?> restrictionT = method.restrictionT();

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
    } else {
      // Validate that none of the splittable DoFn only methods have been declared.
      List<String> forbiddenMethods = new ArrayList<>();
      if (getInitialRestrictionMethod != null) {
        forbiddenMethods.add("@" + format(DoFn.GetInitialRestriction.class));
      }
      if (splitRestrictionMethod != null) {
        forbiddenMethods.add("@" + format(DoFn.SplitRestriction.class));
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

    ErrorReporter processElementErrors =
        errors.forMethod(DoFn.ProcessElement.class, processElement.targetMethod());

    TypeDescriptor<?> restrictionT = getInitialRestriction.restrictionT();

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

    Type[] params = m.getGenericParameterTypes();

    TypeDescriptor<?> trackerT = getTrackerType(fnClass, m);
    TypeDescriptor<? extends BoundedWindow> windowT = getWindowType(fnClass, m);

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

    // The allowed parameters depend on whether this DoFn is splittable
    if (methodContext.hasRestrictionTrackerParameter()) {
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
    TypeDescriptor<?> expectedOnTimerContextT = doFnOnTimerContextTypeOf(inputT, outputT);

    TypeDescriptor<?> paramT = param.getType();
    Class<?> rawType = paramT.getRawType();

    ErrorReporter paramErrors = methodErrors.forParameter(param);

    String fieldAccessString = getFieldAccessId(param.getAnnotations());
    if (fieldAccessString != null) {
      return Parameter.schemaElementParameter(paramT, fieldAccessString, param.getIndex());
    } else if (hasElementAnnotation(param.getAnnotations())) {
      return (paramT.equals(inputT))
          ? Parameter.elementParameter(paramT)
          : Parameter.schemaElementParameter(paramT, null, param.getIndex());
    } else if (hasRestrictionAnnotation(param.getAnnotations())) {
      return Parameter.restrictionParameter(paramT);
    } else if (hasTimestampAnnotation(param.getAnnotations())) {
      methodErrors.checkArgument(
          rawType.equals(Instant.class),
          "@Timestamp argument must have type org.joda.time.Instant.");
      return Parameter.timestampParameter();
    } else if (rawType.equals(TimeDomain.class)) {
      return Parameter.timeDomainParameter();
    } else if (hasSideInputAnnotation(param.getAnnotations())) {
      String sideInputId = getSideInputId(param.getAnnotations());
      paramErrors.checkArgument(
          sideInputId != null, "%s missing %s annotation", format(SideInput.class));
      return Parameter.sideInputParameter(paramT, sideInputId);
    } else if (rawType.equals(PaneInfo.class)) {
      return Parameter.paneInfoParameter();
    } else if (rawType.equals(DoFn.ProcessContext.class)) {
      paramErrors.checkArgument(
          paramT.equals(expectedProcessContextT),
          "ProcessContext argument must have type %s",
          format(expectedProcessContextT));
      return Parameter.processContext();
    } else if (rawType.equals(DoFn.OnTimerContext.class)) {
      paramErrors.checkArgument(
          paramT.equals(expectedOnTimerContextT),
          "OnTimerContext argument must have type %s",
          format(expectedOnTimerContextT));
      return Parameter.onTimerContext();
    } else if (BoundedWindow.class.isAssignableFrom(rawType)) {
      methodErrors.checkArgument(
          !methodContext.hasWindowParameter(),
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
          !methodContext.hasPipelineOptionsParamter(),
          "Multiple %s parameters",
          format(PipelineOptions.class));
      return Parameter.pipelineOptions();
    } else if (RestrictionTracker.class.isAssignableFrom(rawType)) {
      methodErrors.checkArgument(
          !methodContext.hasRestrictionTrackerParameter(),
          "Multiple %s parameters",
          format(RestrictionTracker.class));
      return Parameter.restrictionTracker(paramT);

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

    } else if (hasTimerIdAnnotation(param.getAnnotations())) {
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

      return Parameter.stateParameter(stateDecl);
    } else {
      paramErrors.throwIllegalArgument("%s is not a valid context parameter.", format(paramT));
      // Unreachable
      return null;
    }
  }

  @Nullable
  private static String getTimerId(List<Annotation> annotations) {
    DoFn.TimerId stateId = findFirstOfType(annotations, DoFn.TimerId.class);
    return stateId != null ? stateId.value() : null;
  }

  @Nullable
  private static String getTimerFamilyId(List<Annotation> annotations) {
    DoFn.TimerFamily timerFamilyId = findFirstOfType(annotations, DoFn.TimerFamily.class);
    return timerFamilyId != null ? timerFamilyId.value() : null;
  }

  @Nullable
  private static String getStateId(List<Annotation> annotations) {
    DoFn.StateId stateId = findFirstOfType(annotations, DoFn.StateId.class);
    return stateId != null ? stateId.value() : null;
  }

  @Nullable
  private static String getFieldAccessId(List<Annotation> annotations) {
    DoFn.FieldAccess access = findFirstOfType(annotations, DoFn.FieldAccess.class);
    return access != null ? access.value() : null;
  }

  @Nullable
  private static String getSideInputId(List<Annotation> annotations) {
    DoFn.SideInput sideInputId = findFirstOfType(annotations, DoFn.SideInput.class);
    return sideInputId != null ? sideInputId.value() : null;
  }

  @Nullable
  static <T> T findFirstOfType(List<Annotation> annotations, Class<T> clazz) {
    Optional<Annotation> annotation =
        annotations.stream().filter(a -> a.annotationType().equals(clazz)).findFirst();
    return annotation.isPresent() ? (T) annotation.get() : null;
  }

  private static boolean hasElementAnnotation(List<Annotation> annotations) {
    return annotations.stream().anyMatch(a -> a.annotationType().equals(DoFn.Element.class));
  }

  private static boolean hasRestrictionAnnotation(List<Annotation> annotations) {
    return annotations.stream().anyMatch(a -> a.annotationType().equals(DoFn.Restriction.class));
  }

  private static boolean hasTimestampAnnotation(List<Annotation> annotations) {
    return annotations.stream().anyMatch(a -> a.annotationType().equals(DoFn.Timestamp.class));
  }

  private static boolean hasSideInputAnnotation(List<Annotation> annotations) {
    return annotations.stream().anyMatch(a -> a.annotationType().equals(DoFn.SideInput.class));
  }

  private static boolean hasTimerIdAnnotation(List<Annotation> annotations) {
    return annotations.stream().anyMatch(a -> a.annotationType().equals(DoFn.TimerId.class));
  }

  @Nullable
  private static TypeDescriptor<?> getTrackerType(TypeDescriptor<?> fnClass, Method method) {
    Type[] params = method.getGenericParameterTypes();
    for (Type param : params) {
      TypeDescriptor<?> paramT = fnClass.resolveType(param);
      if (RestrictionTracker.class.isAssignableFrom(paramT.getRawType())) {
        return paramT;
      }
    }
    return null;
  }

  @Nullable
  private static TypeDescriptor<? extends BoundedWindow> getWindowType(
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
      TypeDescriptor<?> outputT) {
    errors.checkArgument(void.class.equals(m.getReturnType()), "Must return void");
    TypeDescriptor<?> expectedContextT = doFnStartBundleContextTypeOf(inputT, outputT);
    Type[] params = m.getGenericParameterTypes();
    errors.checkArgument(
        params.length == 0
            || (params.length == 1 && fnT.resolveType(params[0]).equals(expectedContextT)),
        "Must take a single argument of type %s",
        format(expectedContextT));
    return DoFnSignature.BundleMethod.create(m);
  }

  @VisibleForTesting
  static DoFnSignature.BundleMethod analyzeFinishBundleMethod(
      ErrorReporter errors,
      TypeDescriptor<? extends DoFn<?, ?>> fnT,
      Method m,
      TypeDescriptor<?> inputT,
      TypeDescriptor<?> outputT) {
    errors.checkArgument(void.class.equals(m.getReturnType()), "Must return void");
    TypeDescriptor<?> expectedContextT = doFnFinishBundleContextTypeOf(inputT, outputT);
    Type[] params = m.getGenericParameterTypes();
    errors.checkArgument(
        params.length == 0
            || (params.length == 1 && fnT.resolveType(params[0]).equals(expectedContextT)),
        "Must take a single argument of type %s",
        format(expectedContextT));
    return DoFnSignature.BundleMethod.create(m);
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

  private static ImmutableMap<String, TimerFamilyDeclaration> analyzeTimerFamilyDeclarations(
      ErrorReporter errors, Class<?> fnClazz) {
    Map<String, TimerFamilyDeclaration> declarations = new HashMap<>();
    for (Field field : declaredFieldsWithAnnotation(DoFn.TimerFamily.class, fnClazz, DoFn.class)) {
      // TimerSpec fields may generally be private, but will be accessed via the signature
      field.setAccessible(true);
      String id = field.getAnnotation(DoFn.TimerFamily.class).value();
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
      String id = field.getAnnotation(DoFn.TimerId.class).value();
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

  /**
   * Generates a {@link TypeDescriptor} for {@code RestrictionTracker<RestrictionT>} given {@code
   * RestrictionT}.
   */
  private static <RestrictionT>
      TypeDescriptor<RestrictionTracker<RestrictionT, ?>> restrictionTrackerTypeOf(
          TypeDescriptor<RestrictionT> restrictionT) {
    return new TypeDescriptor<RestrictionTracker<RestrictionT, ?>>() {}.where(
        new TypeParameter<RestrictionT>() {}, restrictionT);
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

  @Nullable
  private static Method findAnnotatedMethod(
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
    return ReflectHelpers.METHOD_FORMATTER.apply(method);
  }

  private static String format(TypeDescriptor<?> t) {
    return ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(t.getType());
  }

  private static String format(Class<?> kls) {
    return ReflectHelpers.CLASS_SIMPLE_NAME.apply(kls);
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
}
