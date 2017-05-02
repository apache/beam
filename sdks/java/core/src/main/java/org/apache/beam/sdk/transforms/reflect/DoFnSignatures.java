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

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
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
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.RestrictionTrackerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.StateParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.TimerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.WindowParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.StateDeclaration;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerDeclaration;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.Timer;
import org.apache.beam.sdk.util.TimerSpec;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.util.state.State;
import org.apache.beam.sdk.util.state.StateSpec;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeParameter;

/**
 * Utilities for working with {@link DoFnSignature}. See {@link #getSignature}.
 */
public class DoFnSignatures {

  private DoFnSignatures() {}

  private static final Map<Class<?>, DoFnSignature> signatureCache = new LinkedHashMap<>();

  private static final Collection<Class<? extends Parameter>>
      ALLOWED_NON_SPLITTABLE_PROCESS_ELEMENT_PARAMETERS =
      ImmutableList.of(
          Parameter.ProcessContextParameter.class,
          Parameter.WindowParameter.class,
          Parameter.TimerParameter.class,
          Parameter.StateParameter.class);

  private static final Collection<Class<? extends Parameter>>
      ALLOWED_SPLITTABLE_PROCESS_ELEMENT_PARAMETERS =
          ImmutableList.of(
              Parameter.ProcessContextParameter.class, Parameter.RestrictionTrackerParameter.class);

  private static final Collection<Class<? extends Parameter>>
      ALLOWED_ON_TIMER_PARAMETERS =
          ImmutableList.of(
              Parameter.OnTimerContextParameter.class,
              Parameter.WindowParameter.class,
              Parameter.TimerParameter.class,
              Parameter.StateParameter.class);

  /** @return the {@link DoFnSignature} for the given {@link DoFn} instance. */
  public static <FnT extends DoFn<?, ?>> DoFnSignature signatureForDoFn(FnT fn) {
    return getSignature(fn.getClass());
  }

  /** @return the {@link DoFnSignature} for the given {@link DoFn} subclass. */
  public static synchronized <FnT extends DoFn<?, ?>> DoFnSignature getSignature(Class<FnT> fn) {
    DoFnSignature signature = signatureCache.get(fn);
    if (signature == null) {
      signatureCache.put(fn, signature = parseSignature(fn));
    }
    return signature;
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

    public void addTimerDeclarations(Iterable<TimerDeclaration> decls) {
      for (TimerDeclaration decl : decls) {
        addTimerDeclaration(decl);
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
    private final List<Parameter> extraParameters = new ArrayList<>();

    @Nullable
    private TypeDescriptor<? extends BoundedWindow> windowT;

    private MethodAnalysisContext() {}

    /** Indicates whether a {@link RestrictionTrackerParameter} is known in this context. */
    public boolean hasRestrictionTrackerParameter() {
      return Iterables.any(
          extraParameters, Predicates.instanceOf(RestrictionTrackerParameter.class));
    }

    /** Indicates whether a {@link WindowParameter} is known in this context. */
    public boolean hasWindowParameter() {
      return Iterables.any(
          extraParameters, Predicates.instanceOf(WindowParameter.class));
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

    /** Extra parameters in their entirety. Unmodifiable. */
    public List<Parameter> getExtraParameters() {
      return Collections.unmodifiableList(extraParameters);
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

    Method processElementMethod =
        findAnnotatedMethod(errors, DoFn.ProcessElement.class, fnClass, true);
    Method startBundleMethod = findAnnotatedMethod(errors, DoFn.StartBundle.class, fnClass, false);
    Method finishBundleMethod =
        findAnnotatedMethod(errors, DoFn.FinishBundle.class, fnClass, false);
    Method setupMethod = findAnnotatedMethod(errors, DoFn.Setup.class, fnClass, false);
    Method teardownMethod = findAnnotatedMethod(errors, DoFn.Teardown.class, fnClass, false);

    Method getInitialRestrictionMethod =
        findAnnotatedMethod(errors, DoFn.GetInitialRestriction.class, fnClass, false);
    Method splitRestrictionMethod =
        findAnnotatedMethod(errors, DoFn.SplitRestriction.class, fnClass, false);
    Method getRestrictionCoderMethod =
        findAnnotatedMethod(errors, DoFn.GetRestrictionCoder.class, fnClass, false);
    Method newTrackerMethod = findAnnotatedMethod(errors, DoFn.NewTracker.class, fnClass, false);

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
          timerDecl.field().getDeclaringClass().equals(onTimerMethod.getDeclaringClass()),
          "Callback %s is for timer %s declared in a different class %s."
              + " Timer callbacks must be declared in the same lexical scope as their timer",
          onTimerMethod,
          id,
          timerDecl.field().getDeclaringClass().getCanonicalName());

      onTimerMethodMap.put(
          id, analyzeOnTimerMethod(errors, fnT, onTimerMethod, id, inputT, outputT, fnContext));
    }
    signatureBuilder.setOnTimerMethods(onTimerMethodMap);

    // Check the converse - that all timers have a callback. This could be relaxed to only
    // those timers used in methods, once method parameter lists support timers.
    for (TimerDeclaration decl : fnContext.getTimerDeclarations().values()) {
      errors.checkArgument(
          onTimerMethodMap.containsKey(decl.id()),
          "No callback registered via %s for timer %s",
          DoFn.OnTimer.class.getSimpleName(),
          decl.id());
    }

    ErrorReporter processElementErrors =
        errors.forMethod(DoFn.ProcessElement.class, processElementMethod);
    DoFnSignature.ProcessElementMethod processElement =
        analyzeProcessElementMethod(
            processElementErrors,
            fnT,
            processElementMethod,
            inputT,
            outputT,
            fnContext);
    signatureBuilder.setProcessElement(processElement);

    if (startBundleMethod != null) {
      ErrorReporter startBundleErrors = errors.forMethod(DoFn.StartBundle.class, startBundleMethod);
      signatureBuilder.setStartBundle(
          analyzeBundleMethod(startBundleErrors, fnT, startBundleMethod, inputT, outputT));
    }

    if (finishBundleMethod != null) {
      ErrorReporter finishBundleErrors =
          errors.forMethod(DoFn.FinishBundle.class, finishBundleMethod);
      signatureBuilder.setFinishBundle(
          analyzeBundleMethod(finishBundleErrors, fnT, finishBundleMethod, inputT, outputT));
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

    ErrorReporter getInitialRestrictionErrors;
    if (getInitialRestrictionMethod != null) {
      getInitialRestrictionErrors =
          errors.forMethod(DoFn.GetInitialRestriction.class, getInitialRestrictionMethod);
      signatureBuilder.setGetInitialRestriction(
              analyzeGetInitialRestrictionMethod(
                  getInitialRestrictionErrors, fnT, getInitialRestrictionMethod, inputT));
    }

    if (splitRestrictionMethod != null) {
      ErrorReporter splitRestrictionErrors =
          errors.forMethod(DoFn.SplitRestriction.class, splitRestrictionMethod);
      signatureBuilder.setSplitRestriction(
              analyzeSplitRestrictionMethod(
                  splitRestrictionErrors, fnT, splitRestrictionMethod, inputT));
    }

    if (getRestrictionCoderMethod != null) {
      ErrorReporter getRestrictionCoderErrors =
          errors.forMethod(DoFn.GetRestrictionCoder.class, getRestrictionCoderMethod);
      signatureBuilder.setGetRestrictionCoder(
              analyzeGetRestrictionCoderMethod(
                  getRestrictionCoderErrors, fnT, getRestrictionCoderMethod));
    }

    if (newTrackerMethod != null) {
      ErrorReporter newTrackerErrors = errors.forMethod(DoFn.NewTracker.class, newTrackerMethod);
      signatureBuilder.setNewTracker(
          analyzeNewTrackerMethod(newTrackerErrors, fnT, newTrackerMethod));
    }

    signatureBuilder.setIsBoundedPerElement(inferBoundedness(fnT, processElement, errors));

    signatureBuilder.setStateDeclarations(fnContext.getStateDeclarations());
    signatureBuilder.setTimerDeclarations(fnContext.getTimerDeclarations());

    DoFnSignature signature = signatureBuilder.build();

    // Additional validation for splittable DoFn's.
    if (processElement.isSplittable()) {
      verifySplittableMethods(signature, errors);
    } else {
      verifyUnsplittableMethods(errors, signature);
    }

    return signature;
  }

  /**
   * Infers the boundedness of the {@link DoFn.ProcessElement} method (whether or not it performs a
   * bounded amount of work per element) using the following criteria:
   *
   * <ol>
   * <li>If the {@link DoFn} is not splittable, then it is bounded, it must not be annotated as
   *     {@link DoFn.BoundedPerElement} or {@link DoFn.UnboundedPerElement}, and {@link
   *     DoFn.ProcessElement} must return {@code void}.
   * <li>If the {@link DoFn} (or any of its supertypes) is annotated as {@link
   *     DoFn.BoundedPerElement} or {@link DoFn.UnboundedPerElement}, use that. Only one of
   *     these must be specified.
   * <li>If {@link DoFn.ProcessElement} returns {@code void}, but the {@link DoFn} is annotated
   *     {@link DoFn.UnboundedPerElement}, this is an error.
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
            DoFn.BoundedPerElement.class.getSimpleName(),
            DoFn.UnboundedPerElement.class.getSimpleName());
        isBounded =
            supertype.getRawType().isAnnotationPresent(DoFn.BoundedPerElement.class)
                ? PCollection.IsBounded.BOUNDED
                : PCollection.IsBounded.UNBOUNDED;
      }
    }
    if (processElement.isSplittable()) {
      if (isBounded == null) {
        isBounded = PCollection.IsBounded.BOUNDED;
      }
    } else {
      errors.checkArgument(
          isBounded == null,
          "Non-splittable, but annotated as @"
              + ((isBounded == PCollection.IsBounded.BOUNDED)
                  ? DoFn.BoundedPerElement.class.getSimpleName()
                  : DoFn.UnboundedPerElement.class.getSimpleName()));
      isBounded = PCollection.IsBounded.BOUNDED;
    }
    return isBounded;
  }

  /**
   * Verifies properties related to methods of splittable {@link DoFn}:
   *
   * <ul>
   * <li>Must declare the required {@link DoFn.GetInitialRestriction} and {@link DoFn.NewTracker}
   *     methods.
   * <li>Types of restrictions and trackers must match exactly between {@link DoFn.ProcessElement},
   *     {@link DoFn.GetInitialRestriction}, {@link DoFn.NewTracker}, {@link
   *     DoFn.GetRestrictionCoder}, {@link DoFn.SplitRestriction}.
   * </ul>
   */
  private static void verifySplittableMethods(DoFnSignature signature, ErrorReporter errors) {
    DoFnSignature.ProcessElementMethod processElement = signature.processElement();
    DoFnSignature.GetInitialRestrictionMethod getInitialRestriction =
        signature.getInitialRestriction();
    DoFnSignature.NewTrackerMethod newTracker = signature.newTracker();
    DoFnSignature.GetRestrictionCoderMethod getRestrictionCoder = signature.getRestrictionCoder();
    DoFnSignature.SplitRestrictionMethod splitRestriction = signature.splitRestriction();

    ErrorReporter processElementErrors =
        errors.forMethod(DoFn.ProcessElement.class, processElement.targetMethod());

    final TypeDescriptor<?> trackerT;
    final String originOfTrackerT;

    List<String> missingRequiredMethods = new ArrayList<>();
    if (getInitialRestriction == null) {
      missingRequiredMethods.add("@" + DoFn.GetInitialRestriction.class.getSimpleName());
    }
    if (newTracker == null) {
      if (getInitialRestriction != null
          && getInitialRestriction
              .restrictionT()
              .isSubtypeOf(TypeDescriptor.of(HasDefaultTracker.class))) {
        trackerT =
            getInitialRestriction
                .restrictionT()
                .resolveType(HasDefaultTracker.class.getTypeParameters()[1]);
        originOfTrackerT =
            String.format(
                "restriction type %s of @%s method %s",
                formatType(getInitialRestriction.restrictionT()),
                DoFn.GetInitialRestriction.class.getSimpleName(),
                format(getInitialRestriction.targetMethod()));
      } else {
        missingRequiredMethods.add("@" + DoFn.NewTracker.class.getSimpleName());
        trackerT = null;
        originOfTrackerT = null;
      }
    } else {
      trackerT = newTracker.trackerT();
      originOfTrackerT =
          String.format(
              "%s method %s",
              DoFn.NewTracker.class.getSimpleName(), format(newTracker.targetMethod()));
      ErrorReporter getInitialRestrictionErrors =
          errors.forMethod(DoFn.GetInitialRestriction.class, getInitialRestriction.targetMethod());
      TypeDescriptor<?> restrictionT = getInitialRestriction.restrictionT();
      getInitialRestrictionErrors.checkArgument(
          restrictionT.equals(newTracker.restrictionT()),
          "Uses restriction type %s, but @%s method %s uses restriction type %s",
          formatType(restrictionT),
          DoFn.NewTracker.class.getSimpleName(),
          format(newTracker.targetMethod()),
          formatType(newTracker.restrictionT()));
    }

    if (!missingRequiredMethods.isEmpty()) {
      processElementErrors.throwIllegalArgument(
          "Splittable, but does not define the following required methods: %s",
          missingRequiredMethods);
    }

    ErrorReporter getInitialRestrictionErrors =
        errors.forMethod(DoFn.GetInitialRestriction.class, getInitialRestriction.targetMethod());
    TypeDescriptor<?> restrictionT = getInitialRestriction.restrictionT();
    processElementErrors.checkArgument(
        processElement.trackerT().equals(trackerT),
        "Has tracker type %s, but the DoFn's tracker type was inferred as %s from %s",
        formatType(processElement.trackerT()),
        trackerT,
        originOfTrackerT);

    if (getRestrictionCoder != null) {
      getInitialRestrictionErrors.checkArgument(
          getRestrictionCoder.coderT().isSubtypeOf(coderTypeOf(restrictionT)),
          "Uses restriction type %s, but @%s method %s returns %s "
              + "which is not a subtype of %s",
          formatType(restrictionT),
          DoFn.GetRestrictionCoder.class.getSimpleName(),
          format(getRestrictionCoder.targetMethod()),
          formatType(getRestrictionCoder.coderT()),
          formatType(coderTypeOf(restrictionT)));
    }

    if (splitRestriction != null) {
      getInitialRestrictionErrors.checkArgument(
          splitRestriction.restrictionT().equals(restrictionT),
          "Uses restriction type %s, but @%s method %s uses restriction type %s",
          formatType(restrictionT),
          DoFn.SplitRestriction.class.getSimpleName(),
          format(splitRestriction.targetMethod()),
          formatType(splitRestriction.restrictionT()));
    }
  }

  /**
   * Verifies that a non-splittable {@link DoFn} does not declare any methods that only make sense
   * for splittable {@link DoFn}: {@link DoFn.GetInitialRestriction}, {@link DoFn.SplitRestriction},
   * {@link DoFn.NewTracker}, {@link DoFn.GetRestrictionCoder}.
   */
  private static void verifyUnsplittableMethods(ErrorReporter errors, DoFnSignature signature) {
    List<String> forbiddenMethods = new ArrayList<>();
    if (signature.getInitialRestriction() != null) {
      forbiddenMethods.add("@" + DoFn.GetInitialRestriction.class.getSimpleName());
    }
    if (signature.splitRestriction() != null) {
      forbiddenMethods.add("@" + DoFn.SplitRestriction.class.getSimpleName());
    }
    if (signature.newTracker() != null) {
      forbiddenMethods.add("@" + DoFn.NewTracker.class.getSimpleName());
    }
    if (signature.getRestrictionCoder() != null) {
      forbiddenMethods.add("@" + DoFn.GetRestrictionCoder.class.getSimpleName());
    }
    errors.checkArgument(
        forbiddenMethods.isEmpty(), "Non-splittable, but defines methods: %s", forbiddenMethods);
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
   * Generates a {@link TypeDescriptor} for {@code DoFn<InputT, OutputT>.Context} given {@code
   * InputT} and {@code OutputT}.
   */
  private static <InputT, OutputT> TypeDescriptor<DoFn<InputT, OutputT>.Context> doFnContextTypeOf(
      TypeDescriptor<InputT> inputT, TypeDescriptor<OutputT> outputT) {
    return new TypeDescriptor<DoFn<InputT, OutputT>.Context>() {}.where(
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

    @Nullable TypeDescriptor<? extends BoundedWindow> windowT = getWindowType(fnClass, m);

    List<DoFnSignature.Parameter> extraParameters = new ArrayList<>();
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

      checkParameterOneOf(
          errors,
          parameter,
          ALLOWED_ON_TIMER_PARAMETERS);

      extraParameters.add(parameter);
    }

    return DoFnSignature.OnTimerMethod.create(m, timerId, windowT, extraParameters);
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
        void.class.equals(m.getReturnType()),
        "Must return void");


    MethodAnalysisContext methodContext = MethodAnalysisContext.create();

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
        m, methodContext.getExtraParameters(), trackerT, windowT);
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
    TypeDescriptor<?> expectedContextT = doFnContextTypeOf(inputT, outputT);
    TypeDescriptor<?> expectedOnTimerContextT = doFnOnTimerContextTypeOf(inputT, outputT);

    TypeDescriptor<?> paramT = param.getType();
    Class<?> rawType = paramT.getRawType();

    ErrorReporter paramErrors = methodErrors.forParameter(param);

    if (rawType.equals(DoFn.ProcessContext.class)) {
      paramErrors.checkArgument(paramT.equals(expectedProcessContextT),
        "ProcessContext argument must have type %s",
        formatType(expectedProcessContextT));
      return Parameter.processContext();
    } else if (rawType.equals(DoFn.Context.class)) {
      paramErrors.checkArgument(paramT.equals(expectedContextT),
          "Context argument must have type %s",
          formatType(expectedContextT));
      return Parameter.context();
    } else if (rawType.equals(DoFn.OnTimerContext.class)) {
      paramErrors.checkArgument(
          paramT.equals(expectedOnTimerContextT),
          "OnTimerContext argument must have type %s",
          formatType(expectedOnTimerContextT));
      return Parameter.onTimerContext();
    } else if (BoundedWindow.class.isAssignableFrom(rawType)) {
      methodErrors.checkArgument(
          !methodContext.hasWindowParameter(),
          "Multiple %s parameters",
          BoundedWindow.class.getSimpleName());
      return Parameter.boundedWindow((TypeDescriptor<? extends BoundedWindow>) paramT);
    } else if (RestrictionTracker.class.isAssignableFrom(rawType)) {
      methodErrors.checkArgument(
          !methodContext.hasRestrictionTrackerParameter(),
          "Multiple %s parameters",
          RestrictionTracker.class.getSimpleName());
      return Parameter.restrictionTracker(paramT);

    } else if (rawType.equals(Timer.class)) {
      // m.getParameters() is not available until Java 8
      String id = getTimerId(param.getAnnotations());

      paramErrors.checkArgument(
          id != null,
          "%s missing %s annotation",
          Timer.class.getSimpleName(),
          TimerId.class.getSimpleName());

      paramErrors.checkArgument(
          !methodContext.getTimerParameters().containsKey(id),
          "duplicate %s: \"%s\"",
          TimerId.class.getSimpleName(),
          id);

      TimerDeclaration timerDecl = fnContext.getTimerDeclarations().get(id);
      paramErrors.checkArgument(
          timerDecl != null,
          "reference to undeclared %s: \"%s\"",
          TimerId.class.getSimpleName(),
          id);

      paramErrors.checkArgument(
          timerDecl.field().getDeclaringClass().equals(param.getMethod().getDeclaringClass()),
          "%s %s declared in a different class %s."
              + " Timers may be referenced only in the lexical scope where they are declared.",
          TimerId.class.getSimpleName(),
          id,
          timerDecl.field().getDeclaringClass().getName());

      return Parameter.timerParameter(timerDecl);

    } else if (State.class.isAssignableFrom(rawType)) {
      // m.getParameters() is not available until Java 8
      String id = getStateId(param.getAnnotations());
      paramErrors.checkArgument(
          id != null,
          "missing %s annotation",
          DoFn.StateId.class.getSimpleName());

      paramErrors.checkArgument(
          !methodContext.getStateParameters().containsKey(id),
          "duplicate %s: \"%s\"",
          DoFn.StateId.class.getSimpleName(),
          id);

      // By static typing this is already a well-formed State subclass
      TypeDescriptor<? extends State> stateType = (TypeDescriptor<? extends State>) param.getType();

      StateDeclaration stateDecl = fnContext.getStateDeclarations().get(id);
      paramErrors.checkArgument(
          stateDecl != null,
          "reference to undeclared %s: \"%s\"",
          DoFn.StateId.class.getSimpleName(),
          id);

      paramErrors.checkArgument(
          stateDecl.stateType().equals(stateType),
          "reference to %s %s with different type %s",
          StateId.class.getSimpleName(),
          id,
          formatType(stateDecl.stateType()));

      paramErrors.checkArgument(
          stateDecl.field().getDeclaringClass().equals(param.getMethod().getDeclaringClass()),
          "%s %s declared in a different class %s."
              + " State may be referenced only in the class where it is declared.",
          StateId.class.getSimpleName(),
          id,
          stateDecl.field().getDeclaringClass().getName());

      return Parameter.stateParameter(stateDecl);
    } else {
      List<String> allowedParamTypes =
          Arrays.asList(
              formatType(new TypeDescriptor<BoundedWindow>() {}),
              formatType(new TypeDescriptor<RestrictionTracker<?>>() {}));
      paramErrors.throwIllegalArgument(
          "%s is not a valid context parameter. Should be one of %s",
          formatType(paramT), allowedParamTypes);
      // Unreachable
      return null;
    }
  }

  @Nullable
  private static String getTimerId(List<Annotation> annotations) {
    for (Annotation anno : annotations) {
      if (anno.annotationType().equals(DoFn.TimerId.class)) {
        return ((DoFn.TimerId) anno).value();
      }
    }
    return null;
  }

  @Nullable
  private static String getStateId(List<Annotation> annotations) {
    for (Annotation anno : annotations) {
      if (anno.annotationType().equals(DoFn.StateId.class)) {
        return ((DoFn.StateId) anno).value();
      }
    }
    return null;
  }

  @Nullable
  private static TypeDescriptor<?> getTrackerType(TypeDescriptor<?> fnClass, Method method) {
    Type[] params = method.getGenericParameterTypes();
    for (int i = 0; i < params.length; i++) {
      TypeDescriptor<?> paramT = fnClass.resolveType(params[i]);
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
    for (int i = 0; i < params.length; i++) {
      TypeDescriptor<?> paramT = fnClass.resolveType(params[i]);
      if (BoundedWindow.class.isAssignableFrom(paramT.getRawType())) {
        return (TypeDescriptor<? extends BoundedWindow>) paramT;
      }
    }
    return null;
  }

  @VisibleForTesting
  static DoFnSignature.BundleMethod analyzeBundleMethod(
      ErrorReporter errors,
      TypeDescriptor<? extends DoFn<?, ?>> fnT,
      Method m,
      TypeDescriptor<?> inputT,
      TypeDescriptor<?> outputT) {
    errors.checkArgument(void.class.equals(m.getReturnType()), "Must return void");
    TypeDescriptor<?> expectedContextT = doFnContextTypeOf(inputT, outputT);
    Type[] params = m.getGenericParameterTypes();
    errors.checkArgument(
        params.length == 1 && fnT.resolveType(params[0]).equals(expectedContextT),
        "Must take a single argument of type %s",
        formatType(expectedContextT));
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
      TypeDescriptor<? extends DoFn> fnT,
      Method m,
      TypeDescriptor<?> inputT) {
    // Method is of the form:
    // @GetInitialRestriction
    // RestrictionT getInitialRestriction(InputT element);
    Type[] params = m.getGenericParameterTypes();
    errors.checkArgument(
        params.length == 1 && fnT.resolveType(params[0]).equals(inputT),
        "Must take a single argument of type %s",
        formatType(inputT));
    return DoFnSignature.GetInitialRestrictionMethod.create(
        m, fnT.resolveType(m.getGenericReturnType()));
  }

  /**
   * Generates a {@link TypeDescriptor} for {@code DoFn.OutputReceiver<OutputT>} given {@code
   * OutputT}.
   */
  private static <OutputT> TypeDescriptor<DoFn.OutputReceiver<OutputT>> outputReceiverTypeOf(
      TypeDescriptor<OutputT> inputT) {
    return new TypeDescriptor<DoFn.OutputReceiver<OutputT>>() {}.where(
        new TypeParameter<OutputT>() {}, inputT);
  }

  @VisibleForTesting
  static DoFnSignature.SplitRestrictionMethod analyzeSplitRestrictionMethod(
      ErrorReporter errors,
      TypeDescriptor<? extends DoFn> fnT,
      Method m,
      TypeDescriptor<?> inputT) {
    // Method is of the form:
    // @SplitRestriction
    // void splitRestriction(InputT element, RestrictionT restriction);
    errors.checkArgument(void.class.equals(m.getReturnType()), "Must return void");

    Type[] params = m.getGenericParameterTypes();
    errors.checkArgument(params.length == 3, "Must have exactly 3 arguments");
    errors.checkArgument(
        fnT.resolveType(params[0]).equals(inputT),
        "First argument must be the element type %s",
        formatType(inputT));

    TypeDescriptor<?> restrictionT = fnT.resolveType(params[1]);
    TypeDescriptor<?> receiverT = fnT.resolveType(params[2]);
    TypeDescriptor<?> expectedReceiverT = outputReceiverTypeOf(restrictionT);
    errors.checkArgument(
        receiverT.equals(expectedReceiverT),
        "Third argument must be %s, but is %s",
        formatType(expectedReceiverT),
        formatType(receiverT));

    return DoFnSignature.SplitRestrictionMethod.create(m, restrictionT);
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
   * Returns successfully if the field is valid, otherwise throws an exception via
   * its {@link ErrorReporter} parameter describing validation failures for the
   * timer declaration.
   */
  private static void validateTimerField(
      ErrorReporter errors, Map<String, TimerDeclaration> declarations, String id, Field field) {
    if (declarations.containsKey(id)) {
      errors.throwIllegalArgument(
          "Duplicate %s \"%s\", used on both of [%s] and [%s]",
          DoFn.TimerId.class.getSimpleName(),
          id,
          field.toString(),
          declarations.get(id).field().toString());
    }

    Class<?> timerSpecRawType = field.getType();
    if (!(timerSpecRawType.equals(TimerSpec.class))) {
      errors.throwIllegalArgument(
          "%s annotation on non-%s field [%s]",
          DoFn.TimerId.class.getSimpleName(),
          TimerSpec.class.getSimpleName(),
          field.toString());
    }

    if (!Modifier.isFinal(field.getModifiers())) {
      errors.throwIllegalArgument(
          "Non-final field %s annotated with %s. Timer declarations must be final.",
          field.toString(), DoFn.TimerId.class.getSimpleName());
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
        formatType(resT));
    return DoFnSignature.GetRestrictionCoderMethod.create(m, resT);
  }

  /**
   * Generates a {@link TypeDescriptor} for {@code RestrictionTracker<RestrictionT>} given {@code
   * RestrictionT}.
   */
  private static <RestrictionT>
      TypeDescriptor<RestrictionTracker<RestrictionT>> restrictionTrackerTypeOf(
          TypeDescriptor<RestrictionT> restrictionT) {
    return new TypeDescriptor<RestrictionTracker<RestrictionT>>() {}.where(
        new TypeParameter<RestrictionT>() {}, restrictionT);
  }

  @VisibleForTesting
  static DoFnSignature.NewTrackerMethod analyzeNewTrackerMethod(
      ErrorReporter errors, TypeDescriptor<? extends DoFn> fnT, Method m) {
    // Method is of the form:
    // @NewTracker
    // TrackerT newTracker(RestrictionT restriction);
    Type[] params = m.getGenericParameterTypes();
    errors.checkArgument(params.length == 1, "Must have a single argument");

    TypeDescriptor<?> restrictionT = fnT.resolveType(params[0]);
    TypeDescriptor<?> trackerT = fnT.resolveType(m.getGenericReturnType());
    TypeDescriptor<?> expectedTrackerT = restrictionTrackerTypeOf(restrictionT);
    errors.checkArgument(
        trackerT.isSubtypeOf(expectedTrackerT),
        "Returns %s, but must return a subtype of %s",
        formatType(trackerT),
        formatType(expectedTrackerT));
    return DoFnSignature.NewTrackerMethod.create(m, restrictionT, trackerT);
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

  // Class::getDeclaredMethods for Java 7
  private static final MemberGetter<Method> GET_METHODS =
      new MemberGetter<Method>() {
        @Override
        public Method[] getMembers(Class<?> clazz) {
          return clazz.getDeclaredMethods();
        }
      };

  // Class::getDeclaredFields for Java 7
  private static final MemberGetter<Field> GET_FIELDS =
      new MemberGetter<Field>() {
        @Override
        public Field[] getMembers(Class<?> clazz) {
          return clazz.getDeclaredFields();
        }
      };

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

  private static Map<String, DoFnSignature.StateDeclaration> analyzeStateDeclarations(
      ErrorReporter errors,
      Class<?> fnClazz) {

    Map<String, DoFnSignature.StateDeclaration> declarations = new HashMap<>();

    for (Field field : declaredFieldsWithAnnotation(DoFn.StateId.class, fnClazz, DoFn.class)) {
      // StateSpec fields may generally be private, but will be accessed via the signature
      field.setAccessible(true);
      String id = field.getAnnotation(DoFn.StateId.class).value();

      if (declarations.containsKey(id)) {
        errors.throwIllegalArgument(
            "Duplicate %s \"%s\", used on both of [%s] and [%s]",
            DoFn.StateId.class.getSimpleName(),
            id,
            field.toString(),
            declarations.get(id).field().toString());
        continue;
      }

      Class<?> stateSpecRawType = field.getType();
      if (!(TypeDescriptor.of(stateSpecRawType)
          .isSubtypeOf(TypeDescriptor.of(StateSpec.class)))) {
        errors.throwIllegalArgument(
                "%s annotation on non-%s field [%s] that has class %s",
            DoFn.StateId.class.getSimpleName(),
            StateSpec.class.getSimpleName(),
            field.toString(),
            stateSpecRawType.getName());
        continue;
      }

      if (!Modifier.isFinal(field.getModifiers())) {
        errors.throwIllegalArgument(
            "Non-final field %s annotated with %s. State declarations must be final.",
            field.toString(),
            DoFn.StateId.class.getSimpleName());
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
          (TypeDescriptor)
      stateSpecSubclassTypeDescriptor.getSupertype(StateSpec.class);

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

    return  ImmutableMap.copyOf(declarations);
  }

  private static Method findAnnotatedMethod(
      ErrorReporter errors, Class<? extends Annotation> anno, Class<?> fnClazz, boolean required) {
    Collection<Method> matches = declaredMethodsWithAnnotation(anno, fnClazz, DoFn.class);

    if (matches.size() == 0) {
      errors.checkArgument(!required, "No method annotated with @%s found", anno.getSimpleName());
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
          anno.getSimpleName(),
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

  private static String formatType(TypeDescriptor<?> t) {
    return ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(t.getType());
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
              "@%s %s",
              annotation.getSimpleName(), (method == null) ? "(absent)" : format(method)));
    }

    ErrorReporter forParameter(ParameterDescription param) {
      return new ErrorReporter(
          this,
          String.format(
              "parameter of type %s at index %s",
              formatType(param.getType()), param.getIndex()));
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
}
