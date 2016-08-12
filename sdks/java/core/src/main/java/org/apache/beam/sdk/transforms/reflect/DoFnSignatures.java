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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.PCollection;

/**
 * Parses a {@link DoFn} and computes its {@link DoFnSignature}. See {@link #getOrParseSignature}.
 */
public class DoFnSignatures {
  public static final DoFnSignatures INSTANCE = new DoFnSignatures();

  private DoFnSignatures() {}

  private final Map<Class<?>, DoFnSignature> signatureCache = new LinkedHashMap<>();

  /** @return the {@link DoFnSignature} for the given {@link DoFn}. */
  public synchronized <FnT extends DoFn<?, ?>> DoFnSignature getOrParseSignature(Class<FnT> fn) {
    DoFnSignature signature = signatureCache.get(fn);
    if (signature == null) {
      signatureCache.put(fn, signature = parseSignature(fn));
    }
    return signature;
  }

  /** Analyzes a given {@link DoFn} class and extracts its {@link DoFnSignature}. */
  private static DoFnSignature parseSignature(Class<? extends DoFn<?, ?>> fnClass) {
    DoFnSignature.Builder builder = DoFnSignature.builder();

    ErrorReporter errors = new ErrorReporter(null, fnClass.getName());
    errors.checkArgument(DoFn.class.isAssignableFrom(fnClass), "Must be subtype of DoFn");
    builder.setFnClass(fnClass);

    TypeToken<? extends DoFn<?, ?>> fnToken = TypeToken.of(fnClass);

    // Extract the input and output type, and whether the fn is bounded.
    PCollection.IsBounded isBounded = null;
    TypeToken<?> inputT = null;
    TypeToken<?> outputT = null;
    for (TypeToken<?> supertype : fnToken.getTypes()) {
      if (supertype.getRawType().isAnnotationPresent(DoFn.Bounded.class)) {
        errors.checkArgument(
            isBounded == null,
            "Both @%s and @%s specified",
            DoFn.Bounded.class.getSimpleName(),
            DoFn.Unbounded.class.getSimpleName());
        isBounded = PCollection.IsBounded.BOUNDED;
      }
      if (supertype.getRawType().isAnnotationPresent(DoFn.Unbounded.class)) {
        errors.checkArgument(
            isBounded == null,
            "Both @%s and @%s specified",
            DoFn.Bounded.class.getSimpleName(),
            DoFn.Unbounded.class.getSimpleName());
        isBounded = PCollection.IsBounded.UNBOUNDED;
      }
      if (!supertype.getRawType().equals(DoFn.class)) {
        continue;
      }
      Type[] args = ((ParameterizedType) supertype.getType()).getActualTypeArguments();
      inputT = TypeToken.of(args[0]);
      outputT = TypeToken.of(args[1]);
    }
    errors.checkNotNull(inputT, "Unable to determine input type");

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

    ErrorReporter processElementErrors =
        errors.forMethod(DoFn.ProcessElement.class, processElementMethod);
    DoFnSignature.ProcessElementMethod processElement =
        analyzeProcessElementMethod(
            processElementErrors, fnToken, processElementMethod, inputT, outputT);
    builder.setProcessElement(processElement);

    if (startBundleMethod != null) {
      ErrorReporter startBundleErrors = errors.forMethod(DoFn.StartBundle.class, startBundleMethod);
      builder.setStartBundle(
          analyzeBundleMethod(startBundleErrors, fnToken, startBundleMethod, inputT, outputT));
    }

    if (finishBundleMethod != null) {
      ErrorReporter finishBundleErrors =
          errors.forMethod(DoFn.FinishBundle.class, finishBundleMethod);
      builder.setFinishBundle(
          analyzeBundleMethod(finishBundleErrors, fnToken, finishBundleMethod, inputT, outputT));
    }

    if (setupMethod != null) {
      builder.setSetup(
          analyzeLifecycleMethod(errors.forMethod(DoFn.Setup.class, setupMethod), setupMethod));
    }

    if (teardownMethod != null) {
      builder.setTeardown(
          analyzeLifecycleMethod(
              errors.forMethod(DoFn.Teardown.class, teardownMethod), teardownMethod));
    }

    DoFnSignature.GetInitialRestrictionMethod getInitialRestriction = null;
    ErrorReporter getInitialRestrictionErrors = null;
    if (getInitialRestrictionMethod != null) {
      getInitialRestrictionErrors =
          errors.forMethod(DoFn.GetInitialRestriction.class, getInitialRestrictionMethod);
      builder.setGetInitialRestriction(
          getInitialRestriction =
              analyzeGetInitialRestrictionMethod(
                  getInitialRestrictionErrors, fnToken, getInitialRestrictionMethod, inputT));
    }

    DoFnSignature.SplitRestrictionMethod splitRestriction = null;
    if (splitRestrictionMethod != null) {
      ErrorReporter splitRestrictionErrors =
          errors.forMethod(DoFn.SplitRestriction.class, splitRestrictionMethod);
      builder.setSplitRestriction(
          splitRestriction =
              analyzeSplitRestrictionMethod(
                  splitRestrictionErrors, fnToken, splitRestrictionMethod, inputT));
    }

    DoFnSignature.GetRestrictionCoderMethod getRestrictionCoder = null;
    if (getRestrictionCoderMethod != null) {
      ErrorReporter getRestrictionCoderErrors =
          errors.forMethod(DoFn.GetRestrictionCoder.class, getRestrictionCoderMethod);
      builder.setGetRestrictionCoder(
          getRestrictionCoder =
              analyzeGetRestrictionCoderMethod(
                  getRestrictionCoderErrors, fnToken, getRestrictionCoderMethod));
    }

    DoFnSignature.NewTrackerMethod newTracker = null;
    if (newTrackerMethod != null) {
      ErrorReporter newTrackerErrors = errors.forMethod(DoFn.NewTracker.class, newTrackerMethod);
      builder.setNewTracker(
          newTracker = analyzeNewTrackerMethod(newTrackerErrors, fnToken, newTrackerMethod));
    }

    // Additional validation for splittable DoFn's.
    if (processElement.isSplittable()) {
      if (isBounded == null) {
        isBounded =
            processElement.hasReturnValue()
                ? PCollection.IsBounded.UNBOUNDED
                : PCollection.IsBounded.BOUNDED;
      }
      List<String> missingRequiredMethods = new ArrayList<>();
      if (getInitialRestriction == null) {
        missingRequiredMethods.add("@" + DoFn.GetInitialRestriction.class.getSimpleName());
      }
      if (newTracker == null) {
        missingRequiredMethods.add("@" + DoFn.NewTracker.class.getSimpleName());
      }
      // @SplitRestriction and @GetRestrictionCoder are optional.
      if (!missingRequiredMethods.isEmpty()) {
        processElementErrors.throwIllegalArgument(
            "Splittable, but does not define the following required methods: %s",
            missingRequiredMethods);
      }
      processElementErrors.checkArgument(
          processElement.trackerT().equals(newTracker.trackerT()),
          "Has tracker type %s, but @%s method %s uses tracker type %s",
          formatType(processElement.trackerT()),
          DoFn.NewTracker.class.getSimpleName(),
          format(newTrackerMethod),
          formatType(newTracker.trackerT()));
      getInitialRestrictionErrors.checkArgument(
          getInitialRestriction.restrictionT().equals(newTracker.restrictionT()),
          "Uses restriction type %s, but @%s method %s uses restriction type %s",
          formatType(getInitialRestriction.restrictionT()),
          DoFn.NewTracker.class.getSimpleName(),
          format(newTrackerMethod),
          formatType(newTracker.restrictionT()));
      if (getRestrictionCoder != null) {
        getInitialRestrictionErrors.checkArgument(
            getRestrictionCoder
                .coderT()
                .isSubtypeOf(coderTypeOf(getInitialRestriction.restrictionT())),
            "Uses restriction type %s, but @%s method %s returns %s "
                + "which is not a subtype of %s",
            formatType(getInitialRestriction.restrictionT()),
            DoFn.GetRestrictionCoder.class.getSimpleName(),
            format(getRestrictionCoderMethod),
            formatType(getRestrictionCoder.coderT()),
            formatType(coderTypeOf(getInitialRestriction.restrictionT())));
      }
      if (splitRestriction != null) {
        getInitialRestrictionErrors.checkArgument(
            splitRestriction.restrictionT().equals(getInitialRestriction.restrictionT()),
            "Uses restriction type %s, but @%s method %s uses restriction type %s",
            formatType(getInitialRestriction.restrictionT()),
            DoFn.SplitRestriction.class.getSimpleName(),
            format(splitRestrictionMethod),
            formatType(splitRestriction.restrictionT()));
      }
    } else {
      processElementErrors.checkArgument(
          isBounded == null,
          "Non-splittable, but annotated as @"
              + ((isBounded == PCollection.IsBounded.BOUNDED)
                  ? DoFn.Bounded.class.getSimpleName()
                  : DoFn.Unbounded.class.getSimpleName()));
      isBounded = PCollection.IsBounded.BOUNDED;

      List<String> forbiddenMethods = new ArrayList<>();
      if (getInitialRestriction != null) {
        forbiddenMethods.add("@" + DoFn.GetInitialRestriction.class.getSimpleName());
      }
      if (splitRestriction != null) {
        forbiddenMethods.add("@" + DoFn.SplitRestriction.class.getSimpleName());
      }
      if (newTracker != null) {
        forbiddenMethods.add("@" + DoFn.NewTracker.class.getSimpleName());
      }
      if (getRestrictionCoder != null) {
        forbiddenMethods.add("@" + DoFn.GetRestrictionCoder.class.getSimpleName());
      }
      processElementErrors.checkArgument(
          forbiddenMethods.isEmpty(), "Non-splittable, but defines methods: %s", forbiddenMethods);
    }

    builder.setIsBounded(isBounded);

    return builder.build();
  }

  /**
   * Generates a type token for {@code DoFn<InputT, OutputT>.ProcessContext} given {@code InputT}
   * and {@code OutputT}.
   */
  private static <InputT, OutputT>
      TypeToken<DoFn<InputT, OutputT>.ProcessContext> doFnProcessContextTypeOf(
          TypeToken<InputT> inputT, TypeToken<OutputT> outputT) {
    return new TypeToken<DoFn<InputT, OutputT>.ProcessContext>() {}.where(
            new TypeParameter<InputT>() {}, inputT)
        .where(new TypeParameter<OutputT>() {}, outputT);
  }

  /**
   * Generates a type token for {@code DoFn<InputT, OutputT>.Context} given {@code InputT} and
   * {@code OutputT}.
   */
  private static <InputT, OutputT> TypeToken<DoFn<InputT, OutputT>.Context> doFnContextTypeOf(
      TypeToken<InputT> inputT, TypeToken<OutputT> outputT) {
    return new TypeToken<DoFn<InputT, OutputT>.Context>() {}.where(
            new TypeParameter<InputT>() {}, inputT)
        .where(new TypeParameter<OutputT>() {}, outputT);
  }

  /** Generates a type token for {@code DoFn.InputProvider<InputT>} given {@code InputT}. */
  private static <InputT> TypeToken<DoFn.InputProvider<InputT>> inputProviderTypeOf(
      TypeToken<InputT> inputT) {
    return new TypeToken<DoFn.InputProvider<InputT>>() {}.where(
        new TypeParameter<InputT>() {}, inputT);
  }

  /** Generates a type token for {@code DoFn.OutputReceiver<OutputT>} given {@code OutputT}. */
  private static <OutputT> TypeToken<DoFn.OutputReceiver<OutputT>> outputReceiverTypeOf(
      TypeToken<OutputT> inputT) {
    return new TypeToken<DoFn.OutputReceiver<OutputT>>() {}.where(
        new TypeParameter<OutputT>() {}, inputT);
  }

  @VisibleForTesting
  static DoFnSignature.ProcessElementMethod analyzeProcessElementMethod(
      ErrorReporter errors,
      TypeToken<? extends DoFn<?, ?>> fnClass,
      Method m,
      TypeToken<?> inputT,
      TypeToken<?> outputT) {
    errors.checkArgument(
        void.class.equals(m.getReturnType())
            || DoFn.ProcessContinuation.class.equals(m.getReturnType()),
        "Must return void or %s",
        DoFn.ProcessContinuation.class.getSimpleName());

    TypeToken<?> processContextToken = doFnProcessContextTypeOf(inputT, outputT);

    Type[] params = m.getGenericParameterTypes();
    TypeToken<?> contextToken = null;
    if (params.length > 0) {
      contextToken = fnClass.resolveType(params[0]);
    }
    errors.checkArgument(
        contextToken != null && contextToken.equals(processContextToken),
        "Must take %s as the first argument",
        formatType(processContextToken));

    List<DoFnSignature.Parameter> extraParameters = new ArrayList<>();
    TypeToken<?> trackerT = null;

    TypeToken<?> expectedInputProviderT = inputProviderTypeOf(inputT);
    TypeToken<?> expectedOutputReceiverT = outputReceiverTypeOf(outputT);
    for (int i = 1; i < params.length; ++i) {
      TypeToken<?> paramT = fnClass.resolveType(params[i]);
      Class<?> rawType = paramT.getRawType();
      if (rawType.equals(BoundedWindow.class)) {
        errors.checkArgument(
            !extraParameters.contains(DoFnSignature.Parameter.BOUNDED_WINDOW),
            "Multiple %s parameters",
            BoundedWindow.class.getSimpleName());
        extraParameters.add(DoFnSignature.Parameter.BOUNDED_WINDOW);
      } else if (rawType.equals(DoFn.InputProvider.class)) {
        errors.checkArgument(
            !extraParameters.contains(DoFnSignature.Parameter.INPUT_PROVIDER),
            "Multiple %s parameters",
            DoFn.InputProvider.class.getSimpleName());
        errors.checkArgument(
            paramT.equals(expectedInputProviderT),
            "Wrong type of %s parameter: %s, should be %s",
            DoFn.InputProvider.class.getSimpleName(),
            formatType(paramT),
            formatType(expectedInputProviderT));
        extraParameters.add(DoFnSignature.Parameter.INPUT_PROVIDER);
      } else if (rawType.equals(DoFn.OutputReceiver.class)) {
        errors.checkArgument(
            !extraParameters.contains(DoFnSignature.Parameter.OUTPUT_RECEIVER),
            "Multiple %s parameters",
            DoFn.OutputReceiver.class.getSimpleName());
        errors.checkArgument(
            paramT.equals(expectedOutputReceiverT),
            "Wrong type of %s parameter: %s, should be %s",
            DoFn.OutputReceiver.class.getSimpleName(),
            formatType(paramT),
            formatType(expectedOutputReceiverT));
        extraParameters.add(DoFnSignature.Parameter.OUTPUT_RECEIVER);
      } else if (RestrictionTracker.class.isAssignableFrom(rawType)) {
        errors.checkArgument(
            !extraParameters.contains(DoFnSignature.Parameter.RESTRICTION_TRACKER),
            "Multiple %s parameters",
            RestrictionTracker.class.getSimpleName());
        extraParameters.add(DoFnSignature.Parameter.RESTRICTION_TRACKER);
        trackerT = paramT;
      } else {
        List<String> allowedParamTypes =
            Arrays.asList(
                formatType(new TypeToken<BoundedWindow>() {}),
                formatType(new TypeToken<RestrictionTracker<?>>() {}));
        errors.throwIllegalArgument(
            "%s is not a valid context parameter. Should be one of %s",
            formatType(paramT), allowedParamTypes);
      }
    }

    // A splittable DoFn can not have any other extra context parameters.
    if (extraParameters.contains(DoFnSignature.Parameter.RESTRICTION_TRACKER)) {
      errors.checkArgument(
          extraParameters.size() == 1,
          "Splittable DoFn must not have any extra context arguments apart from %s, but has: %s",
          trackerT,
          extraParameters);
    }

    return DoFnSignature.ProcessElementMethod.create(
        m, extraParameters, trackerT, DoFn.ProcessContinuation.class.equals(m.getReturnType()));
  }

  @VisibleForTesting
  static DoFnSignature.BundleMethod analyzeBundleMethod(
      ErrorReporter errors,
      TypeToken<? extends DoFn<?, ?>> fnToken,
      Method m,
      TypeToken<?> inputT,
      TypeToken<?> outputT) {
    errors.checkArgument(void.class.equals(m.getReturnType()), "Must return void");
    TypeToken<?> expectedContextToken = doFnContextTypeOf(inputT, outputT);
    Type[] params = m.getGenericParameterTypes();
    errors.checkArgument(
        params.length == 1 && fnToken.resolveType(params[0]).equals(expectedContextToken),
        "Must take a single argument of type %s",
        formatType(expectedContextToken));
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
      ErrorReporter errors, TypeToken<? extends DoFn> fnToken, Method m, TypeToken<?> inputT) {
    // Method is of the form:
    // @GetInitialRestriction
    // RestrictionT getInitialRestriction(InputT element);
    Type[] params = m.getGenericParameterTypes();
    errors.checkArgument(
        params.length == 1 && fnToken.resolveType(params[0]).equals(inputT),
        "Must take a single argument of type %s",
        formatType(inputT));
    return DoFnSignature.GetInitialRestrictionMethod.create(
        m, fnToken.resolveType(m.getGenericReturnType()));
  }

  /** Generates a type token for {@code List<T>} given {@code T}. */
  private static <T> TypeToken<List<T>> listTypeOf(TypeToken<T> elementT) {
    return new TypeToken<List<T>>() {}.where(new TypeParameter<T>() {}, elementT);
  }

  @VisibleForTesting
  static DoFnSignature.SplitRestrictionMethod analyzeSplitRestrictionMethod(
      ErrorReporter errors, TypeToken<? extends DoFn> fnToken, Method m, TypeToken<?> inputT) {
    // Method is of the form:
    // @SplitRestriction
    // List<RestrictionT> splitRestriction(InputT element, RestrictionT restriction);
    Type[] params = m.getGenericParameterTypes();
    errors.checkArgument(params.length == 2, "Must have exactly 2 arguments");
    errors.checkArgument(
        fnToken.resolveType(params[0]).equals(inputT),
        "First argument must be the element type %s",
        formatType(inputT));

    TypeToken<?> restrictionT = fnToken.resolveType(params[1]);
    TypeToken<? extends List<?>> expectedReturnT = listTypeOf(restrictionT);
    TypeToken<?> returnT = fnToken.resolveType(m.getGenericReturnType());
    errors.checkArgument(
        returnT.equals(expectedReturnT),
        "Must return %s, but returns %s",
        formatType(expectedReturnT),
        formatType(returnT));
    return DoFnSignature.SplitRestrictionMethod.create(m, restrictionT);
  }

  /** Generates a type token for {@code Coder<T>} given {@code T}. */
  private static <T> TypeToken<Coder<T>> coderTypeOf(TypeToken<T> elementT) {
    return new TypeToken<Coder<T>>() {}.where(new TypeParameter<T>() {}, elementT);
  }

  @VisibleForTesting
  static DoFnSignature.GetRestrictionCoderMethod analyzeGetRestrictionCoderMethod(
      ErrorReporter errors, TypeToken<? extends DoFn> fnToken, Method m) {
    errors.checkArgument(m.getParameterTypes().length == 0, "Must have zero arguments");
    TypeToken<?> resT = fnToken.resolveType(m.getGenericReturnType());
    errors.checkArgument(
        resT.isSubtypeOf(TypeToken.of(Coder.class)),
        "Must return a Coder, but returns %s",
        formatType(resT));
    return DoFnSignature.GetRestrictionCoderMethod.create(m, resT);
  }

  /**
   * Generates a type token for {@code RestrictionTracker<RestrictionT>} given {@code RestrictionT}.
   */
  private static <RestrictionT>
      TypeToken<RestrictionTracker<RestrictionT>> restrictionTrackerTypeOf(
          TypeToken<RestrictionT> restrictionT) {
    return new TypeToken<RestrictionTracker<RestrictionT>>() {}.where(
        new TypeParameter<RestrictionT>() {}, restrictionT);
  }

  @VisibleForTesting
  static DoFnSignature.NewTrackerMethod analyzeNewTrackerMethod(
      ErrorReporter errors, TypeToken<? extends DoFn> fnToken, Method m) {
    // Method is of the form:
    // @NewTracker
    // TrackerT newTracker(RestrictionT restriction);
    Type[] params = m.getGenericParameterTypes();
    errors.checkArgument(params.length == 1, "Must have a single argument");

    TypeToken<?> restrictionT = fnToken.resolveType(params[0]);
    TypeToken<?> trackerT = fnToken.resolveType(m.getGenericReturnType());
    TypeToken<?> expectedTrackerT = restrictionTrackerTypeOf(restrictionT);
    errors.checkArgument(
        trackerT.isSubtypeOf(expectedTrackerT),
        "Returns %s, but must return a subtype of %s",
        formatType(trackerT),
        formatType(expectedTrackerT));
    return DoFnSignature.NewTrackerMethod.create(m, restrictionT, trackerT);
  }

  private static Collection<Method> declaredMethodsWithAnnotation(
      Class<? extends Annotation> anno, Class<?> startClass, Class<?> stopClass) {
    Collection<Method> matches = new ArrayList<>();

    Class<?> clazz = startClass;
    LinkedHashSet<Class<?>> interfaces = new LinkedHashSet<>();

    // First, find all declared methods on the startClass and parents (up to stopClass)
    while (clazz != null && !clazz.equals(stopClass)) {
      for (Method method : clazz.getDeclaredMethods()) {
        if (method.isAnnotationPresent(anno)) {
          matches.add(method);
        }
      }

      Collections.addAll(interfaces, clazz.getInterfaces());

      clazz = clazz.getSuperclass();
    }

    // Now, iterate over all the discovered interfaces
    for (Method method : ReflectHelpers.getClosureOfMethodsOnInterfaces(interfaces)) {
      if (method.isAnnotationPresent(anno)) {
        matches.add(method);
      }
    }
    return matches;
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

  private static String formatType(TypeToken<?> t) {
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
