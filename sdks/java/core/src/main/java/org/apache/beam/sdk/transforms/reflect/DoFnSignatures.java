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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.common.ReflectHelpers;

/**
 * Parses a {@link DoFn} and computes its {@link DoFnSignature}. See {@link #getOrParseSignature}.
 */
public class DoFnSignatures {
  public static final DoFnSignatures INSTANCE = new DoFnSignatures();

  private DoFnSignatures() {}

  private final Map<Class<?>, DoFnSignature> signatureCache = new LinkedHashMap<>();

  /** @return the {@link DoFnSignature} for the given {@link DoFn}. */
  public synchronized DoFnSignature getOrParseSignature(
      @SuppressWarnings("rawtypes") Class<? extends DoFn> fn) {
    DoFnSignature signature = signatureCache.get(fn);
    if (signature == null) {
      signatureCache.put(fn, signature = parseSignature(fn));
    }
    return signature;
  }

  /** Analyzes a given {@link DoFn} class and extracts its {@link DoFnSignature}. */
  private static DoFnSignature parseSignature(Class<? extends DoFn> fnClass) {
    DoFnSignature.Builder builder = DoFnSignature.builder();

    ErrorReporter errors = new ErrorReporter(null, fnClass.getName());
    errors.checkArgument(DoFn.class.isAssignableFrom(fnClass), "Must be subtype of DoFn");
    builder.setFnClass(fnClass);

    TypeToken<? extends DoFn> fnToken = TypeToken.of(fnClass);

    // Extract the input and output type, and whether the fn is bounded.
    TypeToken<?> inputT = null;
    TypeToken<?> outputT = null;
    for (TypeToken<?> supertype : fnToken.getTypes()) {
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
      TypeToken<? extends DoFn> fnClass,
      Method m,
      TypeToken<?> inputT,
      TypeToken<?> outputT) {
    errors.checkArgument(void.class.equals(m.getReturnType()), "Must return void");

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

    TypeToken<?> expectedInputProviderT = inputProviderTypeOf(inputT);
    TypeToken<?> expectedOutputReceiverT = outputReceiverTypeOf(outputT);
    for (int i = 1; i < params.length; ++i) {
      TypeToken<?> paramT = fnClass.resolveType(params[i]);
      Class<?> rawType = paramT.getRawType();
      if (rawType.equals(BoundedWindow.class)) {
        errors.checkArgument(
            !extraParameters.contains(DoFnSignature.Parameter.BOUNDED_WINDOW),
            "Multiple BoundedWindow parameters");
        extraParameters.add(DoFnSignature.Parameter.BOUNDED_WINDOW);
      } else if (rawType.equals(DoFn.InputProvider.class)) {
        errors.checkArgument(
            !extraParameters.contains(DoFnSignature.Parameter.INPUT_PROVIDER),
            "Multiple InputProvider parameters");
        errors.checkArgument(
            paramT.equals(expectedInputProviderT),
            "Wrong type of InputProvider parameter: %s, should be %s",
            formatType(paramT),
            formatType(expectedInputProviderT));
        extraParameters.add(DoFnSignature.Parameter.INPUT_PROVIDER);
      } else if (rawType.equals(DoFn.OutputReceiver.class)) {
        errors.checkArgument(
            !extraParameters.contains(DoFnSignature.Parameter.OUTPUT_RECEIVER),
            "Multiple OutputReceiver parameters");
        errors.checkArgument(
            paramT.equals(expectedOutputReceiverT),
            "Wrong type of OutputReceiver parameter: %s, should be %s",
            formatType(paramT),
            formatType(expectedOutputReceiverT));
        extraParameters.add(DoFnSignature.Parameter.OUTPUT_RECEIVER);
      } else {
        List<String> allowedParamTypes =
            Arrays.asList(formatType(new TypeToken<BoundedWindow>() {}));
        errors.throwIllegalArgument(
            "%s is not a valid context parameter. Should be one of %s",
            formatType(paramT), allowedParamTypes);
      }
    }

    return DoFnSignature.ProcessElementMethod.create(m, extraParameters);
  }

  @VisibleForTesting
  static DoFnSignature.BundleMethod analyzeBundleMethod(
      ErrorReporter errors,
      TypeToken<? extends DoFn> fnToken,
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
    return ReflectHelpers.CLASS_AND_METHOD_FORMATTER.apply(method);
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
          String.format("@%s %s", annotation, (method == null) ? "(absent)" : format(method)));
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
