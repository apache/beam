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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
    TypeToken<?> inputT = null;
    TypeToken<?> outputT = null;

    // Extract the input and output type.
    checkArgument(
        DoFn.class.isAssignableFrom(fnClass),
        "%s must be subtype of DoFn",
        fnClass.getSimpleName());
    TypeToken<? extends DoFn> fnToken = TypeToken.of(fnClass);
    for (TypeToken<?> supertype : fnToken.getTypes()) {
      if (!supertype.getRawType().equals(DoFn.class)) {
        continue;
      }
      Type[] args = ((ParameterizedType) supertype.getType()).getActualTypeArguments();
      inputT = TypeToken.of(args[0]);
      outputT = TypeToken.of(args[1]);
    }
    checkNotNull(inputT, "Unable to determine input type from %s", fnClass);

    Method processElementMethod = findAnnotatedMethod(DoFn.ProcessElement.class, fnClass, true);
    Method startBundleMethod = findAnnotatedMethod(DoFn.StartBundle.class, fnClass, false);
    Method finishBundleMethod = findAnnotatedMethod(DoFn.FinishBundle.class, fnClass, false);
    Method setupMethod = findAnnotatedMethod(DoFn.Setup.class, fnClass, false);
    Method teardownMethod = findAnnotatedMethod(DoFn.Teardown.class, fnClass, false);

    return DoFnSignature.create(
        fnClass,
        analyzeProcessElementMethod(fnToken, processElementMethod, inputT, outputT),
        (startBundleMethod == null)
            ? null
            : analyzeBundleMethod(fnToken, startBundleMethod, inputT, outputT),
        (finishBundleMethod == null)
            ? null
            : analyzeBundleMethod(fnToken, finishBundleMethod, inputT, outputT),
        (setupMethod == null) ? null : analyzeLifecycleMethod(setupMethod),
        (teardownMethod == null) ? null : analyzeLifecycleMethod(teardownMethod));
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
      TypeToken<? extends DoFn> fnClass, Method m, TypeToken<?> inputT, TypeToken<?> outputT) {
    checkArgument(
        void.class.equals(m.getReturnType()), "%s must have a void return type", format(m));
    checkArgument(!m.isVarArgs(), "%s must not have var args", format(m));

    TypeToken<?> processContextToken = doFnProcessContextTypeOf(inputT, outputT);

    Type[] params = m.getGenericParameterTypes();
    TypeToken<?> contextToken = null;
    if (params.length > 0) {
      contextToken = fnClass.resolveType(params[0]);
    }
    checkArgument(
        contextToken != null && contextToken.equals(processContextToken),
        "%s must take a %s as its first argument",
        format(m),
        formatType(processContextToken));

    List<DoFnSignature.ProcessElementMethod.Parameter> extraParameters = new ArrayList<>();
    TypeToken<?> expectedInputProviderT = inputProviderTypeOf(inputT);
    TypeToken<?> expectedOutputReceiverT = outputReceiverTypeOf(outputT);
    for (int i = 1; i < params.length; ++i) {
      TypeToken<?> param = fnClass.resolveType(params[i]);
      Class<?> rawType = param.getRawType();
      if (rawType.equals(BoundedWindow.class)) {
        checkArgument(
            !extraParameters.contains(DoFnSignature.ProcessElementMethod.Parameter.BOUNDED_WINDOW),
            "Multiple BoundedWindow parameters in %s",
            format(m));
        extraParameters.add(DoFnSignature.ProcessElementMethod.Parameter.BOUNDED_WINDOW);
      } else if (rawType.equals(DoFn.InputProvider.class)) {
        checkArgument(
            !extraParameters.contains(DoFnSignature.ProcessElementMethod.Parameter.INPUT_PROVIDER),
            "Multiple InputProvider parameters in %s",
            format(m));
        checkArgument(
            param.equals(expectedInputProviderT),
            "Wrong type of InputProvider parameter for method %s: %s, should be %s",
            format(m),
            formatType(param),
            formatType(expectedInputProviderT));
        extraParameters.add(DoFnSignature.ProcessElementMethod.Parameter.INPUT_PROVIDER);
      } else if (rawType.equals(DoFn.OutputReceiver.class)) {
        checkArgument(
            !extraParameters.contains(DoFnSignature.ProcessElementMethod.Parameter.OUTPUT_RECEIVER),
            "Multiple OutputReceiver parameters in %s",
            format(m));
        checkArgument(
            param.equals(expectedOutputReceiverT),
            "Wrong type of OutputReceiver parameter for method %s: %s, should be %s",
            format(m),
            formatType(param),
            formatType(expectedOutputReceiverT));
        extraParameters.add(DoFnSignature.ProcessElementMethod.Parameter.OUTPUT_RECEIVER);
      } else {
        List<String> allowedParamTypes =
            Arrays.asList(formatType(new TypeToken<BoundedWindow>() {}));
        checkArgument(
            false,
            "%s is not a valid context parameter for method %s. Should be one of %s",
            formatType(param),
            format(m),
            allowedParamTypes);
      }
    }

    return DoFnSignature.ProcessElementMethod.create(m, extraParameters);
  }

  @VisibleForTesting
  static DoFnSignature.BundleMethod analyzeBundleMethod(
      TypeToken<? extends DoFn> fnToken, Method m, TypeToken<?> inputT, TypeToken<?> outputT) {
    checkArgument(
        void.class.equals(m.getReturnType()), "%s must have a void return type", format(m));
    checkArgument(!m.isVarArgs(), "%s must not have var args", format(m));

    TypeToken<?> expectedContextToken = doFnContextTypeOf(inputT, outputT);

    Type[] params = m.getGenericParameterTypes();
    checkArgument(
        params.length == 1,
        "%s must have a single argument of type %s",
        format(m),
        formatType(expectedContextToken));
    TypeToken<?> contextToken = fnToken.resolveType(params[0]);
    checkArgument(
        contextToken.equals(expectedContextToken),
        "Wrong type of context argument to %s: %s, must be %s",
        format(m),
        formatType(contextToken),
        formatType(expectedContextToken));

    return DoFnSignature.BundleMethod.create(m);
  }

  private static DoFnSignature.LifecycleMethod analyzeLifecycleMethod(Method m) {
    checkArgument(
        void.class.equals(m.getReturnType()), "%s must have a void return type", format(m));
    checkArgument(
        m.getGenericParameterTypes().length == 0, "%s must take zero arguments", format(m));
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
      Class<? extends Annotation> anno, Class<?> fnClazz, boolean required) {
    Collection<Method> matches = declaredMethodsWithAnnotation(anno, fnClazz, DoFn.class);

    if (matches.size() == 0) {
      checkArgument(
          !required,
          "No method annotated with @%s found in %s",
          anno.getSimpleName(),
          fnClazz.getName());
      return null;
    }

    // If we have at least one match, then either it should be the only match
    // or it should be an extension of the other matches (which came from parent
    // classes).
    Method first = matches.iterator().next();
    for (Method other : matches) {
      checkArgument(
          first.getName().equals(other.getName())
              && Arrays.equals(first.getParameterTypes(), other.getParameterTypes()),
          "Found multiple methods annotated with @%s. [%s] and [%s]",
          anno.getSimpleName(),
          format(first),
          format(other));
    }

    // We need to be able to call it. We require it is public.
    checkArgument(
        (first.getModifiers() & Modifier.PUBLIC) != 0, "%s must be public", format(first));

    // And make sure its not static.
    checkArgument(
        (first.getModifiers() & Modifier.STATIC) == 0, "%s must not be static", format(first));

    return first;
  }

  private static String format(Method m) {
    return ReflectHelpers.CLASS_AND_METHOD_FORMATTER.apply(m);
  }

  private static String formatType(TypeToken<?> t) {
    return ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(t.getType());
  }
}
