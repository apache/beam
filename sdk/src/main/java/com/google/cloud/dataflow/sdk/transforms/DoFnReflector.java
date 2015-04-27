/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn.KeyedState;
import com.google.cloud.dataflow.sdk.transforms.DoFnWithContext.ExtraContextFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFnWithContext.FinishBundle;
import com.google.cloud.dataflow.sdk.transforms.DoFnWithContext.ProcessElement;
import com.google.cloud.dataflow.sdk.transforms.DoFnWithContext.StartBundle;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.UserCodeException;
import com.google.cloud.dataflow.sdk.util.WindowingInternals;
import com.google.cloud.dataflow.sdk.util.common.ReflectHelpers;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import org.joda.time.Instant;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
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
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Utility implementing the necessary reflection for working with {@link DoFnWithContext}s.
 */
public abstract class DoFnReflector {

  private interface ExtraContextInfo {
    /**
     * Create an instance of the given instance using the instance factory.
     */
    <I, O> Object createInstance(DoFnWithContext.ExtraContextFactory<I, O> factory);

    /**
     * Create the type token for the given type, filling in the generics.
     */
    <I, O> TypeToken<?> tokenFor(TypeToken<I> in, TypeToken<O> out);
  }

  private static final Map<Class<?>, ExtraContextInfo> EXTRA_CONTEXTS = Collections.emptyMap();
  private static final Map<Class<?>, ExtraContextInfo> EXTRA_PROCESS_CONTEXTS =
      ImmutableMap.<Class<?>, ExtraContextInfo>builder()
      .putAll(EXTRA_CONTEXTS)
      .put(KeyedState.class, new ExtraContextInfo() {
        @Override
        public <I, O> Object createInstance(ExtraContextFactory<I, O> factory) {
          return factory.keyedState();
        }

        @Override
        public <I, O> TypeToken<?> tokenFor(TypeToken<I> in, TypeToken<O> out) {
          return TypeToken.of(KeyedState.class);
        }
      })
      .put(BoundedWindow.class, new ExtraContextInfo() {
        @Override
        public <I, O> Object createInstance(ExtraContextFactory<I, O> factory) {
          return factory.window();
        }

        @Override
        public <I, O> TypeToken<?> tokenFor(TypeToken<I> in, TypeToken<O> out) {
          return TypeToken.of(BoundedWindow.class);
        }
      })
      .put(WindowingInternals.class, new ExtraContextInfo() {
        @Override
        public <I, O> Object createInstance(ExtraContextFactory<I, O> factory) {
          return factory.windowingInternals();
        }

        @Override
        public <I, O> TypeToken<?> tokenFor(TypeToken<I> in, TypeToken<O> out) {
          return new TypeToken<WindowingInternals<I, O>>() {
            private static final long serialVersionUID = 0;
          }
          .where(new TypeParameter<I>() {}, in)
          .where(new TypeParameter<O>() {}, out);
        }
      })
      .build();

  /**
   * @return true if the reflected {@link DoFnWithContext} uses Keyed State.
   */
  public abstract boolean usesKeyedState();

  /**
   * @return true if the reflected {@link DoFnWithContext} uses a Single Window.
   */
  public abstract boolean usesSingleWindow();

  /**
   * Invoke the reflected {@link ProcessElement} method on the given instance.
   *
   * @param fn an instance of the {@link DoFnWithContext} to invoke {@link ProcessElement} on.
   * @param c the {@link com.google.cloud.dataflow.sdk.transforms.DoFnWithContext.ProcessContext}
   *     to pass to {@link ProcessElement}.
   */
  abstract <I, O> void invokeProcessElement(
      DoFnWithContext<I, O> fn,
      DoFnWithContext<I, O>.ProcessContext c,
      ExtraContextFactory<I, O> extra);

  /**
   * Invoke the reflected {@link StartBundle} method on the given instance.
   *
   * @param fn an instance of the {@link DoFnWithContext} to invoke {@link StartBundle} on.
   * @param c the {@link com.google.cloud.dataflow.sdk.transforms.DoFnWithContext.Context}
   *     to pass to {@link StartBundle}.
   */
  abstract <I, O> void invokeStartBundle(
     DoFnWithContext<I, O> fn,
     DoFnWithContext<I, O>.Context c,
     ExtraContextFactory<I, O> extra);

  /**
   * Invoke the reflected {@link FinishBundle} method on the given instance.
   *
   * @param fn an instance of the {@link DoFnWithContext} to invoke {@link FinishBundle} on.
   * @param c the {@link com.google.cloud.dataflow.sdk.transforms.DoFnWithContext.Context}
   *     to pass to {@link FinishBundle}.
   */
  abstract <I, O> void invokeFinishBundle(
      DoFnWithContext<I, O> fn,
      DoFnWithContext<I, O>.Context c,
      ExtraContextFactory<I, O> extra);

  private static final Map<Class<?>, DoFnReflector> REFLECTOR_CACHE =
      new LinkedHashMap<Class<?>, DoFnReflector>();

  /**
   * @return the {@link DoFnReflector} for the given {@link DoFnWithContext}.
   */
  public static DoFnReflector of(
      @SuppressWarnings("rawtypes") Class<? extends DoFnWithContext> fn) {
    DoFnReflector reflector = REFLECTOR_CACHE.get(fn);
    if (reflector != null) {
      return reflector;
    }

    reflector = new GenericDoFnReflector(fn);
    REFLECTOR_CACHE.put(fn, reflector);
    return reflector;
  }

  /**
   * Create a {@link DoFn} that the {@link DoFnWithContext}.
   */
  public <I, O> DoFn<I, O> toDoFn(DoFnWithContext<I, O> fn) {
    if (usesKeyedState() && usesSingleWindow()) {
      return new WindowAndKeyedStateDoFnAdapter<I, O>(this, fn);
    } else if (usesKeyedState()) {
      return new KeyedStateDoFnAdapter<I, O>(this, fn);
    } else if (usesSingleWindow()) {
      return new WindowDoFnAdapter<I, O>(this, fn);
    } else {
      return new SimpleDoFnAdapter<I, O>(this, fn);
    }
  }

  private static String formatType(TypeToken<?> t) {
    return ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(t.getType());
  }

  private static String format(Method m) {
    return ReflectHelpers.CLASS_AND_METHOD_FORMATTER.apply(m);
  }

  private static Collection<String> describeSupportedTypes(
      Map<Class<?>, ExtraContextInfo> extraProcessContexts,
      final TypeToken<?> in, final TypeToken<?> out) {
    return FluentIterable
        .from(extraProcessContexts.values())
        .transform(new Function<ExtraContextInfo, String>() {
          @Override
          @Nullable
          public String apply(@Nullable ExtraContextInfo input) {
            return formatType(input.tokenFor(in, out));
          }
        })
        .toSortedSet(String.CASE_INSENSITIVE_ORDER);
  }

  @VisibleForTesting static <I, O> ExtraContextInfo[] verifyProcessMethodArguments(Method m) {
    return verifyMethodArguments(m,
        EXTRA_PROCESS_CONTEXTS,
        new TypeToken<DoFnWithContext<I, O>.ProcessContext>() {
          private static final long serialVersionUID = 0;
        },
        new TypeParameter<I>() {},
        new TypeParameter<O>() {});
  }

  @VisibleForTesting static <I, O> ExtraContextInfo[] verifyBundleMethodArguments(Method m) {
    return verifyMethodArguments(m,
        EXTRA_CONTEXTS,
        new TypeToken<DoFnWithContext<I, O>.Context>() {
          private static final long serialVersionUID = 0;
        },
        new TypeParameter<I>() {},
        new TypeParameter<O>() {});
  }

  /**
   * Verify the method arguments for a given {@link DoFnWithContext} method.
   *
   * <p>The requirements for a method to be valid, are:
   * <ol>
   * <li>The method has at least one argument.
   * <li>The first argument is of type firstContextArg.
   * <li>The remaining arguments have raw types that appear in {@code contexts}
   * <li>Any generics on the extra context arguments match what is expected. Eg.,
   *     {@code WindowingInternals<I, O>} either matches the {@code I} and {@code O} parameters of
   *     the {@code DoFn<I, O>.ProcessContext}, or it uses a wildcard, etc.
   * </ol>
   *
   * @param m the method to verify
   * @param contexts mapping from raw classes to the {@link ExtraContextInfo} used
   *     to create new instances.
   * @param firstContextArg the expected type of the first context argument
   * @param iParam TypeParameter representing the input type
   * @param oParam TypeParameter representing the output type
   */
  @VisibleForTesting static <I, O> ExtraContextInfo[] verifyMethodArguments(Method m,
      Map<Class<?>, ExtraContextInfo> contexts,
      TypeToken<?> firstContextArg, TypeParameter<I> iParam, TypeParameter<O> oParam) {

    if (!void.class.equals(m.getReturnType())) {
      throw new IllegalStateException(String.format(
          "%s must have a void return type", format(m)));
    }
    if (m.isVarArgs()) {
      throw new IllegalStateException(String.format(
          "%s must not have var args", format(m)));
    }

    // The first parameter must be present, and must be the specified type
    Type[] params = m.getGenericParameterTypes();
    TypeToken<?> contextToken = null;
    if (params.length > 0) {
      contextToken = TypeToken.of(params[0]);
    }
    if (contextToken == null
        || !contextToken.getRawType().equals(firstContextArg.getRawType())) {
      throw new IllegalStateException(String.format(
          "%s must take a %s as its first argument",
          format(m), firstContextArg.getRawType().getSimpleName()));
    }
    ExtraContextInfo[] contextInfos = new ExtraContextInfo[params.length - 1];

    // Fill in the generics in the allExtraContextArgs interface from the types in the
    // Context or ProcessContext DoFn.
    ParameterizedType pt = (ParameterizedType) contextToken.getType();
    // We actually want the owner, since ProcessContext and Context are owned by DoFnWithContext.
    pt = (ParameterizedType) pt.getOwnerType();
    @SuppressWarnings("unchecked")
    TypeToken<I> iActual = (TypeToken<I>) TypeToken.of(pt.getActualTypeArguments()[0]);
    @SuppressWarnings("unchecked")
    TypeToken<O> oActual = (TypeToken<O>) TypeToken.of(pt.getActualTypeArguments()[1]);

    // All of the remaining parameters must be a super-interface of allExtraContextArgs
    // that is not listed in the EXCLUDED_INTERFACES set.
    for (int i = 1; i < params.length; i++) {
      TypeToken<?> param = TypeToken.of(params[i]);

      ExtraContextInfo info = contexts.get(param.getRawType());
      if (info == null) {
        throw new IllegalStateException(String.format(
            "%s is not a valid context parameter for method %s. Should be one of %s",
            formatType(param), format(m),
            describeSupportedTypes(contexts, iActual, oActual)));
      }

      // If we get here, the class matches, but maybe the generics don't:
      TypeToken<?> expected = info.tokenFor(iActual, oActual);
      if (!isSupertypeOf(param, expected)) {
        throw new IllegalStateException(String.format(
            "Incompatible generics in context parameter %s for method %s. Should be %s",
            formatType(param), format(m), formatType(info.tokenFor(iActual, oActual))));
      }

      // Register the (now validated) context info
      contextInfos[i - 1] = info;
    }
    return contextInfos;
  }

  @SuppressWarnings("deprecation")
  private static boolean isSupertypeOf(TypeToken<?> param, TypeToken<?> expected) {
    return param.isAssignableFrom(expected);
  }

  /**
   * Implementation of {@link DoFnReflector} for the arbitrary {@link DoFnWithContext}.
   */
  private static class GenericDoFnReflector extends DoFnReflector {

    private Method startBundle;
    private Method processElement;
    private Method finishBundle;
    private ExtraContextInfo[] processElementArgs;
    private ExtraContextInfo[] startBundleArgs;
    private ExtraContextInfo[] finishBundleArgs;

    private GenericDoFnReflector(Class<?> fn) {
      // Locate the annotated methods
      this.processElement = findAnnotatedMethod(ProcessElement.class, fn, true);
      this.startBundle = findAnnotatedMethod(StartBundle.class, fn, false);
      this.finishBundle = findAnnotatedMethod(FinishBundle.class, fn, false);

      // Verify that their method arguments satisfy our conditions.
      processElementArgs = verifyProcessMethodArguments(processElement);
      if (startBundle != null) {
        startBundleArgs = verifyBundleMethodArguments(startBundle);
      }
      if (finishBundle != null) {
        finishBundleArgs = verifyBundleMethodArguments(finishBundle);
      }
    }

    private static Collection<Method> declaredMethodsWithAnnotation(
        Class<? extends Annotation> anno,
        Class<?> startClass, Class<?> stopClass) {
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
      Collection<Method> matches = declaredMethodsWithAnnotation(
          anno, fnClazz, DoFnWithContext.class);

      if (matches.size() == 0) {
        if (required == true) {
          throw new IllegalStateException(String.format(
              "No method annotated with @%s found in %s",
              anno.getSimpleName(), fnClazz.getName()));
        } else {
          return null;
        }
      }

      // If we have at least one match, then either it should be the only match
      // or it should be an extension of the other matches (which came from parent
      // classes).
      Method first = matches.iterator().next();
      for (Method other : matches) {
        if (!first.getName().equals(other.getName())
            || !Arrays.equals(first.getParameterTypes(), other.getParameterTypes())) {
          throw new IllegalStateException(String.format(
              "Found multiple methods annotated with @%s. [%s] and [%s]",
              anno.getSimpleName(), format(first), format(other)));
        }
      }

      // We need to be able to call it. We require it is public.
      if ((first.getModifiers() & Modifier.PUBLIC) == 0) {
        throw new IllegalStateException(format(first) + " must be public");
      }

      // And make sure its not static.
      if ((first.getModifiers() & Modifier.STATIC) != 0) {
        throw new IllegalStateException(format(first) + " must not be static");
      }

      first.setAccessible(true);
      return first;
    }

    @Override
    public boolean usesKeyedState() {
      return usesContext(DoFn.KeyedState.class);
    }

    @Override
    public boolean usesSingleWindow() {
      return usesContext(BoundedWindow.class);
    }

    private boolean usesContext(Class<?> context) {
      for (Class<?> clazz : processElement.getParameterTypes()) {
        if (clazz.equals(context)) {
          return true;
        }
      }
      return false;
    }

    @Override
    <I, O> void invokeProcessElement(
        DoFnWithContext<I, O> fn,
        DoFnWithContext<I, O>.ProcessContext c,
        ExtraContextFactory<I, O> extra) {
      invoke(processElement, fn, c, extra, processElementArgs);
    }

    @Override
    <I, O> void invokeStartBundle(
        DoFnWithContext<I, O> fn,
        DoFnWithContext<I, O>.Context c,
        ExtraContextFactory<I, O> extra) {
      if (startBundle != null) {
        invoke(startBundle, fn, c, extra, startBundleArgs);
      }
    }

    @Override
    <I, O> void invokeFinishBundle(
        DoFnWithContext<I, O> fn,
        DoFnWithContext<I, O>.Context c,
        ExtraContextFactory<I, O> extra) {
      if (finishBundle != null) {
        invoke(finishBundle, fn, c, extra, finishBundleArgs);
      }
    }

    private <I, O> void invoke(Method m,
        DoFnWithContext<I, O> on,
        DoFnWithContext<I, O>.Context contextArg,
        ExtraContextFactory<I, O> extraArgFactory,
        ExtraContextInfo[] extraArgs) {

      Class<?>[] parameterTypes = m.getParameterTypes();
      Object[] args = new Object[parameterTypes.length];
      args[0] = contextArg;
      for (int i = 1; i < args.length; i++) {
        args[i] = extraArgs[i - 1].createInstance(extraArgFactory);
      }

      try {
        m.invoke(on, args);
      } catch (InvocationTargetException e) {
        // Exception in user code.
        Throwables.propagateIfInstanceOf(e.getCause(), UserCodeException.class);
        throw new UserCodeException(e.getCause());
      } catch (IllegalAccessException | IllegalArgumentException e) {
        // Exception in our code.
        throw Throwables.propagate(e);
      }
    }
  }

  private static class ContextAdapter<I, O> extends DoFnWithContext<I, O>.Context
      implements DoFnWithContext.ExtraContextFactory<I, O> {

    private DoFn<I, O>.Context context;

    private ContextAdapter(DoFnWithContext<I, O> fn, DoFn<I, O>.Context context) {
      fn.super();
      this.context = context;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return context.getPipelineOptions();
    }

    @Override
    public void output(O output) {
      context.output(output);
    }

    @Override
    public void outputWithTimestamp(O output, Instant timestamp) {
      context.outputWithTimestamp(output, timestamp);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      context.sideOutput(tag, output);
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      context.sideOutputWithTimestamp(tag, output, timestamp);
    }

    @Override
    public KeyedState keyedState() {
      // The DoFnWithContext doesn't allow us to ask for these outside ProcessElements, so this
      // should be unreachable.
      throw new UnsupportedOperationException("Can only get keyedState in ProcessElements");
    }

    @Override
    public BoundedWindow window() {
      // The DoFnWithContext doesn't allow us to ask for these outside ProcessElements, so this
      // should be unreachable.
      throw new UnsupportedOperationException("Can only get the window in ProcessElements");
    }

    @Override
    public WindowingInternals<I, O> windowingInternals() {
      // The DoFnWithContext doesn't allow us to ask for these outside ProcessElements, so this
      // should be unreachable.
      throw new UnsupportedOperationException(
          "Can only get the windowingInternals in ProcessElements");
    }
  }

  private static class ProcessContextAdapter<I, O>
      extends DoFnWithContext<I, O>.ProcessContext
      implements DoFnWithContext.ExtraContextFactory<I, O> {

    private DoFn<I, O>.ProcessContext context;

    private ProcessContextAdapter(DoFnWithContext<I, O> fn, DoFn<I, O>.ProcessContext context) {
      fn.super();
      this.context = context;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return context.getPipelineOptions();
    }

    @Override
    public <T> T sideInput(PCollectionView<T> view) {
      return context.sideInput(view);
    }

    @Override
    public void output(O output) {
      context.output(output);
    }

    @Override
    public void outputWithTimestamp(O output, Instant timestamp) {
      context.outputWithTimestamp(output, timestamp);
    }

    @Override
    public <T> void sideOutput(TupleTag<T> tag, T output) {
      context.sideOutput(tag, output);
    }

    @Override
    public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
      context.sideOutputWithTimestamp(tag, output, timestamp);
    }

    @Override
    public I element() {
      return context.element();
    }

    @Override
    public Instant timestamp() {
      return context.timestamp();
    }

    @Override
    public KeyedState keyedState() {
      return context.keyedState();
    }

    @Override
    public BoundedWindow window() {
      return context.window();
    }

    @Override
    public WindowingInternals<I, O> windowingInternals() {
      return context.windowingInternals();
    }
  }

  public static Class<?> getDoFnClass(DoFn<?, ?> fn) {
    if (fn instanceof SimpleDoFnAdapter) {
      return ((SimpleDoFnAdapter<?, ?>) fn).fn.getClass();
    } else {
      return fn.getClass();
    }
  }

  private static class SimpleDoFnAdapter<I, O> extends DoFn<I, O> {

    private static final long serialVersionUID = 0;

    private transient DoFnReflector reflector;
    private DoFnWithContext<I, O> fn;

    private SimpleDoFnAdapter(DoFnReflector reflector, DoFnWithContext<I, O> fn) {
      super(fn.aggregators);
      this.reflector = reflector;
      this.fn = fn;
    }

    @Override
    public void startBundle(DoFn<I, O>.Context c) throws Exception {
      ContextAdapter<I, O> adapter = new ContextAdapter<>(fn, c);
      reflector.invokeStartBundle(fn, adapter, adapter);
    }

    @Override
    public void finishBundle(DoFn<I, O>.Context c) throws Exception {
      ContextAdapter<I, O> adapter = new ContextAdapter<>(fn, c);
      reflector.invokeFinishBundle(fn, adapter, adapter);
    }

    @Override
    public void processElement(DoFn<I, O>.ProcessContext c) throws Exception {
      ProcessContextAdapter<I, O> adapter = new ProcessContextAdapter<>(fn, c);
      reflector.invokeProcessElement(fn, adapter, adapter);
    }

    @Override
    protected TypeToken<I> getInputTypeToken() {
      return fn.getInputTypeToken();
    }

    @Override
    protected TypeToken<O> getOutputTypeToken() {
      return fn.getOutputTypeToken();
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      reflector = DoFnReflector.of(fn.getClass());
    }
  }

  private static class KeyedStateDoFnAdapter<I, O>
      extends SimpleDoFnAdapter<I, O> implements DoFn.RequiresKeyedState {

    private static final long serialVersionUID = 0;
    private KeyedStateDoFnAdapter(DoFnReflector reflector, DoFnWithContext<I, O> fn) {
      super(reflector, fn);
    }
  }

  private static class WindowDoFnAdapter<I, O>
  extends SimpleDoFnAdapter<I, O> implements DoFn.RequiresWindowAccess {

    private static final long serialVersionUID = 0;
    private WindowDoFnAdapter(DoFnReflector reflector, DoFnWithContext<I, O> fn) {
      super(reflector, fn);
    }
  }

  private static class WindowAndKeyedStateDoFnAdapter<I, O>
  extends SimpleDoFnAdapter<I, O> implements DoFn.RequiresKeyedState, DoFn.RequiresWindowAccess {

    private static final long serialVersionUID = 0;
    private WindowAndKeyedStateDoFnAdapter(DoFnReflector reflector, DoFnWithContext<I, O> fn) {
      super(reflector, fn);
    }
  }
}
