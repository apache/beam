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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFnWithContext.ExtraContextFactory;
import org.apache.beam.sdk.transforms.DoFnWithContext.FinishBundle;
import org.apache.beam.sdk.transforms.DoFnWithContext.ProcessElement;
import org.apache.beam.sdk.transforms.DoFnWithContext.StartBundle;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.util.WindowingInternals;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
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
    <InputT, OutputT> Object createInstance(
        DoFnWithContext.ExtraContextFactory<InputT, OutputT> factory);

    /**
     * Create the type token for the given type, filling in the generics.
     */
    <InputT, OutputT> TypeToken<?> tokenFor(TypeToken<InputT> in, TypeToken<OutputT> out);
  }

  private static final Map<Class<?>, ExtraContextInfo> EXTRA_CONTEXTS = Collections.emptyMap();
  private static final Map<Class<?>, ExtraContextInfo> EXTRA_PROCESS_CONTEXTS =
      ImmutableMap.<Class<?>, ExtraContextInfo>builder()
      .putAll(EXTRA_CONTEXTS)
      .put(BoundedWindow.class, new ExtraContextInfo() {
        @Override
        public <InputT, OutputT> Object
            createInstance(ExtraContextFactory<InputT, OutputT> factory) {
          return factory.window();
        }

        @Override
        public <InputT, OutputT> TypeToken<?>
            tokenFor(TypeToken<InputT> in, TypeToken<OutputT> out) {
          return TypeToken.of(BoundedWindow.class);
        }
      })
      .put(WindowingInternals.class, new ExtraContextInfo() {
        @Override
        public <InputT, OutputT> Object
            createInstance(ExtraContextFactory<InputT, OutputT> factory) {
          return factory.windowingInternals();
        }

        @Override
        public <InputT, OutputT> TypeToken<?>
            tokenFor(TypeToken<InputT> in, TypeToken<OutputT> out) {
          return new TypeToken<WindowingInternals<InputT, OutputT>>() {
            }
          .where(new TypeParameter<InputT>() {}, in)
          .where(new TypeParameter<OutputT>() {}, out);
        }
      })
      .build();

  /**
   * @return true if the reflected {@link DoFnWithContext} uses a Single Window.
   */
  public abstract boolean usesSingleWindow();

  /**
   * Invoke the reflected {@link ProcessElement} method on the given instance.
   *
   * @param fn an instance of the {@link DoFnWithContext} to invoke {@link ProcessElement} on.
   * @param c the {@link org.apache.beam.sdk.transforms.DoFnWithContext.ProcessContext}
   *     to pass to {@link ProcessElement}.
   */
  public abstract <InputT, OutputT> void invokeProcessElement(
      DoFnWithContext<InputT, OutputT> fn,
      DoFnWithContext<InputT, OutputT>.ProcessContext c,
      ExtraContextFactory<InputT, OutputT> extra);

  /**
   * Invoke the reflected {@link StartBundle} method on the given instance.
   *
   * @param fn an instance of the {@link DoFnWithContext} to invoke {@link StartBundle} on.
   * @param c the {@link org.apache.beam.sdk.transforms.DoFnWithContext.Context}
   *     to pass to {@link StartBundle}.
   */
  public <InputT, OutputT> void invokeStartBundle(
     DoFnWithContext<InputT, OutputT> fn,
     DoFnWithContext<InputT, OutputT>.Context c,
     ExtraContextFactory<InputT, OutputT> extra) {
    fn.prepareForProcessing();
  }

  /**
   * Invoke the reflected {@link FinishBundle} method on the given instance.
   *
   * @param fn an instance of the {@link DoFnWithContext} to invoke {@link FinishBundle} on.
   * @param c the {@link org.apache.beam.sdk.transforms.DoFnWithContext.Context}
   *     to pass to {@link FinishBundle}.
   */
  public abstract <InputT, OutputT> void invokeFinishBundle(
      DoFnWithContext<InputT, OutputT> fn,
      DoFnWithContext<InputT, OutputT>.Context c,
      ExtraContextFactory<InputT, OutputT> extra);

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
  public <InputT, OutputT> DoFn<InputT, OutputT> toDoFn(DoFnWithContext<InputT, OutputT> fn) {
    if (usesSingleWindow()) {
      return new WindowDoFnAdapter<InputT, OutputT>(this, fn);
    } else {
      return new SimpleDoFnAdapter<InputT, OutputT>(this, fn);
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
            if (input == null) {
              return null;
            } else {
              return formatType(input.tokenFor(in, out));
            }
          }
        })
        .toSortedSet(String.CASE_INSENSITIVE_ORDER);
  }

  @VisibleForTesting
  static <InputT, OutputT> ExtraContextInfo[] verifyProcessMethodArguments(Method m) {
    return verifyMethodArguments(m,
        EXTRA_PROCESS_CONTEXTS,
        new TypeToken<DoFnWithContext<InputT, OutputT>.ProcessContext>() {
          },
        new TypeParameter<InputT>() {},
        new TypeParameter<OutputT>() {});
  }

  @VisibleForTesting
  static <InputT, OutputT> ExtraContextInfo[] verifyBundleMethodArguments(Method m) {
    return verifyMethodArguments(m,
        EXTRA_CONTEXTS,
        new TypeToken<DoFnWithContext<InputT, OutputT>.Context>() {
          },
        new TypeParameter<InputT>() {},
        new TypeParameter<OutputT>() {});
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
   *     {@code WindowingInternals<InputT, OutputT>} either matches the
   *     {@code InputT} and {@code OutputT} parameters of the
   *     {@code DoFn<InputT, OutputT>.ProcessContext}, or it uses a wildcard, etc.
   * </ol>
   *
   * @param m the method to verify
   * @param contexts mapping from raw classes to the {@link ExtraContextInfo} used
   *     to create new instances.
   * @param firstContextArg the expected type of the first context argument
   * @param iParam TypeParameter representing the input type
   * @param oParam TypeParameter representing the output type
   */
  @VisibleForTesting static <InputT, OutputT> ExtraContextInfo[] verifyMethodArguments(Method m,
      Map<Class<?>, ExtraContextInfo> contexts,
      TypeToken<?> firstContextArg, TypeParameter<InputT> iParam, TypeParameter<OutputT> oParam) {

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
    TypeToken<InputT> iActual = (TypeToken<InputT>) TypeToken.of(pt.getActualTypeArguments()[0]);
    @SuppressWarnings("unchecked")
    TypeToken<OutputT> oActual = (TypeToken<OutputT>) TypeToken.of(pt.getActualTypeArguments()[1]);

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
      if (!expected.isSubtypeOf(param)) {
        throw new IllegalStateException(String.format(
            "Incompatible generics in context parameter %s for method %s. Should be %s",
            formatType(param), format(m), formatType(info.tokenFor(iActual, oActual))));
      }

      // Register the (now validated) context info
      contextInfos[i - 1] = info;
    }
    return contextInfos;
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
    public <InputT, OutputT> void invokeProcessElement(
        DoFnWithContext<InputT, OutputT> fn,
        DoFnWithContext<InputT, OutputT>.ProcessContext c,
        ExtraContextFactory<InputT, OutputT> extra) {
      invoke(processElement, fn, c, extra, processElementArgs);
    }

    @Override
    public <InputT, OutputT> void invokeStartBundle(
        DoFnWithContext<InputT, OutputT> fn,
        DoFnWithContext<InputT, OutputT>.Context c,
        ExtraContextFactory<InputT, OutputT> extra) {
      super.invokeStartBundle(fn, c, extra);
      if (startBundle != null) {
        invoke(startBundle, fn, c, extra, startBundleArgs);
      }
    }

    @Override
    public <InputT, OutputT> void invokeFinishBundle(
        DoFnWithContext<InputT, OutputT> fn,
        DoFnWithContext<InputT, OutputT>.Context c,
        ExtraContextFactory<InputT, OutputT> extra) {
      if (finishBundle != null) {
        invoke(finishBundle, fn, c, extra, finishBundleArgs);
      }
    }

    private <InputT, OutputT> void invoke(Method m,
        DoFnWithContext<InputT, OutputT> on,
        DoFnWithContext<InputT, OutputT>.Context contextArg,
        ExtraContextFactory<InputT, OutputT> extraArgFactory,
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
        throw UserCodeException.wrap(e.getCause());
      } catch (IllegalAccessException | IllegalArgumentException e) {
        // Exception in our code.
        throw new RuntimeException(e);
      }
    }
  }

  private static class ContextAdapter<InputT, OutputT>
      extends DoFnWithContext<InputT, OutputT>.Context
      implements DoFnWithContext.ExtraContextFactory<InputT, OutputT> {

    private DoFn<InputT, OutputT>.Context context;

    private ContextAdapter(
        DoFnWithContext<InputT, OutputT> fn, DoFn<InputT, OutputT>.Context context) {
      fn.super();
      this.context = context;
    }

    @Override
    public PipelineOptions getPipelineOptions() {
      return context.getPipelineOptions();
    }

    @Override
    public void output(OutputT output) {
      context.output(output);
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
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
    public BoundedWindow window() {
      // The DoFnWithContext doesn't allow us to ask for these outside ProcessElements, so this
      // should be unreachable.
      throw new UnsupportedOperationException("Can only get the window in ProcessElements");
    }

    @Override
    public WindowingInternals<InputT, OutputT> windowingInternals() {
      // The DoFnWithContext doesn't allow us to ask for these outside ProcessElements, so this
      // should be unreachable.
      throw new UnsupportedOperationException(
          "Can only get the windowingInternals in ProcessElements");
    }
  }

  private static class ProcessContextAdapter<InputT, OutputT>
      extends DoFnWithContext<InputT, OutputT>.ProcessContext
      implements DoFnWithContext.ExtraContextFactory<InputT, OutputT> {

    private DoFn<InputT, OutputT>.ProcessContext context;

    private ProcessContextAdapter(
        DoFnWithContext<InputT, OutputT> fn,
        DoFn<InputT, OutputT>.ProcessContext context) {
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
    public void output(OutputT output) {
      context.output(output);
    }

    @Override
    public void outputWithTimestamp(OutputT output, Instant timestamp) {
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
    public InputT element() {
      return context.element();
    }

    @Override
    public Instant timestamp() {
      return context.timestamp();
    }

    @Override
    public PaneInfo pane() {
      return context.pane();
    }

    @Override
    public BoundedWindow window() {
      return context.window();
    }

    @Override
    public WindowingInternals<InputT, OutputT> windowingInternals() {
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

  private static class SimpleDoFnAdapter<InputT, OutputT> extends DoFn<InputT, OutputT> {

    private transient DoFnReflector reflector;
    private DoFnWithContext<InputT, OutputT> fn;

    private SimpleDoFnAdapter(DoFnReflector reflector, DoFnWithContext<InputT, OutputT> fn) {
      super(fn.aggregators);
      this.reflector = reflector;
      this.fn = fn;
    }

    @Override
    public void startBundle(DoFn<InputT, OutputT>.Context c) throws Exception {
      ContextAdapter<InputT, OutputT> adapter = new ContextAdapter<>(fn, c);
      reflector.invokeStartBundle(fn, (DoFnWithContext<InputT, OutputT>.Context) adapter, adapter);
    }

    @Override
    public void finishBundle(DoFn<InputT, OutputT>.Context c) throws Exception {
      ContextAdapter<InputT, OutputT> adapter = new ContextAdapter<>(fn, c);
      reflector.invokeFinishBundle(fn, (DoFnWithContext<InputT, OutputT>.Context) adapter, adapter);
    }

    @Override
    public void processElement(DoFn<InputT, OutputT>.ProcessContext c) throws Exception {
      ProcessContextAdapter<InputT, OutputT> adapter = new ProcessContextAdapter<>(fn, c);
      reflector.invokeProcessElement(
          fn, (DoFnWithContext<InputT, OutputT>.ProcessContext) adapter, adapter);
    }

    @Override
    protected TypeDescriptor<InputT> getInputTypeDescriptor() {
      return fn.getInputTypeDescriptor();
    }

    @Override
    protected TypeDescriptor<OutputT> getOutputTypeDescriptor() {
      return fn.getOutputTypeDescriptor();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.include(fn);
    }

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      reflector = DoFnReflector.of(fn.getClass());
    }
  }

  private static class WindowDoFnAdapter<InputT, OutputT>
  extends SimpleDoFnAdapter<InputT, OutputT> implements DoFn.RequiresWindowAccess {

    private WindowDoFnAdapter(DoFnReflector reflector, DoFnWithContext<InputT, OutputT> fn) {
      super(reflector, fn);
    }
  }
}
