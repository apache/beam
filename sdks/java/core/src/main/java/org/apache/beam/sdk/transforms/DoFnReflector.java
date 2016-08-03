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
import org.apache.beam.sdk.transforms.DoFn.ExtraContextFactory;
import org.apache.beam.sdk.transforms.DoFn.FinishBundle;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.StartBundle;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.NamingStrategy.SuffixingRandom;
import net.bytebuddy.description.field.FieldDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.method.ParameterList;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeDescription.Generic;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy.Default;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodCall.MethodLocator;
import net.bytebuddy.implementation.StubMethod;
import net.bytebuddy.implementation.bind.MethodDelegationBinder.MethodInvoker;
import net.bytebuddy.implementation.bind.annotation.TargetMethodAnnotationDrivenBinder.TerminationHandler;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.Duplication;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.Throw;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.matcher.ElementMatchers;
import org.joda.time.Instant;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
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
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Utility implementing the necessary reflection for working with {@link DoFn}s.
 */
public abstract class DoFnReflector {

  private static final String FN_DELEGATE_FIELD_NAME = "delegate";

  private enum Availability {
    /** Indicates parameters only available in {@code @ProcessElement} methods. */
    PROCESS_ELEMENT_ONLY,
    /** Indicates parameters available in all methods. */
    EVERYWHERE
  }

  /**
   * Enumeration of the parameters available from the {@link ExtraContextFactory} to use as
   * additional parameters for {@link DoFn} methods.
   * <p>
   * We don't rely on looking for properly annotated methods within {@link ExtraContextFactory}
   * because erasure would make it impossible to completely fill in the type token for context
   * parameters that depend on the input/output type.
   */
  private enum AdditionalParameter {

    /** Any {@link BoundedWindow} parameter is populated by the window of the current element. */
    WINDOW_OF_ELEMENT(Availability.PROCESS_ELEMENT_ONLY, BoundedWindow.class, "window") {
      @Override
      public <InputT, OutputT> TypeToken<?>
          tokenFor(TypeToken<InputT> in, TypeToken<OutputT> out) {
        return TypeToken.of(BoundedWindow.class);
      }
    },

    WINDOWING_INTERNALS(Availability.PROCESS_ELEMENT_ONLY,
        WindowingInternals.class, "windowingInternals") {
      @Override
      public <InputT, OutputT> TypeToken<?> tokenFor(
          TypeToken<InputT> in, TypeToken<OutputT> out) {
        return new TypeToken<WindowingInternals<InputT, OutputT>>() {}
            .where(new TypeParameter<InputT>() {}, in)
            .where(new TypeParameter<OutputT>() {}, out);
      }
    };

    /**
     * Create a type token representing the given parameter. May use the type token associated
     * with the input and output types of the {@link DoFn}, depending on the extra
     * context.
     */
    abstract <InputT, OutputT> TypeToken<?> tokenFor(
        TypeToken<InputT> in, TypeToken<OutputT> out);

    private final Class<?> rawType;
    private final Availability availability;
    private final transient MethodDescription method;

    private AdditionalParameter(Availability availability, Class<?> rawType, String method) {
      this.availability = availability;
      this.rawType = rawType;
      try {
        this.method = new MethodDescription.ForLoadedMethod(
            ExtraContextFactory.class.getMethod(method));
      } catch (NoSuchMethodException | SecurityException e) {
        throw new RuntimeException(
            "Unable to access method " + method + " on " + ExtraContextFactory.class, e);
      }
    }
  }

  private static final Map<Class<?>, AdditionalParameter> EXTRA_CONTEXTS;
  private static final Map<Class<?>, AdditionalParameter> EXTRA_PROCESS_CONTEXTS;

  static {
    ImmutableMap.Builder<Class<?>, AdditionalParameter> everywhereBuilder =
        ImmutableMap.<Class<?>, AdditionalParameter>builder();
    ImmutableMap.Builder<Class<?>, AdditionalParameter> processElementBuilder =
        ImmutableMap.<Class<?>, AdditionalParameter>builder();

    for (AdditionalParameter value : AdditionalParameter.values()) {
      switch (value.availability) {
        case EVERYWHERE:
          everywhereBuilder.put(value.rawType, value);
          break;
        case PROCESS_ELEMENT_ONLY:
          processElementBuilder.put(value.rawType, value);
          break;
      }
    }

    EXTRA_CONTEXTS = everywhereBuilder.build();
    EXTRA_PROCESS_CONTEXTS = processElementBuilder
        // Process Element contexts include everything available everywhere
        .putAll(EXTRA_CONTEXTS)
        .build();
  }

  /**
   * @return true if the reflected {@link DoFn} uses a Single Window.
   */
  public abstract boolean usesSingleWindow();

  /** Create an {@link DoFnInvoker} bound to the given {@link OldDoFn}. */
  public abstract <InputT, OutputT> DoFnInvoker<InputT, OutputT> bindInvoker(
      DoFn<InputT, OutputT> fn);

  private static final Map<Class<?>, DoFnReflector> REFLECTOR_CACHE =
      new LinkedHashMap<Class<?>, DoFnReflector>();

  /**
   * @return the {@link DoFnReflector} for the given {@link DoFn}.
   */
  public static DoFnReflector of(
      @SuppressWarnings("rawtypes") Class<? extends DoFn> fn) {
    DoFnReflector reflector = REFLECTOR_CACHE.get(fn);
    if (reflector != null) {
      return reflector;
    }

    reflector = new GenericDoFnReflector(fn);
    REFLECTOR_CACHE.put(fn, reflector);
    return reflector;
  }

  /**
   * Create a {@link OldDoFn} that the {@link DoFn}.
   */
  public <InputT, OutputT> OldDoFn<InputT, OutputT> toDoFn(DoFn<InputT, OutputT> fn) {
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
      Map<Class<?>, AdditionalParameter> extraProcessContexts,
      final TypeToken<?> in, final TypeToken<?> out) {
    return FluentIterable
        .from(extraProcessContexts.values())
        .transform(new Function<AdditionalParameter, String>() {

          @Override
          @Nullable
          public String apply(@Nullable AdditionalParameter input) {
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
  static <InputT, OutputT> List<AdditionalParameter> verifyProcessMethodArguments(Method m) {
    return verifyMethodArguments(m,
        EXTRA_PROCESS_CONTEXTS,
        new TypeToken<DoFn<InputT, OutputT>.ProcessContext>() {},
        new TypeParameter<InputT>() {},
        new TypeParameter<OutputT>() {});
  }

  @VisibleForTesting
  static <InputT, OutputT> List<AdditionalParameter> verifyBundleMethodArguments(Method m) {
    if (m == null) {
      return null;
    }
    return verifyMethodArguments(m,
        EXTRA_CONTEXTS,
        new TypeToken<DoFn<InputT, OutputT>.Context>() {},
        new TypeParameter<InputT>() {},
        new TypeParameter<OutputT>() {});
  }

  /**
   * Verify the method arguments for a given {@link DoFn} method.
   *
   * <p>The requirements for a method to be valid, are:
   * <ol>
   * <li>The method has at least one argument.
   * <li>The first argument is of type firstContextArg.
   * <li>The remaining arguments have raw types that appear in {@code contexts}
   * <li>Any generics on the extra context arguments match what is expected. Eg.,
   *     {@code WindowingInternals<InputT, OutputT>} either matches the
   *     {@code InputT} and {@code OutputT} parameters of the
   *     {@code OldDoFn<InputT, OutputT>.ProcessContext}, or it uses a wildcard, etc.
   * </ol>
   *
   * @param m the method to verify
   * @param contexts mapping from raw classes to the {@link AdditionalParameter} used
   *     to create new instances.
   * @param firstContextArg the expected type of the first context argument
   * @param iParam TypeParameter representing the input type
   * @param oParam TypeParameter representing the output type
   */
  @VisibleForTesting static <InputT, OutputT> List<AdditionalParameter> verifyMethodArguments(
      Method m,
      Map<Class<?>, AdditionalParameter> contexts,
      TypeToken<?> firstContextArg,
      TypeParameter<InputT> iParam,
      TypeParameter<OutputT> oParam) {

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
    AdditionalParameter[] contextInfos = new AdditionalParameter[params.length - 1];

    // Fill in the generics in the allExtraContextArgs interface from the types in the
    // Context or ProcessContext OldDoFn.
    ParameterizedType pt = (ParameterizedType) contextToken.getType();
    // We actually want the owner, since ProcessContext and Context are owned by DoFn.
    pt = (ParameterizedType) pt.getOwnerType();
    @SuppressWarnings("unchecked")
    TypeToken<InputT> iActual = (TypeToken<InputT>) TypeToken.of(pt.getActualTypeArguments()[0]);
    @SuppressWarnings("unchecked")
    TypeToken<OutputT> oActual = (TypeToken<OutputT>) TypeToken.of(pt.getActualTypeArguments()[1]);

    // All of the remaining parameters must be a super-interface of allExtraContextArgs
    // that is not listed in the EXCLUDED_INTERFACES set.
    for (int i = 1; i < params.length; i++) {
      TypeToken<?> param = TypeToken.of(params[i]);

      AdditionalParameter info = contexts.get(param.getRawType());
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
    return ImmutableList.copyOf(contextInfos);
  }

  /** Interface for invoking the {@code OldDoFn} processing methods. */
  public interface DoFnInvoker<InputT, OutputT>  {
    /** Invoke {@link OldDoFn#startBundle} on the bound {@code OldDoFn}. */
    void invokeStartBundle(
        DoFn<InputT, OutputT>.Context c,
        ExtraContextFactory<InputT, OutputT> extra);
    /** Invoke {@link OldDoFn#finishBundle} on the bound {@code OldDoFn}. */
    void invokeFinishBundle(
        DoFn<InputT, OutputT>.Context c,
        ExtraContextFactory<InputT, OutputT> extra);

    /** Invoke {@link OldDoFn#processElement} on the bound {@code OldDoFn}. */
    public void invokeProcessElement(
        DoFn<InputT, OutputT>.ProcessContext c,
        ExtraContextFactory<InputT, OutputT> extra);
  }

  /**
   * Implementation of {@link DoFnReflector} for the arbitrary {@link DoFn}.
   */
  private static class GenericDoFnReflector extends DoFnReflector {

    private final Method startBundle;
    private final Method processElement;
    private final Method finishBundle;
    private final List<AdditionalParameter> processElementArgs;
    private final List<AdditionalParameter> startBundleArgs;
    private final List<AdditionalParameter> finishBundleArgs;
    private final Constructor<?> constructor;

    private GenericDoFnReflector(
        @SuppressWarnings("rawtypes") Class<? extends DoFn> fn) {
      // Locate the annotated methods
      this.processElement = findAnnotatedMethod(ProcessElement.class, fn, true);
      this.startBundle = findAnnotatedMethod(StartBundle.class, fn, false);
      this.finishBundle = findAnnotatedMethod(FinishBundle.class, fn, false);

      // Verify that their method arguments satisfy our conditions.
      this.processElementArgs = verifyProcessMethodArguments(processElement);
      this.startBundleArgs = verifyBundleMethodArguments(startBundle);
      this.finishBundleArgs = verifyBundleMethodArguments(finishBundle);

      this.constructor = createInvokerConstructor(fn);
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
          anno, fnClazz, DoFn.class);

      if (matches.size() == 0) {
        if (required) {
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

      return first;
    }

    @Override
    public boolean usesSingleWindow() {
      return usesContext(AdditionalParameter.WINDOW_OF_ELEMENT);
    }

    private boolean usesContext(AdditionalParameter param) {
      return processElementArgs.contains(param)
          || (startBundleArgs != null && startBundleArgs.contains(param))
          || (finishBundleArgs != null && finishBundleArgs.contains(param));
    }

    /**
     * Use ByteBuddy to generate the code for a {@link DoFnInvoker} that invokes the given
     * {@link DoFn}.
     * @param clazz
     * @return
     */
    private Constructor<? extends DoFnInvoker<?, ?>> createInvokerConstructor(
        @SuppressWarnings("rawtypes") Class<? extends DoFn> clazz) {

      final TypeDescription clazzDescription = new TypeDescription.ForLoadedType(clazz);

      DynamicType.Builder<?> builder = new ByteBuddy()
          // Create subclasses inside the target class, to have access to
          // private and package-private bits
          .with(new SuffixingRandom("auxiliary") {
                @Override
                public String subclass(Generic superClass) {
                  return super.name(clazzDescription);
                }
              })
          // Create a subclass of DoFnInvoker
          .subclass(DoFnInvoker.class, Default.NO_CONSTRUCTORS)
          .defineField(FN_DELEGATE_FIELD_NAME, clazz, Visibility.PRIVATE, FieldManifestation.FINAL)
          // Define a constructor to populate fields appropriately.
          .defineConstructor(Visibility.PUBLIC)
          .withParameter(clazz)
          .intercept(new InvokerConstructor())
          // Implement the three methods by calling into the appropriate functions on the fn.
          .method(ElementMatchers.named("invokeProcessElement"))
          .intercept(InvokerDelegation.create(
              processElement, BeforeDelegation.NOOP, processElementArgs))
          .method(ElementMatchers.named("invokeStartBundle"))
          .intercept(InvokerDelegation.create(
              startBundle, BeforeDelegation.INVOKE_PREPARE_FOR_PROCESSING, startBundleArgs))
          .method(ElementMatchers.named("invokeFinishBundle"))
          .intercept(InvokerDelegation.create(
              finishBundle, BeforeDelegation.NOOP, finishBundleArgs));

      @SuppressWarnings("unchecked")
      Class<? extends DoFnInvoker<?, ?>> dynamicClass = (Class<? extends DoFnInvoker<?, ?>>) builder
          .make()
          .load(getClass().getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
          .getLoaded();
      try {
        return dynamicClass.getConstructor(clazz);
      } catch (IllegalArgumentException
          | NoSuchMethodException
          | SecurityException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public <InputT, OutputT> DoFnInvoker<InputT, OutputT> bindInvoker(
        DoFn<InputT, OutputT> fn) {
      try {
        @SuppressWarnings("unchecked")
        DoFnInvoker<InputT, OutputT> invoker =
            (DoFnInvoker<InputT, OutputT>) constructor.newInstance(fn);
        return invoker;
      } catch (InstantiationException
          | IllegalAccessException
          | IllegalArgumentException
          | InvocationTargetException
          | SecurityException e) {
        throw new RuntimeException("Unable to bind invoker for " + fn.getClass(), e);
      }
    }
  }

  private static class ContextAdapter<InputT, OutputT>
      extends DoFn<InputT, OutputT>.Context
      implements DoFn.ExtraContextFactory<InputT, OutputT> {

    private OldDoFn<InputT, OutputT>.Context context;

    private ContextAdapter(
        DoFn<InputT, OutputT> fn, OldDoFn<InputT, OutputT>.Context context) {
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
      // The DoFn doesn't allow us to ask for these outside ProcessElements, so this
      // should be unreachable.
      throw new UnsupportedOperationException("Can only get the window in ProcessElements");
    }

    @Override
    public WindowingInternals<InputT, OutputT> windowingInternals() {
      // The DoFn doesn't allow us to ask for these outside ProcessElements, so this
      // should be unreachable.
      throw new UnsupportedOperationException(
          "Can only get the windowingInternals in ProcessElements");
    }
  }

  private static class ProcessContextAdapter<InputT, OutputT>
      extends DoFn<InputT, OutputT>.ProcessContext
      implements DoFn.ExtraContextFactory<InputT, OutputT> {

    private OldDoFn<InputT, OutputT>.ProcessContext context;

    private ProcessContextAdapter(
        DoFn<InputT, OutputT> fn,
        OldDoFn<InputT, OutputT>.ProcessContext context) {
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

  public static Class<?> getDoFnClass(OldDoFn<?, ?> fn) {
    if (fn instanceof SimpleDoFnAdapter) {
      return ((SimpleDoFnAdapter<?, ?>) fn).fn.getClass();
    } else {
      return fn.getClass();
    }
  }

  private static class SimpleDoFnAdapter<InputT, OutputT> extends OldDoFn<InputT, OutputT> {

    private final DoFn<InputT, OutputT> fn;
    private transient DoFnInvoker<InputT, OutputT> invoker;

    private SimpleDoFnAdapter(DoFnReflector reflector, DoFn<InputT, OutputT> fn) {
      super(fn.aggregators);
      this.fn = fn;
      this.invoker = reflector.bindInvoker(fn);
    }

    @Override
    public void startBundle(OldDoFn<InputT, OutputT>.Context c) throws Exception {
      ContextAdapter<InputT, OutputT> adapter = new ContextAdapter<>(fn, c);
      invoker.invokeStartBundle(adapter, adapter);
    }

    @Override
    public void finishBundle(OldDoFn<InputT, OutputT>.Context c) throws Exception {
      ContextAdapter<InputT, OutputT> adapter = new ContextAdapter<>(fn, c);
      invoker.invokeFinishBundle(adapter, adapter);
    }

    @Override
    public void processElement(OldDoFn<InputT, OutputT>.ProcessContext c) throws Exception {
      ProcessContextAdapter<InputT, OutputT> adapter = new ProcessContextAdapter<>(fn, c);
      invoker.invokeProcessElement(adapter, adapter);
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
      invoker = DoFnReflector.of(fn.getClass()).bindInvoker(fn);
    }
  }

  private static class WindowDoFnAdapter<InputT, OutputT>
  extends SimpleDoFnAdapter<InputT, OutputT> implements OldDoFn.RequiresWindowAccess {

    private WindowDoFnAdapter(DoFnReflector reflector, DoFn<InputT, OutputT> fn) {
      super(reflector, fn);
    }
  }

  private static enum BeforeDelegation {
    NOOP {
      @Override
      StackManipulation manipulation(
          TypeDescription delegateType, MethodDescription instrumentedMethod, boolean finalStep) {
        Preconditions.checkArgument(!finalStep,
            "Shouldn't use NOOP delegation if there is nothing to do afterwards.");
        return StackManipulation.Trivial.INSTANCE;
      }
    },
    INVOKE_PREPARE_FOR_PROCESSING {
      private final Assigner assigner = Assigner.DEFAULT;

      @Override
      StackManipulation manipulation(
          TypeDescription delegateType, MethodDescription instrumentedMethod, boolean finalStep) {
        MethodDescription prepareMethod;
        try {
          prepareMethod = new MethodLocator.ForExplicitMethod(
              new MethodDescription.ForLoadedMethod(
                  DoFn.class.getDeclaredMethod("prepareForProcessing")))
          .resolve(instrumentedMethod);
        } catch (NoSuchMethodException | SecurityException e) {
          throw new RuntimeException("Unable to locate prepareForProcessing method", e);
        }

        if (finalStep) {
          return new StackManipulation.Compound(
              // Invoke the prepare method
              MethodInvoker.Simple.INSTANCE.invoke(prepareMethod),
              // Return from the invokeStartBundle when we're done.
              TerminationHandler.Returning.INSTANCE.resolve(
                  assigner, instrumentedMethod, prepareMethod));
        } else {
          return new StackManipulation.Compound(
              // Duplicate the delegation target so that it remains after we invoke prepare
              Duplication.duplicate(delegateType),
              // Invoke the prepare method
              MethodInvoker.Simple.INSTANCE.invoke(prepareMethod),
              // Drop the return value from prepareForProcessing
              TerminationHandler.Dropping.INSTANCE.resolve(
                  assigner, instrumentedMethod, prepareMethod));
        }
      }
    };

    /**
     * Stack manipulation to perform prior to the delegate call.
     *
     * <ul>
     * <li>Precondition: Stack has the delegate target on top of the stack
     * <li>Postcondition: If finalStep is true, then we've returned from the method. Otherwise, the
     * stack still has the delegate target on top of the stack.
     * </ul>
     *
     * @param delegateType The type of the delegate target, in case it needs to be duplicated.
     * @param instrumentedMethod The method bing instrumented. Necessary for resolving types and
     *     other information.
     * @param finalStep If true, return from the {@code invokeStartBundle} method after invoking
     * {@code prepareForProcessing} on the delegate.
     */
    abstract StackManipulation manipulation(
        TypeDescription delegateType, MethodDescription instrumentedMethod, boolean finalStep);
  }

  /**
   * A byte-buddy {@link Implementation} that delegates a call that receives
   * {@link AdditionalParameter} to the given {@link DoFn} method.
   */
  private static final class InvokerDelegation implements Implementation {
    @Nullable
    private final Method target;
    private final BeforeDelegation before;
    private final List<AdditionalParameter> args;
    private final Assigner assigner = Assigner.DEFAULT;
    private FieldDescription field;

    /**
     * Create the {@link InvokerDelegation} for the specified method.
     *
     * @param target the method to delegate to
     * @param isStartBundle whether or not this is the {@code startBundle} call
     * @param args the {@link AdditionalParameter} to be passed to the {@code target}
     */
    private InvokerDelegation(
        @Nullable Method target,
        BeforeDelegation before,
        List<AdditionalParameter> args) {
      this.target = target;
      this.before = before;
      this.args = args;
    }

    /**
     * Generate the {@link Implementation} of one of the life-cycle methods of a
     * {@link DoFn}.
     */
    private static Implementation create(
        @Nullable final Method target, BeforeDelegation before, List<AdditionalParameter> args) {
      if (target == null && before == BeforeDelegation.NOOP) {
        // There is no target to call and nothing needs to happen before. Just produce a stub.
        return StubMethod.INSTANCE;
      } else {
        // We need to generate a non-empty method implementation.
        return new InvokerDelegation(target, before, args);
      }
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      // Remember the field description of the instrumented type.
      field = instrumentedType.getDeclaredFields()
          .filter(ElementMatchers.named(FN_DELEGATE_FIELD_NAME)).getOnly();

      // Delegating the method call doesn't require any changes to the instrumented type.
      return instrumentedType;
    }

    /**
     * Stack manipulation to push the {@link DoFn} reference stored in the
     * delegate field of the invoker on to the top of the stack.
     *
     * <p>This implementation is derived from the code for
     * {@code MethodCall.invoke(m).onInstanceField(clazz, delegateField)} with two key differences.
     * First, it doesn't add a synthetic field each time, which is critical to avoid duplicate field
     * definitions. Second, it uses the {@link AdditionalParameter} to populate the arguments to the
     * method.
     */
    private StackManipulation pushDelegateField() {
      return new StackManipulation.Compound(
          // Push "this" reference to the stack
          MethodVariableAccess.REFERENCE.loadOffset(0),
          // Access the delegate field of the the invoker
          FieldAccess.forField(field).getter());
    }

    private StackManipulation pushArgument(
        AdditionalParameter arg, MethodDescription instrumentedMethod) {
      MethodDescription transform = arg.method;

      return new StackManipulation.Compound(
          // Push the ExtraContextFactory which must have been argument 2 of the instrumented method
          MethodVariableAccess.REFERENCE.loadOffset(2),
          // Invoke the appropriate method to produce the context argument
          MethodInvocation.invoke(transform));
    }

    private StackManipulation invokeTargetMethod(MethodDescription instrumentedMethod) {
      MethodDescription targetMethod = new MethodLocator.ForExplicitMethod(
          new MethodDescription.ForLoadedMethod(target)).resolve(instrumentedMethod);
      ParameterList<?> params = targetMethod.getParameters();

      // Instructions to setup the parameters for the call
      ArrayList<StackManipulation> parameters = new ArrayList<>(args.size() + 1);
      // 1. The first argument in the delegate method must be the context. This corresponds to
      //    the first argument in the instrumented method, so copy that.
      parameters.add(MethodVariableAccess.of(
          params.get(0).getType().getSuperClass()).loadOffset(1));
      // 2. For each of the extra arguments push the appropriate value.
      for (AdditionalParameter arg : args) {
        parameters.add(pushArgument(arg, instrumentedMethod));
      }

      return new StackManipulation.Compound(
          // Push the parameters
          new StackManipulation.Compound(parameters),
          // Invoke the target method
          wrapWithUserCodeException(MethodInvoker.Simple.INSTANCE.invoke(targetMethod)),
          // Return from the instrumented method
          TerminationHandler.Returning.INSTANCE.resolve(
              assigner, instrumentedMethod, targetMethod));
    }

    /**
     * Wrap a given stack manipulation in a try catch block. Any exceptions thrown within the
     * try are wrapped with a {@link UserCodeException}.
     */
    private StackManipulation wrapWithUserCodeException(
        final StackManipulation tryBody) {
      final MethodDescription createUserCodeException;
      try {
        createUserCodeException = new MethodDescription.ForLoadedMethod(
                UserCodeException.class.getDeclaredMethod("wrap", Throwable.class));
      } catch (NoSuchMethodException | SecurityException e) {
        throw new RuntimeException("Unable to find UserCodeException.wrap", e);
      }

      return new StackManipulation() {
        @Override
        public boolean isValid() {
          return tryBody.isValid();
        }

        @Override
        public Size apply(MethodVisitor mv, Context implementationContext) {
          Label tryBlockStart = new Label();
          Label tryBlockEnd = new Label();
          Label catchBlockStart = new Label();
          Label catchBlockEnd = new Label();

          String throwableName =
              new TypeDescription.ForLoadedType(Throwable.class).getInternalName();
          mv.visitTryCatchBlock(tryBlockStart, tryBlockEnd, catchBlockStart, throwableName);

          // The try block attempts to perform the expected operations, then jumps to success
          mv.visitLabel(tryBlockStart);
          Size trySize = tryBody.apply(mv, implementationContext);
          mv.visitJumpInsn(Opcodes.GOTO, catchBlockEnd);
          mv.visitLabel(tryBlockEnd);

          // The handler wraps the exception, and then throws.
          mv.visitLabel(catchBlockStart);
          // Add the exception to the frame
          mv.visitFrame(Opcodes.F_SAME1,
              // No local variables
              0, new Object[] {},
              // 1 stack element (the throwable)
              1, new Object[] { throwableName });

          Size catchSize = new StackManipulation.Compound(
              MethodInvocation.invoke(createUserCodeException),
              Throw.INSTANCE)
              .apply(mv, implementationContext);

          mv.visitLabel(catchBlockEnd);
          // The frame contents after the try/catch block is the same
          // as it was before.
          mv.visitFrame(Opcodes.F_SAME,
              // No local variables
              0, new Object[] {},
              // No new stack variables
              0, new Object[] {});

          return new Size(
              trySize.getSizeImpact(),
              Math.max(trySize.getMaximalSize(), catchSize.getMaximalSize()));
        }
      };
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return new ByteCodeAppender() {
        @Override
        public Size apply(
            MethodVisitor methodVisitor,
            Context implementationContext,
            MethodDescription instrumentedMethod) {
          StackManipulation.Size size = new StackManipulation.Compound(
              // Put the target on top of the stack
              pushDelegateField(),
              // Do any necessary pre-delegation work
              before.manipulation(field.getType().asErasure(), instrumentedMethod, target == null),
              // Invoke the target method, if there is one. If there wasn't, then isStartBundle was
              // true, and we've already emitted the appropriate return instructions.
              target != null
                  ? invokeTargetMethod(instrumentedMethod)
                  : StackManipulation.Trivial.INSTANCE)
              .apply(methodVisitor, implementationContext);
          return new Size(size.getMaximalSize(), instrumentedMethod.getStackSize());
        }
      };
    }
  }

  /**
   * A constructor {@link Implementation} for a {@link DoFnInvoker class}. Produces the byte code
   * for a constructor that takes a single argument and assigns it to the delegate field.
   * {@link AdditionalParameter} to the given {@link DoFn} method.
   */
  private static final class InvokerConstructor implements Implementation {
    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return new ByteCodeAppender() {
        @Override
        public Size apply(
            MethodVisitor methodVisitor,
            Context implementationContext,
            MethodDescription instrumentedMethod) {
          StackManipulation.Size size = new StackManipulation.Compound(
              // Load the this reference
              MethodVariableAccess.REFERENCE.loadOffset(0),
              // Invoke the super constructor (default constructor of Object)
              MethodInvocation
                  .invoke(new TypeDescription.ForLoadedType(Object.class)
                    .getDeclaredMethods()
                    .filter(ElementMatchers.isConstructor()
                      .and(ElementMatchers.takesArguments(0)))
                    .getOnly()),
              // Load the this reference
              MethodVariableAccess.REFERENCE.loadOffset(0),
              // Load the delegate argument
              MethodVariableAccess.REFERENCE.loadOffset(1),
              // Assign the delegate argument to the delegate field
              FieldAccess.forField(implementationTarget.getInstrumentedType()
                  .getDeclaredFields()
                  .filter(ElementMatchers.named(FN_DELEGATE_FIELD_NAME))
                  .getOnly()).putter(),
              // Return void.
              MethodReturn.VOID
            ).apply(methodVisitor, implementationContext);
            return new Size(size.getMaximalSize(), instrumentedMethod.getStackSize());
        }
      };
    }
  }
}
