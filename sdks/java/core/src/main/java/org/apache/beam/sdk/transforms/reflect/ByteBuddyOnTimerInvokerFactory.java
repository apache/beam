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

import static org.apache.beam.sdk.util.common.ReflectHelpers.findClassLoader;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OnTimer;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.reflect.ByteBuddyDoFnInvokerFactory.DoFnMethodWithExtraParametersDelegation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.ByteBuddy;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.modifier.FieldManifestation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.modifier.Visibility;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.type.TypeDescription;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.dynamic.DynamicType;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.dynamic.scaffold.InstrumentedType;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.Implementation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.StackManipulation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.FieldAccess;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.MethodReturn;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CharMatcher;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.BaseEncoding;

/**
 * Dynamically generates {@link OnTimerInvoker} instances for invoking a particular {@link TimerId}
 * on a particular {@link DoFn}.
 */
class ByteBuddyOnTimerInvokerFactory implements OnTimerInvokerFactory {

  @Override
  public <InputT, OutputT> OnTimerInvoker<InputT, OutputT> forTimer(
      DoFn<InputT, OutputT> fn, String timerId) {

    @SuppressWarnings("unchecked")
    Class<? extends DoFn<?, ?>> fnClass = (Class<? extends DoFn<?, ?>>) fn.getClass();
    try {
      OnTimerMethodSpecifier onTimerMethodSpecifier =
          OnTimerMethodSpecifier.forClassAndTimerId(fnClass, timerId);
      Constructor<?> constructor = constructorCache.get(onTimerMethodSpecifier);

      return (OnTimerInvoker<InputT, OutputT>) constructor.newInstance(fn);
    } catch (InstantiationException
        | IllegalAccessException
        | IllegalArgumentException
        | InvocationTargetException
        | SecurityException
        | ExecutionException e) {
      throw new RuntimeException(
          String.format(
              "Unable to construct @%s invoker for %s",
              OnTimer.class.getSimpleName(), fn.getClass().getName()),
          e);
    }
  }

  public <InputT, OutputT> OnTimerInvoker<InputT, OutputT> forTimerFamily(
      DoFn<InputT, OutputT> fn, String timerId) {

    @SuppressWarnings("unchecked")
    Class<? extends DoFn<?, ?>> fnClass = (Class<? extends DoFn<?, ?>>) fn.getClass();
    try {
      OnTimerMethodSpecifier onTimerMethodSpecifier =
          OnTimerMethodSpecifier.forClassAndTimerId(fnClass, timerId);
      Constructor<?> constructor = constructorTimerFamilyCache.get(onTimerMethodSpecifier);

      return (OnTimerInvoker<InputT, OutputT>) constructor.newInstance(fn);
    } catch (InstantiationException
        | IllegalAccessException
        | IllegalArgumentException
        | InvocationTargetException
        | SecurityException
        | ExecutionException e) {
      throw new RuntimeException(
          String.format(
              "Unable to construct @%s invoker for %s",
              DoFn.OnTimerFamily.class.getSimpleName(), fn.getClass().getName()),
          e);
    }
  }

  public static ByteBuddyOnTimerInvokerFactory only() {
    return INSTANCE;
  }

  private static final ByteBuddyOnTimerInvokerFactory INSTANCE =
      new ByteBuddyOnTimerInvokerFactory();

  private ByteBuddyOnTimerInvokerFactory() {}

  /**
   * The field name for the delegate of {@link DoFn} subclass that a bytebuddy invoker will call.
   */
  private static final String FN_DELEGATE_FIELD_NAME = "delegate";

  /**
   * A cache of constructors of generated {@link OnTimerInvoker} classes, keyed by {@link
   * OnTimerMethodSpecifier}.
   *
   * <p>Needed because generating an invoker class is expensive, and to avoid generating an
   * excessive number of classes consuming PermGen memory in Java's that still have PermGen.
   */
  private final LoadingCache<OnTimerMethodSpecifier, Constructor<?>> constructorCache =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<OnTimerMethodSpecifier, Constructor<?>>() {
                @Override
                public Constructor<?> load(final OnTimerMethodSpecifier onTimerMethodSpecifier)
                    throws Exception {
                  DoFnSignature signature =
                      DoFnSignatures.getSignature(onTimerMethodSpecifier.fnClass());
                  Class<? extends OnTimerInvoker<?, ?>> invokerClass =
                      generateOnTimerInvokerClass(signature, onTimerMethodSpecifier.timerId());
                  try {
                    return invokerClass.getConstructor(signature.fnClass());
                  } catch (IllegalArgumentException | NoSuchMethodException | SecurityException e) {
                    throw new RuntimeException(e);
                  }
                }
              });

  /**
   * A cache of constructors of generated {@link OnTimerInvoker} classes, keyed by {@link
   * OnTimerMethodSpecifier}.
   *
   * <p>Needed because generating an invoker class is expensive, and to avoid generating an
   * excessive number of classes consuming PermGen memory in Java's that still have PermGen.
   */
  private final LoadingCache<OnTimerMethodSpecifier, Constructor<?>> constructorTimerFamilyCache =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<OnTimerMethodSpecifier, Constructor<?>>() {
                @Override
                public Constructor<?> load(final OnTimerMethodSpecifier onTimerMethodSpecifier)
                    throws Exception {
                  DoFnSignature signature =
                      DoFnSignatures.getSignature(onTimerMethodSpecifier.fnClass());
                  Class<? extends OnTimerInvoker<?, ?>> invokerClass =
                      generateOnTimerFamilyInvokerClass(
                          signature, onTimerMethodSpecifier.timerId());
                  try {
                    return invokerClass.getConstructor(signature.fnClass());
                  } catch (IllegalArgumentException | NoSuchMethodException | SecurityException e) {
                    throw new RuntimeException(e);
                  }
                }
              });

  /**
   * Generates a {@link OnTimerInvoker} class for the given {@link DoFnSignature} and {@link
   * TimerId}.
   */
  private static Class<? extends OnTimerInvoker<?, ?>> generateOnTimerInvokerClass(
      DoFnSignature signature, String timerId) {
    Class<? extends DoFn<?, ?>> fnClass = signature.fnClass();

    final TypeDescription clazzDescription = new TypeDescription.ForLoadedType(fnClass);

    final String suffix =
        String.format(
            "%s$%s$%s",
            OnTimerInvoker.class.getSimpleName(),
            CharMatcher.javaLetterOrDigit().retainFrom(timerId),
            BaseEncoding.base64().omitPadding().encode(timerId.getBytes(Charsets.UTF_8)));

    DynamicType.Builder<?> builder =
        new ByteBuddy()
            // Create subclasses inside the target class, to have access to
            // private and package-private bits
            .with(StableInvokerNamingStrategy.forDoFnClass(fnClass).withSuffix(suffix))

            // class <invoker class> implements OnTimerInvoker {
            .subclass(OnTimerInvoker.class, ConstructorStrategy.Default.NO_CONSTRUCTORS)

            //   private final <fn class> delegate;
            .defineField(
                FN_DELEGATE_FIELD_NAME, fnClass, Visibility.PRIVATE, FieldManifestation.FINAL)

            //   <invoker class>(<fn class> delegate) { this.delegate = delegate; }
            .defineConstructor(Visibility.PUBLIC)
            .withParameter(fnClass)
            .intercept(new InvokerConstructor())

            //   public invokeOnTimer(DoFn.ArgumentProvider) {
            //     this.delegate.<@OnTimer method>(... pass the right args ...)
            //   }
            .method(ElementMatchers.named("invokeOnTimer"))
            .intercept(
                new InvokeOnTimerDelegation(
                    clazzDescription, signature.onTimerMethods().get(timerId)));

    DynamicType.Unloaded<?> unloaded = builder.make();

    @SuppressWarnings("unchecked")
    Class<? extends OnTimerInvoker<?, ?>> res =
        (Class<? extends OnTimerInvoker<?, ?>>)
            unloaded
                .load(
                    findClassLoader(fnClass.getClassLoader()),
                    ClassLoadingStrategy.Default.INJECTION)
                .getLoaded();
    return res;
  }

  private static Class<? extends OnTimerInvoker<?, ?>> generateOnTimerFamilyInvokerClass(
      DoFnSignature signature, String timerId) {
    Class<? extends DoFn<?, ?>> fnClass = signature.fnClass();

    final TypeDescription clazzDescription = new TypeDescription.ForLoadedType(fnClass);

    final String suffix =
        String.format(
            "%s$%s$%s",
            OnTimerInvoker.class.getSimpleName(),
            CharMatcher.javaLetterOrDigit().retainFrom(timerId),
            BaseEncoding.base64().omitPadding().encode(timerId.getBytes(Charsets.UTF_8)));

    DynamicType.Builder<?> builder =
        new ByteBuddy()
            // Create subclasses inside the target class, to have access to
            // private and package-private bits
            .with(StableInvokerNamingStrategy.forDoFnClass(fnClass).withSuffix(suffix))

            // class <invoker class> implements OnTimerInvoker {
            .subclass(OnTimerInvoker.class, ConstructorStrategy.Default.NO_CONSTRUCTORS)

            //   private final <fn class> delegate;
            .defineField(
                FN_DELEGATE_FIELD_NAME, fnClass, Visibility.PRIVATE, FieldManifestation.FINAL)

            //   <invoker class>(<fn class> delegate) { this.delegate = delegate; }
            .defineConstructor(Visibility.PUBLIC)
            .withParameter(fnClass)
            .intercept(new InvokerConstructor())

            //   public invokeOnTimer(DoFn.ArgumentProvider) {
            //     this.delegate.<@OnTimer method>(... pass the right args ...)
            //   }
            .method(ElementMatchers.named("invokeOnTimer"))
            .intercept(
                new InvokeOnTimerFamilyDelegation(
                    clazzDescription, signature.onTimerFamilyMethods().get(timerId)));

    DynamicType.Unloaded<?> unloaded = builder.make();

    @SuppressWarnings("unchecked")
    Class<? extends OnTimerInvoker<?, ?>> res =
        (Class<? extends OnTimerInvoker<?, ?>>)
            unloaded
                .load(
                    findClassLoader(fnClass.getClassLoader()),
                    ClassLoadingStrategy.Default.INJECTION)
                .getLoaded();
    return res;
  }

  /**
   * An "invokeOnTimer" method implementation akin to @ProcessElement, but simpler because no
   * splitting-related parameters need to be handled.
   */
  private static class InvokeOnTimerFamilyDelegation
      extends DoFnMethodWithExtraParametersDelegation {

    private final DoFnSignature.OnTimerFamilyMethod signature;

    public InvokeOnTimerFamilyDelegation(
        TypeDescription clazzDescription, DoFnSignature.OnTimerFamilyMethod signature) {
      super(clazzDescription, signature);
      this.signature = signature;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      // Remember the field description of the instrumented type.
      // Kind of a hack to set the protected value, because the instrumentedType
      // is only available to prepare, while we need this information in
      // beforeDelegation
      delegateField =
          instrumentedType
              .getDeclaredFields() // the delegate is declared on the OnTimerInvoker
              .filter(ElementMatchers.named(FN_DELEGATE_FIELD_NAME))
              .getOnly();
      // Delegating the method call doesn't require any changes to the instrumented type.
      return instrumentedType;
    }
  }

  /**
   * An "invokeOnTimer" method implementation akin to @ProcessElement, but simpler because no
   * splitting-related parameters need to be handled.
   */
  private static class InvokeOnTimerDelegation extends DoFnMethodWithExtraParametersDelegation {

    private final DoFnSignature.OnTimerMethod signature;

    public InvokeOnTimerDelegation(
        TypeDescription clazzDescription, DoFnSignature.OnTimerMethod signature) {
      super(clazzDescription, signature);
      this.signature = signature;
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      // Remember the field description of the instrumented type.
      // Kind of a hack to set the protected value, because the instrumentedType
      // is only available to prepare, while we need this information in
      // beforeDelegation
      delegateField =
          instrumentedType
              .getDeclaredFields() // the delegate is declared on the OnTimerInvoker
              .filter(ElementMatchers.named(FN_DELEGATE_FIELD_NAME))
              .getOnly();
      // Delegating the method call doesn't require any changes to the instrumented type.
      return instrumentedType;
    }
  }

  /**
   * A constructor {@link Implementation} for a {@link DoFnInvoker class}. Produces the byte code
   * for a constructor that takes a single argument and assigns it to the delegate field.
   */
  private static final class InvokerConstructor implements Implementation {
    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return (methodVisitor, implementationContext, instrumentedMethod) -> {
        StackManipulation.Size size =
            new StackManipulation.Compound(
                    // Load the this reference
                    MethodVariableAccess.REFERENCE.loadFrom(0),
                    // Invoke the super constructor (default constructor of Object)
                    MethodInvocation.invoke(
                        new TypeDescription.ForLoadedType(Object.class)
                            .getDeclaredMethods()
                            .filter(
                                ElementMatchers.isConstructor()
                                    .and(ElementMatchers.takesArguments(0)))
                            .getOnly()),
                    // Load the this reference
                    MethodVariableAccess.REFERENCE.loadFrom(0),
                    // Load the delegate argument
                    MethodVariableAccess.REFERENCE.loadFrom(1),
                    // Assign the delegate argument to the delegate field
                    FieldAccess.forField(
                            implementationTarget
                                .getInstrumentedType()
                                .getDeclaredFields()
                                .filter(ElementMatchers.named(FN_DELEGATE_FIELD_NAME))
                                .getOnly())
                        .write(),
                    // Return void.
                    MethodReturn.VOID)
                .apply(methodVisitor, implementationContext);
        return new ByteCodeAppender.Size(size.getMaximalSize(), instrumentedMethod.getStackSize());
      };
    }
  }
}
