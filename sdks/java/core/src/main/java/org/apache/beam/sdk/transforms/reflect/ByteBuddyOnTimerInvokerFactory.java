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

import com.google.common.base.CharMatcher;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.NamingStrategy;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.OnTimer;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.reflect.ByteBuddyDoFnInvokerFactory.DoFnMethodDelegation;

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
      Constructor<?> constructor = constructorCache.get(fnClass).get(timerId);
      @SuppressWarnings("unchecked")
      OnTimerInvoker<InputT, OutputT> invoker =
          (OnTimerInvoker<InputT, OutputT>) constructor.newInstance(fn);
      return invoker;
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
   * A cache of constructors of generated {@link OnTimerInvoker} classes, keyed by {@link DoFn}
   * class and then by {@link TimerId}.
   *
   * <p>Needed because generating an invoker class is expensive, and to avoid generating an
   * excessive number of classes consuming PermGen memory in Java's that still have PermGen.
   */
  private final LoadingCache<Class<? extends DoFn<?, ?>>, LoadingCache<String, Constructor<?>>>
      constructorCache =
          CacheBuilder.newBuilder()
              .build(
                  new CacheLoader<
                      Class<? extends DoFn<?, ?>>, LoadingCache<String, Constructor<?>>>() {
                    @Override
                    public LoadingCache<String, Constructor<?>> load(
                        final Class<? extends DoFn<?, ?>> fnClass) throws Exception {
                      return CacheBuilder.newBuilder().build(new OnTimerConstructorLoader(fnClass));
                    }
                  });

  /**
   * A cache loader fixed to a particular {@link DoFn} class that loads constructors for the
   * invokers for its {@link OnTimer @OnTimer} methods.
   */
  private static class OnTimerConstructorLoader extends CacheLoader<String, Constructor<?>> {

    private final DoFnSignature signature;

    public OnTimerConstructorLoader(Class<? extends DoFn<?, ?>> clazz) {
      this.signature = DoFnSignatures.getSignature(clazz);
    }

    @Override
    public Constructor<?> load(String timerId) throws Exception {
      Class<? extends OnTimerInvoker<?, ?>> invokerClass =
          generateOnTimerInvokerClass(signature, timerId);
      try {
        return invokerClass.getConstructor(signature.fnClass());
      } catch (IllegalArgumentException | NoSuchMethodException | SecurityException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Generates a {@link OnTimerInvoker} class for the given {@link DoFnSignature} and {@link
   * TimerId}.
   */
  private static Class<? extends OnTimerInvoker<?, ?>> generateOnTimerInvokerClass(
      DoFnSignature signature, String timerId) {
    Class<? extends DoFn<?, ?>> fnClass = signature.fnClass();

    final TypeDescription clazzDescription = new TypeDescription.ForLoadedType(fnClass);

    final String className =
        "auxiliary_OnTimer_" + CharMatcher.JAVA_LETTER_OR_DIGIT.retainFrom(timerId);

    DynamicType.Builder<?> builder =
        new ByteBuddy()
            // Create subclasses inside the target class, to have access to
            // private and package-private bits
            .with(
                new NamingStrategy.SuffixingRandom(className) {
                  @Override
                  public String subclass(TypeDescription.Generic superClass) {
                    return super.name(clazzDescription);
                  }
                })
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
                    ByteBuddyOnTimerInvokerFactory.class.getClassLoader(),
                    ClassLoadingStrategy.Default.INJECTION)
                .getLoaded();
    return res;
  }

  /**
   * An "invokeOnTimer" method implementation akin to @ProcessElement, but simpler because no
   * splitting-related parameters need to be handled.
   */
  private static class InvokeOnTimerDelegation extends DoFnMethodDelegation {

    private final DoFnSignature.OnTimerMethod signature;

    public InvokeOnTimerDelegation(
        TypeDescription clazzDescription, DoFnSignature.OnTimerMethod signature) {
      super(clazzDescription, signature.targetMethod());
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

    @Override
    protected StackManipulation beforeDelegation(MethodDescription instrumentedMethod) {
      // Parameters of the wrapper invoker method:
      //   DoFn.ArgumentProvider
      // Parameters of the wrapped DoFn method:
      //   a dynamic set of allowed "extra" parameters in any order subject to
      //   validation prior to getting the DoFnSignature
      ArrayList<StackManipulation> parameters = new ArrayList<>();

      // To load the delegate, push `this` and then access the field
      StackManipulation pushDelegate =
          new StackManipulation.Compound(
              MethodVariableAccess.REFERENCE.loadFrom(0),
              FieldAccess.forField(delegateField).read());

      StackManipulation pushExtraContextFactory = MethodVariableAccess.REFERENCE.loadFrom(1);

      // Push the extra arguments in their actual order.
      for (DoFnSignature.Parameter param : signature.extraParameters()) {
        parameters.add(
            new StackManipulation.Compound(
                pushExtraContextFactory,
                ByteBuddyDoFnInvokerFactory.getExtraContextParameter(param, pushDelegate)));
      }
      return new StackManipulation.Compound(parameters);
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
      return new ByteCodeAppender() {
        @Override
        public Size apply(
            MethodVisitor methodVisitor,
            Context implementationContext,
            MethodDescription instrumentedMethod) {
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
          return new Size(size.getMaximalSize(), instrumentedMethod.getStackSize());
        }
      };
    }
  }
}
