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
import static org.apache.beam.sdk.util.common.ReflectHelpers.findClassLoader;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.field.FieldDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeList;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import net.bytebuddy.implementation.ExceptionMethod;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.Implementation.Context;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.Throw;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import net.bytebuddy.implementation.bytecode.assign.Assigner.Typing;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.constant.TextConstant;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.OnTimerMethod;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.Cases;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.ElementParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.FinishBundleContextParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.OnTimerContextParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.ProcessContextParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.RestrictionTrackerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.StartBundleContextParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.StateParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.TimerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.TimestampParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.WindowParameter;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Dynamically generates a {@link DoFnInvoker} instances for invoking a {@link DoFn}. */
public class ByteBuddyDoFnInvokerFactory implements DoFnInvokerFactory {

  public static final String CONTEXT_PARAMETER_METHOD = "context";
  public static final String START_BUNDLE_CONTEXT_PARAMETER_METHOD = "startBundleContext";
  public static final String FINISH_BUNDLE_CONTEXT_PARAMETER_METHOD = "finishBundleContext";
  public static final String PROCESS_CONTEXT_PARAMETER_METHOD = "processContext";
  public static final String ELEMENT_PARAMETER_METHOD = "element";
  public static final String TIMESTAMP_PARAMETER_METHOD = "timestamp";
  public static final String ON_TIMER_CONTEXT_PARAMETER_METHOD = "onTimerContext";
  public static final String WINDOW_PARAMETER_METHOD = "window";
  public static final String PIPELINE_OPTIONS_PARAMETER_METHOD = "pipelineOptions";
  public static final String RESTRICTION_TRACKER_PARAMETER_METHOD = "restrictionTracker";
  public static final String STATE_PARAMETER_METHOD = "state";
  public static final String TIMER_PARAMETER_METHOD = "timer";

  /**
   * Returns a {@link ByteBuddyDoFnInvokerFactory} shared with all other invocations, so that its
   * cache of generated classes is global.
   */
  public static ByteBuddyDoFnInvokerFactory only() {
    return INSTANCE;
  }

  /**
   * Creates a {@link DoFnInvoker} for the given {@link DoFn} by generating bytecode that directly
   * invokes its methods with arguments extracted from the {@link DoFnInvoker.ArgumentProvider}.
   */
  @Override
  public <InputT, OutputT> DoFnInvoker<InputT, OutputT> invokerFor(DoFn<InputT, OutputT> fn) {
    return newByteBuddyInvoker(fn);
  }

  private static final ByteBuddyDoFnInvokerFactory INSTANCE = new ByteBuddyDoFnInvokerFactory();

  private static final String FN_DELEGATE_FIELD_NAME = "delegate";

  /**
   * A cache of constructors of generated {@link DoFnInvoker} classes, keyed by {@link DoFn} class.
   * Needed because generating an invoker class is expensive, and to avoid generating an excessive
   * number of classes consuming PermGen memory.
   */
  private final Map<Class<?>, Constructor<?>> byteBuddyInvokerConstructorCache =
      new LinkedHashMap<>();

  private ByteBuddyDoFnInvokerFactory() {}

  /** @return the {@link DoFnInvoker} for the given {@link DoFn}. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <InputT, OutputT> DoFnInvoker<InputT, OutputT> newByteBuddyInvoker(
      DoFn<InputT, OutputT> fn) {
    return newByteBuddyInvoker(DoFnSignatures.getSignature((Class) fn.getClass()), fn);
  }

  /**
   * Internal base class for generated {@link DoFnInvoker} instances.
   *
   * <p>This class should <i>not</i> be extended directly, or by Beam users. It must be public for
   * generated instances to have adequate access, as they are generated "inside" the invoked {@link
   * DoFn} class.
   */
  public abstract static class DoFnInvokerBase<InputT, OutputT, DoFnT extends DoFn<InputT, OutputT>>
      implements DoFnInvoker<InputT, OutputT> {
    protected DoFnT delegate;

    private Map<String, OnTimerInvoker> onTimerInvokers = new HashMap<>();

    public DoFnInvokerBase(DoFnT delegate) {
      this.delegate = delegate;
    }

    /**
     * Associates the given timer ID with the given {@link OnTimerInvoker}.
     *
     * <p>ByteBuddy does not like to generate conditional code, so we use a map + lookup of the
     * timer ID rather than a generated conditional branch to choose which OnTimerInvoker to invoke.
     *
     * <p>This method has package level access as it is intended only for assembly of the {@link
     * DoFnInvokerBase} not by any subclass.
     */
    void addOnTimerInvoker(String timerId, OnTimerInvoker onTimerInvoker) {
      this.onTimerInvokers.put(timerId, onTimerInvoker);
    }

    @Override
    public void invokeOnTimer(
        String timerId, DoFnInvoker.ArgumentProvider<InputT, OutputT> arguments) {
      @Nullable OnTimerInvoker onTimerInvoker = onTimerInvokers.get(timerId);

      if (onTimerInvoker != null) {
        onTimerInvoker.invokeOnTimer(arguments);
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Attempted to invoke timer %s on %s, but that timer is not registered."
                    + " This is the responsibility of the runner, which must only deliver"
                    + " registered timers.",
                timerId, delegate.getClass().getName()));
      }
    }

    @Override
    public DoFn<InputT, OutputT> getFn() {
      return delegate;
    }
  }

  /** @return the {@link DoFnInvoker} for the given {@link DoFn}. */
  public <InputT, OutputT> DoFnInvoker<InputT, OutputT> newByteBuddyInvoker(
      DoFnSignature signature, DoFn<InputT, OutputT> fn) {
    checkArgument(
        signature.fnClass().equals(fn.getClass()),
        "Signature is for class %s, but fn is of class %s",
        signature.fnClass(),
        fn.getClass());

    try {
      @SuppressWarnings("unchecked")
      DoFnInvokerBase<InputT, OutputT, DoFn<InputT, OutputT>> invoker =
          (DoFnInvokerBase<InputT, OutputT, DoFn<InputT, OutputT>>)
              getByteBuddyInvokerConstructor(signature).newInstance(fn);

      for (OnTimerMethod onTimerMethod : signature.onTimerMethods().values()) {
        invoker.addOnTimerInvoker(
            onTimerMethod.id(), OnTimerInvokers.forTimer(fn, onTimerMethod.id()));
      }

      return invoker;
    } catch (InstantiationException
        | IllegalAccessException
        | IllegalArgumentException
        | InvocationTargetException
        | SecurityException e) {
      throw new RuntimeException("Unable to bind invoker for " + fn.getClass(), e);
    }
  }

  /**
   * Returns a generated constructor for a {@link DoFnInvoker} for the given {@link DoFn} class.
   *
   * <p>These are cached such that at most one {@link DoFnInvoker} class exists for a given {@link
   * DoFn} class.
   */
  private synchronized Constructor<?> getByteBuddyInvokerConstructor(DoFnSignature signature) {
    Class<? extends DoFn<?, ?>> fnClass = signature.fnClass();
    Constructor<?> constructor = byteBuddyInvokerConstructorCache.get(fnClass);
    if (constructor == null) {
      Class<? extends DoFnInvoker<?, ?>> invokerClass = generateInvokerClass(signature);
      try {
        constructor = invokerClass.getConstructor(fnClass);
      } catch (IllegalArgumentException | NoSuchMethodException | SecurityException e) {
        throw new RuntimeException(e);
      }
      byteBuddyInvokerConstructorCache.put(fnClass, constructor);
    }
    return constructor;
  }

  /** Default implementation of {@link DoFn.SplitRestriction}, for delegation by bytebuddy. */
  public static class DefaultSplitRestriction {
    /** Doesn't split the restriction. */
    @SuppressWarnings("unused")
    public static <InputT, RestrictionT> void invokeSplitRestriction(
        InputT element, RestrictionT restriction, DoFn.OutputReceiver<RestrictionT> receiver) {
      receiver.output(restriction);
    }
  }

  /** Default implementation of {@link DoFn.GetRestrictionCoder}, for delegation by bytebuddy. */
  public static class DefaultRestrictionCoder {
    private final TypeDescriptor<?> restrictionType;

    DefaultRestrictionCoder(TypeDescriptor<?> restrictionType) {
      this.restrictionType = restrictionType;
    }

    /** Doesn't split the restriction. */
    @SuppressWarnings({"unused", "unchecked"})
    public <RestrictionT> Coder<RestrictionT> invokeGetRestrictionCoder(CoderRegistry registry)
        throws CannotProvideCoderException {
      return (Coder) registry.getCoder(restrictionType);
    }
  }

  /** Default implementation of {@link DoFn.NewTracker}, for delegation by bytebuddy. */
  public static class DefaultNewTracker {
    /** Uses {@link HasDefaultTracker} to produce the tracker. */
    @SuppressWarnings("unused")
    public static RestrictionTracker invokeNewTracker(Object restriction) {
      return ((HasDefaultTracker) restriction).newTracker();
    }
  }

  /** Generates a {@link DoFnInvoker} class for the given {@link DoFnSignature}. */
  private static Class<? extends DoFnInvoker<?, ?>> generateInvokerClass(DoFnSignature signature) {
    Class<? extends DoFn<?, ?>> fnClass = signature.fnClass();

    final TypeDescription clazzDescription = new TypeDescription.ForLoadedType(fnClass);

    DynamicType.Builder<?> builder =
        new ByteBuddy()
            // Create subclasses inside the target class, to have access to
            // private and package-private bits
            .with(
                StableInvokerNamingStrategy.forDoFnClass(fnClass)
                    .withSuffix(DoFnInvoker.class.getSimpleName()))

            // class <invoker class> extends DoFnInvokerBase {
            .subclass(DoFnInvokerBase.class, ConstructorStrategy.Default.NO_CONSTRUCTORS)

            //   public <invoker class>(<fn class> delegate) { this.delegate = delegate; }
            .defineConstructor(Visibility.PUBLIC)
            .withParameter(fnClass)
            .intercept(new InvokerConstructor())

            //   public invokeProcessElement(ProcessContext, ExtraContextFactory) {
            //     delegate.<@ProcessElement>(... pass just the right args ...);
            //   }
            .method(ElementMatchers.named("invokeProcessElement"))
            .intercept(new ProcessElementDelegation(clazzDescription, signature.processElement()))

            //   public invokeStartBundle(Context c) { delegate.<@StartBundle>(c); }
            //   ... etc ...
            .method(ElementMatchers.named("invokeStartBundle"))
            .intercept(delegateOrNoop(clazzDescription, signature.startBundle()))
            .method(ElementMatchers.named("invokeFinishBundle"))
            .intercept(delegateOrNoop(clazzDescription, signature.finishBundle()))
            .method(ElementMatchers.named("invokeSetup"))
            .intercept(delegateOrNoop(clazzDescription, signature.setup()))
            .method(ElementMatchers.named("invokeTeardown"))
            .intercept(delegateOrNoop(clazzDescription, signature.teardown()))
            .method(ElementMatchers.named("invokeGetInitialRestriction"))
            .intercept(
                delegateWithDowncastOrThrow(clazzDescription, signature.getInitialRestriction()))
            .method(ElementMatchers.named("invokeSplitRestriction"))
            .intercept(splitRestrictionDelegation(clazzDescription, signature))
            .method(ElementMatchers.named("invokeGetRestrictionCoder"))
            .intercept(getRestrictionCoderDelegation(clazzDescription, signature))
            .method(ElementMatchers.named("invokeNewTracker"))
            .intercept(newTrackerDelegation(clazzDescription, signature.newTracker()));

    DynamicType.Unloaded<?> unloaded = builder.make();

    @SuppressWarnings("unchecked")
    Class<? extends DoFnInvoker<?, ?>> res =
        (Class<? extends DoFnInvoker<?, ?>>)
            unloaded
                .load(
                    findClassLoader(fnClass.getClassLoader()),
                    ClassLoadingStrategy.Default.INJECTION)
                .getLoaded();
    return res;
  }

  private static Implementation getRestrictionCoderDelegation(
      TypeDescription doFnType, DoFnSignature signature) {
    if (signature.processElement().isSplittable()) {
      if (signature.getRestrictionCoder() == null) {
        return MethodDelegation.to(
            new DefaultRestrictionCoder(signature.getInitialRestriction().restrictionT()));
      } else {
        return new DowncastingParametersMethodDelegation(
            doFnType, signature.getRestrictionCoder().targetMethod());
      }
    } else {
      return ExceptionMethod.throwing(UnsupportedOperationException.class);
    }
  }

  private static Implementation splitRestrictionDelegation(
      TypeDescription doFnType, DoFnSignature signature) {
    if (signature.splitRestriction() == null) {
      return MethodDelegation.to(DefaultSplitRestriction.class);
    } else {
      return new DowncastingParametersMethodDelegation(
          doFnType, signature.splitRestriction().targetMethod());
    }
  }

  private static Implementation newTrackerDelegation(
      TypeDescription doFnType, @Nullable DoFnSignature.NewTrackerMethod signature) {
    if (signature == null) {
      // We must have already verified that in this case the restriction type
      // is a subtype of HasDefaultTracker.
      return MethodDelegation.to(DefaultNewTracker.class);
    } else {
      return delegateWithDowncastOrThrow(doFnType, signature);
    }
  }

  /** Delegates to the given method if available, or does nothing. */
  private static Implementation delegateOrNoop(
      TypeDescription doFnType, DoFnSignature.DoFnMethod method) {
    return (method == null)
        ? FixedValue.originType()
        : new DoFnMethodDelegation(doFnType, method.targetMethod());
  }

  /** Delegates to the given method if available, or throws UnsupportedOperationException. */
  private static Implementation delegateWithDowncastOrThrow(
      TypeDescription doFnType, DoFnSignature.DoFnMethod method) {
    return (method == null)
        ? ExceptionMethod.throwing(UnsupportedOperationException.class)
        : new DowncastingParametersMethodDelegation(doFnType, method.targetMethod());
  }

  /**
   * Implements a method of {@link DoFnInvoker} (the "instrumented method") by delegating to a
   * "target method" of the wrapped {@link DoFn}.
   */
  static class DoFnMethodDelegation implements Implementation {
    /** The {@link MethodDescription} of the wrapped {@link DoFn}'s method. */
    protected final MethodDescription targetMethod;
    /** Whether the target method returns non-void. */
    private final boolean targetHasReturn;

    /** Starts {@code null}, initialized by {@link #prepare(InstrumentedType)}. */
    @Nullable protected FieldDescription delegateField;

    private final TypeDescription doFnType;

    public DoFnMethodDelegation(TypeDescription doFnType, Method targetMethod) {
      this.doFnType = doFnType;
      this.targetMethod = new MethodDescription.ForLoadedMethod(targetMethod);
      targetHasReturn = !TypeDescription.VOID.equals(this.targetMethod.getReturnType().asErasure());
    }

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      // Remember the field description of the instrumented type.
      delegateField =
          instrumentedType
              .getSuperClass() // always DoFnInvokerBase
              .getDeclaredFields()
              .filter(ElementMatchers.named(FN_DELEGATE_FIELD_NAME))
              .getOnly();
      // Delegating the method call doesn't require any changes to the instrumented type.
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return new ByteCodeAppender() {
        /**
         * @param instrumentedMethod The {@link DoFnInvoker} method for which we're generating code.
         */
        @Override
        public Size apply(
            MethodVisitor methodVisitor,
            Context implementationContext,
            MethodDescription instrumentedMethod) {
          // Figure out how many locals we'll need. This corresponds to "this", the parameters
          // of the instrumented method, and an argument to hold the return value if the target
          // method has a return value.
          int numLocals = 1 + instrumentedMethod.getParameters().size() + (targetHasReturn ? 1 : 0);

          Integer returnVarIndex = null;
          if (targetHasReturn) {
            // Local comes after formal parameters, so figure out where that is.
            returnVarIndex = 1; // "this"
            for (Type param : Type.getArgumentTypes(instrumentedMethod.getDescriptor())) {
              returnVarIndex += param.getSize();
            }
          }

          StackManipulation manipulation =
              new StackManipulation.Compound(
                  // Push "this" (DoFnInvoker on top of the stack)
                  MethodVariableAccess.REFERENCE.loadFrom(0),
                  // Access this.delegate (DoFn on top of the stack)
                  FieldAccess.forField(delegateField).read(),
                  // Cast it to the more precise type
                  TypeCasting.to(doFnType),
                  // Run the beforeDelegation manipulations.
                  // The arguments necessary to invoke the target are on top of the stack.
                  beforeDelegation(instrumentedMethod),
                  // Perform the method delegation.
                  // This will consume the arguments on top of the stack
                  // Either the stack is now empty (because the targetMethod returns void) or the
                  // stack contains the return value.
                  new UserCodeMethodInvocation(returnVarIndex, targetMethod, instrumentedMethod),
                  // Run the afterDelegation manipulations.
                  // Either the stack is now empty (because the instrumentedMethod returns void)
                  // or the stack contains the return value.
                  afterDelegation(instrumentedMethod));

          StackManipulation.Size size = manipulation.apply(methodVisitor, implementationContext);
          return new Size(size.getMaximalSize(), numLocals);
        }
      };
    }

    /**
     * Return the code to the prepare the operand stack for the method delegation.
     *
     * <p>Before this method is called, the stack delegate will be the only thing on the stack.
     *
     * <p>After this method is called, the stack contents should contain exactly the arguments
     * necessary to invoke the target method.
     */
    protected StackManipulation beforeDelegation(MethodDescription instrumentedMethod) {
      return MethodVariableAccess.allArgumentsOf(targetMethod);
    }

    /**
     * Return the code to execute after the method delegation.
     *
     * <p>Before this method is called, the stack will either be empty (if the target method returns
     * void) or contain the method return value.
     *
     * <p>After this method is called, the stack should either be empty (if the instrumented method
     * returns void) or contain the value for the instrumented method to return).
     */
    protected StackManipulation afterDelegation(MethodDescription instrumentedMethod) {
      return new StackManipulation.Compound(
          Assigner.DEFAULT.assign(
              targetMethod.getReturnType(),
              instrumentedMethod.getReturnType(), Typing.STATIC),
          MethodReturn.of(instrumentedMethod.getReturnType()));
    }
  }

  /**
   * Passes parameters to the delegated method by downcasting each parameter of non-primitive type
   * to its expected type.
   */
  private static class DowncastingParametersMethodDelegation extends DoFnMethodDelegation {
    DowncastingParametersMethodDelegation(TypeDescription doFnType, Method method) {
      super(doFnType, method);
    }

    @Override
    protected StackManipulation beforeDelegation(MethodDescription instrumentedMethod) {
      List<StackManipulation> pushParameters = new ArrayList<>();
      TypeList.Generic paramTypes = targetMethod.getParameters().asTypeList();
      for (int i = 0; i < paramTypes.size(); i++) {
        TypeDescription.Generic paramT = paramTypes.get(i);
        pushParameters.add(MethodVariableAccess.of(paramT).loadFrom(i + 1));
        if (!paramT.isPrimitive()) {
          pushParameters.add(TypeCasting.to(paramT));
        }
      }
      return new StackManipulation.Compound(pushParameters);
    }
  }

  /**
   * This wrapper exists to convert checked exceptions to unchecked exceptions, since if this fails
   * the library itself is malformed.
   */
  private static MethodDescription getExtraContextFactoryMethodDescription(
      String methodName, Class<?>... parameterTypes) {
    try {
      return new MethodDescription.ForLoadedMethod(
          DoFnInvoker.ArgumentProvider.class.getMethod(methodName, parameterTypes));
    } catch (Exception e) {
      throw new IllegalStateException(
          String.format(
              "Failed to locate required method %s.%s",
              DoFnInvoker.ArgumentProvider.class.getSimpleName(), methodName),
          e);
    }
  }

  /**
   * Calls a zero-parameter getter on the {@link DoFnInvoker.ArgumentProvider}, which must be on top
   * of the stack.
   */
  private static StackManipulation simpleExtraContextParameter(String methodName) {
    return new StackManipulation.Compound(
        MethodInvocation.invoke(getExtraContextFactoryMethodDescription(methodName)));
  }

  static StackManipulation getExtraContextParameter(
      DoFnSignature.Parameter parameter, final StackManipulation pushDelegate) {

    return parameter.match(
        new Cases<StackManipulation>() {

          @Override
          public StackManipulation dispatch(StartBundleContextParameter p) {
            return new StackManipulation.Compound(
                pushDelegate,
                MethodInvocation.invoke(
                    getExtraContextFactoryMethodDescription(
                        START_BUNDLE_CONTEXT_PARAMETER_METHOD, DoFn.class)));
          }

          @Override
          public StackManipulation dispatch(FinishBundleContextParameter p) {
            return new StackManipulation.Compound(
                pushDelegate,
                MethodInvocation.invoke(
                    getExtraContextFactoryMethodDescription(
                        FINISH_BUNDLE_CONTEXT_PARAMETER_METHOD, DoFn.class)));
          }

          @Override
          public StackManipulation dispatch(ProcessContextParameter p) {
            return new StackManipulation.Compound(
                pushDelegate,
                MethodInvocation.invoke(
                    getExtraContextFactoryMethodDescription(
                        PROCESS_CONTEXT_PARAMETER_METHOD, DoFn.class)));
          }

          @Override
          public StackManipulation dispatch(ElementParameter p) {
            return new StackManipulation.Compound(
                pushDelegate,
                MethodInvocation.invoke(
                    getExtraContextFactoryMethodDescription(
                        ELEMENT_PARAMETER_METHOD, DoFn.class)));
          }

          @Override
          public StackManipulation dispatch(TimestampParameter p) {
            return new StackManipulation.Compound(
                pushDelegate,
                MethodInvocation.invoke(
                    getExtraContextFactoryMethodDescription(
                        TIMESTAMP_PARAMETER_METHOD, DoFn.class)));
          }

          @Override
          public StackManipulation dispatch(OnTimerContextParameter p) {
            return new StackManipulation.Compound(
                pushDelegate,
                MethodInvocation.invoke(
                    getExtraContextFactoryMethodDescription(
                        ON_TIMER_CONTEXT_PARAMETER_METHOD, DoFn.class)));
          }

          @Override
          public StackManipulation dispatch(WindowParameter p) {
            return new StackManipulation.Compound(
                simpleExtraContextParameter(WINDOW_PARAMETER_METHOD),
                TypeCasting.to(new TypeDescription.ForLoadedType(p.windowT().getRawType())));
          }

          @Override
          public StackManipulation dispatch(RestrictionTrackerParameter p) {
            // DoFnInvoker.ArgumentProvider.restrictionTracker() returns a RestrictionTracker,
            // but the @ProcessElement method expects a concrete subtype of it.
            // Insert a downcast.
            return new StackManipulation.Compound(
                simpleExtraContextParameter(RESTRICTION_TRACKER_PARAMETER_METHOD),
                TypeCasting.to(new TypeDescription.ForLoadedType(p.trackerT().getRawType())));
          }

          @Override
          public StackManipulation dispatch(StateParameter p) {
            return new StackManipulation.Compound(
                new TextConstant(p.referent().id()),
                MethodInvocation.invoke(
                    getExtraContextFactoryMethodDescription(STATE_PARAMETER_METHOD, String.class)),
                TypeCasting.to(
                    new TypeDescription.ForLoadedType(p.referent().stateType().getRawType())));
          }

          @Override
          public StackManipulation dispatch(TimerParameter p) {
            return new StackManipulation.Compound(
                new TextConstant(p.referent().id()),
                MethodInvocation.invoke(
                    getExtraContextFactoryMethodDescription(TIMER_PARAMETER_METHOD, String.class)),
                TypeCasting.to(new TypeDescription.ForLoadedType(Timer.class)));
          }

          @Override
          public StackManipulation dispatch(DoFnSignature.Parameter.PipelineOptionsParameter p) {
            return simpleExtraContextParameter(PIPELINE_OPTIONS_PARAMETER_METHOD);
          }
        });
  }

  /**
   * Implements the invoker's {@link DoFnInvoker#invokeProcessElement} method by delegating to the
   * {@link ProcessElement} method.
   */
  private static final class ProcessElementDelegation extends DoFnMethodDelegation {
    private static final MethodDescription PROCESS_CONTINUATION_STOP_METHOD;

    static {
      try {
        PROCESS_CONTINUATION_STOP_METHOD =
            new MethodDescription.ForLoadedMethod(DoFn.ProcessContinuation.class.getMethod("stop"));
      } catch (NoSuchMethodException e) {
        throw new RuntimeException("Failed to locate ProcessContinuation.stop()");
      }
    }

    private final DoFnSignature.ProcessElementMethod signature;

    /** Implementation of {@link MethodDelegation} for the {@link ProcessElement} method. */
    private ProcessElementDelegation(
        TypeDescription doFnType, DoFnSignature.ProcessElementMethod signature) {
      super(doFnType, signature.targetMethod());
      this.signature = signature;
    }

    @Override
    protected StackManipulation beforeDelegation(MethodDescription instrumentedMethod) {
      // Parameters of the wrapper invoker method:
      //   DoFnInvoker.ArgumentProvider
      // Parameters of the wrapped DoFn method:
      //   [DoFn.ProcessContext, BoundedWindow, InputProvider, OutputReceiver] in any order
      ArrayList<StackManipulation> pushParameters = new ArrayList<>();

      // To load the delegate, push `this` and then access the field
      StackManipulation pushDelegate =
          new StackManipulation.Compound(
              MethodVariableAccess.REFERENCE.loadFrom(0),
              FieldAccess.forField(delegateField).read());

      StackManipulation pushExtraContextFactory = MethodVariableAccess.REFERENCE.loadFrom(1);

      // Push the arguments in their actual order.
      for (DoFnSignature.Parameter param : signature.extraParameters()) {
        pushParameters.add(
            new StackManipulation.Compound(
                pushExtraContextFactory, getExtraContextParameter(param, pushDelegate)));
      }
      return new StackManipulation.Compound(pushParameters);
    }

    @Override
    protected StackManipulation afterDelegation(MethodDescription instrumentedMethod) {
      if (TypeDescription.VOID.equals(targetMethod.getReturnType().asErasure())) {
        return new StackManipulation.Compound(
            MethodInvocation.invoke(PROCESS_CONTINUATION_STOP_METHOD), MethodReturn.REFERENCE);
      } else {
        return MethodReturn.of(targetMethod.getReturnType().asErasure());
      }
    }
  }

  private static class UserCodeMethodInvocation implements StackManipulation {

    @Nullable private final Integer returnVarIndex;
    private final MethodDescription targetMethod;
    private final MethodDescription instrumentedMethod;
    private final TypeDescription returnType;

    private final Label wrapStart = new Label();
    private final Label wrapEnd = new Label();
    private final Label tryBlockStart = new Label();
    private final Label tryBlockEnd = new Label();
    private final Label catchBlockStart = new Label();
    private final Label catchBlockEnd = new Label();

    private final MethodDescription createUserCodeException;

    UserCodeMethodInvocation(
        @Nullable Integer returnVarIndex,
        MethodDescription targetMethod,
        MethodDescription instrumentedMethod) {
      this.returnVarIndex = returnVarIndex;
      this.targetMethod = targetMethod;
      this.instrumentedMethod = instrumentedMethod;
      this.returnType = targetMethod.getReturnType().asErasure();

      boolean targetMethodReturnsVoid = TypeDescription.VOID.equals(returnType);
      checkArgument(
          (returnVarIndex == null) == targetMethodReturnsVoid,
          "returnVarIndex should be defined if and only if the target method has a return value");

      try {
        createUserCodeException =
            new MethodDescription.ForLoadedMethod(
                UserCodeException.class.getDeclaredMethod("wrap", Throwable.class));
      } catch (NoSuchMethodException | SecurityException e) {
        throw new RuntimeException("Unable to find UserCodeException.wrap", e);
      }
    }

    @Override
    public boolean isValid() {
      return true;
    }

    private Object describeType(Type type) {
      switch (type.getSort()) {
        case Type.OBJECT:
          return type.getInternalName();
        case Type.INT:
        case Type.BYTE:
        case Type.BOOLEAN:
        case Type.SHORT:
          return Opcodes.INTEGER;
        case Type.LONG:
          return Opcodes.LONG;
        case Type.DOUBLE:
          return Opcodes.DOUBLE;
        case Type.FLOAT:
          return Opcodes.FLOAT;
        default:
          throw new IllegalArgumentException("Unhandled type as method argument: " + type);
      }
    }

    private void visitFrame(
        MethodVisitor mv, boolean localsIncludeReturn, @Nullable String stackTop) {
      boolean hasReturnLocal = (returnVarIndex != null) && localsIncludeReturn;

      Type[] localTypes = Type.getArgumentTypes(instrumentedMethod.getDescriptor());
      Object[] locals = new Object[1 + localTypes.length + (hasReturnLocal ? 1 : 0)];
      locals[0] = instrumentedMethod.getReceiverType().asErasure().getInternalName();
      for (int i = 0; i < localTypes.length; i++) {
        locals[i + 1] = describeType(localTypes[i]);
      }
      if (hasReturnLocal) {
        locals[locals.length - 1] = returnType.getInternalName();
      }

      Object[] stack = stackTop == null ? new Object[] {} : new Object[] {stackTop};

      mv.visitFrame(Opcodes.F_NEW, locals.length, locals, stack.length, stack);
    }

    @Override
    public Size apply(MethodVisitor mv, Context context) {
      Size size = new Size(0, 0);

      mv.visitLabel(wrapStart);

      String throwableName = new TypeDescription.ForLoadedType(Throwable.class).getInternalName();
      mv.visitTryCatchBlock(tryBlockStart, tryBlockEnd, catchBlockStart, throwableName);

      // The try block attempts to perform the expected operations, then jumps to success
      mv.visitLabel(tryBlockStart);
      size = size.aggregate(MethodInvocation.invoke(targetMethod).apply(mv, context));

      if (returnVarIndex != null) {
        mv.visitVarInsn(Opcodes.ASTORE, returnVarIndex);
        size = size.aggregate(new Size(-1, 0)); // Reduces the size of the stack
      }
      mv.visitJumpInsn(Opcodes.GOTO, catchBlockEnd);
      mv.visitLabel(tryBlockEnd);

      // The handler wraps the exception, and then throws.
      mv.visitLabel(catchBlockStart);
      // In catch block, should have same locals and {Throwable} on the stack.
      visitFrame(mv, false, throwableName);

      // Create the user code exception and throw
      size =
          size.aggregate(
              new Compound(MethodInvocation.invoke(createUserCodeException), Throw.INSTANCE)
                  .apply(mv, context));

      mv.visitLabel(catchBlockEnd);

      // After the catch block we should have the return in scope, but nothing on the stack.
      visitFrame(mv, true, null);

      // After catch block, should have same locals and will have the return on the stack.
      if (returnVarIndex != null) {
        mv.visitVarInsn(Opcodes.ALOAD, returnVarIndex);
        size = size.aggregate(new Size(1, 0)); // Increases the size of the stack
      }
      mv.visitLabel(wrapEnd);
      if (returnVarIndex != null) {
        // Drop the return type from the locals
        mv.visitLocalVariable(
            "res",
            returnType.getDescriptor(),
            returnType.getGenericSignature(),
            wrapStart,
            wrapEnd,
            returnVarIndex);
      }

      return size;
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
                    // Load the delegate argument
                    MethodVariableAccess.REFERENCE.loadFrom(1),
                    // Invoke the super constructor (default constructor of Object)
                    MethodInvocation.invoke(
                        new TypeDescription.ForLoadedType(DoFnInvokerBase.class)
                            .getDeclaredMethods()
                            .filter(
                                ElementMatchers.isConstructor()
                                    .and(ElementMatchers.takesArguments(DoFn.class)))
                            .getOnly()),
                    // Return void.
                    MethodReturn.VOID)
                .apply(methodVisitor, implementationContext);
        return new ByteCodeAppender.Size(size.getMaximalSize(), instrumentedMethod.getStackSize());
      };
    }
  }
}
