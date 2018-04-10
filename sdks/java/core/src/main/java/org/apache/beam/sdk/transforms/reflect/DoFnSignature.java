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
import com.google.common.base.Predicates;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.RestrictionTrackerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.StateParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.TimerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.WindowParameter;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Describes the signature of a {@link DoFn}, in particular, which features it uses, which extra
 * context it requires, types of the input and output elements, etc.
 *
 * <p>See <a href="https://s.apache.org/a-new-dofn">A new DoFn</a>.
 */
@AutoValue
public abstract class DoFnSignature {
  /** Class of the original {@link DoFn} from which this signature was produced. */
  public abstract Class<? extends DoFn<?, ?>> fnClass();

  /** Whether this {@link DoFn} does a bounded amount of work per element. */
  public abstract PCollection.IsBounded isBoundedPerElement();

  /** Details about this {@link DoFn}'s {@link DoFn.ProcessElement} method. */
  public abstract ProcessElementMethod processElement();

  /** Details about the state cells that this {@link DoFn} declares. Immutable. */
  public abstract Map<String, StateDeclaration> stateDeclarations();

  /** Details about this {@link DoFn}'s {@link DoFn.StartBundle} method. */
  @Nullable
  public abstract BundleMethod startBundle();

  /** Details about this {@link DoFn}'s {@link DoFn.FinishBundle} method. */
  @Nullable
  public abstract BundleMethod finishBundle();

  /** Details about this {@link DoFn}'s {@link DoFn.Setup} method. */
  @Nullable
  public abstract LifecycleMethod setup();

  /** Details about this {@link DoFn}'s {@link DoFn.Teardown} method. */
  @Nullable
  public abstract LifecycleMethod teardown();

  /** Timer declarations present on the {@link DoFn} class. Immutable. */
  public abstract Map<String, TimerDeclaration> timerDeclarations();

  /** Details about this {@link DoFn}'s {@link DoFn.GetInitialRestriction} method. */
  @Nullable
  public abstract GetInitialRestrictionMethod getInitialRestriction();

  /** Details about this {@link DoFn}'s {@link DoFn.SplitRestriction} method. */
  @Nullable
  public abstract SplitRestrictionMethod splitRestriction();

  /** Details about this {@link DoFn}'s {@link DoFn.GetRestrictionCoder} method. */
  @Nullable
  public abstract GetRestrictionCoderMethod getRestrictionCoder();

  /** Details about this {@link DoFn}'s {@link DoFn.NewTracker} method. */
  @Nullable
  public abstract NewTrackerMethod newTracker();

  /** Details about this {@link DoFn}'s {@link DoFn.OnTimer} methods. */
  @Nullable
  public abstract Map<String, OnTimerMethod> onTimerMethods();

  /** @deprecated use {@link #usesState()}, it's cleaner */
  @Deprecated
  public boolean isStateful() {
    return stateDeclarations().size() > 0;
  }

  /** Whether the {@link DoFn} described by this signature uses state. */
  public boolean usesState() {
    return stateDeclarations().size() > 0;
  }

  /** Whether the {@link DoFn} described by this signature uses timers. */
  public boolean usesTimers() {
    return timerDeclarations().size() > 0;
  }

  static Builder builder() {
    return new AutoValue_DoFnSignature.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setFnClass(Class<? extends DoFn<?, ?>> fnClass);
    abstract Builder setIsBoundedPerElement(PCollection.IsBounded isBounded);
    abstract Builder setProcessElement(ProcessElementMethod processElement);
    abstract Builder setStartBundle(BundleMethod startBundle);
    abstract Builder setFinishBundle(BundleMethod finishBundle);
    abstract Builder setSetup(LifecycleMethod setup);
    abstract Builder setTeardown(LifecycleMethod teardown);
    abstract Builder setGetInitialRestriction(GetInitialRestrictionMethod getInitialRestriction);
    abstract Builder setSplitRestriction(SplitRestrictionMethod splitRestriction);
    abstract Builder setGetRestrictionCoder(GetRestrictionCoderMethod getRestrictionCoder);
    abstract Builder setNewTracker(NewTrackerMethod newTracker);
    abstract Builder setStateDeclarations(Map<String, StateDeclaration> stateDeclarations);
    abstract Builder setTimerDeclarations(Map<String, TimerDeclaration> timerDeclarations);
    abstract Builder setOnTimerMethods(Map<String, OnTimerMethod> onTimerMethods);
    abstract DoFnSignature build();
  }

  /** A method delegated to an annotated method of an underlying {@link DoFn}. */
  public interface DoFnMethod {
    /** The annotated method itself. */
    Method targetMethod();
  }

  /**
   * A method delegated to an annotated method of an underlying {@link DoFn} that accepts a dynamic
   * list of parameters.
   */
  public interface MethodWithExtraParameters extends DoFnMethod {
    /**
     * Types of optional parameters of the annotated method, in the order they appear.
     *
     * <p>Validation that these are allowed is external to this class.
     */
    List<Parameter> extraParameters();

    /** The type of window expected by this method, if any. */
    @Nullable
    TypeDescriptor<? extends BoundedWindow> windowT();
  }

  /** A descriptor for an optional parameter of the {@link DoFn.ProcessElement} method. */
  public abstract static class Parameter {

    // Private as no extensions other than those nested here are permitted
    private Parameter() {}

    /**
     * Performs case analysis on this {@link Parameter}, processing it with the appropriate
     * {@link Cases#dispatch} case of the provided {@link Cases} object.
     */
    public <ResultT> ResultT match(Cases<ResultT> cases) {
      // This could be done with reflection, but since the number of cases is small and known,
      // they are simply inlined.
      if (this instanceof StartBundleContextParameter) {
        return cases.dispatch((StartBundleContextParameter) this);
      } else if (this instanceof FinishBundleContextParameter) {
        return cases.dispatch((FinishBundleContextParameter) this);
      } else if (this instanceof ProcessContextParameter) {
        return cases.dispatch((ProcessContextParameter) this);
      } else if (this instanceof OnTimerContextParameter) {
        return cases.dispatch((OnTimerContextParameter) this);
      } else if (this instanceof WindowParameter) {
        return cases.dispatch((WindowParameter) this);
      } else if (this instanceof RestrictionTrackerParameter) {
        return cases.dispatch((RestrictionTrackerParameter) this);
      } else if (this instanceof StateParameter) {
        return cases.dispatch((StateParameter) this);
      } else if (this instanceof TimerParameter) {
        return cases.dispatch((TimerParameter) this);
      } else if (this instanceof PipelineOptionsParameter) {
        return cases.dispatch((PipelineOptionsParameter) this);
      } else {
        throw new IllegalStateException(
            String.format("Attempt to case match on unknown %s subclass %s",
                Parameter.class.getCanonicalName(), this.getClass().getCanonicalName()));
      }
    }

    /**
     * An interface for destructuring a {@link Parameter}.
     */
    public interface Cases<ResultT> {
      ResultT dispatch(StartBundleContextParameter p);
      ResultT dispatch(FinishBundleContextParameter p);
      ResultT dispatch(ProcessContextParameter p);
      ResultT dispatch(OnTimerContextParameter p);
      ResultT dispatch(WindowParameter p);
      ResultT dispatch(RestrictionTrackerParameter p);
      ResultT dispatch(StateParameter p);
      ResultT dispatch(TimerParameter p);
      ResultT dispatch(PipelineOptionsParameter p);

      /**
       * A base class for a visitor with a default method for cases it is not interested in.
       */
      abstract class WithDefault<ResultT> implements Cases<ResultT> {

        protected abstract ResultT dispatchDefault(Parameter p);

        @Override
        public ResultT dispatch(StartBundleContextParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(FinishBundleContextParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(ProcessContextParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(OnTimerContextParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(WindowParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(RestrictionTrackerParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(StateParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(TimerParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(PipelineOptionsParameter p) {
          return dispatchDefault(p);
        }
      }
    }

    // These parameter descriptors are constant
    private static final StartBundleContextParameter START_BUNDLE_CONTEXT_PARAMETER =
        new AutoValue_DoFnSignature_Parameter_StartBundleContextParameter();
    private static final FinishBundleContextParameter FINISH_BUNDLE_CONTEXT_PARAMETER =
        new AutoValue_DoFnSignature_Parameter_FinishBundleContextParameter();
    private static final ProcessContextParameter PROCESS_CONTEXT_PARAMETER =
          new AutoValue_DoFnSignature_Parameter_ProcessContextParameter();
    private static final OnTimerContextParameter ON_TIMER_CONTEXT_PARAMETER =
        new AutoValue_DoFnSignature_Parameter_OnTimerContextParameter();

    /** Returns a {@link ProcessContextParameter}. */
    public static ProcessContextParameter processContext() {
      return PROCESS_CONTEXT_PARAMETER;
    }

    /** Returns a {@link OnTimerContextParameter}. */
    public static OnTimerContextParameter onTimerContext() {
      return ON_TIMER_CONTEXT_PARAMETER;
    }

    /** Returns a {@link WindowParameter}. */
    public static WindowParameter boundedWindow(TypeDescriptor<? extends BoundedWindow> windowT) {
      return new AutoValue_DoFnSignature_Parameter_WindowParameter(windowT);
    }

    /** Returns a {@link PipelineOptionsParameter}. */
    public static PipelineOptionsParameter pipelineOptions() {
      return new AutoValue_DoFnSignature_Parameter_PipelineOptionsParameter();
    }

    /**
     * Returns a {@link RestrictionTrackerParameter}.
     */
    public static RestrictionTrackerParameter restrictionTracker(TypeDescriptor<?> trackerT) {
      return new AutoValue_DoFnSignature_Parameter_RestrictionTrackerParameter(trackerT);
    }

    /**
     * Returns a {@link StateParameter} referring to the given {@link StateDeclaration}.
     */
    public static StateParameter stateParameter(StateDeclaration decl) {
      return new AutoValue_DoFnSignature_Parameter_StateParameter(decl);
    }

    public static TimerParameter timerParameter(TimerDeclaration decl) {
      return new AutoValue_DoFnSignature_Parameter_TimerParameter(decl);
    }

    /**
     * Descriptor for a {@link Parameter} of a subtype of {@link PipelineOptions}.
     */
    @AutoValue
    public abstract static class PipelineOptionsParameter extends Parameter {
      PipelineOptionsParameter() {}
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link DoFn.StartBundleContext}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class StartBundleContextParameter extends Parameter {
      StartBundleContextParameter() {}
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link DoFn.FinishBundleContext}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class FinishBundleContextParameter extends Parameter {
      FinishBundleContextParameter() {}
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link DoFn.ProcessContext}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class ProcessContextParameter extends Parameter {
      ProcessContextParameter() {}
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link DoFn.OnTimerContext}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class OnTimerContextParameter extends Parameter {
      OnTimerContextParameter() {}
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link BoundedWindow}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class WindowParameter extends Parameter {
      WindowParameter() {}

      public abstract TypeDescriptor<? extends BoundedWindow> windowT();
    }

    /**
     * Descriptor for a {@link Parameter} of a subclass of {@link RestrictionTracker}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class RestrictionTrackerParameter extends Parameter {
      // Package visible for AutoValue
      RestrictionTrackerParameter() {}

      public abstract TypeDescriptor<?> trackerT();
    }

    /**
     * Descriptor for a {@link Parameter} of a subclass of {@link State}, with an id indicated by
     * its {@link StateId} annotation.
     *
     * <p>All descriptors for the same declared state are equal.
     */
    @AutoValue
    public abstract static class StateParameter extends Parameter {
      // Package visible for AutoValue
      StateParameter() {}

      public abstract StateDeclaration referent();
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link Timer}, with an id indicated by
     * its {@link TimerId} annotation.
     */
    @AutoValue
    public abstract static class TimerParameter extends Parameter {
      // Package visible for AutoValue
      TimerParameter() {}

      public abstract TimerDeclaration referent();
    }
  }

  /** Describes a {@link DoFn.ProcessElement} method. */
  @AutoValue
  public abstract static class ProcessElementMethod implements MethodWithExtraParameters {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Types of optional parameters of the annotated method, in the order they appear. */
    @Override
    public abstract List<Parameter> extraParameters();

    /**
     * Whether this method requires stable input, expressed via {@link
     * org.apache.beam.sdk.transforms.DoFn.RequiresStableInput}.
     */
    public abstract boolean requiresStableInput();

    /** Concrete type of the {@link RestrictionTracker} parameter, if present. */
    @Nullable
    public abstract TypeDescriptor<?> trackerT();

    /** The window type used by this method, if any. */
    @Nullable
    public abstract TypeDescriptor<? extends BoundedWindow> windowT();

    /** Whether this {@link DoFn} returns a {@link ProcessContinuation} or void. */
    public abstract boolean hasReturnValue();

    static ProcessElementMethod create(
        Method targetMethod,
        List<Parameter> extraParameters,
        boolean requiresStableInput,
        TypeDescriptor<?> trackerT,
        @Nullable TypeDescriptor<? extends BoundedWindow> windowT,
        boolean hasReturnValue) {
      return new AutoValue_DoFnSignature_ProcessElementMethod(
          targetMethod,
          Collections.unmodifiableList(extraParameters),
          requiresStableInput,
          trackerT,
          windowT,
          hasReturnValue);
    }

    /**
     * Whether this {@link DoFn} observes - directly or indirectly - the window that an element
     * resides in.
     *
     * <p>{@link State} and {@link Timer} parameters indirectly observe the window, because
     * they are each scoped to a single window.
     */
    public boolean observesWindow() {
      return extraParameters()
          .stream()
          .anyMatch(
              Predicates.or(
                      Predicates.instanceOf(WindowParameter.class),
                      Predicates.instanceOf(TimerParameter.class),
                      Predicates.instanceOf(StateParameter.class))
                  ::apply);
    }

    /**
     * Whether this {@link DoFn} is <a href="https://s.apache.org/splittable-do-fn">splittable</a>.
     */
    public boolean isSplittable() {
      return extraParameters()
          .stream()
          .anyMatch(Predicates.instanceOf(RestrictionTrackerParameter.class)::apply);
    }
  }

  /** Describes a {@link DoFn.OnTimer} method. */
  @AutoValue
  public abstract static class OnTimerMethod implements MethodWithExtraParameters {

    /** The id on the method's {@link DoFn.TimerId} annotation. */
    public abstract String id();

    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /**
     * Whether this method requires stable input, expressed via {@link
     * org.apache.beam.sdk.transforms.DoFn.RequiresStableInput}. For timers, this means that any
     * state must be stably persisted prior to calling it.
     */
    public abstract boolean requiresStableInput();

    /** The window type used by this method, if any. */
    @Nullable
    public abstract TypeDescriptor<? extends BoundedWindow> windowT();

    /** Types of optional parameters of the annotated method, in the order they appear. */
    @Override
    public abstract List<Parameter> extraParameters();

    static OnTimerMethod create(
        Method targetMethod,
        String id,
        boolean requiresStableInput,
        TypeDescriptor<? extends BoundedWindow> windowT,
        List<Parameter> extraParameters) {
      return new AutoValue_DoFnSignature_OnTimerMethod(
          id,
          targetMethod,
          requiresStableInput,
          windowT,
          Collections.unmodifiableList(extraParameters));
    }
  }

  /**
   * Describes a timer declaration; a field of type {@link TimerSpec} annotated with
   * {@link DoFn.TimerId}.
   */
  @AutoValue
  public abstract static class TimerDeclaration {
    public abstract String id();
    public abstract Field field();

    static TimerDeclaration create(String id, Field field) {
      return new AutoValue_DoFnSignature_TimerDeclaration(id, field);
    }
  }


  /** Describes a {@link DoFn.StartBundle} or {@link DoFn.FinishBundle} method. */
  @AutoValue
  public abstract static class BundleMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    static BundleMethod create(Method targetMethod) {
      return new AutoValue_DoFnSignature_BundleMethod(targetMethod);
    }
  }

  /**
   * Describes a state declaration; a field of type {@link StateSpec} annotated with
   * {@link DoFn.StateId}.
   */
  @AutoValue
  public abstract static class StateDeclaration {
    public abstract String id();
    public abstract Field field();
    public abstract TypeDescriptor<? extends State> stateType();

    static StateDeclaration create(
        String id, Field field, TypeDescriptor<? extends State> stateType) {
      field.setAccessible(true);
      return new AutoValue_DoFnSignature_StateDeclaration(id, field, stateType);
    }
  }

  /** Describes a {@link DoFn.Setup} or {@link DoFn.Teardown} method. */
  @AutoValue
  public abstract static class LifecycleMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    static LifecycleMethod create(Method targetMethod) {
      return new AutoValue_DoFnSignature_LifecycleMethod(targetMethod);
    }
  }

  /** Describes a {@link DoFn.GetInitialRestriction} method. */
  @AutoValue
  public abstract static class GetInitialRestrictionMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Type of the returned restriction. */
    public abstract TypeDescriptor<?> restrictionT();

    static GetInitialRestrictionMethod create(Method targetMethod, TypeDescriptor<?> restrictionT) {
      return new AutoValue_DoFnSignature_GetInitialRestrictionMethod(targetMethod, restrictionT);
    }
  }

  /** Describes a {@link DoFn.SplitRestriction} method. */
  @AutoValue
  public abstract static class SplitRestrictionMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Type of the restriction taken and returned. */
    public abstract TypeDescriptor<?> restrictionT();

    static SplitRestrictionMethod create(Method targetMethod, TypeDescriptor<?> restrictionT) {
      return new AutoValue_DoFnSignature_SplitRestrictionMethod(targetMethod, restrictionT);
    }
  }

  /** Describes a {@link DoFn.NewTracker} method. */
  @AutoValue
  public abstract static class NewTrackerMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Type of the input restriction. */
    public abstract TypeDescriptor<?> restrictionT();

    /** Type of the returned {@link RestrictionTracker}. */
    public abstract TypeDescriptor<?> trackerT();

    static NewTrackerMethod create(
        Method targetMethod, TypeDescriptor<?> restrictionT, TypeDescriptor<?> trackerT) {
      return new AutoValue_DoFnSignature_NewTrackerMethod(targetMethod, restrictionT, trackerT);
    }
  }

  /** Describes a {@link DoFn.GetRestrictionCoder} method. */
  @AutoValue
  public abstract static class GetRestrictionCoderMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Type of the returned {@link Coder}. */
    public abstract TypeDescriptor<?> coderT();

    static GetRestrictionCoderMethod create(Method targetMethod, TypeDescriptor<?> coderT) {
      return new AutoValue_DoFnSignature_GetRestrictionCoderMethod(targetMethod, coderT);
    }
  }
}
