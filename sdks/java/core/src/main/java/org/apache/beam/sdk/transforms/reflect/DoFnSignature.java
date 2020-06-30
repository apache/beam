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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.DoFn.TruncateRestriction;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.OutputReceiverParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.RestrictionTrackerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.SchemaElementParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.SideInputParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.StateParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.TimerFamilyParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.TimerParameter;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.Parameter.WindowParameter;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicates;

/**
 * Describes the signature of a {@link DoFn}, in particular, which features it uses, which extra
 * context it requires, types of the input and output elements, etc.
 *
 * <p>See <a href="https://s.apache.org/a-new-dofn">A new DoFn</a>.
 */
@AutoValue
@Internal
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

  /** Details about this {@link DoFn}'s {@link DoFn.OnWindowExpiration} method. */
  @Nullable
  public abstract OnWindowExpirationMethod onWindowExpiration();

  /** Timer declarations present on the {@link DoFn} class. Immutable. */
  public abstract Map<String, TimerDeclaration> timerDeclarations();

  /** TimerMap declarations present on the {@link DoFn} class. Immutable. */
  public abstract Map<String, TimerFamilyDeclaration> timerFamilyDeclarations();

  /** Field access declaration. */
  @Nullable
  public abstract Map<String, FieldAccessDeclaration> fieldAccessDeclarations();

  /** Details about this {@link DoFn}'s {@link DoFn.GetInitialRestriction} method. */
  @Nullable
  public abstract GetInitialRestrictionMethod getInitialRestriction();

  /** Details about this {@link DoFn}'s {@link DoFn.SplitRestriction} method. */
  @Nullable
  public abstract SplitRestrictionMethod splitRestriction();

  /** Details about this {@link DoFn}'s {@link TruncateRestriction} method. */
  @Nullable
  public abstract TruncateRestrictionMethod truncateRestriction();

  /** Details about this {@link DoFn}'s {@link DoFn.GetRestrictionCoder} method. */
  @Nullable
  public abstract GetRestrictionCoderMethod getRestrictionCoder();

  /** Details about this {@link DoFn}'s {@link DoFn.GetWatermarkEstimatorStateCoder} method. */
  @Nullable
  public abstract GetWatermarkEstimatorStateCoderMethod getWatermarkEstimatorStateCoder();

  /** Details about this {@link DoFn}'s {@link DoFn.GetInitialWatermarkEstimatorState} method. */
  @Nullable
  public abstract GetInitialWatermarkEstimatorStateMethod getInitialWatermarkEstimatorState();

  /** Details about this {@link DoFn}'s {@link DoFn.NewWatermarkEstimator} method. */
  @Nullable
  public abstract NewWatermarkEstimatorMethod newWatermarkEstimator();

  /** Details about this {@link DoFn}'s {@link DoFn.NewTracker} method. */
  @Nullable
  public abstract NewTrackerMethod newTracker();

  /** Details about this {@link DoFn}'s {@link DoFn.GetSize} method. */
  @Nullable
  public abstract GetSizeMethod getSize();

  /** Details about this {@link DoFn}'s {@link DoFn.OnTimer} methods. */
  @Nullable
  public abstract Map<String, OnTimerMethod> onTimerMethods();

  /** Details about this {@link DoFn}'s {@link DoFn.OnTimerFamily} methods. */
  @Nullable
  public abstract Map<String, OnTimerFamilyMethod> onTimerFamilyMethods();

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
    return timerDeclarations().size() > 0 || timerFamilyDeclarations().size() > 0;
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

    abstract Builder setOnWindowExpiration(OnWindowExpirationMethod onWindowExpiration);

    abstract Builder setGetInitialRestriction(GetInitialRestrictionMethod getInitialRestriction);

    abstract Builder setSplitRestriction(SplitRestrictionMethod splitRestriction);

    abstract Builder setTruncateRestriction(TruncateRestrictionMethod truncateRestriction);

    abstract Builder setGetRestrictionCoder(GetRestrictionCoderMethod getRestrictionCoder);

    abstract Builder setNewTracker(NewTrackerMethod newTracker);

    abstract Builder setGetSize(GetSizeMethod getSize);

    abstract Builder setGetInitialWatermarkEstimatorState(
        GetInitialWatermarkEstimatorStateMethod getInitialWatermarkEstimatorState);

    abstract Builder setNewWatermarkEstimator(NewWatermarkEstimatorMethod newWatermarkEstimator);

    abstract Builder setGetWatermarkEstimatorStateCoder(
        GetWatermarkEstimatorStateCoderMethod getWatermarkEstimatorStateCoder);

    abstract Builder setStateDeclarations(Map<String, StateDeclaration> stateDeclarations);

    abstract Builder setTimerDeclarations(Map<String, TimerDeclaration> timerDeclarations);

    abstract Builder setTimerFamilyDeclarations(
        Map<String, TimerFamilyDeclaration> timerFamilyDeclarations);

    abstract Builder setFieldAccessDeclarations(
        Map<String, FieldAccessDeclaration> fieldAccessDeclaration);

    abstract Builder setOnTimerMethods(Map<String, OnTimerMethod> onTimerMethods);

    abstract Builder setOnTimerFamilyMethods(Map<String, OnTimerFamilyMethod> onTimerFamilyMethods);

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

    /**
     * Whether this method observes - directly or indirectly - the window that an element resides
     * in.
     *
     * <p>{@link State} and {@link Timer} parameters indirectly observe the window, because they are
     * each scoped to a single window.
     */
    default boolean observesWindow() {
      return extraParameters().stream()
          .anyMatch(
              Predicates.or(
                      Predicates.instanceOf(WindowParameter.class),
                      Predicates.instanceOf(TimerParameter.class),
                      Predicates.instanceOf(TimerFamilyParameter.class),
                      Predicates.instanceOf(StateParameter.class))
                  ::apply);
    }

    /** The type of window expected by this method, if any. */
    @Nullable
    TypeDescriptor<? extends BoundedWindow> windowT();
  }

  /** A descriptor for an optional parameter of the {@link DoFn.ProcessElement} method. */
  public abstract static class Parameter {

    // Private as no extensions other than those nested here are permitted
    private Parameter() {}

    /**
     * Performs case analysis on this {@link Parameter}, processing it with the appropriate {@link
     * Cases#dispatch} case of the provided {@link Cases} object.
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
      } else if (this instanceof PaneInfoParameter) {
        return cases.dispatch((PaneInfoParameter) this);
      } else if (this instanceof RestrictionParameter) {
        return cases.dispatch((RestrictionParameter) this);
      } else if (this instanceof RestrictionTrackerParameter) {
        return cases.dispatch((RestrictionTrackerParameter) this);
      } else if (this instanceof WatermarkEstimatorParameter) {
        return cases.dispatch((WatermarkEstimatorParameter) this);
      } else if (this instanceof WatermarkEstimatorStateParameter) {
        return cases.dispatch((WatermarkEstimatorStateParameter) this);
      } else if (this instanceof StateParameter) {
        return cases.dispatch((StateParameter) this);
      } else if (this instanceof TimerParameter) {
        return cases.dispatch((TimerParameter) this);
      } else if (this instanceof PipelineOptionsParameter) {
        return cases.dispatch((PipelineOptionsParameter) this);
      } else if (this instanceof ElementParameter) {
        return cases.dispatch((ElementParameter) this);
      } else if (this instanceof SchemaElementParameter) {
        return cases.dispatch((SchemaElementParameter) this);
      } else if (this instanceof TimestampParameter) {
        return cases.dispatch((TimestampParameter) this);
      } else if (this instanceof OutputReceiverParameter) {
        return cases.dispatch((OutputReceiverParameter) this);
      } else if (this instanceof TaggedOutputReceiverParameter) {
        return cases.dispatch((TaggedOutputReceiverParameter) this);
      } else if (this instanceof TimeDomainParameter) {
        return cases.dispatch((TimeDomainParameter) this);
      } else if (this instanceof SideInputParameter) {
        return cases.dispatch((SideInputParameter) this);
      } else if (this instanceof TimerFamilyParameter) {
        return cases.dispatch((TimerFamilyParameter) this);
      } else if (this instanceof TimerIdParameter) {
        return cases.dispatch((TimerIdParameter) this);
      } else if (this instanceof BundleFinalizerParameter) {
        return cases.dispatch((BundleFinalizerParameter) this);
      } else if (this instanceof KeyParameter) {
        return cases.dispatch((KeyParameter) this);
      } else {
        throw new IllegalStateException(
            String.format(
                "Attempt to case match on unknown %s subclass %s",
                Parameter.class.getCanonicalName(), this.getClass().getCanonicalName()));
      }
    }

    /** An interface for destructuring a {@link Parameter}. */
    public interface Cases<ResultT> {
      ResultT dispatch(StartBundleContextParameter p);

      ResultT dispatch(FinishBundleContextParameter p);

      ResultT dispatch(ProcessContextParameter p);

      ResultT dispatch(ElementParameter p);

      ResultT dispatch(SchemaElementParameter p);

      ResultT dispatch(TimestampParameter p);

      ResultT dispatch(TimeDomainParameter p);

      ResultT dispatch(OutputReceiverParameter p);

      ResultT dispatch(TaggedOutputReceiverParameter p);

      ResultT dispatch(OnTimerContextParameter p);

      ResultT dispatch(WindowParameter p);

      ResultT dispatch(PaneInfoParameter p);

      ResultT dispatch(RestrictionParameter p);

      ResultT dispatch(RestrictionTrackerParameter p);

      ResultT dispatch(WatermarkEstimatorParameter p);

      ResultT dispatch(WatermarkEstimatorStateParameter p);

      ResultT dispatch(StateParameter p);

      ResultT dispatch(TimerParameter p);

      ResultT dispatch(PipelineOptionsParameter p);

      ResultT dispatch(SideInputParameter p);

      ResultT dispatch(TimerFamilyParameter p);

      ResultT dispatch(TimerIdParameter p);

      ResultT dispatch(BundleFinalizerParameter p);

      ResultT dispatch(KeyParameter p);

      /** A base class for a visitor with a default method for cases it is not interested in. */
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
        public ResultT dispatch(ElementParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(SchemaElementParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(TaggedOutputReceiverParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(OutputReceiverParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(TimestampParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(TimerIdParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(TimeDomainParameter p) {
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
        public ResultT dispatch(PaneInfoParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(RestrictionParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(RestrictionTrackerParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(WatermarkEstimatorParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(WatermarkEstimatorStateParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(BundleFinalizerParameter p) {
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

        @Override
        public ResultT dispatch(SideInputParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(TimerFamilyParameter p) {
          return dispatchDefault(p);
        }

        @Override
        public ResultT dispatch(KeyParameter p) {
          return dispatchDefault(p);
        }
      }
    }

    // These parameter descriptors are constant.
    private static final StartBundleContextParameter START_BUNDLE_CONTEXT_PARAMETER =
        new AutoValue_DoFnSignature_Parameter_StartBundleContextParameter();
    private static final FinishBundleContextParameter FINISH_BUNDLE_CONTEXT_PARAMETER =
        new AutoValue_DoFnSignature_Parameter_FinishBundleContextParameter();
    private static final ProcessContextParameter PROCESS_CONTEXT_PARAMETER =
        new AutoValue_DoFnSignature_Parameter_ProcessContextParameter();
    private static final OnTimerContextParameter ON_TIMER_CONTEXT_PARAMETER =
        new AutoValue_DoFnSignature_Parameter_OnTimerContextParameter();
    private static final TimestampParameter TIMESTAMP_PARAMETER =
        new AutoValue_DoFnSignature_Parameter_TimestampParameter();
    private static final TimerIdParameter TIMER_ID_PARAMETER =
        new AutoValue_DoFnSignature_Parameter_TimerIdParameter();
    private static final PaneInfoParameter PANE_INFO_PARAMETER =
        new AutoValue_DoFnSignature_Parameter_PaneInfoParameter();
    private static final TimeDomainParameter TIME_DOMAIN_PARAMETER =
        new AutoValue_DoFnSignature_Parameter_TimeDomainParameter();
    private static final TaggedOutputReceiverParameter TAGGED_OUTPUT_RECEIVER_PARAMETER =
        new AutoValue_DoFnSignature_Parameter_TaggedOutputReceiverParameter();
    private static final PipelineOptionsParameter PIPELINE_OPTIONS_PARAMETER =
        new AutoValue_DoFnSignature_Parameter_PipelineOptionsParameter();
    private static final BundleFinalizerParameter BUNDLE_FINALIZER_PARAMETER =
        new AutoValue_DoFnSignature_Parameter_BundleFinalizerParameter();
    private static final OnWindowExpirationContextParameter ON_WINDOW_EXPIRATION_CONTEXT_PARAMETER =
        new AutoValue_DoFnSignature_Parameter_OnWindowExpirationContextParameter();

    /** Returns a {@link ProcessContextParameter}. */
    public static ProcessContextParameter processContext() {
      return PROCESS_CONTEXT_PARAMETER;
    }

    /** Returns a {@link StartBundleContextParameter}. */
    public static StartBundleContextParameter startBundleContext() {
      return START_BUNDLE_CONTEXT_PARAMETER;
    }

    /** Returns a {@link FinishBundleContextParameter}. */
    public static FinishBundleContextParameter finishBundleContext() {
      return FINISH_BUNDLE_CONTEXT_PARAMETER;
    }

    /** Returns a {@link BundleFinalizerParameter}. */
    public static BundleFinalizerParameter bundleFinalizer() {
      return BUNDLE_FINALIZER_PARAMETER;
    }

    public static ElementParameter elementParameter(TypeDescriptor<?> elementT) {
      return new AutoValue_DoFnSignature_Parameter_ElementParameter(elementT);
    }

    public static SchemaElementParameter schemaElementParameter(
        TypeDescriptor<?> elementT, @Nullable String fieldAccessString, int index) {
      return new AutoValue_DoFnSignature_Parameter_SchemaElementParameter.Builder()
          .setElementT(elementT)
          .setFieldAccessString(fieldAccessString)
          .setIndex(index)
          .build();
    }

    public static TimestampParameter timestampParameter() {
      return TIMESTAMP_PARAMETER;
    }

    public static TimerIdParameter timerIdParameter() {
      return TIMER_ID_PARAMETER;
    }

    public static SideInputParameter sideInputParameter(
        TypeDescriptor<?> elementT, String sideInputId) {
      return new AutoValue_DoFnSignature_Parameter_SideInputParameter.Builder()
          .setElementT(elementT)
          .setSideInputId(sideInputId)
          .build();
    }

    public static TimeDomainParameter timeDomainParameter() {
      return TIME_DOMAIN_PARAMETER;
    }

    public static OutputReceiverParameter outputReceiverParameter(boolean rowReceiver) {
      return new AutoValue_DoFnSignature_Parameter_OutputReceiverParameter(rowReceiver);
    }

    public static TaggedOutputReceiverParameter taggedOutputReceiverParameter() {
      return TAGGED_OUTPUT_RECEIVER_PARAMETER;
    }

    /** Returns a {@link OnTimerContextParameter}. */
    public static OnTimerContextParameter onTimerContext() {
      return ON_TIMER_CONTEXT_PARAMETER;
    }

    /** Returns a {@link OnWindowExpirationContextParameter}. */
    public static OnWindowExpirationContextParameter onWindowExpirationContext() {
      return ON_WINDOW_EXPIRATION_CONTEXT_PARAMETER;
    }

    public static PaneInfoParameter paneInfoParameter() {
      return PANE_INFO_PARAMETER;
    }

    /** Returns a {@link WindowParameter}. */
    public static WindowParameter boundedWindow(TypeDescriptor<? extends BoundedWindow> windowT) {
      return new AutoValue_DoFnSignature_Parameter_WindowParameter(windowT);
    }

    /** Returns a {@link KeyParameter}. */
    public static KeyParameter keyT(TypeDescriptor<?> keyT) {
      return new AutoValue_DoFnSignature_Parameter_KeyParameter(keyT);
    }

    /** Returns a {@link PipelineOptionsParameter}. */
    public static PipelineOptionsParameter pipelineOptions() {
      return PIPELINE_OPTIONS_PARAMETER;
    }

    /** Returns a {@link RestrictionParameter}. */
    public static RestrictionParameter restrictionParameter(TypeDescriptor<?> restrictionT) {
      return new AutoValue_DoFnSignature_Parameter_RestrictionParameter(restrictionT);
    }

    /** Returns a {@link RestrictionTrackerParameter}. */
    public static RestrictionTrackerParameter restrictionTracker(TypeDescriptor<?> trackerT) {
      return new AutoValue_DoFnSignature_Parameter_RestrictionTrackerParameter(trackerT);
    }

    /** Returns a {@link WatermarkEstimatorParameter}. */
    public static WatermarkEstimatorParameter watermarkEstimator(
        TypeDescriptor<?> watermarkEstimatorT) {
      return new AutoValue_DoFnSignature_Parameter_WatermarkEstimatorParameter(watermarkEstimatorT);
    }

    /** Returns a {@link WatermarkEstimatorStateParameter}. */
    public static WatermarkEstimatorStateParameter watermarkEstimatorState(
        TypeDescriptor<?> watermarkEstimatorStateT) {
      return new AutoValue_DoFnSignature_Parameter_WatermarkEstimatorStateParameter(
          watermarkEstimatorStateT);
    }

    /** Returns a {@link StateParameter} referring to the given {@link StateDeclaration}. */
    public static StateParameter stateParameter(StateDeclaration decl, boolean alwaysFetched) {
      return new AutoValue_DoFnSignature_Parameter_StateParameter(decl, alwaysFetched);
    }

    public static TimerParameter timerParameter(TimerDeclaration decl) {
      return new AutoValue_DoFnSignature_Parameter_TimerParameter(decl);
    }

    public static TimerFamilyParameter timerFamilyParameter(TimerFamilyDeclaration decl) {
      return new AutoValue_DoFnSignature_Parameter_TimerFamilyParameter(decl);
    }

    /** Descriptor for a {@link Parameter} of a subtype of {@link PipelineOptions}. */
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
     * Descriptor for a {@link Parameter} of type {@link DoFn.BundleFinalizer}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class BundleFinalizerParameter extends Parameter {
      BundleFinalizerParameter() {}
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link DoFn.Element}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class ElementParameter extends Parameter {
      ElementParameter() {}

      public abstract TypeDescriptor<?> elementT();
    }

    /**
     * Descriptor for a (@link Parameter} of type {@link DoFn.Element} where the type does not match
     * the DoFn's input type. This implies that the input must have a schema that is compatible.
     */
    @AutoValue
    public abstract static class SchemaElementParameter extends Parameter {
      SchemaElementParameter() {}

      public abstract TypeDescriptor<?> elementT();

      @Nullable
      public abstract String fieldAccessString();

      public abstract int index();

      /** Builder class. */
      @AutoValue.Builder
      public abstract static class Builder {
        public abstract Builder setElementT(TypeDescriptor<?> elementT);

        public abstract Builder setFieldAccessString(@Nullable String fieldAccess);

        public abstract Builder setIndex(int index);

        public abstract SchemaElementParameter build();
      }

      public abstract Builder toBuilder();
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link DoFn.Timestamp}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class TimestampParameter extends Parameter {
      TimestampParameter() {}
    }

    @AutoValue
    public abstract static class TimerIdParameter extends Parameter {
      TimerIdParameter() {}
    }

    @AutoValue
    public abstract static class KeyParameter extends Parameter {
      KeyParameter() {}

      public abstract TypeDescriptor<?> keyT();
    }

    /**
     * Descriptor for a {@link Parameter} representing the time domain of a timer.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class TimeDomainParameter extends Parameter {
      TimeDomainParameter() {}
    }

    /** Descriptor for a {@link Parameter} of type {@link DoFn.SideInput}. */
    @AutoValue
    public abstract static class SideInputParameter extends Parameter {
      SideInputParameter() {}

      public abstract TypeDescriptor<?> elementT();

      public abstract String sideInputId();

      /** Builder class. */
      @AutoValue.Builder
      public abstract static class Builder {
        public abstract SideInputParameter.Builder setElementT(TypeDescriptor<?> elementT);

        public abstract SideInputParameter.Builder setSideInputId(String sideInput);

        public abstract SideInputParameter build();
      }

      public abstract SideInputParameter.Builder toBuilder();
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link DoFn.OutputReceiver}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class OutputReceiverParameter extends Parameter {
      OutputReceiverParameter() {}

      public abstract boolean isRowReceiver();
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link MultiOutputReceiver}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class TaggedOutputReceiverParameter extends Parameter {
      TaggedOutputReceiverParameter() {}
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
     * Descriptor for a {@link Parameter} of type {@link DoFn.OnWindowExpirationContext}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class OnWindowExpirationContextParameter extends Parameter {
      OnWindowExpirationContextParameter() {}
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
     * Descriptor for a {@link Parameter} of type {@link
     * org.apache.beam.sdk.transforms.windowing.PaneInfo}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class PaneInfoParameter extends Parameter {
      PaneInfoParameter() {}
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link DoFn.Restriction}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class RestrictionParameter extends Parameter {
      // Package visible for AutoValue
      RestrictionParameter() {}

      public abstract TypeDescriptor<?> restrictionT();
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link DoFn.WatermarkEstimatorState}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class WatermarkEstimatorStateParameter extends Parameter {
      // Package visible for AutoValue
      WatermarkEstimatorStateParameter() {}

      public abstract TypeDescriptor<?> estimatorStateT();
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link DoFn.WatermarkEstimatorState}.
     *
     * <p>All such descriptors are equal.
     */
    @AutoValue
    public abstract static class WatermarkEstimatorParameter extends Parameter {
      // Package visible for AutoValue
      WatermarkEstimatorParameter() {}

      public abstract TypeDescriptor<?> estimatorT();
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

      public abstract boolean alwaysFetched();
    }

    /**
     * Descriptor for a {@link Parameter} of type {@link Timer}, with an id indicated by its {@link
     * TimerId} annotation.
     */
    @AutoValue
    public abstract static class TimerParameter extends Parameter {
      // Package visible for AutoValue
      TimerParameter() {}

      public abstract TimerDeclaration referent();
    }

    /** Descriptor for a {@link Parameter} of type {@link DoFn.TimerFamily}. */
    @AutoValue
    public abstract static class TimerFamilyParameter extends Parameter {
      // Package visible for AutoValue
      TimerFamilyParameter() {}

      public abstract TimerFamilyDeclaration referent();
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

    /**
     * Whether this method requires time sorted input, expressed via {@link
     * org.apache.beam.sdk.transforms.DoFn.RequiresTimeSortedInput}.
     */
    public abstract boolean requiresTimeSortedInput();

    /** Concrete type of the {@link RestrictionTracker} parameter, if present. */
    @Nullable
    public abstract TypeDescriptor<?> trackerT();

    /** Concrete type of the {@link WatermarkEstimator} parameter, if present. */
    @Nullable
    public abstract TypeDescriptor<?> watermarkEstimatorT();

    /** The window type used by this method, if any. */
    @Nullable
    @Override
    public abstract TypeDescriptor<? extends BoundedWindow> windowT();

    /** Whether this {@link DoFn} returns a {@link ProcessContinuation} or void. */
    public abstract boolean hasReturnValue();

    static ProcessElementMethod create(
        Method targetMethod,
        List<Parameter> extraParameters,
        boolean requiresStableInput,
        boolean requiresTimeSortedInput,
        TypeDescriptor<?> trackerT,
        TypeDescriptor<?> watermarkEstimatorT,
        @Nullable TypeDescriptor<? extends BoundedWindow> windowT,
        boolean hasReturnValue) {
      return new AutoValue_DoFnSignature_ProcessElementMethod(
          targetMethod,
          Collections.unmodifiableList(extraParameters),
          requiresStableInput,
          requiresTimeSortedInput,
          trackerT,
          watermarkEstimatorT,
          windowT,
          hasReturnValue);
    }

    @Nullable
    public List<SchemaElementParameter> getSchemaElementParameters() {
      return extraParameters().stream()
          .filter(Predicates.instanceOf(SchemaElementParameter.class)::apply)
          .map(SchemaElementParameter.class::cast)
          .collect(Collectors.toList());
    }

    @Nullable
    public List<SideInputParameter> getSideInputParameters() {
      return extraParameters().stream()
          .filter(Predicates.instanceOf(SideInputParameter.class)::apply)
          .map(SideInputParameter.class::cast)
          .collect(Collectors.toList());
    }

    /** The {@link OutputReceiverParameter} for a main output, or null if there is none. */
    @Nullable
    public OutputReceiverParameter getMainOutputReceiver() {
      Optional<Parameter> parameter =
          extraParameters().stream()
              .filter(Predicates.instanceOf(OutputReceiverParameter.class)::apply)
              .findFirst();
      return parameter.isPresent() ? ((OutputReceiverParameter) parameter.get()) : null;
    }

    /**
     * Whether this {@link DoFn} is <a href="https://s.apache.org/splittable-do-fn">splittable</a>.
     */
    public boolean isSplittable() {
      return extraParameters().stream()
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
    @Override
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

  /** Describes a {@link DoFn.OnTimerFamily} method. */
  @AutoValue
  public abstract static class OnTimerFamilyMethod implements MethodWithExtraParameters {

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
    @Override
    public abstract TypeDescriptor<? extends BoundedWindow> windowT();

    /** Types of optional parameters of the annotated method, in the order they appear. */
    @Override
    public abstract List<Parameter> extraParameters();

    static OnTimerFamilyMethod create(
        Method targetMethod,
        String id,
        boolean requiresStableInput,
        TypeDescriptor<? extends BoundedWindow> windowT,
        List<Parameter> extraParameters) {
      return new AutoValue_DoFnSignature_OnTimerFamilyMethod(
          id,
          targetMethod,
          requiresStableInput,
          windowT,
          Collections.unmodifiableList(extraParameters));
    }
  }

  /** Describes a {@link DoFn.OnWindowExpiration} method. */
  @AutoValue
  public abstract static class OnWindowExpirationMethod implements MethodWithExtraParameters {

    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /**
     * Whether this method requires stable input, expressed via {@link
     * org.apache.beam.sdk.transforms.DoFn.RequiresStableInput}. For {@link
     * org.apache.beam.sdk.transforms.DoFn.OnWindowExpiration}, this means that any state must be
     * stably persisted prior to calling it.
     */
    public abstract boolean requiresStableInput();

    /** The window type used by this method, if any. */
    @Nullable
    @Override
    public abstract TypeDescriptor<? extends BoundedWindow> windowT();

    /** Types of optional parameters of the annotated method, in the order they appear. */
    @Override
    public abstract List<Parameter> extraParameters();

    static OnWindowExpirationMethod create(
        Method targetMethod,
        boolean requiresStableInput,
        TypeDescriptor<? extends BoundedWindow> windowT,
        List<Parameter> extraParameters) {
      return new AutoValue_DoFnSignature_OnWindowExpirationMethod(
          targetMethod,
          requiresStableInput,
          windowT,
          Collections.unmodifiableList(extraParameters));
    }
  }

  /**
   * Describes a timer declaration; a field of type {@link TimerSpec} annotated with {@link
   * DoFn.TimerId}.
   */
  @AutoValue
  public abstract static class TimerDeclaration {

    public static final String PREFIX = "ts-";

    public abstract String id();

    public abstract Field field();

    static TimerDeclaration create(String id, Field field) {
      return new AutoValue_DoFnSignature_TimerDeclaration(id, field);
    }
  }

  /**
   * Describes a timer family declaration; a field of type {@link TimerSpec} annotated with {@link
   * DoFn.TimerFamily}.
   */
  @AutoValue
  public abstract static class TimerFamilyDeclaration {

    public static final String PREFIX = "tfs-";

    public abstract String id();

    public abstract Field field();

    static TimerFamilyDeclaration create(String id, Field field) {
      return new AutoValue_DoFnSignature_TimerFamilyDeclaration(id, field);
    }
  }

  /** Describes a {@link DoFn.StartBundle} or {@link DoFn.FinishBundle} method. */
  @AutoValue
  public abstract static class BundleMethod implements MethodWithExtraParameters {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Types of optional parameters of the annotated method, in the order they appear. */
    @Override
    public abstract List<Parameter> extraParameters();

    /** The type of window expected by this method, if any. */
    @Override
    @Nullable
    public abstract TypeDescriptor<? extends BoundedWindow> windowT();

    static BundleMethod create(Method targetMethod, List<Parameter> extraParameters) {
      /* start bundle/finish bundle currently do not get invoked on a per window basis and can't accept a BoundedWindow parameter */
      return new AutoValue_DoFnSignature_BundleMethod(targetMethod, extraParameters, null);
    }
  }

  /**
   * Describes a state declaration; a field of type {@link StateSpec} annotated with {@link
   * DoFn.StateId}.
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

  /**
   * Decscribes a field access declaration. This is used when the input {@link PCollection} has an
   * associated schema, to specify exactly which fields in the row are accessed. Any fields not
   * specified are not guaranteed to be present when reading the row.
   */
  @AutoValue
  public abstract static class FieldAccessDeclaration {
    public abstract String id();

    public abstract Field field();

    static FieldAccessDeclaration create(String id, Field field) {
      field.setAccessible(true);
      return new AutoValue_DoFnSignature_FieldAccessDeclaration(id, field);
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
  public abstract static class GetInitialRestrictionMethod implements MethodWithExtraParameters {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Type of the returned restriction. */
    public abstract TypeDescriptor<?> restrictionT();

    /** The window type used by this method, if any. */
    @Nullable
    @Override
    public abstract TypeDescriptor<? extends BoundedWindow> windowT();

    /** Types of optional parameters of the annotated method, in the order they appear. */
    @Override
    public abstract List<Parameter> extraParameters();

    static GetInitialRestrictionMethod create(
        Method targetMethod,
        TypeDescriptor<?> restrictionT,
        TypeDescriptor<? extends BoundedWindow> windowT,
        List<Parameter> extraParameters) {
      return new AutoValue_DoFnSignature_GetInitialRestrictionMethod(
          targetMethod, restrictionT, windowT, extraParameters);
    }
  }

  /** Describes a {@link DoFn.SplitRestriction} method. */
  @AutoValue
  public abstract static class SplitRestrictionMethod implements MethodWithExtraParameters {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** The window type used by this method, if any. */
    @Nullable
    @Override
    public abstract TypeDescriptor<? extends BoundedWindow> windowT();

    /** Types of parameters of the annotated method, in the order they appear. */
    @Override
    public abstract List<Parameter> extraParameters();

    static SplitRestrictionMethod create(
        Method targetMethod,
        TypeDescriptor<? extends BoundedWindow> windowT,
        List<Parameter> extraParameters) {
      return new AutoValue_DoFnSignature_SplitRestrictionMethod(
          targetMethod, windowT, extraParameters);
    }
  }

  /** Describes a {@link TruncateRestriction} method. */
  @AutoValue
  public abstract static class TruncateRestrictionMethod implements MethodWithExtraParameters {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** The window type used by this method, if any. */
    @Nullable
    @Override
    public abstract TypeDescriptor<? extends BoundedWindow> windowT();

    /** Types of parameters of the annotated method, in the order they appear. */
    @Override
    public abstract List<Parameter> extraParameters();

    static TruncateRestrictionMethod create(
        Method targetMethod,
        TypeDescriptor<? extends BoundedWindow> windowT,
        List<Parameter> extraParameters) {
      return new AutoValue_DoFnSignature_TruncateRestrictionMethod(
          targetMethod, windowT, extraParameters);
    }
  }

  /** Describes a {@link DoFn.NewTracker} method. */
  @AutoValue
  public abstract static class NewTrackerMethod implements MethodWithExtraParameters {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Type of the returned {@link RestrictionTracker}. */
    public abstract TypeDescriptor<?> trackerT();

    /** The window type used by this method, if any. */
    @Nullable
    @Override
    public abstract TypeDescriptor<? extends BoundedWindow> windowT();

    /** Types of optional parameters of the annotated method, in the order they appear. */
    @Override
    public abstract List<Parameter> extraParameters();

    static NewTrackerMethod create(
        Method targetMethod,
        TypeDescriptor<?> trackerT,
        TypeDescriptor<? extends BoundedWindow> windowT,
        List<Parameter> extraParameters) {
      return new AutoValue_DoFnSignature_NewTrackerMethod(
          targetMethod, trackerT, windowT, extraParameters);
    }
  }

  /** Describes a {@link DoFn.GetSize} method. */
  @AutoValue
  public abstract static class GetSizeMethod implements MethodWithExtraParameters {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** The window type used by this method, if any. */
    @Nullable
    @Override
    public abstract TypeDescriptor<? extends BoundedWindow> windowT();

    /** Types of optional parameters of the annotated method, in the order they appear. */
    @Override
    public abstract List<Parameter> extraParameters();

    static GetSizeMethod create(
        Method targetMethod,
        TypeDescriptor<? extends BoundedWindow> windowT,
        List<Parameter> extraParameters) {
      return new AutoValue_DoFnSignature_GetSizeMethod(targetMethod, windowT, extraParameters);
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

  /** Describes a {@link DoFn.GetInitialWatermarkEstimatorState} method. */
  @AutoValue
  public abstract static class GetInitialWatermarkEstimatorStateMethod
      implements MethodWithExtraParameters {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Type of the returned watermark estimator state. */
    public abstract TypeDescriptor<?> watermarkEstimatorStateT();

    /** The window type used by this method, if any. */
    @Nullable
    @Override
    public abstract TypeDescriptor<? extends BoundedWindow> windowT();

    /** Types of optional parameters of the annotated method, in the order they appear. */
    @Override
    public abstract List<Parameter> extraParameters();

    static GetInitialWatermarkEstimatorStateMethod create(
        Method targetMethod,
        TypeDescriptor<?> watermarkEstimatorStateT,
        TypeDescriptor<? extends BoundedWindow> windowT,
        List<Parameter> extraParameters) {
      return new AutoValue_DoFnSignature_GetInitialWatermarkEstimatorStateMethod(
          targetMethod, watermarkEstimatorStateT, windowT, extraParameters);
    }
  }

  /** Describes a {@link DoFn.NewWatermarkEstimator} method. */
  @AutoValue
  public abstract static class NewWatermarkEstimatorMethod implements MethodWithExtraParameters {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Type of the returned {@link WatermarkEstimator}. */
    public abstract TypeDescriptor<?> watermarkEstimatorT();

    /** The window type used by this method, if any. */
    @Nullable
    @Override
    public abstract TypeDescriptor<? extends BoundedWindow> windowT();

    /** Types of optional parameters of the annotated method, in the order they appear. */
    @Override
    public abstract List<Parameter> extraParameters();

    static NewWatermarkEstimatorMethod create(
        Method targetMethod,
        TypeDescriptor<?> watermarkEstimatorT,
        TypeDescriptor<? extends BoundedWindow> windowT,
        List<Parameter> extraParameters) {
      return new AutoValue_DoFnSignature_NewWatermarkEstimatorMethod(
          targetMethod, watermarkEstimatorT, windowT, extraParameters);
    }
  }

  /** Describes a {@link DoFn.GetRestrictionCoder} method. */
  @AutoValue
  public abstract static class GetWatermarkEstimatorStateCoderMethod implements DoFnMethod {
    /** The annotated method itself. */
    @Override
    public abstract Method targetMethod();

    /** Type of the returned {@link Coder}. */
    public abstract TypeDescriptor<?> coderT();

    static GetWatermarkEstimatorStateCoderMethod create(
        Method targetMethod, TypeDescriptor<?> coderT) {
      return new AutoValue_DoFnSignature_GetWatermarkEstimatorStateCoderMethod(
          targetMethod, coderT);
    }
  }
}
