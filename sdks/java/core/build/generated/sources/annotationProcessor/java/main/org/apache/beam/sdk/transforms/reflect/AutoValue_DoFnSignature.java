package org.apache.beam.sdk.transforms.reflect;

import java.util.Map;
import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature extends DoFnSignature {

  private final Class<? extends DoFn<?, ?>> fnClass;

  private final PCollection.IsBounded isBoundedPerElement;

  private final DoFnSignature.ProcessElementMethod processElement;

  private final Map<String, DoFnSignature.StateDeclaration> stateDeclarations;

  private final DoFnSignature.@Nullable BundleMethod startBundle;

  private final DoFnSignature.@Nullable BundleMethod finishBundle;

  private final DoFnSignature.@Nullable LifecycleMethod setup;

  private final DoFnSignature.@Nullable LifecycleMethod teardown;

  private final DoFnSignature.@Nullable OnWindowExpirationMethod onWindowExpiration;

  private final Map<String, DoFnSignature.TimerDeclaration> timerDeclarations;

  private final Map<String, DoFnSignature.TimerFamilyDeclaration> timerFamilyDeclarations;

  private final @Nullable Map<String, DoFnSignature.FieldAccessDeclaration> fieldAccessDeclarations;

  private final DoFnSignature.@Nullable GetInitialRestrictionMethod getInitialRestriction;

  private final DoFnSignature.@Nullable SplitRestrictionMethod splitRestriction;

  private final DoFnSignature.@Nullable TruncateRestrictionMethod truncateRestriction;

  private final DoFnSignature.@Nullable GetRestrictionCoderMethod getRestrictionCoder;

  private final DoFnSignature.@Nullable GetWatermarkEstimatorStateCoderMethod getWatermarkEstimatorStateCoder;

  private final DoFnSignature.@Nullable GetInitialWatermarkEstimatorStateMethod getInitialWatermarkEstimatorState;

  private final DoFnSignature.@Nullable NewWatermarkEstimatorMethod newWatermarkEstimator;

  private final DoFnSignature.@Nullable NewTrackerMethod newTracker;

  private final DoFnSignature.@Nullable GetSizeMethod getSize;

  private final @Nullable Map<String, DoFnSignature.OnTimerMethod> onTimerMethods;

  private final @Nullable Map<String, DoFnSignature.OnTimerFamilyMethod> onTimerFamilyMethods;

  private AutoValue_DoFnSignature(
      Class<? extends DoFn<?, ?>> fnClass,
      PCollection.IsBounded isBoundedPerElement,
      DoFnSignature.ProcessElementMethod processElement,
      Map<String, DoFnSignature.StateDeclaration> stateDeclarations,
      DoFnSignature.@Nullable BundleMethod startBundle,
      DoFnSignature.@Nullable BundleMethod finishBundle,
      DoFnSignature.@Nullable LifecycleMethod setup,
      DoFnSignature.@Nullable LifecycleMethod teardown,
      DoFnSignature.@Nullable OnWindowExpirationMethod onWindowExpiration,
      Map<String, DoFnSignature.TimerDeclaration> timerDeclarations,
      Map<String, DoFnSignature.TimerFamilyDeclaration> timerFamilyDeclarations,
      @Nullable Map<String, DoFnSignature.FieldAccessDeclaration> fieldAccessDeclarations,
      DoFnSignature.@Nullable GetInitialRestrictionMethod getInitialRestriction,
      DoFnSignature.@Nullable SplitRestrictionMethod splitRestriction,
      DoFnSignature.@Nullable TruncateRestrictionMethod truncateRestriction,
      DoFnSignature.@Nullable GetRestrictionCoderMethod getRestrictionCoder,
      DoFnSignature.@Nullable GetWatermarkEstimatorStateCoderMethod getWatermarkEstimatorStateCoder,
      DoFnSignature.@Nullable GetInitialWatermarkEstimatorStateMethod getInitialWatermarkEstimatorState,
      DoFnSignature.@Nullable NewWatermarkEstimatorMethod newWatermarkEstimator,
      DoFnSignature.@Nullable NewTrackerMethod newTracker,
      DoFnSignature.@Nullable GetSizeMethod getSize,
      @Nullable Map<String, DoFnSignature.OnTimerMethod> onTimerMethods,
      @Nullable Map<String, DoFnSignature.OnTimerFamilyMethod> onTimerFamilyMethods) {
    this.fnClass = fnClass;
    this.isBoundedPerElement = isBoundedPerElement;
    this.processElement = processElement;
    this.stateDeclarations = stateDeclarations;
    this.startBundle = startBundle;
    this.finishBundle = finishBundle;
    this.setup = setup;
    this.teardown = teardown;
    this.onWindowExpiration = onWindowExpiration;
    this.timerDeclarations = timerDeclarations;
    this.timerFamilyDeclarations = timerFamilyDeclarations;
    this.fieldAccessDeclarations = fieldAccessDeclarations;
    this.getInitialRestriction = getInitialRestriction;
    this.splitRestriction = splitRestriction;
    this.truncateRestriction = truncateRestriction;
    this.getRestrictionCoder = getRestrictionCoder;
    this.getWatermarkEstimatorStateCoder = getWatermarkEstimatorStateCoder;
    this.getInitialWatermarkEstimatorState = getInitialWatermarkEstimatorState;
    this.newWatermarkEstimator = newWatermarkEstimator;
    this.newTracker = newTracker;
    this.getSize = getSize;
    this.onTimerMethods = onTimerMethods;
    this.onTimerFamilyMethods = onTimerFamilyMethods;
  }

  @Override
  public Class<? extends DoFn<?, ?>> fnClass() {
    return fnClass;
  }

  @Override
  public PCollection.IsBounded isBoundedPerElement() {
    return isBoundedPerElement;
  }

  @Override
  public DoFnSignature.ProcessElementMethod processElement() {
    return processElement;
  }

  @Override
  public Map<String, DoFnSignature.StateDeclaration> stateDeclarations() {
    return stateDeclarations;
  }

  @Override
  public DoFnSignature.@Nullable BundleMethod startBundle() {
    return startBundle;
  }

  @Override
  public DoFnSignature.@Nullable BundleMethod finishBundle() {
    return finishBundle;
  }

  @Override
  public DoFnSignature.@Nullable LifecycleMethod setup() {
    return setup;
  }

  @Override
  public DoFnSignature.@Nullable LifecycleMethod teardown() {
    return teardown;
  }

  @Override
  public DoFnSignature.@Nullable OnWindowExpirationMethod onWindowExpiration() {
    return onWindowExpiration;
  }

  @Override
  public Map<String, DoFnSignature.TimerDeclaration> timerDeclarations() {
    return timerDeclarations;
  }

  @Override
  public Map<String, DoFnSignature.TimerFamilyDeclaration> timerFamilyDeclarations() {
    return timerFamilyDeclarations;
  }

  @Override
  public @Nullable Map<String, DoFnSignature.FieldAccessDeclaration> fieldAccessDeclarations() {
    return fieldAccessDeclarations;
  }

  @Override
  public DoFnSignature.@Nullable GetInitialRestrictionMethod getInitialRestriction() {
    return getInitialRestriction;
  }

  @Override
  public DoFnSignature.@Nullable SplitRestrictionMethod splitRestriction() {
    return splitRestriction;
  }

  @Override
  public DoFnSignature.@Nullable TruncateRestrictionMethod truncateRestriction() {
    return truncateRestriction;
  }

  @Override
  public DoFnSignature.@Nullable GetRestrictionCoderMethod getRestrictionCoder() {
    return getRestrictionCoder;
  }

  @Override
  public DoFnSignature.@Nullable GetWatermarkEstimatorStateCoderMethod getWatermarkEstimatorStateCoder() {
    return getWatermarkEstimatorStateCoder;
  }

  @Override
  public DoFnSignature.@Nullable GetInitialWatermarkEstimatorStateMethod getInitialWatermarkEstimatorState() {
    return getInitialWatermarkEstimatorState;
  }

  @Override
  public DoFnSignature.@Nullable NewWatermarkEstimatorMethod newWatermarkEstimator() {
    return newWatermarkEstimator;
  }

  @Override
  public DoFnSignature.@Nullable NewTrackerMethod newTracker() {
    return newTracker;
  }

  @Override
  public DoFnSignature.@Nullable GetSizeMethod getSize() {
    return getSize;
  }

  @Override
  public @Nullable Map<String, DoFnSignature.OnTimerMethod> onTimerMethods() {
    return onTimerMethods;
  }

  @Override
  public @Nullable Map<String, DoFnSignature.OnTimerFamilyMethod> onTimerFamilyMethods() {
    return onTimerFamilyMethods;
  }

  @Override
  public String toString() {
    return "DoFnSignature{"
        + "fnClass=" + fnClass + ", "
        + "isBoundedPerElement=" + isBoundedPerElement + ", "
        + "processElement=" + processElement + ", "
        + "stateDeclarations=" + stateDeclarations + ", "
        + "startBundle=" + startBundle + ", "
        + "finishBundle=" + finishBundle + ", "
        + "setup=" + setup + ", "
        + "teardown=" + teardown + ", "
        + "onWindowExpiration=" + onWindowExpiration + ", "
        + "timerDeclarations=" + timerDeclarations + ", "
        + "timerFamilyDeclarations=" + timerFamilyDeclarations + ", "
        + "fieldAccessDeclarations=" + fieldAccessDeclarations + ", "
        + "getInitialRestriction=" + getInitialRestriction + ", "
        + "splitRestriction=" + splitRestriction + ", "
        + "truncateRestriction=" + truncateRestriction + ", "
        + "getRestrictionCoder=" + getRestrictionCoder + ", "
        + "getWatermarkEstimatorStateCoder=" + getWatermarkEstimatorStateCoder + ", "
        + "getInitialWatermarkEstimatorState=" + getInitialWatermarkEstimatorState + ", "
        + "newWatermarkEstimator=" + newWatermarkEstimator + ", "
        + "newTracker=" + newTracker + ", "
        + "getSize=" + getSize + ", "
        + "onTimerMethods=" + onTimerMethods + ", "
        + "onTimerFamilyMethods=" + onTimerFamilyMethods
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature) {
      DoFnSignature that = (DoFnSignature) o;
      return this.fnClass.equals(that.fnClass())
          && this.isBoundedPerElement.equals(that.isBoundedPerElement())
          && this.processElement.equals(that.processElement())
          && this.stateDeclarations.equals(that.stateDeclarations())
          && (this.startBundle == null ? that.startBundle() == null : this.startBundle.equals(that.startBundle()))
          && (this.finishBundle == null ? that.finishBundle() == null : this.finishBundle.equals(that.finishBundle()))
          && (this.setup == null ? that.setup() == null : this.setup.equals(that.setup()))
          && (this.teardown == null ? that.teardown() == null : this.teardown.equals(that.teardown()))
          && (this.onWindowExpiration == null ? that.onWindowExpiration() == null : this.onWindowExpiration.equals(that.onWindowExpiration()))
          && this.timerDeclarations.equals(that.timerDeclarations())
          && this.timerFamilyDeclarations.equals(that.timerFamilyDeclarations())
          && (this.fieldAccessDeclarations == null ? that.fieldAccessDeclarations() == null : this.fieldAccessDeclarations.equals(that.fieldAccessDeclarations()))
          && (this.getInitialRestriction == null ? that.getInitialRestriction() == null : this.getInitialRestriction.equals(that.getInitialRestriction()))
          && (this.splitRestriction == null ? that.splitRestriction() == null : this.splitRestriction.equals(that.splitRestriction()))
          && (this.truncateRestriction == null ? that.truncateRestriction() == null : this.truncateRestriction.equals(that.truncateRestriction()))
          && (this.getRestrictionCoder == null ? that.getRestrictionCoder() == null : this.getRestrictionCoder.equals(that.getRestrictionCoder()))
          && (this.getWatermarkEstimatorStateCoder == null ? that.getWatermarkEstimatorStateCoder() == null : this.getWatermarkEstimatorStateCoder.equals(that.getWatermarkEstimatorStateCoder()))
          && (this.getInitialWatermarkEstimatorState == null ? that.getInitialWatermarkEstimatorState() == null : this.getInitialWatermarkEstimatorState.equals(that.getInitialWatermarkEstimatorState()))
          && (this.newWatermarkEstimator == null ? that.newWatermarkEstimator() == null : this.newWatermarkEstimator.equals(that.newWatermarkEstimator()))
          && (this.newTracker == null ? that.newTracker() == null : this.newTracker.equals(that.newTracker()))
          && (this.getSize == null ? that.getSize() == null : this.getSize.equals(that.getSize()))
          && (this.onTimerMethods == null ? that.onTimerMethods() == null : this.onTimerMethods.equals(that.onTimerMethods()))
          && (this.onTimerFamilyMethods == null ? that.onTimerFamilyMethods() == null : this.onTimerFamilyMethods.equals(that.onTimerFamilyMethods()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= fnClass.hashCode();
    h$ *= 1000003;
    h$ ^= isBoundedPerElement.hashCode();
    h$ *= 1000003;
    h$ ^= processElement.hashCode();
    h$ *= 1000003;
    h$ ^= stateDeclarations.hashCode();
    h$ *= 1000003;
    h$ ^= (startBundle == null) ? 0 : startBundle.hashCode();
    h$ *= 1000003;
    h$ ^= (finishBundle == null) ? 0 : finishBundle.hashCode();
    h$ *= 1000003;
    h$ ^= (setup == null) ? 0 : setup.hashCode();
    h$ *= 1000003;
    h$ ^= (teardown == null) ? 0 : teardown.hashCode();
    h$ *= 1000003;
    h$ ^= (onWindowExpiration == null) ? 0 : onWindowExpiration.hashCode();
    h$ *= 1000003;
    h$ ^= timerDeclarations.hashCode();
    h$ *= 1000003;
    h$ ^= timerFamilyDeclarations.hashCode();
    h$ *= 1000003;
    h$ ^= (fieldAccessDeclarations == null) ? 0 : fieldAccessDeclarations.hashCode();
    h$ *= 1000003;
    h$ ^= (getInitialRestriction == null) ? 0 : getInitialRestriction.hashCode();
    h$ *= 1000003;
    h$ ^= (splitRestriction == null) ? 0 : splitRestriction.hashCode();
    h$ *= 1000003;
    h$ ^= (truncateRestriction == null) ? 0 : truncateRestriction.hashCode();
    h$ *= 1000003;
    h$ ^= (getRestrictionCoder == null) ? 0 : getRestrictionCoder.hashCode();
    h$ *= 1000003;
    h$ ^= (getWatermarkEstimatorStateCoder == null) ? 0 : getWatermarkEstimatorStateCoder.hashCode();
    h$ *= 1000003;
    h$ ^= (getInitialWatermarkEstimatorState == null) ? 0 : getInitialWatermarkEstimatorState.hashCode();
    h$ *= 1000003;
    h$ ^= (newWatermarkEstimator == null) ? 0 : newWatermarkEstimator.hashCode();
    h$ *= 1000003;
    h$ ^= (newTracker == null) ? 0 : newTracker.hashCode();
    h$ *= 1000003;
    h$ ^= (getSize == null) ? 0 : getSize.hashCode();
    h$ *= 1000003;
    h$ ^= (onTimerMethods == null) ? 0 : onTimerMethods.hashCode();
    h$ *= 1000003;
    h$ ^= (onTimerFamilyMethods == null) ? 0 : onTimerFamilyMethods.hashCode();
    return h$;
  }

  static final class Builder extends DoFnSignature.Builder {
    private Class<? extends DoFn<?, ?>> fnClass;
    private PCollection.IsBounded isBoundedPerElement;
    private DoFnSignature.ProcessElementMethod processElement;
    private Map<String, DoFnSignature.StateDeclaration> stateDeclarations;
    private DoFnSignature.@Nullable BundleMethod startBundle;
    private DoFnSignature.@Nullable BundleMethod finishBundle;
    private DoFnSignature.@Nullable LifecycleMethod setup;
    private DoFnSignature.@Nullable LifecycleMethod teardown;
    private DoFnSignature.@Nullable OnWindowExpirationMethod onWindowExpiration;
    private Map<String, DoFnSignature.TimerDeclaration> timerDeclarations;
    private Map<String, DoFnSignature.TimerFamilyDeclaration> timerFamilyDeclarations;
    private @Nullable Map<String, DoFnSignature.FieldAccessDeclaration> fieldAccessDeclarations;
    private DoFnSignature.@Nullable GetInitialRestrictionMethod getInitialRestriction;
    private DoFnSignature.@Nullable SplitRestrictionMethod splitRestriction;
    private DoFnSignature.@Nullable TruncateRestrictionMethod truncateRestriction;
    private DoFnSignature.@Nullable GetRestrictionCoderMethod getRestrictionCoder;
    private DoFnSignature.@Nullable GetWatermarkEstimatorStateCoderMethod getWatermarkEstimatorStateCoder;
    private DoFnSignature.@Nullable GetInitialWatermarkEstimatorStateMethod getInitialWatermarkEstimatorState;
    private DoFnSignature.@Nullable NewWatermarkEstimatorMethod newWatermarkEstimator;
    private DoFnSignature.@Nullable NewTrackerMethod newTracker;
    private DoFnSignature.@Nullable GetSizeMethod getSize;
    private @Nullable Map<String, DoFnSignature.OnTimerMethod> onTimerMethods;
    private @Nullable Map<String, DoFnSignature.OnTimerFamilyMethod> onTimerFamilyMethods;
    Builder() {
    }
    @Override
    DoFnSignature.Builder setFnClass(Class<? extends DoFn<?, ?>> fnClass) {
      if (fnClass == null) {
        throw new NullPointerException("Null fnClass");
      }
      this.fnClass = fnClass;
      return this;
    }
    @Override
    DoFnSignature.Builder setIsBoundedPerElement(PCollection.IsBounded isBoundedPerElement) {
      if (isBoundedPerElement == null) {
        throw new NullPointerException("Null isBoundedPerElement");
      }
      this.isBoundedPerElement = isBoundedPerElement;
      return this;
    }
    @Override
    DoFnSignature.Builder setProcessElement(DoFnSignature.ProcessElementMethod processElement) {
      if (processElement == null) {
        throw new NullPointerException("Null processElement");
      }
      this.processElement = processElement;
      return this;
    }
    @Override
    DoFnSignature.Builder setStateDeclarations(Map<String, DoFnSignature.StateDeclaration> stateDeclarations) {
      if (stateDeclarations == null) {
        throw new NullPointerException("Null stateDeclarations");
      }
      this.stateDeclarations = stateDeclarations;
      return this;
    }
    @Override
    DoFnSignature.Builder setStartBundle(DoFnSignature.BundleMethod startBundle) {
      this.startBundle = startBundle;
      return this;
    }
    @Override
    DoFnSignature.Builder setFinishBundle(DoFnSignature.BundleMethod finishBundle) {
      this.finishBundle = finishBundle;
      return this;
    }
    @Override
    DoFnSignature.Builder setSetup(DoFnSignature.LifecycleMethod setup) {
      this.setup = setup;
      return this;
    }
    @Override
    DoFnSignature.Builder setTeardown(DoFnSignature.LifecycleMethod teardown) {
      this.teardown = teardown;
      return this;
    }
    @Override
    DoFnSignature.Builder setOnWindowExpiration(DoFnSignature.OnWindowExpirationMethod onWindowExpiration) {
      this.onWindowExpiration = onWindowExpiration;
      return this;
    }
    @Override
    DoFnSignature.Builder setTimerDeclarations(Map<String, DoFnSignature.TimerDeclaration> timerDeclarations) {
      if (timerDeclarations == null) {
        throw new NullPointerException("Null timerDeclarations");
      }
      this.timerDeclarations = timerDeclarations;
      return this;
    }
    @Override
    DoFnSignature.Builder setTimerFamilyDeclarations(Map<String, DoFnSignature.TimerFamilyDeclaration> timerFamilyDeclarations) {
      if (timerFamilyDeclarations == null) {
        throw new NullPointerException("Null timerFamilyDeclarations");
      }
      this.timerFamilyDeclarations = timerFamilyDeclarations;
      return this;
    }
    @Override
    DoFnSignature.Builder setFieldAccessDeclarations(Map<String, DoFnSignature.FieldAccessDeclaration> fieldAccessDeclarations) {
      this.fieldAccessDeclarations = fieldAccessDeclarations;
      return this;
    }
    @Override
    DoFnSignature.Builder setGetInitialRestriction(DoFnSignature.GetInitialRestrictionMethod getInitialRestriction) {
      this.getInitialRestriction = getInitialRestriction;
      return this;
    }
    @Override
    DoFnSignature.Builder setSplitRestriction(DoFnSignature.SplitRestrictionMethod splitRestriction) {
      this.splitRestriction = splitRestriction;
      return this;
    }
    @Override
    DoFnSignature.Builder setTruncateRestriction(DoFnSignature.TruncateRestrictionMethod truncateRestriction) {
      this.truncateRestriction = truncateRestriction;
      return this;
    }
    @Override
    DoFnSignature.Builder setGetRestrictionCoder(DoFnSignature.GetRestrictionCoderMethod getRestrictionCoder) {
      this.getRestrictionCoder = getRestrictionCoder;
      return this;
    }
    @Override
    DoFnSignature.Builder setGetWatermarkEstimatorStateCoder(DoFnSignature.GetWatermarkEstimatorStateCoderMethod getWatermarkEstimatorStateCoder) {
      this.getWatermarkEstimatorStateCoder = getWatermarkEstimatorStateCoder;
      return this;
    }
    @Override
    DoFnSignature.Builder setGetInitialWatermarkEstimatorState(DoFnSignature.GetInitialWatermarkEstimatorStateMethod getInitialWatermarkEstimatorState) {
      this.getInitialWatermarkEstimatorState = getInitialWatermarkEstimatorState;
      return this;
    }
    @Override
    DoFnSignature.Builder setNewWatermarkEstimator(DoFnSignature.NewWatermarkEstimatorMethod newWatermarkEstimator) {
      this.newWatermarkEstimator = newWatermarkEstimator;
      return this;
    }
    @Override
    DoFnSignature.Builder setNewTracker(DoFnSignature.NewTrackerMethod newTracker) {
      this.newTracker = newTracker;
      return this;
    }
    @Override
    DoFnSignature.Builder setGetSize(DoFnSignature.GetSizeMethod getSize) {
      this.getSize = getSize;
      return this;
    }
    @Override
    DoFnSignature.Builder setOnTimerMethods(Map<String, DoFnSignature.OnTimerMethod> onTimerMethods) {
      this.onTimerMethods = onTimerMethods;
      return this;
    }
    @Override
    DoFnSignature.Builder setOnTimerFamilyMethods(Map<String, DoFnSignature.OnTimerFamilyMethod> onTimerFamilyMethods) {
      this.onTimerFamilyMethods = onTimerFamilyMethods;
      return this;
    }
    @Override
    DoFnSignature build() {
      if (this.fnClass == null
          || this.isBoundedPerElement == null
          || this.processElement == null
          || this.stateDeclarations == null
          || this.timerDeclarations == null
          || this.timerFamilyDeclarations == null) {
        StringBuilder missing = new StringBuilder();
        if (this.fnClass == null) {
          missing.append(" fnClass");
        }
        if (this.isBoundedPerElement == null) {
          missing.append(" isBoundedPerElement");
        }
        if (this.processElement == null) {
          missing.append(" processElement");
        }
        if (this.stateDeclarations == null) {
          missing.append(" stateDeclarations");
        }
        if (this.timerDeclarations == null) {
          missing.append(" timerDeclarations");
        }
        if (this.timerFamilyDeclarations == null) {
          missing.append(" timerFamilyDeclarations");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_DoFnSignature(
          this.fnClass,
          this.isBoundedPerElement,
          this.processElement,
          this.stateDeclarations,
          this.startBundle,
          this.finishBundle,
          this.setup,
          this.teardown,
          this.onWindowExpiration,
          this.timerDeclarations,
          this.timerFamilyDeclarations,
          this.fieldAccessDeclarations,
          this.getInitialRestriction,
          this.splitRestriction,
          this.truncateRestriction,
          this.getRestrictionCoder,
          this.getWatermarkEstimatorStateCoder,
          this.getInitialWatermarkEstimatorState,
          this.newWatermarkEstimator,
          this.newTracker,
          this.getSize,
          this.onTimerMethods,
          this.onTimerFamilyMethods);
    }
  }

}
