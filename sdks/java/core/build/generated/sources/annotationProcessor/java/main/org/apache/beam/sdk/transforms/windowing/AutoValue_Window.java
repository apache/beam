package org.apache.beam.sdk.transforms.windowing;

import javax.annotation.Generated;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Window<T> extends Window<T> {

  private final @Nullable WindowFn<? super T, ?> windowFn;

  private final @Nullable Trigger trigger;

  private final WindowingStrategy.@Nullable AccumulationMode accumulationMode;

  private final @Nullable Duration allowedLateness;

  private final Window.@Nullable ClosingBehavior closingBehavior;

  private final Window.@Nullable OnTimeBehavior onTimeBehavior;

  private final @Nullable TimestampCombiner timestampCombiner;

  private AutoValue_Window(
      @Nullable WindowFn<? super T, ?> windowFn,
      @Nullable Trigger trigger,
      WindowingStrategy.@Nullable AccumulationMode accumulationMode,
      @Nullable Duration allowedLateness,
      Window.@Nullable ClosingBehavior closingBehavior,
      Window.@Nullable OnTimeBehavior onTimeBehavior,
      @Nullable TimestampCombiner timestampCombiner) {
    this.windowFn = windowFn;
    this.trigger = trigger;
    this.accumulationMode = accumulationMode;
    this.allowedLateness = allowedLateness;
    this.closingBehavior = closingBehavior;
    this.onTimeBehavior = onTimeBehavior;
    this.timestampCombiner = timestampCombiner;
  }

  @Override
  public @Nullable WindowFn<? super T, ?> getWindowFn() {
    return windowFn;
  }

  @Override
  @Nullable Trigger getTrigger() {
    return trigger;
  }

  @Override
  WindowingStrategy.@Nullable AccumulationMode getAccumulationMode() {
    return accumulationMode;
  }

  @Override
  @Nullable Duration getAllowedLateness() {
    return allowedLateness;
  }

  @Override
  Window.@Nullable ClosingBehavior getClosingBehavior() {
    return closingBehavior;
  }

  @Override
  Window.@Nullable OnTimeBehavior getOnTimeBehavior() {
    return onTimeBehavior;
  }

  @Override
  @Nullable TimestampCombiner getTimestampCombiner() {
    return timestampCombiner;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Window) {
      Window<?> that = (Window<?>) o;
      return (this.windowFn == null ? that.getWindowFn() == null : this.windowFn.equals(that.getWindowFn()))
          && (this.trigger == null ? that.getTrigger() == null : this.trigger.equals(that.getTrigger()))
          && (this.accumulationMode == null ? that.getAccumulationMode() == null : this.accumulationMode.equals(that.getAccumulationMode()))
          && (this.allowedLateness == null ? that.getAllowedLateness() == null : this.allowedLateness.equals(that.getAllowedLateness()))
          && (this.closingBehavior == null ? that.getClosingBehavior() == null : this.closingBehavior.equals(that.getClosingBehavior()))
          && (this.onTimeBehavior == null ? that.getOnTimeBehavior() == null : this.onTimeBehavior.equals(that.getOnTimeBehavior()))
          && (this.timestampCombiner == null ? that.getTimestampCombiner() == null : this.timestampCombiner.equals(that.getTimestampCombiner()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (windowFn == null) ? 0 : windowFn.hashCode();
    h$ *= 1000003;
    h$ ^= (trigger == null) ? 0 : trigger.hashCode();
    h$ *= 1000003;
    h$ ^= (accumulationMode == null) ? 0 : accumulationMode.hashCode();
    h$ *= 1000003;
    h$ ^= (allowedLateness == null) ? 0 : allowedLateness.hashCode();
    h$ *= 1000003;
    h$ ^= (closingBehavior == null) ? 0 : closingBehavior.hashCode();
    h$ *= 1000003;
    h$ ^= (onTimeBehavior == null) ? 0 : onTimeBehavior.hashCode();
    h$ *= 1000003;
    h$ ^= (timestampCombiner == null) ? 0 : timestampCombiner.hashCode();
    return h$;
  }

  @Override
  Window.Builder<T> toBuilder() {
    return new Builder<T>(this);
  }

  static final class Builder<T> extends Window.Builder<T> {
    private @Nullable WindowFn<? super T, ?> windowFn;
    private @Nullable Trigger trigger;
    private WindowingStrategy.@Nullable AccumulationMode accumulationMode;
    private @Nullable Duration allowedLateness;
    private Window.@Nullable ClosingBehavior closingBehavior;
    private Window.@Nullable OnTimeBehavior onTimeBehavior;
    private @Nullable TimestampCombiner timestampCombiner;
    Builder() {
    }
    private Builder(Window<T> source) {
      this.windowFn = source.getWindowFn();
      this.trigger = source.getTrigger();
      this.accumulationMode = source.getAccumulationMode();
      this.allowedLateness = source.getAllowedLateness();
      this.closingBehavior = source.getClosingBehavior();
      this.onTimeBehavior = source.getOnTimeBehavior();
      this.timestampCombiner = source.getTimestampCombiner();
    }
    @Override
    Window.Builder<T> setWindowFn(WindowFn<? super T, ?> windowFn) {
      this.windowFn = windowFn;
      return this;
    }
    @Override
    Window.Builder<T> setTrigger(Trigger trigger) {
      this.trigger = trigger;
      return this;
    }
    @Override
    Window.Builder<T> setAccumulationMode(WindowingStrategy.AccumulationMode accumulationMode) {
      this.accumulationMode = accumulationMode;
      return this;
    }
    @Override
    Window.Builder<T> setAllowedLateness(Duration allowedLateness) {
      this.allowedLateness = allowedLateness;
      return this;
    }
    @Override
    Window.Builder<T> setClosingBehavior(Window.ClosingBehavior closingBehavior) {
      this.closingBehavior = closingBehavior;
      return this;
    }
    @Override
    Window.Builder<T> setOnTimeBehavior(Window.OnTimeBehavior onTimeBehavior) {
      this.onTimeBehavior = onTimeBehavior;
      return this;
    }
    @Override
    Window.Builder<T> setTimestampCombiner(TimestampCombiner timestampCombiner) {
      this.timestampCombiner = timestampCombiner;
      return this;
    }
    @Override
    Window<T> build() {
      return new AutoValue_Window<T>(
          this.windowFn,
          this.trigger,
          this.accumulationMode,
          this.allowedLateness,
          this.closingBehavior,
          this.onTimeBehavior,
          this.timestampCombiner);
    }
  }

}
