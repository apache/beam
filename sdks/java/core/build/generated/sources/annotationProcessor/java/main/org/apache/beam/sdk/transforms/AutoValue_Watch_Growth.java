package org.apache.beam.sdk.transforms;

import javax.annotation.Generated;
import org.apache.beam.sdk.coders.Coder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Watch_Growth<InputT, OutputT, KeyT> extends Watch.Growth<InputT, OutputT, KeyT> {

  private final Contextful<Watch.Growth.PollFn<InputT, OutputT>> pollFn;

  private final @Nullable SerializableFunction<OutputT, KeyT> outputKeyFn;

  private final @Nullable Coder<KeyT> outputKeyCoder;

  private final @Nullable Duration pollInterval;

  private final Watch.Growth.@Nullable TerminationCondition<InputT, ?> terminationPerInput;

  private final @Nullable Coder<OutputT> outputCoder;

  private AutoValue_Watch_Growth(
      Contextful<Watch.Growth.PollFn<InputT, OutputT>> pollFn,
      @Nullable SerializableFunction<OutputT, KeyT> outputKeyFn,
      @Nullable Coder<KeyT> outputKeyCoder,
      @Nullable Duration pollInterval,
      Watch.Growth.@Nullable TerminationCondition<InputT, ?> terminationPerInput,
      @Nullable Coder<OutputT> outputCoder) {
    this.pollFn = pollFn;
    this.outputKeyFn = outputKeyFn;
    this.outputKeyCoder = outputKeyCoder;
    this.pollInterval = pollInterval;
    this.terminationPerInput = terminationPerInput;
    this.outputCoder = outputCoder;
  }

  @Override
  Contextful<Watch.Growth.PollFn<InputT, OutputT>> getPollFn() {
    return pollFn;
  }

  @Override
  @Nullable SerializableFunction<OutputT, KeyT> getOutputKeyFn() {
    return outputKeyFn;
  }

  @Override
  @Nullable Coder<KeyT> getOutputKeyCoder() {
    return outputKeyCoder;
  }

  @Override
  @Nullable Duration getPollInterval() {
    return pollInterval;
  }

  @Override
  Watch.Growth.@Nullable TerminationCondition<InputT, ?> getTerminationPerInput() {
    return terminationPerInput;
  }

  @Override
  @Nullable Coder<OutputT> getOutputCoder() {
    return outputCoder;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Watch.Growth) {
      Watch.Growth<?, ?, ?> that = (Watch.Growth<?, ?, ?>) o;
      return this.pollFn.equals(that.getPollFn())
          && (this.outputKeyFn == null ? that.getOutputKeyFn() == null : this.outputKeyFn.equals(that.getOutputKeyFn()))
          && (this.outputKeyCoder == null ? that.getOutputKeyCoder() == null : this.outputKeyCoder.equals(that.getOutputKeyCoder()))
          && (this.pollInterval == null ? that.getPollInterval() == null : this.pollInterval.equals(that.getPollInterval()))
          && (this.terminationPerInput == null ? that.getTerminationPerInput() == null : this.terminationPerInput.equals(that.getTerminationPerInput()))
          && (this.outputCoder == null ? that.getOutputCoder() == null : this.outputCoder.equals(that.getOutputCoder()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= pollFn.hashCode();
    h$ *= 1000003;
    h$ ^= (outputKeyFn == null) ? 0 : outputKeyFn.hashCode();
    h$ *= 1000003;
    h$ ^= (outputKeyCoder == null) ? 0 : outputKeyCoder.hashCode();
    h$ *= 1000003;
    h$ ^= (pollInterval == null) ? 0 : pollInterval.hashCode();
    h$ *= 1000003;
    h$ ^= (terminationPerInput == null) ? 0 : terminationPerInput.hashCode();
    h$ *= 1000003;
    h$ ^= (outputCoder == null) ? 0 : outputCoder.hashCode();
    return h$;
  }

  @Override
  Watch.Growth.Builder<InputT, OutputT, KeyT> toBuilder() {
    return new Builder<InputT, OutputT, KeyT>(this);
  }

  static final class Builder<InputT, OutputT, KeyT> extends Watch.Growth.Builder<InputT, OutputT, KeyT> {
    private Contextful<Watch.Growth.PollFn<InputT, OutputT>> pollFn;
    private @Nullable SerializableFunction<OutputT, KeyT> outputKeyFn;
    private @Nullable Coder<KeyT> outputKeyCoder;
    private @Nullable Duration pollInterval;
    private Watch.Growth.@Nullable TerminationCondition<InputT, ?> terminationPerInput;
    private @Nullable Coder<OutputT> outputCoder;
    Builder() {
    }
    private Builder(Watch.Growth<InputT, OutputT, KeyT> source) {
      this.pollFn = source.getPollFn();
      this.outputKeyFn = source.getOutputKeyFn();
      this.outputKeyCoder = source.getOutputKeyCoder();
      this.pollInterval = source.getPollInterval();
      this.terminationPerInput = source.getTerminationPerInput();
      this.outputCoder = source.getOutputCoder();
    }
    @Override
    Watch.Growth.Builder<InputT, OutputT, KeyT> setPollFn(Contextful<Watch.Growth.PollFn<InputT, OutputT>> pollFn) {
      if (pollFn == null) {
        throw new NullPointerException("Null pollFn");
      }
      this.pollFn = pollFn;
      return this;
    }
    @Override
    Watch.Growth.Builder<InputT, OutputT, KeyT> setOutputKeyFn(@Nullable SerializableFunction<OutputT, KeyT> outputKeyFn) {
      this.outputKeyFn = outputKeyFn;
      return this;
    }
    @Override
    Watch.Growth.Builder<InputT, OutputT, KeyT> setOutputKeyCoder(Coder<KeyT> outputKeyCoder) {
      this.outputKeyCoder = outputKeyCoder;
      return this;
    }
    @Override
    Watch.Growth.Builder<InputT, OutputT, KeyT> setPollInterval(Duration pollInterval) {
      this.pollInterval = pollInterval;
      return this;
    }
    @Override
    Watch.Growth.Builder<InputT, OutputT, KeyT> setTerminationPerInput(Watch.Growth.TerminationCondition<InputT, ?> terminationPerInput) {
      this.terminationPerInput = terminationPerInput;
      return this;
    }
    @Override
    Watch.Growth.Builder<InputT, OutputT, KeyT> setOutputCoder(Coder<OutputT> outputCoder) {
      this.outputCoder = outputCoder;
      return this;
    }
    @Override
    Watch.Growth<InputT, OutputT, KeyT> build() {
      if (this.pollFn == null) {
        String missing = " pollFn";
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_Watch_Growth<InputT, OutputT, KeyT>(
          this.pollFn,
          this.outputKeyFn,
          this.outputKeyCoder,
          this.pollInterval,
          this.terminationPerInput,
          this.outputCoder);
    }
  }

}
