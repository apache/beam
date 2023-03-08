package org.apache.beam.sdk.transforms;

import javax.annotation.Generated;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_WithFailures_Result<OutputT extends POutput, FailureElementT> extends WithFailures.Result<OutputT, FailureElementT> {

  private final OutputT output;

  private final @Nullable TupleTag<?> outputTag;

  private final PCollection<FailureElementT> failures;

  private final TupleTag<FailureElementT> failuresTag;

  AutoValue_WithFailures_Result(
      OutputT output,
      @Nullable TupleTag<?> outputTag,
      PCollection<FailureElementT> failures,
      TupleTag<FailureElementT> failuresTag) {
    if (output == null) {
      throw new NullPointerException("Null output");
    }
    this.output = output;
    this.outputTag = outputTag;
    if (failures == null) {
      throw new NullPointerException("Null failures");
    }
    this.failures = failures;
    if (failuresTag == null) {
      throw new NullPointerException("Null failuresTag");
    }
    this.failuresTag = failuresTag;
  }

  @Override
  public OutputT output() {
    return output;
  }

  @Override
  @Nullable TupleTag<?> outputTag() {
    return outputTag;
  }

  @Override
  public PCollection<FailureElementT> failures() {
    return failures;
  }

  @Override
  TupleTag<FailureElementT> failuresTag() {
    return failuresTag;
  }

  @Override
  public String toString() {
    return "Result{"
        + "output=" + output + ", "
        + "outputTag=" + outputTag + ", "
        + "failures=" + failures + ", "
        + "failuresTag=" + failuresTag
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof WithFailures.Result) {
      WithFailures.Result<?, ?> that = (WithFailures.Result<?, ?>) o;
      return this.output.equals(that.output())
          && (this.outputTag == null ? that.outputTag() == null : this.outputTag.equals(that.outputTag()))
          && this.failures.equals(that.failures())
          && this.failuresTag.equals(that.failuresTag());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= output.hashCode();
    h$ *= 1000003;
    h$ ^= (outputTag == null) ? 0 : outputTag.hashCode();
    h$ *= 1000003;
    h$ ^= failures.hashCode();
    h$ *= 1000003;
    h$ ^= failuresTag.hashCode();
    return h$;
  }

}
