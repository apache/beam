package org.apache.beam.sdk.schemas.transforms;

import java.util.List;
import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_CoGroup_Result extends CoGroup.Result {

  private final Row key;

  private final List<Iterable<Row>> iterables;

  private final List<String> tags;

  private final CoGroup.JoinArguments joinArguments;

  private final Schema outputSchema;

  AutoValue_CoGroup_Result(
      Row key,
      List<Iterable<Row>> iterables,
      List<String> tags,
      CoGroup.JoinArguments joinArguments,
      Schema outputSchema) {
    if (key == null) {
      throw new NullPointerException("Null key");
    }
    this.key = key;
    if (iterables == null) {
      throw new NullPointerException("Null iterables");
    }
    this.iterables = iterables;
    if (tags == null) {
      throw new NullPointerException("Null tags");
    }
    this.tags = tags;
    if (joinArguments == null) {
      throw new NullPointerException("Null joinArguments");
    }
    this.joinArguments = joinArguments;
    if (outputSchema == null) {
      throw new NullPointerException("Null outputSchema");
    }
    this.outputSchema = outputSchema;
  }

  @Override
  Row getKey() {
    return key;
  }

  @Override
  List<Iterable<Row>> getIterables() {
    return iterables;
  }

  @Override
  List<String> getTags() {
    return tags;
  }

  @Override
  CoGroup.JoinArguments getJoinArguments() {
    return joinArguments;
  }

  @Override
  Schema getOutputSchema() {
    return outputSchema;
  }

  @Override
  public String toString() {
    return "Result{"
        + "key=" + key + ", "
        + "iterables=" + iterables + ", "
        + "tags=" + tags + ", "
        + "joinArguments=" + joinArguments + ", "
        + "outputSchema=" + outputSchema
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof CoGroup.Result) {
      CoGroup.Result that = (CoGroup.Result) o;
      return this.key.equals(that.getKey())
          && this.iterables.equals(that.getIterables())
          && this.tags.equals(that.getTags())
          && this.joinArguments.equals(that.getJoinArguments())
          && this.outputSchema.equals(that.getOutputSchema());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= key.hashCode();
    h$ *= 1000003;
    h$ ^= iterables.hashCode();
    h$ *= 1000003;
    h$ ^= tags.hashCode();
    h$ *= 1000003;
    h$ ^= joinArguments.hashCode();
    h$ *= 1000003;
    h$ ^= outputSchema.hashCode();
    return h$;
  }

}
