package org.apache.beam.sdk.util;

import java.util.Map;
import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnWithExecutionInformation extends DoFnWithExecutionInformation {

  private final DoFn<?, ?> doFn;

  private final TupleTag<?> mainOutputTag;

  private final Map<String, PCollectionView<?>> sideInputMapping;

  private final DoFnSchemaInformation schemaInformation;

  AutoValue_DoFnWithExecutionInformation(
      DoFn<?, ?> doFn,
      TupleTag<?> mainOutputTag,
      Map<String, PCollectionView<?>> sideInputMapping,
      DoFnSchemaInformation schemaInformation) {
    if (doFn == null) {
      throw new NullPointerException("Null doFn");
    }
    this.doFn = doFn;
    if (mainOutputTag == null) {
      throw new NullPointerException("Null mainOutputTag");
    }
    this.mainOutputTag = mainOutputTag;
    if (sideInputMapping == null) {
      throw new NullPointerException("Null sideInputMapping");
    }
    this.sideInputMapping = sideInputMapping;
    if (schemaInformation == null) {
      throw new NullPointerException("Null schemaInformation");
    }
    this.schemaInformation = schemaInformation;
  }

  @Override
  public DoFn<?, ?> getDoFn() {
    return doFn;
  }

  @Override
  public TupleTag<?> getMainOutputTag() {
    return mainOutputTag;
  }

  @Override
  public Map<String, PCollectionView<?>> getSideInputMapping() {
    return sideInputMapping;
  }

  @Override
  public DoFnSchemaInformation getSchemaInformation() {
    return schemaInformation;
  }

  @Override
  public String toString() {
    return "DoFnWithExecutionInformation{"
        + "doFn=" + doFn + ", "
        + "mainOutputTag=" + mainOutputTag + ", "
        + "sideInputMapping=" + sideInputMapping + ", "
        + "schemaInformation=" + schemaInformation
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnWithExecutionInformation) {
      DoFnWithExecutionInformation that = (DoFnWithExecutionInformation) o;
      return this.doFn.equals(that.getDoFn())
          && this.mainOutputTag.equals(that.getMainOutputTag())
          && this.sideInputMapping.equals(that.getSideInputMapping())
          && this.schemaInformation.equals(that.getSchemaInformation());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= doFn.hashCode();
    h$ *= 1000003;
    h$ ^= mainOutputTag.hashCode();
    h$ *= 1000003;
    h$ ^= sideInputMapping.hashCode();
    h$ *= 1000003;
    h$ ^= schemaInformation.hashCode();
    return h$;
  }

}
