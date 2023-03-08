package org.apache.beam.sdk.transforms.reflect;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import javax.annotation.Generated;
import org.apache.beam.sdk.values.TypeDescriptor;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignatures_ParameterDescription extends DoFnSignatures.ParameterDescription {

  private final Method method;

  private final int index;

  private final TypeDescriptor<?> type;

  private final List<Annotation> annotations;

  AutoValue_DoFnSignatures_ParameterDescription(
      Method method,
      int index,
      TypeDescriptor<?> type,
      List<Annotation> annotations) {
    if (method == null) {
      throw new NullPointerException("Null method");
    }
    this.method = method;
    this.index = index;
    if (type == null) {
      throw new NullPointerException("Null type");
    }
    this.type = type;
    if (annotations == null) {
      throw new NullPointerException("Null annotations");
    }
    this.annotations = annotations;
  }

  @Override
  public Method getMethod() {
    return method;
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public TypeDescriptor<?> getType() {
    return type;
  }

  @Override
  public List<Annotation> getAnnotations() {
    return annotations;
  }

  @Override
  public String toString() {
    return "ParameterDescription{"
        + "method=" + method + ", "
        + "index=" + index + ", "
        + "type=" + type + ", "
        + "annotations=" + annotations
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignatures.ParameterDescription) {
      DoFnSignatures.ParameterDescription that = (DoFnSignatures.ParameterDescription) o;
      return this.method.equals(that.getMethod())
          && this.index == that.getIndex()
          && this.type.equals(that.getType())
          && this.annotations.equals(that.getAnnotations());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= method.hashCode();
    h$ *= 1000003;
    h$ ^= index;
    h$ *= 1000003;
    h$ ^= type.hashCode();
    h$ *= 1000003;
    h$ ^= annotations.hashCode();
    return h$;
  }

}
