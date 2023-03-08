package org.apache.beam.sdk.options;

import java.lang.reflect.Method;
import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_PipelineOptionSpec extends PipelineOptionSpec {

  private final Class<? extends PipelineOptions> definingInterface;

  private final String name;

  private final Method getterMethod;

  AutoValue_PipelineOptionSpec(
      Class<? extends PipelineOptions> definingInterface,
      String name,
      Method getterMethod) {
    if (definingInterface == null) {
      throw new NullPointerException("Null definingInterface");
    }
    this.definingInterface = definingInterface;
    if (name == null) {
      throw new NullPointerException("Null name");
    }
    this.name = name;
    if (getterMethod == null) {
      throw new NullPointerException("Null getterMethod");
    }
    this.getterMethod = getterMethod;
  }

  @Override
  Class<? extends PipelineOptions> getDefiningInterface() {
    return definingInterface;
  }

  @Override
  String getName() {
    return name;
  }

  @Override
  Method getGetterMethod() {
    return getterMethod;
  }

  @Override
  public String toString() {
    return "PipelineOptionSpec{"
        + "definingInterface=" + definingInterface + ", "
        + "name=" + name + ", "
        + "getterMethod=" + getterMethod
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof PipelineOptionSpec) {
      PipelineOptionSpec that = (PipelineOptionSpec) o;
      return this.definingInterface.equals(that.getDefiningInterface())
          && this.name.equals(that.getName())
          && this.getterMethod.equals(that.getGetterMethod());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= definingInterface.hashCode();
    h$ *= 1000003;
    h$ ^= name.hashCode();
    h$ *= 1000003;
    h$ ^= getterMethod.hashCode();
    return h$;
  }

}
