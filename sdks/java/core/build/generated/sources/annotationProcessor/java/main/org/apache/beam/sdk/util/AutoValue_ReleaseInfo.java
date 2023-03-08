package org.apache.beam.sdk.util;

import java.util.Map;
import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_ReleaseInfo extends ReleaseInfo {

  private final Map<String, String> properties;

  AutoValue_ReleaseInfo(
      Map<String, String> properties) {
    if (properties == null) {
      throw new NullPointerException("Null properties");
    }
    this.properties = properties;
  }

  @Override
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String toString() {
    return "ReleaseInfo{"
        + "properties=" + properties
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ReleaseInfo) {
      ReleaseInfo that = (ReleaseInfo) o;
      return this.properties.equals(that.getProperties());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= properties.hashCode();
    return h$;
  }

}
