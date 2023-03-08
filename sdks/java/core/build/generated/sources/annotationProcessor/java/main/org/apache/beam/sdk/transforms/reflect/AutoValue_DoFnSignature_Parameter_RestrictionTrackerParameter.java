package org.apache.beam.sdk.transforms.reflect;

import javax.annotation.Generated;
import org.apache.beam.sdk.values.TypeDescriptor;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_Parameter_RestrictionTrackerParameter extends DoFnSignature.Parameter.RestrictionTrackerParameter {

  private final TypeDescriptor<?> trackerT;

  AutoValue_DoFnSignature_Parameter_RestrictionTrackerParameter(
      TypeDescriptor<?> trackerT) {
    if (trackerT == null) {
      throw new NullPointerException("Null trackerT");
    }
    this.trackerT = trackerT;
  }

  @Override
  public TypeDescriptor<?> trackerT() {
    return trackerT;
  }

  @Override
  public String toString() {
    return "RestrictionTrackerParameter{"
        + "trackerT=" + trackerT
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.Parameter.RestrictionTrackerParameter) {
      DoFnSignature.Parameter.RestrictionTrackerParameter that = (DoFnSignature.Parameter.RestrictionTrackerParameter) o;
      return this.trackerT.equals(that.trackerT());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= trackerT.hashCode();
    return h$;
  }

}
