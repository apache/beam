package org.apache.beam.sdk.schemas.transforms;

import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_RenameFields_RenamePair extends RenameFields.RenamePair {

  private final FieldAccessDescriptor fieldAccessDescriptor;

  private final String newName;

  AutoValue_RenameFields_RenamePair(
      FieldAccessDescriptor fieldAccessDescriptor,
      String newName) {
    if (fieldAccessDescriptor == null) {
      throw new NullPointerException("Null fieldAccessDescriptor");
    }
    this.fieldAccessDescriptor = fieldAccessDescriptor;
    if (newName == null) {
      throw new NullPointerException("Null newName");
    }
    this.newName = newName;
  }

  @Override
  FieldAccessDescriptor getFieldAccessDescriptor() {
    return fieldAccessDescriptor;
  }

  @Override
  String getNewName() {
    return newName;
  }

  @Override
  public String toString() {
    return "RenamePair{"
        + "fieldAccessDescriptor=" + fieldAccessDescriptor + ", "
        + "newName=" + newName
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof RenameFields.RenamePair) {
      RenameFields.RenamePair that = (RenameFields.RenamePair) o;
      return this.fieldAccessDescriptor.equals(that.getFieldAccessDescriptor())
          && this.newName.equals(that.getNewName());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= fieldAccessDescriptor.hashCode();
    h$ *= 1000003;
    h$ ^= newName.hashCode();
    return h$;
  }

}
