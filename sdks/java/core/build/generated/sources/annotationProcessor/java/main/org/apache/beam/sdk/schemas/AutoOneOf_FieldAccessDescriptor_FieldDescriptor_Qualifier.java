package org.apache.beam.sdk.schemas;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoOneOfProcessor")
final class AutoOneOf_FieldAccessDescriptor_FieldDescriptor_Qualifier {
  private AutoOneOf_FieldAccessDescriptor_FieldDescriptor_Qualifier() {} // There are no instances of this type.

  static FieldAccessDescriptor.FieldDescriptor.Qualifier list(FieldAccessDescriptor.FieldDescriptor.ListQualifier list) {
    if (list == null) {
      throw new NullPointerException();
    }
    return new Impl_list(list);
  }

  static FieldAccessDescriptor.FieldDescriptor.Qualifier map(FieldAccessDescriptor.FieldDescriptor.MapQualifier map) {
    if (map == null) {
      throw new NullPointerException();
    }
    return new Impl_map(map);
  }

  // Parent class that each implementation will inherit from.
  private abstract static class Parent_ extends FieldAccessDescriptor.FieldDescriptor.Qualifier {
    @Override
    public FieldAccessDescriptor.FieldDescriptor.ListQualifier getList() {
      throw new UnsupportedOperationException(getKind().toString());
    }
    @Override
    public FieldAccessDescriptor.FieldDescriptor.MapQualifier getMap() {
      throw new UnsupportedOperationException(getKind().toString());
    }
  }

  // Implementation when the contained property is "list".
  private static final class Impl_list extends Parent_ {
    private final FieldAccessDescriptor.FieldDescriptor.ListQualifier list;
    Impl_list(FieldAccessDescriptor.FieldDescriptor.ListQualifier list) {
      this.list = list;
    }
    @Override
    public FieldAccessDescriptor.FieldDescriptor.ListQualifier getList() {
      return list;
    }
    @Override
    public String toString() {
      return "Qualifier{list=" + this.list + "}";
    }
    @Override
    public boolean equals(Object x) {
      if (x instanceof FieldAccessDescriptor.FieldDescriptor.Qualifier) {
        FieldAccessDescriptor.FieldDescriptor.Qualifier that = (FieldAccessDescriptor.FieldDescriptor.Qualifier) x;
        return this.getKind() == that.getKind()
            && this.list.equals(that.getList());
      } else {
        return false;
      }
    }
    @Override
    public int hashCode() {
      return list.hashCode();
    }
    @Override
    public FieldAccessDescriptor.FieldDescriptor.Qualifier.Kind getKind() {
      return FieldAccessDescriptor.FieldDescriptor.Qualifier.Kind.LIST;
    }
  }

  // Implementation when the contained property is "map".
  private static final class Impl_map extends Parent_ {
    private final FieldAccessDescriptor.FieldDescriptor.MapQualifier map;
    Impl_map(FieldAccessDescriptor.FieldDescriptor.MapQualifier map) {
      this.map = map;
    }
    @Override
    public FieldAccessDescriptor.FieldDescriptor.MapQualifier getMap() {
      return map;
    }
    @Override
    public String toString() {
      return "Qualifier{map=" + this.map + "}";
    }
    @Override
    public boolean equals(Object x) {
      if (x instanceof FieldAccessDescriptor.FieldDescriptor.Qualifier) {
        FieldAccessDescriptor.FieldDescriptor.Qualifier that = (FieldAccessDescriptor.FieldDescriptor.Qualifier) x;
        return this.getKind() == that.getKind()
            && this.map.equals(that.getMap());
      } else {
        return false;
      }
    }
    @Override
    public int hashCode() {
      return map.hashCode();
    }
    @Override
    public FieldAccessDescriptor.FieldDescriptor.Qualifier.Kind getKind() {
      return FieldAccessDescriptor.FieldDescriptor.Qualifier.Kind.MAP;
    }
  }

}
