package org.apache.beam.sdk.schemas.utils;

import java.util.List;
import java.util.Optional;
import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.Schema;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_SchemaZipFold_Context extends SchemaZipFold.Context {

  private final List<String> path;

  private final Optional<Schema.TypeName> parent;

  AutoValue_SchemaZipFold_Context(
      List<String> path,
      Optional<Schema.TypeName> parent) {
    if (path == null) {
      throw new NullPointerException("Null path");
    }
    this.path = path;
    if (parent == null) {
      throw new NullPointerException("Null parent");
    }
    this.parent = parent;
  }

  @Override
  public List<String> path() {
    return path;
  }

  @Override
  public Optional<Schema.TypeName> parent() {
    return parent;
  }

  @Override
  public String toString() {
    return "Context{"
        + "path=" + path + ", "
        + "parent=" + parent
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof SchemaZipFold.Context) {
      SchemaZipFold.Context that = (SchemaZipFold.Context) o;
      return this.path.equals(that.path())
          && this.parent.equals(that.parent());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= path.hashCode();
    h$ *= 1000003;
    h$ ^= parent.hashCode();
    return h$;
  }

}
