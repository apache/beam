package org.apache.beam.sdk.schemas;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import javax.annotation.Generated;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_FieldValueTypeInformation extends FieldValueTypeInformation {

  private final @Nullable Integer number;

  private final String name;

  private final boolean nullable;

  private final TypeDescriptor<?> type;

  private final Class<?> rawType;

  private final @Nullable Field field;

  private final @Nullable Method method;

  private final Map<String, FieldValueTypeInformation> oneOfTypes;

  private final @Nullable FieldValueTypeInformation elementType;

  private final @Nullable FieldValueTypeInformation mapKeyType;

  private final @Nullable FieldValueTypeInformation mapValueType;

  private AutoValue_FieldValueTypeInformation(
      @Nullable Integer number,
      String name,
      boolean nullable,
      TypeDescriptor<?> type,
      Class<?> rawType,
      @Nullable Field field,
      @Nullable Method method,
      Map<String, FieldValueTypeInformation> oneOfTypes,
      @Nullable FieldValueTypeInformation elementType,
      @Nullable FieldValueTypeInformation mapKeyType,
      @Nullable FieldValueTypeInformation mapValueType) {
    this.number = number;
    this.name = name;
    this.nullable = nullable;
    this.type = type;
    this.rawType = rawType;
    this.field = field;
    this.method = method;
    this.oneOfTypes = oneOfTypes;
    this.elementType = elementType;
    this.mapKeyType = mapKeyType;
    this.mapValueType = mapValueType;
  }

  @Override
  public @Nullable Integer getNumber() {
    return number;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean isNullable() {
    return nullable;
  }

  @Override
  public TypeDescriptor<?> getType() {
    return type;
  }

  @Override
  public Class<?> getRawType() {
    return rawType;
  }

  @Override
  public @Nullable Field getField() {
    return field;
  }

  @Override
  public @Nullable Method getMethod() {
    return method;
  }

  @Override
  public Map<String, FieldValueTypeInformation> getOneOfTypes() {
    return oneOfTypes;
  }

  @Override
  public @Nullable FieldValueTypeInformation getElementType() {
    return elementType;
  }

  @Override
  public @Nullable FieldValueTypeInformation getMapKeyType() {
    return mapKeyType;
  }

  @Override
  public @Nullable FieldValueTypeInformation getMapValueType() {
    return mapValueType;
  }

  @Override
  public String toString() {
    return "FieldValueTypeInformation{"
        + "number=" + number + ", "
        + "name=" + name + ", "
        + "nullable=" + nullable + ", "
        + "type=" + type + ", "
        + "rawType=" + rawType + ", "
        + "field=" + field + ", "
        + "method=" + method + ", "
        + "oneOfTypes=" + oneOfTypes + ", "
        + "elementType=" + elementType + ", "
        + "mapKeyType=" + mapKeyType + ", "
        + "mapValueType=" + mapValueType
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof FieldValueTypeInformation) {
      FieldValueTypeInformation that = (FieldValueTypeInformation) o;
      return (this.number == null ? that.getNumber() == null : this.number.equals(that.getNumber()))
          && this.name.equals(that.getName())
          && this.nullable == that.isNullable()
          && this.type.equals(that.getType())
          && this.rawType.equals(that.getRawType())
          && (this.field == null ? that.getField() == null : this.field.equals(that.getField()))
          && (this.method == null ? that.getMethod() == null : this.method.equals(that.getMethod()))
          && this.oneOfTypes.equals(that.getOneOfTypes())
          && (this.elementType == null ? that.getElementType() == null : this.elementType.equals(that.getElementType()))
          && (this.mapKeyType == null ? that.getMapKeyType() == null : this.mapKeyType.equals(that.getMapKeyType()))
          && (this.mapValueType == null ? that.getMapValueType() == null : this.mapValueType.equals(that.getMapValueType()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (number == null) ? 0 : number.hashCode();
    h$ *= 1000003;
    h$ ^= name.hashCode();
    h$ *= 1000003;
    h$ ^= nullable ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= type.hashCode();
    h$ *= 1000003;
    h$ ^= rawType.hashCode();
    h$ *= 1000003;
    h$ ^= (field == null) ? 0 : field.hashCode();
    h$ *= 1000003;
    h$ ^= (method == null) ? 0 : method.hashCode();
    h$ *= 1000003;
    h$ ^= oneOfTypes.hashCode();
    h$ *= 1000003;
    h$ ^= (elementType == null) ? 0 : elementType.hashCode();
    h$ *= 1000003;
    h$ ^= (mapKeyType == null) ? 0 : mapKeyType.hashCode();
    h$ *= 1000003;
    h$ ^= (mapValueType == null) ? 0 : mapValueType.hashCode();
    return h$;
  }

  @Override
  FieldValueTypeInformation.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends FieldValueTypeInformation.Builder {
    private @Nullable Integer number;
    private String name;
    private Boolean nullable;
    private TypeDescriptor<?> type;
    private Class<?> rawType;
    private @Nullable Field field;
    private @Nullable Method method;
    private Map<String, FieldValueTypeInformation> oneOfTypes;
    private @Nullable FieldValueTypeInformation elementType;
    private @Nullable FieldValueTypeInformation mapKeyType;
    private @Nullable FieldValueTypeInformation mapValueType;
    Builder() {
    }
    private Builder(FieldValueTypeInformation source) {
      this.number = source.getNumber();
      this.name = source.getName();
      this.nullable = source.isNullable();
      this.type = source.getType();
      this.rawType = source.getRawType();
      this.field = source.getField();
      this.method = source.getMethod();
      this.oneOfTypes = source.getOneOfTypes();
      this.elementType = source.getElementType();
      this.mapKeyType = source.getMapKeyType();
      this.mapValueType = source.getMapValueType();
    }
    @Override
    public FieldValueTypeInformation.Builder setNumber(@Nullable Integer number) {
      this.number = number;
      return this;
    }
    @Override
    public FieldValueTypeInformation.Builder setName(String name) {
      if (name == null) {
        throw new NullPointerException("Null name");
      }
      this.name = name;
      return this;
    }
    @Override
    public FieldValueTypeInformation.Builder setNullable(boolean nullable) {
      this.nullable = nullable;
      return this;
    }
    @Override
    public FieldValueTypeInformation.Builder setType(TypeDescriptor<?> type) {
      if (type == null) {
        throw new NullPointerException("Null type");
      }
      this.type = type;
      return this;
    }
    @Override
    public FieldValueTypeInformation.Builder setRawType(Class<?> rawType) {
      if (rawType == null) {
        throw new NullPointerException("Null rawType");
      }
      this.rawType = rawType;
      return this;
    }
    @Override
    public FieldValueTypeInformation.Builder setField(@Nullable Field field) {
      this.field = field;
      return this;
    }
    @Override
    public FieldValueTypeInformation.Builder setMethod(@Nullable Method method) {
      this.method = method;
      return this;
    }
    @Override
    public FieldValueTypeInformation.Builder setOneOfTypes(Map<String, FieldValueTypeInformation> oneOfTypes) {
      if (oneOfTypes == null) {
        throw new NullPointerException("Null oneOfTypes");
      }
      this.oneOfTypes = oneOfTypes;
      return this;
    }
    @Override
    public FieldValueTypeInformation.Builder setElementType(@Nullable FieldValueTypeInformation elementType) {
      this.elementType = elementType;
      return this;
    }
    @Override
    public FieldValueTypeInformation.Builder setMapKeyType(@Nullable FieldValueTypeInformation mapKeyType) {
      this.mapKeyType = mapKeyType;
      return this;
    }
    @Override
    public FieldValueTypeInformation.Builder setMapValueType(@Nullable FieldValueTypeInformation mapValueType) {
      this.mapValueType = mapValueType;
      return this;
    }
    @Override
    FieldValueTypeInformation build() {
      if (this.name == null
          || this.nullable == null
          || this.type == null
          || this.rawType == null
          || this.oneOfTypes == null) {
        StringBuilder missing = new StringBuilder();
        if (this.name == null) {
          missing.append(" name");
        }
        if (this.nullable == null) {
          missing.append(" nullable");
        }
        if (this.type == null) {
          missing.append(" type");
        }
        if (this.rawType == null) {
          missing.append(" rawType");
        }
        if (this.oneOfTypes == null) {
          missing.append(" oneOfTypes");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_FieldValueTypeInformation(
          this.number,
          this.name,
          this.nullable,
          this.type,
          this.rawType,
          this.field,
          this.method,
          this.oneOfTypes,
          this.elementType,
          this.mapKeyType,
          this.mapValueType);
    }
  }

}
