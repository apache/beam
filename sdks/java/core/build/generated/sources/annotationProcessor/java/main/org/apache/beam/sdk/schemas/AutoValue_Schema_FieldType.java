package org.apache.beam.sdk.schemas;

import java.util.Map;
import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Schema_FieldType extends Schema.FieldType {

  private final Schema.TypeName typeName;

  private final Boolean nullable;

  private final Schema.@Nullable LogicalType<?, ?> logicalType;

  private final Schema.@Nullable FieldType collectionElementType;

  private final Schema.@Nullable FieldType mapKeyType;

  private final Schema.@Nullable FieldType mapValueType;

  private final @Nullable Schema rowSchema;

  private final Map<String, Schema.ByteArrayWrapper> metadata;

  private AutoValue_Schema_FieldType(
      Schema.TypeName typeName,
      Boolean nullable,
      Schema.@Nullable LogicalType<?, ?> logicalType,
      Schema.@Nullable FieldType collectionElementType,
      Schema.@Nullable FieldType mapKeyType,
      Schema.@Nullable FieldType mapValueType,
      @Nullable Schema rowSchema,
      Map<String, Schema.ByteArrayWrapper> metadata) {
    this.typeName = typeName;
    this.nullable = nullable;
    this.logicalType = logicalType;
    this.collectionElementType = collectionElementType;
    this.mapKeyType = mapKeyType;
    this.mapValueType = mapValueType;
    this.rowSchema = rowSchema;
    this.metadata = metadata;
  }

  @Override
  public Schema.TypeName getTypeName() {
    return typeName;
  }

  @Override
  public Boolean getNullable() {
    return nullable;
  }

  @Override
  public Schema.@Nullable LogicalType<?, ?> getLogicalType() {
    return logicalType;
  }

  @Override
  public Schema.@Nullable FieldType getCollectionElementType() {
    return collectionElementType;
  }

  @Override
  public Schema.@Nullable FieldType getMapKeyType() {
    return mapKeyType;
  }

  @Override
  public Schema.@Nullable FieldType getMapValueType() {
    return mapValueType;
  }

  @Override
  public @Nullable Schema getRowSchema() {
    return rowSchema;
  }

  @Deprecated
  @SuppressWarnings("mutable")
  @Override
  Map<String, Schema.ByteArrayWrapper> getMetadata() {
    return metadata;
  }

  @Override
  public Schema.FieldType.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends Schema.FieldType.Builder {
    private Schema.TypeName typeName;
    private Boolean nullable;
    private Schema.@Nullable LogicalType<?, ?> logicalType;
    private Schema.@Nullable FieldType collectionElementType;
    private Schema.@Nullable FieldType mapKeyType;
    private Schema.@Nullable FieldType mapValueType;
    private @Nullable Schema rowSchema;
    private Map<String, Schema.ByteArrayWrapper> metadata;
    Builder() {
    }
    private Builder(Schema.FieldType source) {
      this.typeName = source.getTypeName();
      this.nullable = source.getNullable();
      this.logicalType = source.getLogicalType();
      this.collectionElementType = source.getCollectionElementType();
      this.mapKeyType = source.getMapKeyType();
      this.mapValueType = source.getMapValueType();
      this.rowSchema = source.getRowSchema();
      this.metadata = source.getMetadata();
    }
    @Override
    Schema.FieldType.Builder setTypeName(Schema.TypeName typeName) {
      if (typeName == null) {
        throw new NullPointerException("Null typeName");
      }
      this.typeName = typeName;
      return this;
    }
    @Override
    Schema.FieldType.Builder setNullable(Boolean nullable) {
      if (nullable == null) {
        throw new NullPointerException("Null nullable");
      }
      this.nullable = nullable;
      return this;
    }
    @Override
    Schema.FieldType.Builder setLogicalType(Schema.LogicalType<?, ?> logicalType) {
      this.logicalType = logicalType;
      return this;
    }
    @Override
    Schema.FieldType.Builder setCollectionElementType(Schema.@Nullable FieldType collectionElementType) {
      this.collectionElementType = collectionElementType;
      return this;
    }
    @Override
    Schema.FieldType.Builder setMapKeyType(Schema.@Nullable FieldType mapKeyType) {
      this.mapKeyType = mapKeyType;
      return this;
    }
    @Override
    Schema.FieldType.Builder setMapValueType(Schema.@Nullable FieldType mapValueType) {
      this.mapValueType = mapValueType;
      return this;
    }
    @Override
    Schema.FieldType.Builder setRowSchema(@Nullable Schema rowSchema) {
      this.rowSchema = rowSchema;
      return this;
    }
    @Override
    Schema.FieldType.Builder setMetadata(Map<String, Schema.ByteArrayWrapper> metadata) {
      if (metadata == null) {
        throw new NullPointerException("Null metadata");
      }
      this.metadata = metadata;
      return this;
    }
    @Override
    Schema.FieldType build() {
      if (this.typeName == null
          || this.nullable == null
          || this.metadata == null) {
        StringBuilder missing = new StringBuilder();
        if (this.typeName == null) {
          missing.append(" typeName");
        }
        if (this.nullable == null) {
          missing.append(" nullable");
        }
        if (this.metadata == null) {
          missing.append(" metadata");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_Schema_FieldType(
          this.typeName,
          this.nullable,
          this.logicalType,
          this.collectionElementType,
          this.mapKeyType,
          this.mapValueType,
          this.rowSchema,
          this.metadata);
    }
  }

}
