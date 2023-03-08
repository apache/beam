package org.apache.beam.sdk.schemas;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Schema_Field extends Schema.Field {

  private final String name;

  private final String description;

  private final Schema.FieldType type;

  private final Schema.Options options;

  private AutoValue_Schema_Field(
      String name,
      String description,
      Schema.FieldType type,
      Schema.Options options) {
    this.name = name;
    this.description = description;
    this.type = type;
    this.options = options;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public Schema.FieldType getType() {
    return type;
  }

  @Override
  public Schema.Options getOptions() {
    return options;
  }

  @Override
  public String toString() {
    return "Field{"
        + "name=" + name + ", "
        + "description=" + description + ", "
        + "type=" + type + ", "
        + "options=" + options
        + "}";
  }

  @Override
  public Schema.Field.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends Schema.Field.Builder {
    private String name;
    private String description;
    private Schema.FieldType type;
    private Schema.Options options;
    Builder() {
    }
    private Builder(Schema.Field source) {
      this.name = source.getName();
      this.description = source.getDescription();
      this.type = source.getType();
      this.options = source.getOptions();
    }
    @Override
    public Schema.Field.Builder setName(String name) {
      if (name == null) {
        throw new NullPointerException("Null name");
      }
      this.name = name;
      return this;
    }
    @Override
    public Schema.Field.Builder setDescription(String description) {
      if (description == null) {
        throw new NullPointerException("Null description");
      }
      this.description = description;
      return this;
    }
    @Override
    public Schema.Field.Builder setType(Schema.FieldType type) {
      if (type == null) {
        throw new NullPointerException("Null type");
      }
      this.type = type;
      return this;
    }
    @Override
    public Schema.Field.Builder setOptions(Schema.Options options) {
      if (options == null) {
        throw new NullPointerException("Null options");
      }
      this.options = options;
      return this;
    }
    @Override
    public Schema.Field build() {
      if (this.name == null
          || this.description == null
          || this.type == null
          || this.options == null) {
        StringBuilder missing = new StringBuilder();
        if (this.name == null) {
          missing.append(" name");
        }
        if (this.description == null) {
          missing.append(" description");
        }
        if (this.type == null) {
          missing.append(" type");
        }
        if (this.options == null) {
          missing.append(" options");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_Schema_Field(
          this.name,
          this.description,
          this.type,
          this.options);
    }
  }

}
