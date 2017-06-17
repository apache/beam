package org.apache.beam.sdk.extensions.sql.meta;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;

/**
 * Metadata class for a {@code BeamSqlTable} column.
 */
@AutoValue
public abstract class Column implements Serializable {
  public abstract String getName();
  public abstract Integer getType();
  @Nullable
  public abstract String getComment();
  public abstract boolean isPrimaryKey();

  public static Builder builder() {
    return new org.apache.beam.sdk.extensions.sql.meta.AutoValue_Column.Builder();
  }

  /**
   * Builder class for {@link Column}.
   */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder name(String name);
    public abstract Builder type(Integer type);
    public abstract Builder comment(String comment);
    public abstract Builder primaryKey(boolean isPrimaryKey);
    public abstract Column build();
  }
}
