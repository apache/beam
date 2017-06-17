package org.apache.beam.sdk.extensions.sql.meta;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.net.URI;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Represents the metadata of a {@code BeamSqlTable}.
 */
@AutoValue
public abstract class Table implements Serializable {
  /** type of the table. */
  public abstract String getType();
  public abstract String getName();
  public abstract List<Column> getColumns();
  @Nullable
  public abstract String getComment();
  @Nullable
  public abstract URI getLocation();
  @Nullable
  public abstract JSONObject getProperties();


  public static Builder builder() {
    return new org.apache.beam.sdk.extensions.sql.meta.AutoValue_Table.Builder();
  }

  /**
   * Builder class for {@link Table}.
   */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder type(String type);
    public abstract Builder name(String name);
    public abstract Builder columns(List<Column> columns);
    public abstract Builder comment(String name);
    public abstract Builder location(URI location);
    public abstract Builder properties(JSONObject properties);
    public abstract Table build();
  }
}
